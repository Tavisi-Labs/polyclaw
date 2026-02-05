#!/usr/bin/env python3
"""PolyClaw execution daemon (WS spot agg + 15m market discovery).

Design goals:
- Persistent websocket connections to spot venues (Binance, Coinbase, Kraken)
- Periodically discover the *current* rotating 15-minute up/down Polymarket markets
- Poll Polymarket CLOB price/book as needed (avoid rate limits)
- Write deterministic heartbeat file for supervision

NOTE: Trading logic is scaffolded (safe/no-trade by default) until configured.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import signal
import time
from dataclasses import dataclass
from pathlib import Path

import httpx

# Optional websocket lib (installed into venv)
import websockets

# Skill root
SKILL_DIR = Path(__file__).resolve().parent.parent
WORKSPACE_DIR = Path(os.getenv("OPENCLAW_WORKSPACE", str(Path.home() / ".openclaw/workspace")))
MEM_DIR = WORKSPACE_DIR / "memory"
MEM_DIR.mkdir(parents=True, exist_ok=True)

HEARTBEAT_PATH = MEM_DIR / "polyclaw-exec-heartbeat.json"
STATE_PATH = MEM_DIR / "polyclaw-exec-state.json"

# Polymarket Gamma API
GAMMA_BASE = os.getenv("POLY_GAMMA_BASE_URL", "https://gamma-api.polymarket.com")

ASSETS = ["BTC", "ETH", "SOL", "XRP"]
DISCOVERY_INTERVAL_S = int(os.getenv("POLY_15M_DISCOVERY_INTERVAL_S", "30"))
HEARTBEAT_INTERVAL_S = int(os.getenv("POLY_EXEC_HEARTBEAT_INTERVAL_S", "5"))

# Spot aggregation
SPOT_OUTLIER_PCT = float(os.getenv("SPOT_OUTLIER_PCT", "0.007"))  # 0.7%

# Trading mode gate (default safe)
EXECUTION_ENABLED = os.getenv("POLY_EXECUTION_ENABLED", "false").lower() in ("1", "true", "yes")

# Execution loop settings
EXEC_LOOP_INTERVAL_S = float(os.getenv("POLY_EXEC_LOOP_INTERVAL_S", "2"))  # internal tick
POLY_BOOK_POLL_S = float(os.getenv("POLY_BOOK_POLL_S", "8"))
SPOT_LOOKBACK_S = int(os.getenv("SPOT_LOOKBACK_S", str(15 * 60)))

# Risk defaults (can be overridden by strategy-directives.json)
EDGE_MIN_DEFAULT = float(os.getenv("EDGE_MIN", "0.08"))
MAX_TRADE_FRACTION_DEFAULT = float(os.getenv("RISK_MAX_TRADE_FRACTION", "0.20"))
MAX_MARKET_FRACTION_DEFAULT = float(os.getenv("RISK_MAX_MARKET_FRACTION", "0.35"))
COOLDOWN_S_DEFAULT = int(os.getenv("RISK_COOLDOWN_S", "45"))


@dataclass
class VenuePrice:
    mid: float | None = None
    ts: float = 0.0


class SpotAggregator:
    def __init__(self) -> None:
        # prices[asset][venue] -> VenuePrice
        self.prices: dict[str, dict[str, VenuePrice]] = {
            a: {"binance": VenuePrice(), "coinbase": VenuePrice(), "kraken": VenuePrice()}
            for a in ASSETS
        }
        # history[asset] -> list[(ts, agg_mid)] for lookback-based direction signal
        self.history: dict[str, list[tuple[float, float]]] = {a: [] for a in ASSETS}

    def update(self, asset: str, venue: str, mid: float) -> None:
        vp = self.prices[asset][venue]
        vp.mid = float(mid)
        vp.ts = time.time()

    def aggregate(self, asset: str) -> tuple[float | None, dict]:
        """Return (agg_mid, debug). Uses median of live venues with outlier rejection."""
        now = time.time()
        mids = []
        raw = {}
        for venue, vp in self.prices[asset].items():
            raw[venue] = {"mid": vp.mid, "age_s": None if vp.ts == 0 else round(now - vp.ts, 3)}
            if vp.mid is not None and (now - vp.ts) < 20:
                mids.append(vp.mid)
        if len(mids) < 2:
            return None, {"raw": raw, "reason": "insufficient_live_venues"}
        mids_sorted = sorted(mids)
        median = mids_sorted[len(mids_sorted) // 2]
        kept = [m for m in mids if abs(m - median) / median <= SPOT_OUTLIER_PCT]
        if len(kept) < 2:
            return median, {"raw": raw, "median": median, "kept": kept, "reason": "outlier_reject_left_too_few"}
        kept_sorted = sorted(kept)
        agg = kept_sorted[len(kept_sorted) // 2]
        return agg, {"raw": raw, "median": median, "kept": kept}

    def record_agg(self, asset: str, agg_mid: float) -> None:
        now = time.time()
        h = self.history[asset]
        h.append((now, float(agg_mid)))
        # prune to lookback * 2
        cutoff = now - (SPOT_LOOKBACK_S * 2)
        while h and h[0][0] < cutoff:
            h.pop(0)

    def direction(self, asset: str) -> dict:
        """Return simple direction signal based on lookback delta."""
        now = time.time()
        h = self.history[asset]
        if len(h) < 2:
            return {"ok": False, "reason": "insufficient_history"}
        target_ts = now - SPOT_LOOKBACK_S
        # find closest point <= target_ts
        past = None
        for ts, px in reversed(h):
            if ts <= target_ts:
                past = (ts, px)
                break
        if not past:
            return {"ok": False, "reason": "insufficient_lookback"}
        cur = h[-1]
        delta = cur[1] - past[1]
        pct = (delta / past[1]) if past[1] else 0.0
        return {
            "ok": True,
            "lookback_s": SPOT_LOOKBACK_S,
            "past": {"ts": int(past[0]), "px": past[1]},
            "cur": {"ts": int(cur[0]), "px": cur[1]},
            "delta": delta,
            "pct": pct,
            "dir": "up" if delta > 0 else ("down" if delta < 0 else "flat"),
        }


async def ws_binance(agg: SpotAggregator, stop: asyncio.Event) -> None:
    # Binance combined stream for tickers
    # Symbols: btcusdt, ethusdt, solusdt, xrpusdt
    stream = "/".join([f"{a.lower()}usdt@bookTicker" for a in ASSETS])
    url = f"wss://stream.binance.com:9443/stream?streams={stream}"
    while not stop.is_set():
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                async for msg in ws:
                    if stop.is_set():
                        break
                    data = json.loads(msg)
                    d = data.get("data", {})
                    sym = d.get("s", "")
                    bid = float(d.get("b"))
                    ask = float(d.get("a"))
                    mid = (bid + ask) / 2.0
                    for a in ASSETS:
                        if sym == f"{a}USDT":
                            agg.update(a, "binance", mid)
                            break
        except Exception:
            await asyncio.sleep(1)


async def ws_coinbase(agg: SpotAggregator, stop: asyncio.Event) -> None:
    # Coinbase Advanced Trade WS (public) is more complex; use Coinbase Exchange (public) feed for tickers.
    # We’ll subscribe to ticker for product_ids.
    url = "wss://ws-feed.exchange.coinbase.com"
    product_ids = [f"{a}-USD" for a in ASSETS]
    sub = {"type": "subscribe", "channels": [{"name": "ticker", "product_ids": product_ids}]}
    while not stop.is_set():
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps(sub))
                async for msg in ws:
                    if stop.is_set():
                        break
                    data = json.loads(msg)
                    if data.get("type") != "ticker":
                        continue
                    pid = data.get("product_id", "")
                    price = data.get("price")
                    if price is None:
                        continue
                    mid = float(price)
                    for a in ASSETS:
                        if pid == f"{a}-USD":
                            agg.update(a, "coinbase", mid)
                            break
        except Exception:
            await asyncio.sleep(1)


async def ws_kraken(agg: SpotAggregator, stop: asyncio.Event) -> None:
    # Kraken WS v1 ticker channel
    url = "wss://ws.kraken.com"
    # Kraken pairs differ (XBT/USD)
    pair_map = {"BTC": "XBT/USD", "ETH": "ETH/USD", "SOL": "SOL/USD", "XRP": "XRP/USD"}
    sub = {"event": "subscribe", "pair": [pair_map[a] for a in ASSETS], "subscription": {"name": "ticker"}}

    while not stop.is_set():
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps(sub))
                async for msg in ws:
                    if stop.is_set():
                        break
                    data = json.loads(msg)
                    if not isinstance(data, list) or len(data) < 4:
                        continue
                    pair = data[-1]
                    ticker = data[1]
                    # ticker['b'] bid [price, wholeLotVolume, lotVolume]
                    # ticker['a'] ask
                    try:
                        bid = float(ticker["b"][0])
                        ask = float(ticker["a"][0])
                    except Exception:
                        continue
                    mid = (bid + ask) / 2.0
                    for a, p in pair_map.items():
                        if pair == p:
                            agg.update(a, "kraken", mid)
                            break
        except Exception:
            await asyncio.sleep(1)


_15m_re = re.compile(r"\b(15\s*-?\s*minute|next\s+15\s+minutes)\b", re.IGNORECASE)
_updown_re = re.compile(r"\b(up\s+or\s+down)\b", re.IGNORECASE)


async def discover_15m_markets(client: httpx.AsyncClient) -> list[dict]:
    """Discover the currently relevant rotating "Up or Down" markets.

    Reality check: The live Gamma dataset (as of 2026-02-05) does not appear to
    expose 15-minute up/down markets with obvious "15 minute" wording.

    So for now we discover via the official "Up or Down" tag and then filter
    to crypto underlyings (BTC/ETH/SOL/XRP). If/when the 15-minute markets are
    available, we can tighten this to a true 15-minute selector.

    Returns list of dicts: {id, question, clobTokenIds, slug}
    """
    url = f"{GAMMA_BASE}/events"
    params = {
        "active": "true",
        "closed": "false",
        "limit": "200",
        "tag_id": "102127",  # Up or Down
        "order": "updatedAt",
        "ascending": "false",
    }
    r = await client.get(url, params=params)
    r.raise_for_status()
    events = r.json()

    markets: list[dict] = []
    for ev in events:
        for m in ev.get("markets", []) or []:
            q = (m.get("question") or "").strip()
            if not q:
                continue
            q_up = q.upper()
            if not any(a in q_up for a in ASSETS):
                continue
            if not m.get("clobTokenIds"):
                continue
            markets.append({
                "id": m.get("id"),
                "slug": m.get("slug"),
                "question": q,
                "clobTokenIds": m.get("clobTokenIds"),
            })

    # de-dupe by question
    seen=set()
    out=[]
    for mm in markets:
        if mm["question"] in seen:
            continue
        seen.add(mm["question"])
        out.append(mm)

    return out


def write_heartbeat(payload: dict) -> None:
    tmp = HEARTBEAT_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2))
    tmp.replace(HEARTBEAT_PATH)


async def main() -> int:
    stop = asyncio.Event()

    def _handle(*_):
        stop.set()

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)

    agg = SpotAggregator()

    async with httpx.AsyncClient(timeout=15.0) as client:
        last_discovery = 0.0
        markets: list[dict] = []
        last_poly_update = 0.0

        tasks = [
            asyncio.create_task(ws_binance(agg, stop)),
            asyncio.create_task(ws_coinbase(agg, stop)),
            asyncio.create_task(ws_kraken(agg, stop)),
        ]

        try:
            while not stop.is_set():
                now = time.time()

                # market discovery
                if now - last_discovery >= DISCOVERY_INTERVAL_S:
                    try:
                        markets = await discover_15m_markets(client)
                        last_discovery = now
                    except Exception:
                        # keep old markets
                        last_discovery = now

                # Execution loop (deterministic)
                # This is intentionally conservative: at most 1 new order per asset per cooldown window.
                if EXECUTION_ENABLED:
                    # Load directives if present
                    directives = {
                        "riskMode": "normal",
                        "edgeMin": EDGE_MIN_DEFAULT,
                        "maxTradeFraction": MAX_TRADE_FRACTION_DEFAULT,
                        "maxMarketFraction": MAX_MARKET_FRACTION_DEFAULT,
                        "cooldownS": COOLDOWN_S_DEFAULT,
                    }
                    try:
                        dpath = MEM_DIR / "strategy-directives.json"
                        if dpath.exists():
                            dj = json.loads(dpath.read_text())
                            directives["riskMode"] = dj.get("riskMode", directives["riskMode"])
                            directives["edgeMin"] = float(dj.get("edgeMin", directives["edgeMin"]))
                            directives["cooldownS"] = int(dj.get("cooldownS", directives["cooldownS"]))
                            directives["maxTradeFraction"] = float(dj.get("maxTradeFraction", directives["maxTradeFraction"]))
                            directives["maxMarketFraction"] = float(dj.get("maxMarketFraction", directives["maxMarketFraction"]))
                    except Exception:
                        pass

                    if directives.get("riskMode") == "halted":
                        last_poly_update = now
                    else:
                        # init CLOB client on-demand
                        from lib.wallet_manager import WalletManager
                        from lib.clob_client import ClobClientWrapper

                        wallet = WalletManager()
                        if not wallet.is_unlocked:
                            # Can't trade without wallet
                            last_poly_update = now
                        else:
                            clob = ClobClientWrapper(wallet.get_unlocked_key(), wallet.address)

                            # simple per-asset cooldown state
                            st = {}
                            try:
                                if STATE_PATH.exists():
                                    st = json.loads(STATE_PATH.read_text())
                            except Exception:
                                st = {}
                            cd = st.get("cooldowns", {})

                            # Select one "current" market per asset by choosing the soonest end_date if available.
                            # We don't currently parse end_date here (Gamma events payload differs), so we just take the first match per asset.
                            current = {}
                            for m in markets:
                                q = m.get("question", "").upper()
                                for a in ASSETS:
                                    if a in q and a not in current:
                                        current[a] = m

                            for a, m in current.items():
                                last_t = float(cd.get(a, 0))
                                if now - last_t < directives["cooldownS"]:
                                    continue

                                # spot agg + record history
                                spot_px, _dbg = agg.aggregate(a)
                                if spot_px is None:
                                    continue
                                agg.record_agg(a, spot_px)
                                sig = agg.direction(a)
                                if not sig.get("ok"):
                                    continue

                                # choose token to buy based on direction
                                tokens = json.loads(m.get("clobTokenIds") or "[]")
                                if len(tokens) < 2:
                                    continue
                                yes_token, no_token = tokens[0], tokens[1]
                                side_dir = sig.get("dir")
                                if side_dir == "flat":
                                    continue
                                token_id = yes_token if side_dir == "up" else no_token

                                # Get order book to estimate price
                                try:
                                    book = clob.get_order_book(token_id)
                                except Exception:
                                    continue

                                bids = book.get("bids") or []
                                asks = book.get("asks") or []
                                if not asks:
                                    continue
                                best_ask = float(asks[0]["price"] if isinstance(asks[0], dict) else asks[0][0])

                                # crude probability estimate: if spot pct move over lookback is positive, assign p=0.55..0.70 based on magnitude
                                pct = abs(float(sig.get("pct") or 0.0))
                                p = min(0.70, 0.55 + pct * 10.0)
                                implied = best_ask
                                edge = (p - implied) if side_dir == "up" else (p - implied)  # same token prob model

                                edge_min = directives["edgeMin"]
                                if directives.get("riskMode") == "cautious":
                                    edge_min = max(edge_min, 0.10)

                                if edge < edge_min:
                                    continue

                                # Size: fraction of USDC.e balance
                                try:
                                    bal = wallet.get_balances().usdc_e
                                except Exception:
                                    continue
                                amt = max(1.0, bal * directives["maxTradeFraction"])  # min $1

                                # Place buy GTC slightly below best ask to reduce taker fees
                                px = max(0.01, round(best_ask - 0.01, 2))
                                order_id, err = clob.buy_gtc(token_id, amt, px)
                                cd[a] = now

                                # persist cooldowns
                                st["cooldowns"] = cd
                                try:
                                    STATE_PATH.write_text(json.dumps(st, indent=2))
                                except Exception:
                                    pass

                                # log to daily memory
                                try:
                                    day = time.strftime("%Y-%m-%d", time.gmtime())
                                    mpath = WORKSPACE_DIR / "memory" / f"{day}.md"
                                    line = f"- {time.strftime('%H:%MZ', time.gmtime())} PolyClaw exec: asset={a} dir={side_dir} p={p:.2f} implied={implied:.2f} edge={edge:.2f} buy_px={px:.2f} amt=${amt:.2f} order={order_id or 'na'} err={err or 'none'}\n"
                                    txt = mpath.read_text() if mpath.exists() else f"# {day}\n\n## Research / Scans\n"
                                    if "## Research / Scans" not in txt:
                                        txt += "\n## Research / Scans\n"
                                    txt += line
                                    mpath.write_text(txt)
                                except Exception:
                                    pass

                            last_poly_update = now

                # write heartbeat
                hb = {
                    "ts": int(now),
                    "executionEnabled": EXECUTION_ENABLED,
                    "assets": ASSETS,
                    "marketsDiscovered": len(markets),
                    "lastDiscoveryTs": int(last_discovery) if last_discovery else None,
                    "lastPolyUpdateTs": int(last_poly_update) if last_poly_update else None,
                    "spot": {a: agg.aggregate(a)[1] | {"agg": agg.aggregate(a)[0]} for a in ASSETS},
                }
                write_heartbeat(hb)

                # state snapshot (minimal)
                STATE_PATH.write_text(json.dumps({"ts": int(now), "markets": markets[:50]}, indent=2))

                await asyncio.sleep(HEARTBEAT_INTERVAL_S)

        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
