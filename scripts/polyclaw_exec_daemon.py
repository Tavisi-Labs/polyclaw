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
        # Outlier reject
        kept = [m for m in mids if abs(m - median) / median <= SPOT_OUTLIER_PCT]
        if len(kept) < 2:
            return median, {"raw": raw, "median": median, "kept": kept, "reason": "outlier_reject_left_too_few"}
        kept_sorted = sorted(kept)
        agg = kept_sorted[len(kept_sorted) // 2]
        return agg, {"raw": raw, "median": median, "kept": kept}


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

                # (placeholder) Polymarket updates / execution
                # For now, we only track that spot is live and markets are discovered.
                # Trading logic will be enabled when POLY_EXECUTION_ENABLED=true.
                if EXECUTION_ENABLED:
                    # TODO: implement CLOB mid/book pulls and order placement
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
