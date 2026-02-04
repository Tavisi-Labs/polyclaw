"""LLM client for hedge discovery.

This is used for extracting logically necessary implications between markets.
Default backend: Venice (OpenAI-compatible) using GPT-5.2.

Env:
- VENICE_API_KEY (required for Venice)
- VENICE_BASE_URL (optional; default https://api.venice.ai/api/v1)
- VENICE_MODEL (optional; default openai-gpt-52)

Legacy:
- OPENROUTER_API_KEY is no longer required when using Venice.
"""

import asyncio
import os

import httpx

# =============================================================================
# CONFIGURATION
# =============================================================================

VENICE_BASE_URL = os.getenv("VENICE_BASE_URL", "https://api.venice.ai/api/v1")
DEFAULT_MODEL = os.getenv("VENICE_MODEL", "openai-gpt-52")

# Request settings
LLM_TIMEOUT = float(os.getenv("VENICE_TIMEOUT", "60"))
LLM_MAX_RETRIES = 3


# =============================================================================
# LLM CLIENT
# =============================================================================


class LLMClient:
    """Async client for Venice (OpenAI-compatible) chat completions."""

    def __init__(
        self,
        model: str = DEFAULT_MODEL,
        api_key: str | None = None,
        timeout: float = LLM_TIMEOUT,
    ):
        self.api_key = api_key or os.getenv("VENICE_API_KEY")
        if not self.api_key:
            raise ValueError(
                "VENICE_API_KEY not set. "
                "Create one and export it (or add it to .env)."
            )

        self.model = model
        self.timeout = timeout
        self.base_url = VENICE_BASE_URL

        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create async client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=self.timeout,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
            )
        return self._client

    async def complete(
        self,
        messages: list[dict],
        temperature: float = 0.1,
        max_tokens: int | None = None,
    ) -> str:
        """
        Send a chat completion request.

        Args:
            messages: List of message dicts with 'role' and 'content'
            temperature: Sampling temperature (0.0-1.0)
            max_tokens: Maximum tokens in response

        Returns:
            The assistant's response text
        """
        client = await self._get_client()

        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
        }
        if max_tokens:
            payload["max_tokens"] = max_tokens

        for attempt in range(LLM_MAX_RETRIES):
            try:
                response = await client.post(
                    f"{self.base_url}/chat/completions",
                    json=payload,
                )
                response.raise_for_status()
                data = response.json()
                return data["choices"][0]["message"]["content"]

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    # Rate limited, wait and retry
                    wait_time = 2**attempt
                    await asyncio.sleep(wait_time)
                    continue
                raise

            except httpx.RequestError:
                if attempt < LLM_MAX_RETRIES - 1:
                    await asyncio.sleep(1)
                    continue
                raise

        raise RuntimeError(f"Failed after {LLM_MAX_RETRIES} attempts")

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> "LLMClient":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()


# =============================================================================
# SINGLETON ACCESS
# =============================================================================

_llm_client: LLMClient | None = None


def get_llm_client(model: str = DEFAULT_MODEL) -> LLMClient:
    """
    Get LLM client singleton.

    Args:
        model: Model identifier (Venice/OpenAI-compatible)

    Returns:
        LLMClient instance
    """
    global _llm_client

    if _llm_client is None or _llm_client.model != model:
        _llm_client = LLMClient(model=model)

    return _llm_client


async def close_llm_client() -> None:
    """Close the LLM client connection."""
    global _llm_client
    if _llm_client:
        await _llm_client.close()
        _llm_client = None
