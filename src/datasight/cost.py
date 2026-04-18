"""Shared token-cost helpers for datasight.

Both the web server and CLI ``ask`` path need to summarize per-turn LLM
cost (api calls, token counts, estimated dollar cost) and hand that
summary to ``QueryLogger.log_cost``. Keeping the pricing table and the
summary builder here avoids the CLI importing from ``datasight.web``.
"""

from __future__ import annotations

from typing import Any

from loguru import logger


# Input/output price per million tokens, USD.
MODEL_PRICING: dict[str, tuple[float, float]] = {
    # Anthropic
    "claude-sonnet-4-6": (3.0, 15.0),
    "claude-haiku-4-5-20251001": (0.80, 4.0),
    "claude-opus-4-7": (15.0, 75.0),
    # OpenAI
    "gpt-4o": (2.50, 10.0),
    "gpt-4o-mini": (0.15, 0.60),
}

# Providers that bill per token and are covered by ``MODEL_PRICING``. Other
# providers (GitHub Models quota-based free tier, local Ollama) should not
# produce a dollar estimate.
_PROVIDERS_WITH_PRICING = {"anthropic", "openai"}


def build_cost_data(
    model: str,
    api_calls: int,
    input_tokens: int,
    output_tokens: int,
    *,
    cache_creation_input_tokens: int = 0,
    cache_read_input_tokens: int = 0,
    provider: str | None = None,
) -> dict[str, Any]:
    """Build a cost/token summary dict for a single turn.

    When ``provider`` is supplied and is not in :data:`_PROVIDERS_WITH_PRICING`
    (e.g. ``"github"`` or ``"ollama"``), ``estimated_cost`` is left as
    ``None`` even if the model name happens to be in ``MODEL_PRICING`` —
    GitHub Models reuses OpenAI model names but is billed by quota, not
    per-token, so pricing them against OpenAI rates is misleading.
    """
    data: dict[str, Any] = {
        "api_calls": api_calls,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cache_creation_input_tokens": cache_creation_input_tokens,
        "cache_read_input_tokens": cache_read_input_tokens,
        "estimated_cost": None,
    }
    if provider is not None and provider not in _PROVIDERS_WITH_PRICING:
        return data
    pricing = MODEL_PRICING.get(model)
    if pricing:
        input_cost = input_tokens * pricing[0] / 1_000_000
        output_cost = output_tokens * pricing[1] / 1_000_000
        # Anthropic prompt-cache writes are billed at 1.25x input price and
        # cache reads at 0.1x input price for the ephemeral cache used here.
        cache_creation_cost = cache_creation_input_tokens * pricing[0] * 1.25 / 1_000_000
        cache_read_cost = cache_read_input_tokens * pricing[0] * 0.1 / 1_000_000
        data["estimated_cost"] = round(
            input_cost + output_cost + cache_creation_cost + cache_read_cost,
            6,
        )
    return data


def log_query_cost(
    model: str,
    api_calls: int,
    input_tokens: int,
    output_tokens: int,
    *,
    cache_creation_input_tokens: int = 0,
    cache_read_input_tokens: int = 0,
    provider: str | None = None,
) -> None:
    """Emit a one-line loguru summary of token usage and estimated cost."""
    data = build_cost_data(
        model,
        api_calls,
        input_tokens,
        output_tokens,
        cache_creation_input_tokens=cache_creation_input_tokens,
        cache_read_input_tokens=cache_read_input_tokens,
        provider=provider,
    )
    cost = data["estimated_cost"]
    cost_str = f" est_cost=${cost:.4f}" if cost is not None else ""
    logger.info(
        f"[tokens] QUERY TOTAL: api_calls={api_calls} "
        f"input={input_tokens} output={output_tokens} "
        f"cache_create={cache_creation_input_tokens} cache_read={cache_read_input_tokens}"
        f"{cost_str}"
    )
