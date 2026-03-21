from __future__ import annotations

import os
from typing import Literal

from anthropic import AsyncAnthropic


DEFAULT_OPENROUTER_BASE_URL = "https://openrouter.ai/api"


def create_llm_client() -> tuple[AsyncAnthropic, Literal["anthropic", "openrouter"]]:
    """
    Build the Anthropic-compatible async client used by agents.

    Resolution order:
    1. OPENROUTER_API_KEY -> OpenRouter's Anthropic-compatible endpoint
    2. ANTHROPIC_API_KEY  -> Anthropic directly
    """
    openrouter_key = os.environ.get("OPENROUTER_API_KEY")
    if openrouter_key:
        headers: dict[str, str] = {}
        if referer := os.environ.get("OPENROUTER_SITE_URL"):
            headers["HTTP-Referer"] = referer
        if title := os.environ.get("OPENROUTER_APP_NAME"):
            headers["X-Title"] = title

        client = AsyncAnthropic(
            auth_token=openrouter_key,
            base_url=os.environ.get("OPENROUTER_BASE_URL", DEFAULT_OPENROUTER_BASE_URL),
            default_headers=headers or None,
        )
        return client, "openrouter"

    anthropic_key = os.environ.get("ANTHROPIC_API_KEY")
    if anthropic_key:
        return AsyncAnthropic(api_key=anthropic_key), "anthropic"

    raise RuntimeError(
        "Missing LLM credentials. Set OPENROUTER_API_KEY or ANTHROPIC_API_KEY."
    )


def resolve_model_name(default_model: str) -> str:
    """
    Resolve the model name from env without hard-coding provider-specific slugs.

    `OPENROUTER_MODEL` takes priority when using OpenRouter.
    `LLM_MODEL` can override either provider.
    """
    return (
        os.environ.get("LLM_MODEL")
        or os.environ.get("OPENROUTER_MODEL")
        or os.environ.get("ANTHROPIC_MODEL")
        or default_model
    )
