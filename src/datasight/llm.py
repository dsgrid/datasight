"""
LLM client abstraction for datasight.

Provides a common async interface for interacting with different LLM providers
(Anthropic, OpenAI, GitHub Models, Ollama) with tool-use support.

Streaming note
--------------
The ``LLMClient`` Protocol only defines non-streaming ``create_message``.
The web UI's token-level SSE streaming path is implemented directly against
the Anthropic SDK in ``datasight.web.app``; for non-Anthropic providers the
web UI falls back to a single-shot request plus word-splitting on the
server side. Adding cross-provider streaming would require per-backend
streaming adapters; until then, ``create_message`` is the contract.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any, Protocol

import anthropic
from loguru import logger

from datasight.exceptions import (
    ConfigurationError,
    LLMConnectionError,
    LLMResponseError,
)

# Default timeout for LLM API calls (seconds).
DEFAULT_LLM_TIMEOUT: float = 120.0

# Retry configuration for transient LLM failures.
DEFAULT_MAX_RETRIES: int = 3
DEFAULT_RETRY_BASE_DELAY: float = 1.0  # seconds, doubled each retry


# ---------------------------------------------------------------------------
# Common types
# ---------------------------------------------------------------------------


@dataclass
class TextBlock:
    """A text content block from an LLM response."""

    text: str


@dataclass
class ToolUseBlock:
    """A tool use request block from an LLM response."""

    id: str
    name: str
    input: dict[str, Any]


@dataclass
class Usage:
    """Token usage statistics from an LLM response."""

    input_tokens: int = 0
    output_tokens: int = 0
    cache_creation_input_tokens: int = 0
    cache_read_input_tokens: int = 0
    retries_performed: int = 0


# Stop reasons surfaced to callers. Backends normalize provider-specific
# values into this set. ``max_tokens`` means the model hit the output budget
# mid-answer and the response is almost certainly truncated.
StopReason = str  # one of: "end_turn", "tool_use", "max_tokens"


@dataclass
class LLMResponse:
    """Response from an LLM API call."""

    content: list[TextBlock | ToolUseBlock]
    stop_reason: StopReason
    usage: Usage = field(default_factory=Usage)


ContentBlock = TextBlock | ToolUseBlock


def serialize_content(content: list[ContentBlock]) -> list[dict[str, Any]]:
    """Serialize content blocks to Anthropic-style dicts for message history.

    Parameters
    ----------
    content:
        List of TextBlock or ToolUseBlock instances.

    Returns
    -------
    List of serialized content block dictionaries.
    """
    serialized: list[dict[str, Any]] = []
    for block in content:
        if isinstance(block, TextBlock):
            serialized.append({"type": "text", "text": block.text})
        elif isinstance(block, ToolUseBlock):
            serialized.append(
                {
                    "type": "tool_use",
                    "id": block.id,
                    "name": block.name,
                    "input": block.input,
                }
            )
    return serialized


class LLMClient(Protocol):
    """Protocol for LLM backends."""

    async def create_message(
        self,
        *,
        model: str,
        system: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        max_tokens: int,
    ) -> LLMResponse:
        """Create a message with the LLM.

        Parameters
        ----------
        model:
            Model name to use.
        system:
            System prompt.
        messages:
            Conversation history.
        tools:
            Available tool definitions.
        max_tokens:
            Maximum tokens in response.

        Returns
        -------
        LLMResponse with content blocks and usage statistics.
        """
        ...


# ---------------------------------------------------------------------------
# Anthropic implementation
# ---------------------------------------------------------------------------


class AnthropicLLMClient:
    """LLM client backed by the Anthropic API."""

    def __init__(
        self,
        api_key: str,
        base_url: str | None = None,
        timeout: float = DEFAULT_LLM_TIMEOUT,
    ):
        """Initialize the Anthropic client.

        Parameters
        ----------
        api_key:
            Anthropic API key.
        base_url:
            Optional custom base URL for the API.
        timeout:
            Request timeout in seconds.
        """
        kwargs: dict[str, Any] = {
            "api_key": api_key,
            "timeout": timeout,
        }
        if base_url:
            kwargs["base_url"] = base_url
        try:
            self._client = anthropic.AsyncAnthropic(**kwargs)
        except anthropic.APIConnectionError as e:
            raise LLMConnectionError(f"Failed to initialize Anthropic client: {e}") from e

    async def create_message(
        self,
        *,
        model: str,
        system: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        max_tokens: int,
    ) -> LLMResponse:
        """Create a message using the Anthropic API with retries."""
        # Mark the last tool with cache_control so the full tools array is
        # cached alongside the system prompt. A single breakpoint at the end
        # covers everything before it in order (tools + system).
        cached_tools: list[dict[str, Any]] = [dict(t) for t in tools]
        if cached_tools:
            cached_tools[-1]["cache_control"] = {"type": "ephemeral"}

        response = None
        retries = 0
        for attempt in range(DEFAULT_MAX_RETRIES):
            is_last = attempt == DEFAULT_MAX_RETRIES - 1
            try:
                response = await self._client.messages.create(
                    model=model,
                    max_tokens=max_tokens,
                    temperature=0,
                    system=[
                        {
                            "type": "text",
                            "text": system,
                            "cache_control": {"type": "ephemeral"},
                        }
                    ],
                    tools=cached_tools,
                    messages=messages,
                )
                break
            except anthropic.APIConnectionError as e:
                if is_last:
                    logger.exception("Anthropic connection error (final attempt)")
                    raise LLMConnectionError(f"Failed to connect to Anthropic API: {e}") from e
                retries += 1
                delay = DEFAULT_RETRY_BASE_DELAY * (2**attempt)
                logger.warning(
                    f"Anthropic connection error (attempt {attempt + 1}/{DEFAULT_MAX_RETRIES}), "
                    f"retrying in {delay:.1f}s: {e}"
                )
                await asyncio.sleep(delay)
            except anthropic.RateLimitError as e:
                if is_last:
                    logger.exception("Anthropic rate limit error (final attempt)")
                    raise LLMResponseError(f"Anthropic rate limit exceeded: {e}") from e
                retries += 1
                delay = DEFAULT_RETRY_BASE_DELAY * (2**attempt)
                logger.warning(
                    f"Anthropic rate limit (attempt {attempt + 1}/{DEFAULT_MAX_RETRIES}), "
                    f"retrying in {delay:.1f}s"
                )
                await asyncio.sleep(delay)
            except anthropic.APIStatusError as e:
                logger.exception("Anthropic API error")
                raise LLMResponseError(f"Anthropic API error: {e}") from e

        assert response is not None  # break above guarantees this

        content: list[ContentBlock] = []
        for block in response.content:
            if isinstance(block, anthropic.types.TextBlock):
                content.append(TextBlock(text=block.text))
            elif isinstance(block, anthropic.types.ToolUseBlock):
                content.append(
                    ToolUseBlock(
                        id=block.id,
                        name=block.name,
                        input=block.input,
                    )
                )

        match response.stop_reason:
            case "tool_use":
                stop: StopReason = "tool_use"
            case "max_tokens":
                stop = "max_tokens"
            case _:
                stop = "end_turn"
        usage = Usage(
            input_tokens=response.usage.input_tokens,
            output_tokens=response.usage.output_tokens,
            cache_creation_input_tokens=getattr(response.usage, "cache_creation_input_tokens", 0)
            or 0,
            cache_read_input_tokens=getattr(response.usage, "cache_read_input_tokens", 0) or 0,
            retries_performed=retries,
        )
        return LLMResponse(content=content, stop_reason=stop, usage=usage)


# ---------------------------------------------------------------------------
# Ollama (OpenAI-compatible) implementation
# ---------------------------------------------------------------------------


def _convert_tools_to_openai(tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert Anthropic-style tool definitions to OpenAI function-calling format."""
    result: list[dict[str, Any]] = []
    for tool in tools:
        result.append(
            {
                "type": "function",
                "function": {
                    "name": tool["name"],
                    "description": tool.get("description", ""),
                    "parameters": tool.get("input_schema", {}),
                },
            }
        )
    return result


def _convert_messages_to_openai(
    system: str, messages: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Convert Anthropic message format to OpenAI chat format."""
    out: list[dict[str, Any]] = [{"role": "system", "content": system}]

    for msg in messages:
        role = msg["role"]
        content = msg["content"]

        if role == "user" and isinstance(content, str):
            out.append({"role": "user", "content": content})

        elif role == "user" and isinstance(content, list):
            # Tool results
            for item in content:
                if isinstance(item, dict) and item.get("type") == "tool_result":
                    out.append(
                        {
                            "role": "tool",
                            "tool_call_id": item["tool_use_id"],
                            "content": item.get("content", ""),
                        }
                    )

        elif role == "assistant" and isinstance(content, str):
            out.append({"role": "assistant", "content": content})

        elif role == "assistant" and isinstance(content, list):
            # Assistant message with potential tool calls
            text_parts: list[str] = []
            tool_calls: list[dict[str, Any]] = []
            for block in content:
                if isinstance(block, dict):
                    if block.get("type") == "text":
                        text_parts.append(block["text"])
                    elif block.get("type") == "tool_use":
                        tool_calls.append(
                            {
                                "id": block["id"],
                                "type": "function",
                                "function": {
                                    "name": block["name"],
                                    "arguments": json.dumps(block["input"]),
                                },
                            }
                        )

            assistant_msg: dict[str, Any] = {"role": "assistant"}
            if text_parts:
                assistant_msg["content"] = "\n".join(text_parts)
            else:
                assistant_msg["content"] = None
            if tool_calls:
                assistant_msg["tool_calls"] = tool_calls
            out.append(assistant_msg)

    return out


class _OpenAICompatibleClient:
    """Base class for LLM clients using OpenAI-compatible APIs."""

    def __init__(
        self, base_url: str, api_key: str = "ollama", timeout: float = DEFAULT_LLM_TIMEOUT
    ):
        """Initialize the OpenAI-compatible client.

        Parameters
        ----------
        base_url:
            Base URL for the API.
        api_key:
            API key (defaults to "ollama" for local Ollama).
        timeout:
            Request timeout in seconds.
        """
        try:
            import openai
            from openai import AsyncOpenAI
        except ImportError as e:
            raise ImportError(
                "The 'openai' package is required for this provider. "
                "Install it with: pip install openai"
            ) from e
        # Stash typed exception classes so the retry loop can catch them
        # without repeating the lazy import on every attempt.
        self._openai = openai
        try:
            self._client = AsyncOpenAI(base_url=base_url, api_key=api_key, timeout=timeout)
        except Exception as e:
            raise LLMConnectionError(f"Failed to initialize OpenAI client: {e}") from e

    async def create_message(
        self,
        *,
        model: str,
        system: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        max_tokens: int,
    ) -> LLMResponse:
        """Create a message using the OpenAI-compatible API with retries."""
        openai_messages = _convert_messages_to_openai(system, messages)
        openai_tools = _convert_tools_to_openai(tools)

        # Typed transient errors for this SDK. Anything not in this tuple is
        # treated as fatal and raised immediately.
        oa = self._openai
        transient_errors: tuple[type[BaseException], ...] = (
            oa.APIConnectionError,
            oa.APITimeoutError,
            oa.RateLimitError,
            oa.InternalServerError,
        )

        response = None
        retries = 0
        for attempt in range(DEFAULT_MAX_RETRIES):
            is_last = attempt == DEFAULT_MAX_RETRIES - 1
            try:
                response = await self._client.chat.completions.create(
                    model=model,
                    messages=openai_messages,
                    tools=openai_tools if openai_tools else None,
                    max_tokens=max_tokens,
                    temperature=0,
                )
                break
            except transient_errors as e:
                if is_last:
                    logger.exception("OpenAI-compatible API error (final attempt)")
                    raise LLMConnectionError(f"API request failed: {e}") from e
                retries += 1
                delay = DEFAULT_RETRY_BASE_DELAY * (2**attempt)
                logger.warning(
                    f"OpenAI-compatible API error (attempt {attempt + 1}/{DEFAULT_MAX_RETRIES}), "
                    f"retrying in {delay:.1f}s: {e}"
                )
                await asyncio.sleep(delay)
            except oa.APIStatusError as e:
                logger.exception("OpenAI-compatible API error")
                raise LLMResponseError(f"API error: {e}") from e

        assert response is not None  # break above guarantees this

        choice = response.choices[0]
        content: list[ContentBlock] = []

        if choice.message.content:
            content.append(TextBlock(text=choice.message.content))

        tool_calls = choice.message.tool_calls or []
        for tc in tool_calls:
            try:
                args = json.loads(tc.function.arguments)
            except (json.JSONDecodeError, TypeError):
                logger.warning(f"Failed to parse tool arguments: {tc.function.arguments}")
                args = {}
            content.append(
                ToolUseBlock(
                    id=tc.id,
                    name=tc.function.name,
                    input=args,
                )
            )

        finish = getattr(choice, "finish_reason", None)
        if tool_calls:
            stop: StopReason = "tool_use"
        elif finish == "length":
            stop = "max_tokens"
        else:
            stop = "end_turn"
        usage = Usage(
            input_tokens=response.usage.prompt_tokens if response.usage else 0,
            output_tokens=response.usage.completion_tokens if response.usage else 0,
            retries_performed=retries,
        )
        return LLMResponse(content=content, stop_reason=stop, usage=usage)


class OllamaLLMClient(_OpenAICompatibleClient):
    """LLM client backed by Ollama's OpenAI-compatible API."""

    def __init__(
        self,
        base_url: str = "http://localhost:11434/v1",
        timeout: float = DEFAULT_LLM_TIMEOUT,
    ):
        """Initialize the Ollama client.

        Parameters
        ----------
        base_url:
            Ollama API base URL.
        timeout:
            Request timeout in seconds.
        """
        super().__init__(base_url=base_url, api_key="ollama", timeout=timeout)


# ---------------------------------------------------------------------------
# GitHub Models implementation
# ---------------------------------------------------------------------------

# Current GitHub Models inference endpoint. The older
# ``models.inference.ai.azure.com`` host still works but is being phased out.
GITHUB_MODELS_BASE_URL = "https://models.github.ai/inference"


class GitHubModelsLLMClient(_OpenAICompatibleClient):
    """LLM client backed by GitHub Models (OpenAI-compatible)."""

    def __init__(
        self,
        api_key: str,
        base_url: str = GITHUB_MODELS_BASE_URL,
        timeout: float = DEFAULT_LLM_TIMEOUT,
    ):
        """Initialize the GitHub Models client.

        Parameters
        ----------
        api_key:
            GitHub token for authentication.
        base_url:
            API base URL.
        timeout:
            Request timeout in seconds.
        """
        super().__init__(base_url=base_url, api_key=api_key, timeout=timeout)


# ---------------------------------------------------------------------------
# OpenAI implementation
# ---------------------------------------------------------------------------

OPENAI_BASE_URL = "https://api.openai.com/v1"


class OpenAILLMClient(_OpenAICompatibleClient):
    """LLM client backed by the OpenAI API."""

    def __init__(
        self,
        api_key: str,
        base_url: str = OPENAI_BASE_URL,
        timeout: float = DEFAULT_LLM_TIMEOUT,
    ):
        """Initialize the OpenAI client.

        Parameters
        ----------
        api_key:
            OpenAI API key.
        base_url:
            API base URL. Override for OpenAI-compatible gateways (Azure
            OpenAI, corporate proxies).
        timeout:
            Request timeout in seconds.
        """
        super().__init__(base_url=base_url, api_key=api_key, timeout=timeout)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


# Providers that require a user-supplied API key. Ollama runs locally and
# accepts any placeholder key.
_PROVIDERS_REQUIRING_KEY = {"anthropic", "openai", "github"}

# Env-var hints used in the missing-key error message.
_PROVIDER_API_KEY_HINT = {
    "anthropic": "ANTHROPIC_API_KEY",
    "openai": "OPENAI_API_KEY",
    "github": "GITHUB_TOKEN",
}


def create_llm_client(
    provider: str,
    api_key: str = "",
    base_url: str | None = None,
    timeout: float = DEFAULT_LLM_TIMEOUT,
) -> LLMClient:
    """Create an LLM client for the given provider.

    Parameters
    ----------
    provider:
        Provider name: "anthropic", "openai", "ollama", or "github".
    api_key:
        API key for the provider. Required for all providers except Ollama.
    base_url:
        Optional custom base URL.
    timeout:
        Per-request timeout in seconds.

    Returns
    -------
    An LLM client instance.

    Raises
    ------
    ConfigurationError:
        If ``provider`` requires an API key and none was supplied.
    LLMConnectionError:
        If client initialization fails.
    ValueError:
        If ``provider`` is not recognized.
    """
    if provider in _PROVIDERS_REQUIRING_KEY and not api_key:
        env_var = _PROVIDER_API_KEY_HINT[provider]
        raise ConfigurationError(
            f"No API key configured for provider {provider!r}. "
            f"Set {env_var} or configure the LLM connection."
        )

    match provider:
        case "ollama":
            url = base_url or "http://localhost:11434/v1"
            logger.info(f"Using Ollama LLM backend: {url}")
            return OllamaLLMClient(base_url=url, timeout=timeout)
        case "github":
            url = base_url or GITHUB_MODELS_BASE_URL
            logger.info(f"Using GitHub Models LLM backend: {url}")
            return GitHubModelsLLMClient(api_key=api_key, base_url=url, timeout=timeout)
        case "openai":
            url = base_url or OPENAI_BASE_URL
            logger.info(f"Using OpenAI LLM backend: {url}")
            return OpenAILLMClient(api_key=api_key, base_url=url, timeout=timeout)
        case "anthropic":
            if base_url:
                logger.info(f"Using Anthropic LLM backend: {base_url}")
            else:
                logger.info("Using Anthropic LLM backend")
            return AnthropicLLMClient(api_key=api_key, base_url=base_url, timeout=timeout)
        case _:
            raise ValueError(f"Unknown LLM provider: {provider}")
