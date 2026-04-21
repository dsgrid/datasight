"""
LLM client abstraction for datasight.

Provides a common async interface for interacting with different LLM providers
(Anthropic, OpenAI, GitHub Models, Ollama) with tool-use support.

Streaming note
--------------
The ``LLMClient`` Protocol only defines non-streaming ``create_message``.
All callers — including the web UI's SSE path in ``datasight.web.app`` —
drive the agent loop through ``create_message`` and emit SSE events around
whole responses. Adding true token-level streaming would require
per-backend streaming adapters added to the Protocol; until then,
``create_message`` is the contract every provider implements.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any, Protocol, cast

import anthropic
from anthropic.types import MessageParam, TextBlockParam, ToolUnionParam
from loguru import logger
from openai.types.chat import (
    ChatCompletionMessageFunctionToolCall,
    ChatCompletionMessageParam,
    ChatCompletionToolUnionParam,
)

from datasight.exceptions import (
    ConfigurationError,
    LLMConnectionError,
    LLMResponseError,
)

# Default timeout for LLM API calls (seconds).
DEFAULT_LLM_TIMEOUT: float = 120.0

# Retry configuration for transient LLM failures. ``DEFAULT_MAX_ATTEMPTS``
# is the total call budget (one initial call + up to N-1 retries), not the
# number of retries after the first failure.
DEFAULT_MAX_ATTEMPTS: int = 3
DEFAULT_RETRY_BASE_DELAY: float = 1.0  # seconds, doubled each retry


# Phrases and error codes each provider uses for "your prompt blew through the
# context window." Matched case-insensitively against the raw error message.
# Conservative on purpose: false positives would point users at
# SCHEMA_INCLUDE_MAX_BYTES when that isn't the actual problem.
_CONTEXT_OVERFLOW_PATTERNS: tuple[str, ...] = (
    "context length",
    "context_length",
    "context_length_exceeded",
    "maximum context",
    "prompt is too long",
    "too many tokens",
    "exceeded tokens",
    "token limit",
    "tokens_limit",
    "tokens_limit_reached",
    "request too large",
    "request body too large",
    "input is too long",
)

# Tail added to ``LLMResponseError`` when the provider's 4xx looks like a
# context-window overflow. Points at the usual culprit (schema includes) and
# the escape hatches (disable the resolver, switch to a larger-context
# backend).
_CONTEXT_OVERFLOW_HINT: str = (
    "\n\nThis looks like a context-window overflow. If your schema_description.md uses "
    "[include:Title](url) directives, the fetched pages may be pushing the prompt past "
    "this model's token limit. Options:\n"
    "  - Set SCHEMA_INCLUDE_MAX_BYTES=0 to skip include-link resolution entirely.\n"
    "  - Lower SCHEMA_INCLUDE_MAX_BYTES to a smaller per-URL cap.\n"
    "  - Switch to a larger-context backend (e.g. LLM_PROVIDER=anthropic)."
)


def _is_context_overflow_error(message: str) -> bool:
    """Return True if the provider error message smells like a context overflow."""
    if not message:
        return False
    lowered = message.lower()
    return any(pattern in lowered for pattern in _CONTEXT_OVERFLOW_PATTERNS)


def _augment_if_context_overflow(message: str) -> str:
    """Append the context-overflow hint to ``message`` when the pattern matches."""
    if _is_context_overflow_error(message):
        return message + _CONTEXT_OVERFLOW_HINT
    return message


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


@dataclass
class CallStats:
    """Per-call LLM client telemetry that isn't token usage."""

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
    call_stats: CallStats = field(default_factory=CallStats)


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
        # ``AsyncAnthropic.__init__`` does argument validation but no network
        # IO, so there is no transient failure to guard against here. Any
        # ``AnthropicError`` raised would be a config problem (e.g. bad
        # api_key shape) and propagates directly to the caller.
        self._client = anthropic.AsyncAnthropic(**kwargs)

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
        #
        # Tool ORDER is load-bearing: Anthropic's prompt cache keys off the
        # exact serialized prefix, so callers must pass tools in a stable
        # order across turns or we silently lose the cache hit. Shallow-copy
        # each tool dict so stamping cache_control here doesn't mutate the
        # caller's list; we only add a top-level key, so sharing nested
        # structures like input_schema is safe.
        cached_tools: list[dict[str, Any]] = [dict(t) for t in tools]
        if cached_tools:
            cached_tools[-1]["cache_control"] = {"type": "ephemeral"}
        anthropic_tools = cast("list[ToolUnionParam]", cached_tools)
        anthropic_messages = cast("list[MessageParam]", messages)
        anthropic_system = [
            cast(
                TextBlockParam,
                {
                    "type": "text",
                    "text": system,
                    "cache_control": {"type": "ephemeral"},
                },
            )
        ]

        response = None
        retries = 0
        for attempt in range(DEFAULT_MAX_ATTEMPTS):
            is_last = attempt == DEFAULT_MAX_ATTEMPTS - 1
            try:
                response = await self._client.messages.create(
                    model=model,
                    max_tokens=max_tokens,
                    temperature=0,
                    system=anthropic_system,
                    tools=anthropic_tools,
                    messages=anthropic_messages,
                )
                break
            except anthropic.AuthenticationError as e:
                # Auth failures are a configuration problem, not a transient
                # API error — retrying won't fix a missing/invalid key.
                logger.exception("Anthropic authentication error")
                raise ConfigurationError(
                    f"Anthropic authentication failed — check ANTHROPIC_API_KEY: {e}"
                ) from e
            except anthropic.APIConnectionError as e:
                if is_last:
                    logger.exception("Anthropic connection error (final attempt)")
                    raise LLMConnectionError(f"Failed to connect to Anthropic API: {e}") from e
                retries += 1
                delay = DEFAULT_RETRY_BASE_DELAY * (2**attempt)
                logger.warning(
                    f"Anthropic connection error "
                    f"(attempt {attempt + 1}/{DEFAULT_MAX_ATTEMPTS}), "
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
                    f"Anthropic rate limit "
                    f"(attempt {attempt + 1}/{DEFAULT_MAX_ATTEMPTS}), "
                    f"retrying in {delay:.1f}s"
                )
                await asyncio.sleep(delay)
            except anthropic.InternalServerError as e:
                if is_last:
                    logger.exception("Anthropic 5xx error (final attempt)")
                    raise LLMResponseError(f"Anthropic API error: {e}") from e
                retries += 1
                delay = DEFAULT_RETRY_BASE_DELAY * (2**attempt)
                logger.warning(
                    f"Anthropic server error "
                    f"(attempt {attempt + 1}/{DEFAULT_MAX_ATTEMPTS}), "
                    f"retrying in {delay:.1f}s: {e}"
                )
                await asyncio.sleep(delay)
            except anthropic.APIStatusError as e:
                logger.exception("Anthropic API error")
                raise LLMResponseError(
                    _augment_if_context_overflow(f"Anthropic API error: {e}")
                ) from e

        if response is None:
            raise LLMResponseError(
                "Anthropic client exhausted retries without returning a response"
            )

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
        )
        return LLMResponse(
            content=content,
            stop_reason=stop,
            usage=usage,
            call_stats=CallStats(retries_performed=retries),
        )


# ---------------------------------------------------------------------------
# Ollama (OpenAI-compatible) implementation
# ---------------------------------------------------------------------------


def _stringify_tool_result_content(raw: Any) -> str:
    """Coerce an Anthropic-style ``tool_result.content`` payload to a string.

    Anthropic allows ``tool_result.content`` to be either a string or a list
    of content blocks (text, image, etc.). The OpenAI chat ``tool`` message
    only accepts a string, so list-shaped payloads are flattened: text
    blocks are concatenated, and anything else is JSON-serialized so we
    don't silently drop information downstream.
    """
    if raw is None:
        return ""
    if isinstance(raw, str):
        return raw
    if isinstance(raw, list):
        parts: list[str] = []
        for block in raw:
            if isinstance(block, dict) and block.get("type") == "text":
                parts.append(str(block.get("text", "")))
            else:
                parts.append(json.dumps(block, default=str))
        return "\n".join(parts)
    return json.dumps(raw, default=str)


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
            # User turn may carry tool_result blocks (one OpenAI "tool"
            # message per block) and/or plain text blocks (a single "user"
            # message). We emit them in input order so conversation state
            # stays consistent.
            text_parts: list[str] = []
            for item in content:
                if not isinstance(item, dict):
                    logger.debug(f"Skipping non-dict user content block: {type(item).__name__}")
                    continue
                block_type = item.get("type")
                if block_type == "tool_result":
                    out.append(
                        {
                            "role": "tool",
                            "tool_call_id": item["tool_use_id"],
                            "content": _stringify_tool_result_content(item.get("content")),
                        }
                    )
                elif block_type == "text":
                    text_parts.append(item.get("text", ""))
                else:
                    logger.debug(f"Skipping unexpected user content block type: {block_type!r}")
            if text_parts:
                out.append({"role": "user", "content": "\n".join(text_parts)})

        elif role == "assistant" and isinstance(content, str):
            out.append({"role": "assistant", "content": content})

        elif role == "assistant" and isinstance(content, list):
            # Assistant message with potential tool calls
            text_parts = []
            tool_calls: list[dict[str, Any]] = []
            for block in content:
                if not isinstance(block, dict):
                    logger.debug(
                        f"Skipping non-dict assistant content block: {type(block).__name__}"
                    )
                    continue
                block_type = block.get("type")
                if block_type == "text":
                    text_parts.append(block["text"])
                elif block_type == "tool_use":
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
                else:
                    logger.debug(
                        f"Skipping unexpected assistant content block type: {block_type!r}"
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

    # Subclasses set this to ``False`` to treat ``APITimeoutError`` as a
    # fatal ``LLMConnectionError`` instead of retrying it. Default keeps the
    # historical retry-on-timeout behavior for hosted backends (OpenAI,
    # GitHub Models) where a timeout usually means a transient network
    # hiccup. See ``OllamaLLMClient`` for the opposite trade-off.
    _retry_on_timeout: bool = True

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
        except openai.OpenAIError as e:
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
        openai_messages = cast(
            "list[ChatCompletionMessageParam]",
            _convert_messages_to_openai(system, messages),
        )
        openai_tools = cast(
            "list[ChatCompletionToolUnionParam]",
            _convert_tools_to_openai(tools),
        )

        oa = self._openai

        response = None
        retries = 0
        for attempt in range(DEFAULT_MAX_ATTEMPTS):
            is_last = attempt == DEFAULT_MAX_ATTEMPTS - 1
            try:
                if openai_tools:
                    response = await self._client.chat.completions.create(
                        model=model,
                        messages=openai_messages,
                        tools=openai_tools,
                        max_tokens=max_tokens,
                        temperature=0,
                    )
                else:
                    response = await self._client.chat.completions.create(
                        model=model,
                        messages=openai_messages,
                        max_tokens=max_tokens,
                        temperature=0,
                    )
                break
            except oa.AuthenticationError as e:
                # Auth failures are a configuration problem — don't retry.
                # AuthenticationError is a subclass of APIStatusError so this
                # branch must come before the generic status-error branch.
                logger.exception("OpenAI-compatible authentication error")
                raise ConfigurationError(
                    f"Authentication failed — check the API key for this provider: {e}"
                ) from e
            except oa.APITimeoutError as e:
                # ``APITimeoutError`` is a subclass of ``APIConnectionError``,
                # so this specific branch must come first when subclasses
                # disable timeout retries (e.g. Ollama, where a local model
                # that's slow on this call will also be slow on the retry).
                if is_last or not self._retry_on_timeout:
                    logger.exception("OpenAI-compatible API timeout (final attempt)")
                    raise LLMConnectionError(f"API request timed out: {e}") from e
                retries += 1
                delay = DEFAULT_RETRY_BASE_DELAY * (2**attempt)
                logger.warning(
                    f"OpenAI-compatible API timeout "
                    f"(attempt {attempt + 1}/{DEFAULT_MAX_ATTEMPTS}), "
                    f"retrying in {delay:.1f}s: {e}"
                )
                await asyncio.sleep(delay)
            except oa.APIConnectionError as e:
                if is_last:
                    logger.exception("OpenAI-compatible connection error (final attempt)")
                    raise LLMConnectionError(f"API request failed: {e}") from e
                retries += 1
                delay = DEFAULT_RETRY_BASE_DELAY * (2**attempt)
                logger.warning(
                    f"OpenAI-compatible connection error "
                    f"(attempt {attempt + 1}/{DEFAULT_MAX_ATTEMPTS}), "
                    f"retrying in {delay:.1f}s: {e}"
                )
                await asyncio.sleep(delay)
            except oa.RateLimitError as e:
                if is_last:
                    logger.exception("OpenAI-compatible rate limit (final attempt)")
                    raise LLMResponseError(f"API rate limit exceeded: {e}") from e
                retries += 1
                delay = DEFAULT_RETRY_BASE_DELAY * (2**attempt)
                logger.warning(
                    f"OpenAI-compatible rate limit "
                    f"(attempt {attempt + 1}/{DEFAULT_MAX_ATTEMPTS}), "
                    f"retrying in {delay:.1f}s"
                )
                await asyncio.sleep(delay)
            except oa.InternalServerError as e:
                if is_last:
                    logger.exception("OpenAI-compatible 5xx error (final attempt)")
                    raise LLMResponseError(f"API server error: {e}") from e
                retries += 1
                delay = DEFAULT_RETRY_BASE_DELAY * (2**attempt)
                logger.warning(
                    f"OpenAI-compatible server error "
                    f"(attempt {attempt + 1}/{DEFAULT_MAX_ATTEMPTS}), "
                    f"retrying in {delay:.1f}s: {e}"
                )
                await asyncio.sleep(delay)
            except oa.APIStatusError as e:
                logger.exception("OpenAI-compatible API error")
                raise LLMResponseError(_augment_if_context_overflow(f"API error: {e}")) from e

        if response is None:
            raise LLMResponseError(
                "OpenAI-compatible client exhausted retries without returning a response"
            )

        choice = response.choices[0]
        content: list[ContentBlock] = []

        if choice.message.content:
            content.append(TextBlock(text=choice.message.content))

        tool_calls = choice.message.tool_calls or []
        for tc in tool_calls:
            tool_call_type = getattr(tc, "type", "function")
            if isinstance(tool_call_type, str) and tool_call_type != "function":
                logger.warning(f"Skipping unsupported OpenAI tool call type: {tool_call_type}")
                continue
            function_call = cast(ChatCompletionMessageFunctionToolCall, tc)
            try:
                args = json.loads(function_call.function.arguments)
            except (json.JSONDecodeError, TypeError):
                logger.warning(
                    f"Failed to parse tool arguments: {function_call.function.arguments}"
                )
                args = {}
            content.append(
                ToolUseBlock(
                    id=function_call.id,
                    name=function_call.function.name,
                    input=args,
                )
            )

        finish = getattr(choice, "finish_reason", None)
        # ``finish_reason == "length"`` outranks ``tool_use`` here: a
        # truncated tool_call payload is worse than a plain text cutoff
        # (unparseable JSON / half an argument), so surface max_tokens so
        # the agent can show the truncation notice instead of running a
        # broken tool call.
        if finish == "length":
            stop: StopReason = "max_tokens"
        elif tool_calls:
            stop = "tool_use"
        else:
            stop = "end_turn"
        # ``cache_creation_input_tokens`` / ``cache_read_input_tokens`` are
        # left at the ``Usage`` defaults (0) — OpenAI-compatible APIs don't
        # report prompt-cache metadata the way Anthropic does, so there's
        # nothing to populate here.
        usage = Usage(
            input_tokens=response.usage.prompt_tokens if response.usage else 0,
            output_tokens=response.usage.completion_tokens if response.usage else 0,
        )
        return LLMResponse(
            content=content,
            stop_reason=stop,
            usage=usage,
            call_stats=CallStats(retries_performed=retries),
        )


class OllamaLLMClient(_OpenAICompatibleClient):
    """LLM client backed by Ollama's OpenAI-compatible API."""

    # Local inference timeouts almost always mean "this model is slow on
    # this prompt," not "transient network blip." Retrying with exponential
    # backoff compounds the wait (e.g. 120s + 1s + 120s + 2s + 120s across
    # three attempts) before the caller sees the failure. Better to fail
    # fast and let the user pick a smaller model or raise ``LLM_TIMEOUT``.
    _retry_on_timeout = False

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


@dataclass(frozen=True)
class _ProviderSpec:
    """Single source of truth for a provider entry in the factory.

    ``api_key_env`` is ``None`` for providers that don't need a user key
    (Ollama runs locally and accepts any placeholder).
    """

    label: str
    default_base_url: str | None
    api_key_env: str | None


# Order is not significant; dict keys are the provider names accepted by the
# public factory. Adding a new provider is a one-place change: register it
# here, handle it in the ``match`` below, and add it to the ``LLMProvider``
# Literal in ``datasight.settings`` (kept in sync with ``SUPPORTED_PROVIDERS``
# below).
_PROVIDERS: dict[str, _ProviderSpec] = {
    "anthropic": _ProviderSpec(
        label="Anthropic",
        default_base_url=None,
        api_key_env="ANTHROPIC_API_KEY",
    ),
    "openai": _ProviderSpec(
        label="OpenAI",
        default_base_url=OPENAI_BASE_URL,
        api_key_env="OPENAI_API_KEY",
    ),
    "github": _ProviderSpec(
        label="GitHub Models",
        default_base_url=GITHUB_MODELS_BASE_URL,
        api_key_env="GITHUB_TOKEN",
    ),
    "ollama": _ProviderSpec(
        label="Ollama",
        default_base_url="http://localhost:11434/v1",
        api_key_env=None,
    ),
}

# Public snapshot of provider names the factory accepts. Used by
# ``datasight.settings`` for runtime validation so the provider list lives in
# one place.
SUPPORTED_PROVIDERS: frozenset[str] = frozenset(_PROVIDERS)


def create_llm_client(
    provider: str,
    api_key: str = "",
    base_url: str | None = None,
    timeout: float = DEFAULT_LLM_TIMEOUT,
    *,
    model: str | None = None,
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
    model:
        Optional model name used only for the startup log line. Does not
        affect client behavior — the model is still selected per call via
        ``create_message``.

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
    spec = _PROVIDERS.get(provider)
    if spec is None:
        raise ValueError(f"Unknown LLM provider: {provider}")

    if spec.api_key_env is not None and not api_key:
        raise ConfigurationError(
            f"No API key configured for provider {provider!r}. "
            f"Set {spec.api_key_env} or configure the LLM connection."
        )

    url = base_url or spec.default_base_url
    model_suffix = f" (model={model})" if model else ""
    if url:
        logger.info(f"Using {spec.label} LLM backend: {url}{model_suffix}")
    else:
        logger.info(f"Using {spec.label} LLM backend{model_suffix}")

    match provider:
        case "ollama":
            # url is non-None: Ollama has a default_base_url.
            assert url is not None
            return OllamaLLMClient(base_url=url, timeout=timeout)
        case "github":
            assert url is not None
            return GitHubModelsLLMClient(api_key=api_key, base_url=url, timeout=timeout)
        case "openai":
            assert url is not None
            return OpenAILLMClient(api_key=api_key, base_url=url, timeout=timeout)
        case "anthropic":
            return AnthropicLLMClient(api_key=api_key, base_url=url, timeout=timeout)
        case _:  # pragma: no cover — guarded by _PROVIDERS lookup above
            raise ValueError(f"Unknown LLM provider: {provider}")
