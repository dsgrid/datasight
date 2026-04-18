"""Tests for LLM client implementations."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import anthropic
import pytest

from datasight.exceptions import LLMConnectionError, LLMResponseError
from datasight.llm import (
    DEFAULT_MAX_ATTEMPTS,
    AnthropicLLMClient,
    CallStats,
    GitHubModelsLLMClient,
    LLMResponse,
    OllamaLLMClient,
    TextBlock,
    ToolUseBlock,
    Usage,
    _convert_messages_to_openai,
    _convert_tools_to_openai,
    create_llm_client,
    serialize_content,
)


# ---------------------------------------------------------------------------
# serialize_content
# ---------------------------------------------------------------------------


def test_serialize_content_text_only():
    result = serialize_content([TextBlock(text="hello")])
    assert result == [{"type": "text", "text": "hello"}]


def test_serialize_content_tool_use():
    result = serialize_content(
        [
            TextBlock(text="ok"),
            ToolUseBlock(id="t1", name="run_sql", input={"sql": "SELECT 1"}),
        ]
    )
    assert result == [
        {"type": "text", "text": "ok"},
        {
            "type": "tool_use",
            "id": "t1",
            "name": "run_sql",
            "input": {"sql": "SELECT 1"},
        },
    ]


def test_serialize_content_empty():
    assert serialize_content([]) == []


# ---------------------------------------------------------------------------
# Conversion helpers
# ---------------------------------------------------------------------------


def test_convert_tools_to_openai():
    tools = [
        {
            "name": "run_sql",
            "description": "Run a query",
            "input_schema": {"type": "object", "properties": {"sql": {"type": "string"}}},
        },
        {"name": "no_desc"},
    ]
    result = _convert_tools_to_openai(tools)
    assert result[0]["type"] == "function"
    assert result[0]["function"]["name"] == "run_sql"
    assert result[0]["function"]["description"] == "Run a query"
    assert result[0]["function"]["parameters"]["type"] == "object"
    assert result[1]["function"]["description"] == ""
    assert result[1]["function"]["parameters"] == {}


def test_convert_messages_to_openai_user_string():
    result = _convert_messages_to_openai("sys", [{"role": "user", "content": "hello"}])
    assert result[0] == {"role": "system", "content": "sys"}
    assert result[1] == {"role": "user", "content": "hello"}


def test_convert_messages_to_openai_tool_result():
    msgs = [
        {
            "role": "user",
            "content": [
                {
                    "type": "tool_result",
                    "tool_use_id": "t1",
                    "content": "42",
                }
            ],
        }
    ]
    result = _convert_messages_to_openai("sys", msgs)
    assert result[-1] == {"role": "tool", "tool_call_id": "t1", "content": "42"}


def test_convert_messages_to_openai_assistant_string():
    result = _convert_messages_to_openai("sys", [{"role": "assistant", "content": "hi there"}])
    assert result[-1] == {"role": "assistant", "content": "hi there"}


def test_convert_messages_to_openai_assistant_with_tool_use():
    msgs = [
        {
            "role": "assistant",
            "content": [
                {"type": "text", "text": "let me query"},
                {
                    "type": "tool_use",
                    "id": "t1",
                    "name": "run_sql",
                    "input": {"sql": "SELECT 1"},
                },
            ],
        }
    ]
    result = _convert_messages_to_openai("sys", msgs)
    asst = result[-1]
    assert asst["role"] == "assistant"
    assert asst["content"] == "let me query"
    assert asst["tool_calls"][0]["id"] == "t1"
    assert asst["tool_calls"][0]["function"]["name"] == "run_sql"
    assert json.loads(asst["tool_calls"][0]["function"]["arguments"]) == {"sql": "SELECT 1"}


def test_convert_messages_to_openai_assistant_tool_only_no_text():
    msgs = [
        {
            "role": "assistant",
            "content": [
                {
                    "type": "tool_use",
                    "id": "t1",
                    "name": "run_sql",
                    "input": {"sql": "SELECT 1"},
                }
            ],
        }
    ]
    result = _convert_messages_to_openai("sys", msgs)
    asst = result[-1]
    assert asst["content"] is None
    assert "tool_calls" in asst


# ---------------------------------------------------------------------------
# AnthropicLLMClient
# ---------------------------------------------------------------------------


def _make_fake_anthropic_response(
    *, text: str | None = None, tool_use: dict | None = None, stop_reason="end_turn"
):
    """Build a fake Anthropic response object."""
    blocks = []
    if text is not None:
        tb = MagicMock(spec=anthropic.types.TextBlock)
        tb.text = text
        blocks.append(tb)
    if tool_use is not None:
        tub = MagicMock(spec=anthropic.types.ToolUseBlock)
        tub.id = tool_use["id"]
        tub.name = tool_use["name"]
        tub.input = tool_use["input"]
        blocks.append(tub)

    resp = MagicMock()
    resp.content = blocks
    resp.stop_reason = stop_reason
    resp.usage = MagicMock(
        input_tokens=10,
        output_tokens=20,
        cache_creation_input_tokens=5,
        cache_read_input_tokens=3,
    )
    return resp


@pytest.mark.asyncio
async def test_anthropic_client_text_response():
    with patch("datasight.llm.anthropic.AsyncAnthropic") as mock_cls:
        instance = MagicMock()
        instance.messages.create = AsyncMock(
            return_value=_make_fake_anthropic_response(text="hello world")
        )
        mock_cls.return_value = instance

        client = AnthropicLLMClient(api_key="sk-test")
        result = await client.create_message(
            model="claude", system="sys", messages=[], tools=[], max_tokens=100
        )
        assert isinstance(result, LLMResponse)
        assert len(result.content) == 1
        assert isinstance(result.content[0], TextBlock)
        assert result.content[0].text == "hello world"
        assert result.stop_reason == "end_turn"
        assert result.usage.input_tokens == 10
        assert result.usage.output_tokens == 20
        assert result.usage.cache_creation_input_tokens == 5
        assert result.usage.cache_read_input_tokens == 3


@pytest.mark.asyncio
async def test_anthropic_client_tool_use_response():
    with patch("datasight.llm.anthropic.AsyncAnthropic") as mock_cls:
        instance = MagicMock()
        instance.messages.create = AsyncMock(
            return_value=_make_fake_anthropic_response(
                text="running",
                tool_use={"id": "t1", "name": "run_sql", "input": {"sql": "SELECT 1"}},
                stop_reason="tool_use",
            )
        )
        mock_cls.return_value = instance

        client = AnthropicLLMClient(api_key="sk-test", base_url="https://example.com")
        result = await client.create_message(
            model="claude", system="sys", messages=[], tools=[], max_tokens=100
        )
        assert result.stop_reason == "tool_use"
        assert len(result.content) == 2
        tool_block = result.content[1]
        assert isinstance(tool_block, ToolUseBlock)
        assert tool_block.name == "run_sql"


@pytest.mark.asyncio
async def test_anthropic_client_connection_error_retries_then_fails():
    with (
        patch("datasight.llm.anthropic.AsyncAnthropic") as mock_cls,
        patch("datasight.llm.asyncio.sleep", new=AsyncMock()),
    ):
        instance = MagicMock()
        instance.messages.create = AsyncMock(
            side_effect=anthropic.APIConnectionError(request=MagicMock())
        )
        mock_cls.return_value = instance

        client = AnthropicLLMClient(api_key="sk-test")
        with pytest.raises(LLMConnectionError):
            await client.create_message(
                model="claude", system="sys", messages=[], tools=[], max_tokens=100
            )
        # Retries are actually exercised, not skipped.
        assert instance.messages.create.await_count == DEFAULT_MAX_ATTEMPTS


@pytest.mark.asyncio
async def test_anthropic_client_connection_error_recovers():
    with (
        patch("datasight.llm.anthropic.AsyncAnthropic") as mock_cls,
        patch("datasight.llm.asyncio.sleep", new=AsyncMock()),
    ):
        instance = MagicMock()
        instance.messages.create = AsyncMock(
            side_effect=[
                anthropic.APIConnectionError(request=MagicMock()),
                _make_fake_anthropic_response(text="recovered"),
            ]
        )
        mock_cls.return_value = instance

        client = AnthropicLLMClient(api_key="sk-test")
        result = await client.create_message(
            model="claude", system="sys", messages=[], tools=[], max_tokens=100
        )
        assert isinstance(result.content[0], TextBlock)
        assert result.content[0].text == "recovered"


@pytest.mark.asyncio
async def test_anthropic_client_rate_limit_retries_then_fails():
    with (
        patch("datasight.llm.anthropic.AsyncAnthropic") as mock_cls,
        patch("datasight.llm.asyncio.sleep", new=AsyncMock()),
    ):
        instance = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 429
        mock_resp.headers = {}
        instance.messages.create = AsyncMock(
            side_effect=anthropic.RateLimitError("rate limited", response=mock_resp, body=None)
        )
        mock_cls.return_value = instance

        client = AnthropicLLMClient(api_key="sk-test")
        with pytest.raises(LLMResponseError, match="rate limit"):
            await client.create_message(
                model="claude", system="sys", messages=[], tools=[], max_tokens=100
            )
        # Rate-limit retries should also exhaust the attempt budget.
        assert instance.messages.create.await_count == DEFAULT_MAX_ATTEMPTS


@pytest.mark.asyncio
async def test_anthropic_client_api_status_error():
    with patch("datasight.llm.anthropic.AsyncAnthropic") as mock_cls:
        instance = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.headers = {}
        instance.messages.create = AsyncMock(
            side_effect=anthropic.APIStatusError("server error", response=mock_resp, body=None)
        )
        mock_cls.return_value = instance

        client = AnthropicLLMClient(api_key="sk-test")
        with pytest.raises(LLMResponseError, match="Anthropic API error"):
            await client.create_message(
                model="claude", system="sys", messages=[], tools=[], max_tokens=100
            )


def test_create_llm_client_missing_api_key_raises_configuration_error():
    """Factory short-circuits with a clear error when a required key is missing."""
    from datasight.exceptions import ConfigurationError

    for provider, env_hint in [
        ("anthropic", "ANTHROPIC_API_KEY"),
        ("openai", "OPENAI_API_KEY"),
        ("github", "GITHUB_TOKEN"),
    ]:
        with pytest.raises(ConfigurationError, match=env_hint):
            create_llm_client(provider, api_key="")


def test_anthropic_client_init_connection_error():
    with patch("datasight.llm.anthropic.AsyncAnthropic") as mock_cls:
        mock_cls.side_effect = anthropic.APIConnectionError(request=MagicMock())
        with pytest.raises(LLMConnectionError, match="Failed to initialize Anthropic"):
            AnthropicLLMClient(api_key="sk-test")


# ---------------------------------------------------------------------------
# OpenAI-compatible / Ollama / GitHub clients
# ---------------------------------------------------------------------------


def _make_fake_openai_response(
    *,
    text: str | None = None,
    tool_calls: list[dict] | None = None,
    finish_reason: str = "stop",
):
    """Build a fake OpenAI chat completion response."""
    message = MagicMock()
    message.content = text
    if tool_calls:
        tc_mocks = []
        for tc in tool_calls:
            m = MagicMock()
            m.id = tc["id"]
            m.function.name = tc["name"]
            m.function.arguments = tc["arguments"]
            tc_mocks.append(m)
        message.tool_calls = tc_mocks
    else:
        message.tool_calls = None

    choice = MagicMock()
    choice.message = message
    choice.finish_reason = finish_reason
    resp = MagicMock()
    resp.choices = [choice]
    resp.usage = MagicMock(prompt_tokens=7, completion_tokens=13)
    return resp


def _fake_openai_module() -> MagicMock:
    """Build a mock ``openai`` module with real typed-exception classes.

    The LLM client's retry loop catches typed ``openai.*`` exceptions. A bare
    ``MagicMock`` returns ``MagicMock`` from attribute access, which is not a
    valid exception class, so tests that want to exercise the retry path
    must seed the fake with real exception classes.
    """
    import openai as real_openai

    mod = MagicMock()
    mod.APIConnectionError = real_openai.APIConnectionError
    mod.APITimeoutError = real_openai.APITimeoutError
    mod.RateLimitError = real_openai.RateLimitError
    mod.InternalServerError = real_openai.InternalServerError
    mod.APIStatusError = real_openai.APIStatusError
    return mod


def _openai_connection_error(message: str = "connection refused"):
    import openai as real_openai

    return real_openai.APIConnectionError(message=message, request=MagicMock())


def _openai_status_error(message: str = "invalid api key"):
    import openai as real_openai

    return real_openai.APIStatusError(message, response=MagicMock(), body=None)


@pytest.mark.asyncio
async def test_ollama_client_text_response():
    fake_module = MagicMock()
    instance = MagicMock()
    instance.chat.completions.create = AsyncMock(
        return_value=_make_fake_openai_response(text="ollama says hi")
    )
    fake_module.AsyncOpenAI = MagicMock(return_value=instance)

    with patch.dict("sys.modules", {"openai": fake_module}):
        client = OllamaLLMClient()
        result = await client.create_message(
            model="llama", system="sys", messages=[], tools=[], max_tokens=50
        )
        assert isinstance(result.content[0], TextBlock)
        assert result.content[0].text == "ollama says hi"
        assert result.stop_reason == "end_turn"
        assert result.usage.input_tokens == 7
        assert result.usage.output_tokens == 13


@pytest.mark.asyncio
async def test_openai_compat_tool_calls():
    fake_module = MagicMock()
    instance = MagicMock()
    instance.chat.completions.create = AsyncMock(
        return_value=_make_fake_openai_response(
            text=None,
            tool_calls=[
                {"id": "c1", "name": "run_sql", "arguments": json.dumps({"sql": "SELECT 1"})}
            ],
        )
    )
    fake_module.AsyncOpenAI = MagicMock(return_value=instance)

    with patch.dict("sys.modules", {"openai": fake_module}):
        client = OllamaLLMClient()
        result = await client.create_message(
            model="m", system="sys", messages=[], tools=[{"name": "run_sql"}], max_tokens=50
        )
        assert result.stop_reason == "tool_use"
        assert isinstance(result.content[0], ToolUseBlock)
        assert result.content[0].input == {"sql": "SELECT 1"}


@pytest.mark.asyncio
async def test_openai_compat_bad_tool_arguments_json():
    fake_module = MagicMock()
    instance = MagicMock()
    instance.chat.completions.create = AsyncMock(
        return_value=_make_fake_openai_response(
            text=None,
            tool_calls=[{"id": "c1", "name": "run_sql", "arguments": "not-json{"}],
        )
    )
    fake_module.AsyncOpenAI = MagicMock(return_value=instance)

    with patch.dict("sys.modules", {"openai": fake_module}):
        client = OllamaLLMClient()
        result = await client.create_message(
            model="m", system="sys", messages=[], tools=[], max_tokens=50
        )
        # Falls back to empty input
        assert isinstance(result.content[0], ToolUseBlock)
        assert result.content[0].input == {}


@pytest.mark.asyncio
async def test_openai_compat_transient_error_retries_then_fails():
    fake_module = _fake_openai_module()
    instance = MagicMock()
    instance.chat.completions.create = AsyncMock(side_effect=_openai_connection_error())
    fake_module.AsyncOpenAI = MagicMock(return_value=instance)

    with (
        patch.dict("sys.modules", {"openai": fake_module}),
        patch("datasight.llm.asyncio.sleep", new=AsyncMock()),
    ):
        client = OllamaLLMClient()
        with pytest.raises(LLMConnectionError):
            await client.create_message(
                model="m", system="sys", messages=[], tools=[], max_tokens=50
            )
        # Entire attempt budget should be consumed before giving up.
        assert instance.chat.completions.create.await_count == DEFAULT_MAX_ATTEMPTS


@pytest.mark.asyncio
async def test_openai_compat_non_transient_error_fails_immediately():
    fake_module = _fake_openai_module()
    instance = MagicMock()
    instance.chat.completions.create = AsyncMock(side_effect=_openai_status_error())
    fake_module.AsyncOpenAI = MagicMock(return_value=instance)

    with patch.dict("sys.modules", {"openai": fake_module}):
        client = OllamaLLMClient()
        with pytest.raises(LLMResponseError, match="invalid api key"):
            await client.create_message(
                model="m", system="sys", messages=[], tools=[], max_tokens=50
            )
        assert instance.chat.completions.create.await_count == 1


@pytest.mark.asyncio
async def test_openai_compat_transient_then_success():
    fake_module = _fake_openai_module()
    instance = MagicMock()
    instance.chat.completions.create = AsyncMock(
        side_effect=[
            _openai_connection_error("503 service unavailable"),
            _make_fake_openai_response(text="recovered"),
        ]
    )
    fake_module.AsyncOpenAI = MagicMock(return_value=instance)

    with (
        patch.dict("sys.modules", {"openai": fake_module}),
        patch("datasight.llm.asyncio.sleep", new=AsyncMock()),
    ):
        client = OllamaLLMClient()
        result = await client.create_message(
            model="m", system="sys", messages=[], tools=[], max_tokens=50
        )
        assert isinstance(result.content[0], TextBlock)
        assert result.content[0].text == "recovered"
        # Retry counter should reflect the one intermediate failure.
        assert result.call_stats.retries_performed == 1


def test_openai_compat_missing_openai_package():
    # Simulate openai package not installed
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "openai":
            raise ImportError("no openai")
        return real_import(name, *args, **kwargs)

    with patch.object(builtins, "__import__", side_effect=fake_import):
        with pytest.raises(ImportError, match="openai"):
            OllamaLLMClient()


def test_openai_compat_init_failure():
    import openai as real_openai

    fake_module = _fake_openai_module()
    fake_module.OpenAIError = real_openai.OpenAIError
    fake_module.AsyncOpenAI = MagicMock(side_effect=real_openai.OpenAIError("init fail"))
    with patch.dict("sys.modules", {"openai": fake_module}):
        with pytest.raises(LLMConnectionError, match="Failed to initialize OpenAI"):
            OllamaLLMClient()


@pytest.mark.asyncio
async def test_github_models_client():
    fake_module = MagicMock()
    instance = MagicMock()
    instance.chat.completions.create = AsyncMock(
        return_value=_make_fake_openai_response(text="gh hi")
    )
    fake_module.AsyncOpenAI = MagicMock(return_value=instance)

    with patch.dict("sys.modules", {"openai": fake_module}):
        client = GitHubModelsLLMClient(api_key="ghp_xxx")
        result = await client.create_message(
            model="gpt-4", system="s", messages=[], tools=[], max_tokens=10
        )
        assert isinstance(result.content[0], TextBlock)
        assert result.content[0].text == "gh hi"
        # Confirm api_key was passed through
        call_kwargs = fake_module.AsyncOpenAI.call_args.kwargs
        assert call_kwargs["api_key"] == "ghp_xxx"


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def test_create_llm_client_ollama():
    fake_module = MagicMock()
    fake_module.AsyncOpenAI = MagicMock(return_value=MagicMock())
    with patch.dict("sys.modules", {"openai": fake_module}):
        client = create_llm_client("ollama")
        assert isinstance(client, OllamaLLMClient)

        client = create_llm_client("ollama", base_url="http://custom:11434/v1")
        assert isinstance(client, OllamaLLMClient)


def test_create_llm_client_github():
    fake_module = MagicMock()
    fake_module.AsyncOpenAI = MagicMock(return_value=MagicMock())
    with patch.dict("sys.modules", {"openai": fake_module}):
        client = create_llm_client("github", api_key="tok")
        assert isinstance(client, GitHubModelsLLMClient)

        client = create_llm_client("github", api_key="tok", base_url="https://custom")
        assert isinstance(client, GitHubModelsLLMClient)


def test_create_llm_client_anthropic():
    with patch("datasight.llm.anthropic.AsyncAnthropic"):
        client = create_llm_client("anthropic", api_key="sk-test")
        assert isinstance(client, AnthropicLLMClient)

        client = create_llm_client("anthropic", api_key="sk-test", base_url="https://proxy")
        assert isinstance(client, AnthropicLLMClient)


def test_create_llm_client_unknown_provider():
    with pytest.raises(ValueError, match="Unknown LLM provider"):
        create_llm_client("nonexistent")


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


def test_usage_defaults():
    u = Usage()
    assert u.input_tokens == 0
    assert u.output_tokens == 0
    assert u.cache_creation_input_tokens == 0
    assert u.cache_read_input_tokens == 0


def test_llm_response_default_usage():
    r = LLMResponse(content=[TextBlock(text="x")], stop_reason="end_turn")
    assert isinstance(r.usage, Usage)
    assert isinstance(r.call_stats, CallStats)
    assert r.call_stats.retries_performed == 0


# ---------------------------------------------------------------------------
# Truncation (max_tokens) handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_anthropic_client_max_tokens_stop_reason():
    """Anthropic stop_reason == 'max_tokens' surfaces as StopReason max_tokens."""
    with patch("datasight.llm.anthropic.AsyncAnthropic") as mock_cls:
        instance = MagicMock()
        instance.messages.create = AsyncMock(
            return_value=_make_fake_anthropic_response(
                text="partial answer", stop_reason="max_tokens"
            )
        )
        mock_cls.return_value = instance

        client = AnthropicLLMClient(api_key="sk-test")
        result = await client.create_message(
            model="claude", system="sys", messages=[], tools=[], max_tokens=100
        )
        assert result.stop_reason == "max_tokens"


@pytest.mark.asyncio
async def test_openai_compat_finish_length_maps_to_max_tokens():
    """OpenAI finish_reason == 'length' surfaces as StopReason max_tokens."""
    fake_module = MagicMock()
    instance = MagicMock()
    instance.chat.completions.create = AsyncMock(
        return_value=_make_fake_openai_response(text="cut off", finish_reason="length")
    )
    fake_module.AsyncOpenAI = MagicMock(return_value=instance)

    with patch.dict("sys.modules", {"openai": fake_module}):
        client = OllamaLLMClient()
        result = await client.create_message(
            model="m", system="sys", messages=[], tools=[], max_tokens=50
        )
        assert result.stop_reason == "max_tokens"


@pytest.mark.asyncio
async def test_openai_compat_length_with_tool_calls_prefers_max_tokens():
    """When both length truncation and tool_calls happen, truncation wins so
    the agent shows the truncation notice instead of executing a possibly
    half-serialized tool call."""
    fake_module = MagicMock()
    instance = MagicMock()
    instance.chat.completions.create = AsyncMock(
        return_value=_make_fake_openai_response(
            text=None,
            tool_calls=[{"id": "c1", "name": "run_sql", "arguments": json.dumps({"sql": "X"})}],
            finish_reason="length",
        )
    )
    fake_module.AsyncOpenAI = MagicMock(return_value=instance)

    with patch.dict("sys.modules", {"openai": fake_module}):
        client = OllamaLLMClient()
        result = await client.create_message(
            model="m",
            system="sys",
            messages=[{"role": "user", "content": "hi"}],
            tools=[{"name": "run_sql"}],
            max_tokens=50,
        )
        assert result.stop_reason == "max_tokens"


# ---------------------------------------------------------------------------
# Anthropic prompt-caching
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_anthropic_client_sets_cache_control_on_last_tool():
    """The final tool entry carries an ephemeral cache breakpoint, which
    covers all tools + system prompt. Tool order is load-bearing for cache
    stability; this test guards the invariant."""
    with patch("datasight.llm.anthropic.AsyncAnthropic") as mock_cls:
        instance = MagicMock()
        instance.messages.create = AsyncMock(return_value=_make_fake_anthropic_response(text="ok"))
        mock_cls.return_value = instance

        tools = [
            {"name": "run_sql", "description": "run", "input_schema": {"type": "object"}},
            {"name": "make_chart", "description": "chart", "input_schema": {"type": "object"}},
        ]

        client = AnthropicLLMClient(api_key="sk-test")
        await client.create_message(
            model="claude", system="sys", messages=[], tools=tools, max_tokens=100
        )

        await_args = instance.messages.create.await_args
        assert await_args is not None
        sent_tools = await_args.kwargs["tools"]
        assert sent_tools[-1]["name"] == "make_chart"
        assert sent_tools[-1]["cache_control"] == {"type": "ephemeral"}
        # Earlier tools do NOT carry cache_control (single breakpoint covers all).
        assert "cache_control" not in sent_tools[0]
        # Original caller's tool list must not have been mutated.
        assert "cache_control" not in tools[-1]


@pytest.mark.asyncio
async def test_anthropic_client_retries_on_5xx():
    """InternalServerError should be retried (not raised as an
    LLMResponseError on the first failure)."""
    import anthropic as real_anthropic

    mock_resp = MagicMock()
    mock_resp.status_code = 503
    mock_resp.headers = {}
    server_err = real_anthropic.InternalServerError(
        "upstream overloaded", response=mock_resp, body=None
    )

    with (
        patch("datasight.llm.anthropic.AsyncAnthropic") as mock_cls,
        patch("datasight.llm.asyncio.sleep", new=AsyncMock()),
    ):
        instance = MagicMock()
        instance.messages.create = AsyncMock(
            side_effect=[server_err, _make_fake_anthropic_response(text="recovered")]
        )
        mock_cls.return_value = instance

        client = AnthropicLLMClient(api_key="sk-test")
        result = await client.create_message(
            model="claude", system="sys", messages=[], tools=[], max_tokens=100
        )
        assert isinstance(result.content[0], TextBlock)
        assert result.content[0].text == "recovered"
        assert result.call_stats.retries_performed == 1


# ---------------------------------------------------------------------------
# User-turn conversion edge cases
# ---------------------------------------------------------------------------


def test_convert_messages_to_openai_user_mixed_text_and_tool_result():
    """User turns that carry both text and tool_result blocks should
    preserve the text as a user message alongside the tool messages."""
    msgs = [
        {
            "role": "user",
            "content": [
                {"type": "tool_result", "tool_use_id": "t1", "content": "42"},
                {"type": "text", "text": "follow-up question"},
            ],
        }
    ]
    result = _convert_messages_to_openai("sys", msgs)
    assert result[0] == {"role": "system", "content": "sys"}
    # Tool message emitted in input order.
    assert result[1] == {"role": "tool", "tool_call_id": "t1", "content": "42"}
    # Text preserved as a user message.
    assert result[2] == {"role": "user", "content": "follow-up question"}
