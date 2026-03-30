"""
LLM client abstraction for datasight.

Provides a common async interface for interacting with different LLM providers
(Anthropic, Ollama/OpenAI-compatible) with tool-use support.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Protocol

import anthropic
from loguru import logger


# ---------------------------------------------------------------------------
# Common types
# ---------------------------------------------------------------------------


@dataclass
class TextBlock:
    text: str


@dataclass
class ToolUseBlock:
    id: str
    name: str
    input: dict[str, Any]


@dataclass
class Usage:
    input_tokens: int = 0
    output_tokens: int = 0


@dataclass
class LLMResponse:
    content: list[TextBlock | ToolUseBlock]
    stop_reason: str  # "end_turn" or "tool_use"
    usage: Usage = field(default_factory=Usage)


ContentBlock = TextBlock | ToolUseBlock


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
    ) -> LLMResponse: ...


# ---------------------------------------------------------------------------
# Anthropic implementation
# ---------------------------------------------------------------------------


class AnthropicLLMClient:
    """LLM client backed by the Anthropic API."""

    def __init__(self, api_key: str):
        self._client = anthropic.AsyncAnthropic(api_key=api_key)

    async def create_message(
        self,
        *,
        model: str,
        system: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        max_tokens: int,
    ) -> LLMResponse:
        response = await self._client.messages.create(
            model=model,
            max_tokens=max_tokens,
            system=system,
            tools=tools,
            messages=messages,
        )

        content: list[ContentBlock] = []
        for block in response.content:
            if isinstance(block, anthropic.types.TextBlock):
                content.append(TextBlock(text=block.text))
            elif isinstance(block, anthropic.types.ToolUseBlock):
                content.append(ToolUseBlock(
                    id=block.id, name=block.name, input=block.input,
                ))

        stop = "tool_use" if response.stop_reason == "tool_use" else "end_turn"
        usage = Usage(
            input_tokens=response.usage.input_tokens,
            output_tokens=response.usage.output_tokens,
        )
        return LLMResponse(content=content, stop_reason=stop, usage=usage)


# ---------------------------------------------------------------------------
# Ollama (OpenAI-compatible) implementation
# ---------------------------------------------------------------------------


def _convert_tools_to_openai(tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert Anthropic-style tool definitions to OpenAI function-calling format."""
    result = []
    for tool in tools:
        result.append({
            "type": "function",
            "function": {
                "name": tool["name"],
                "description": tool.get("description", ""),
                "parameters": tool.get("input_schema", {}),
            },
        })
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
                    out.append({
                        "role": "tool",
                        "tool_call_id": item["tool_use_id"],
                        "content": item.get("content", ""),
                    })

        elif role == "assistant" and isinstance(content, str):
            out.append({"role": "assistant", "content": content})

        elif role == "assistant" and isinstance(content, list):
            # Assistant message with potential tool calls
            text_parts = []
            tool_calls = []
            for block in content:
                if isinstance(block, dict):
                    if block.get("type") == "text":
                        text_parts.append(block["text"])
                    elif block.get("type") == "tool_use":
                        tool_calls.append({
                            "id": block["id"],
                            "type": "function",
                            "function": {
                                "name": block["name"],
                                "arguments": json.dumps(block["input"]),
                            },
                        })

            assistant_msg: dict[str, Any] = {"role": "assistant"}
            if text_parts:
                assistant_msg["content"] = "\n".join(text_parts)
            else:
                assistant_msg["content"] = None
            if tool_calls:
                assistant_msg["tool_calls"] = tool_calls
            out.append(assistant_msg)

    return out


class OllamaLLMClient:
    """LLM client backed by Ollama's OpenAI-compatible API."""

    def __init__(self, base_url: str = "http://localhost:11434/v1"):
        try:
            from openai import AsyncOpenAI
        except ImportError:
            raise ImportError(
                "The 'openai' package is required for Ollama support. "
                "Install it with: pip install openai"
            )
        self._client = AsyncOpenAI(base_url=base_url, api_key="ollama")

    async def create_message(
        self,
        *,
        model: str,
        system: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        max_tokens: int,
    ) -> LLMResponse:
        openai_messages = _convert_messages_to_openai(system, messages)
        openai_tools = _convert_tools_to_openai(tools)

        response = await self._client.chat.completions.create(
            model=model,
            messages=openai_messages,
            tools=openai_tools if openai_tools else None,
            max_tokens=max_tokens,
        )

        choice = response.choices[0]
        content: list[ContentBlock] = []

        if choice.message.content:
            content.append(TextBlock(text=choice.message.content))

        tool_calls = choice.message.tool_calls or []
        for tc in tool_calls:
            try:
                args = json.loads(tc.function.arguments)
            except (json.JSONDecodeError, TypeError):
                args = {}
            content.append(ToolUseBlock(
                id=tc.id, name=tc.function.name, input=args,
            ))

        stop = "tool_use" if tool_calls else "end_turn"
        usage = Usage(
            input_tokens=response.usage.prompt_tokens if response.usage else 0,
            output_tokens=response.usage.completion_tokens if response.usage else 0,
        )
        return LLMResponse(content=content, stop_reason=stop, usage=usage)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def create_llm_client(
    provider: str,
    api_key: str = "",
    base_url: str | None = None,
) -> LLMClient:
    """Create an LLM client for the given provider."""
    if provider == "ollama":
        url = base_url or "http://localhost:11434/v1"
        logger.info(f"Using Ollama LLM backend: {url}")
        return OllamaLLMClient(base_url=url)
    else:
        logger.info("Using Anthropic LLM backend")
        return AnthropicLLMClient(api_key=api_key)
