"""
Event types for datasight conversations.

Provides type-safe constants for event handling throughout the application.
"""

from enum import StrEnum


class EventType(StrEnum):
    """Types of events in a conversation."""

    USER_MESSAGE = "user_message"
    ASSISTANT_MESSAGE = "assistant_message"
    TOOL_START = "tool_start"
    TOOL_RESULT = "tool_result"
    TOOL_DONE = "tool_done"
    SUGGESTIONS = "suggestions"
    ERROR = "error"
    STREAM_START = "stream_start"
    STREAM_END = "stream_end"
    TEXT_DELTA = "text_delta"
    CONFIRM_SQL = "sql_confirm"
    CONFIRM_RESPONSE = "confirm_response"
    CACHE_HIT = "cache_hit"
    PROVENANCE = "provenance"
