"""
Resolve ``[include:Title](url)`` directives in ``schema_description.md``.

Project developers can sprinkle include directives in the schema
description to pull external documentation (API references, fuel-code
glossaries, etc.) into the LLM system prompt without copying static text
into the markdown file. Each URL is fetched once at project-load time,
HTML is stripped to plain text, the body is size-capped, and the directive
is replaced in place. On fetch failure the original ``[include:...](url)``
markdown link is preserved so the LLM still sees the pointer and the
project developer can spot the bad URL.
"""

from __future__ import annotations

import asyncio
import os
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Awaitable, Callable, overload

from loguru import logger

# Default per-URL size cap. Picked so a handful of includes stays comfortable
# for small-context local models (e.g. qwen2.5:7b at ~32K tokens) without
# forcing hosted models to leave most of their window unused. Users on
# long-context backends who want the whole fetched page can override via
# the ``SCHEMA_INCLUDE_MAX_BYTES`` env var — see ``_resolve_max_bytes``.
DEFAULT_MAX_BYTES: int = 20_000

# Env var consulted when the caller doesn't pass ``max_bytes`` explicitly.
# Registered in ``datasight.settings._PROJECT_ENV_VARS`` so project-switching
# restores it cleanly.
_MAX_BYTES_ENV_VAR: str = "SCHEMA_INCLUDE_MAX_BYTES"

# Per-fetch timeout. Project load is not latency-critical, so we can afford
# to wait longer than the LLM call timeout.
DEFAULT_FETCH_TIMEOUT: float = 10.0

_ALLOWED_CONTENT_PREFIXES: tuple[str, ...] = (
    "text/",
    "application/json",
    "application/xhtml",
)

# ``[include:Title](url)`` — title is everything after the colon up to the
# closing bracket (single line only), url is a plain http(s) URL.
_INCLUDE_RE = re.compile(
    r"\[include:([^\]\n]+)\]\((https?://[^\s)]+)\)",
)


Fetcher = Callable[[str], Awaitable[str]]


@dataclass(frozen=True)
class _Match:
    title: str
    url: str
    start: int
    end: int


class _TextExtractor(HTMLParser):
    """Collect text content from HTML, skipping script/style tags."""

    _SKIP_TAGS = frozenset({"script", "style", "noscript", "template"})

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._parts: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in self._SKIP_TAGS:
            self._skip_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag in self._SKIP_TAGS and self._skip_depth > 0:
            self._skip_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._skip_depth == 0:
            self._parts.append(data)

    def get_text(self) -> str:
        raw = "".join(self._parts)
        lines = [re.sub(r"[ \t]+", " ", line).strip() for line in raw.splitlines()]
        out: list[str] = []
        blank = False
        for line in lines:
            if not line:
                if not blank and out:
                    out.append("")
                blank = True
            else:
                out.append(line)
                blank = False
        return "\n".join(out).strip()


def _html_to_text(html: str) -> str:
    """Strip HTML tags and collapse whitespace to a readable plain-text block."""
    parser = _TextExtractor()
    parser.feed(html)
    return parser.get_text()


def _truncate(text: str, max_bytes: int) -> str:
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text
    clipped = encoded[:max_bytes].decode("utf-8", errors="ignore").rstrip()
    return clipped + "\n\n…[content truncated]"


async def _default_fetch(url: str, *, timeout: float, max_bytes: int) -> str:
    """Fetch ``url`` and return plain text. Raises on failure."""
    import httpx

    async with httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=True,
        headers={"User-Agent": "datasight schema-link resolver"},
    ) as client:
        response = await client.get(url)
        response.raise_for_status()
        content_type = response.headers.get("content-type", "").lower().split(";", 1)[0].strip()
        if not any(content_type.startswith(p) for p in _ALLOWED_CONTENT_PREFIXES):
            raise ValueError(f"unsupported content-type {content_type!r}")
        body = response.text
    if content_type.startswith("text/html") or content_type.startswith("application/xhtml"):
        body = _html_to_text(body)
    return _truncate(body, max_bytes)


def _format_included(title: str, url: str, body: str) -> str:
    return f"### Included reference: {title.strip()}\n*Source: <{url}>*\n\n{body}\n"


def _resolve_max_bytes() -> int:
    """Resolve the effective per-URL size cap from the environment.

    Reads ``SCHEMA_INCLUDE_MAX_BYTES`` on each call so tests and project
    switches see the current value. ``0`` is a sentinel meaning "skip
    include-link resolution entirely" — useful when the fetched content
    is pushing the prompt past a small-context model's window. Invalid
    or negative values fall back to ``DEFAULT_MAX_BYTES`` with a warning.
    """
    raw = os.environ.get(_MAX_BYTES_ENV_VAR, "").strip()
    if not raw:
        return DEFAULT_MAX_BYTES
    try:
        parsed = int(raw)
    except ValueError:
        logger.warning(
            f"{_MAX_BYTES_ENV_VAR}={raw!r} is not an integer; falling back to {DEFAULT_MAX_BYTES}"
        )
        return DEFAULT_MAX_BYTES
    if parsed < 0:
        logger.warning(
            f"{_MAX_BYTES_ENV_VAR}={parsed} must be >= 0; falling back to {DEFAULT_MAX_BYTES}"
        )
        return DEFAULT_MAX_BYTES
    return parsed


@overload
async def resolve_schema_description_links(
    markdown: str,
    *,
    fetcher: Fetcher | None = ...,
    max_bytes: int | None = ...,
    timeout: float = ...,
) -> str: ...


@overload
async def resolve_schema_description_links(
    markdown: None,
    *,
    fetcher: Fetcher | None = ...,
    max_bytes: int | None = ...,
    timeout: float = ...,
) -> None: ...


async def resolve_schema_description_links(
    markdown: str | None,
    *,
    fetcher: Fetcher | None = None,
    max_bytes: int | None = None,
    timeout: float = DEFAULT_FETCH_TIMEOUT,
) -> str | None:
    """Expand ``[include:Title](url)`` directives.

    ``None`` and link-free markdown pass through unchanged so callers can
    thread this after ``load_schema_description`` without extra guards.

    Each unique URL is fetched once. On failure, the original directive is
    preserved in the output.

    A custom ``fetcher`` is responsible for its own HTML stripping and
    size-capping; ``max_bytes`` / ``timeout`` only affect the default
    fetcher. When ``max_bytes`` is ``None`` (the default), the cap is read
    from ``SCHEMA_INCLUDE_MAX_BYTES`` and falls back to
    ``DEFAULT_MAX_BYTES``. ``max_bytes == 0`` disables resolution — the
    markdown is returned unchanged, leaving ``[include:...](url)``
    directives visible to the LLM as plain markdown links.
    """
    if max_bytes is None:
        max_bytes = _resolve_max_bytes()
    if markdown is None:
        return None
    if max_bytes == 0:
        return markdown

    matches = [
        _Match(title=m.group(1), url=m.group(2), start=m.start(), end=m.end())
        for m in _INCLUDE_RE.finditer(markdown)
    ]
    if not matches:
        return markdown

    unique_urls = list({m.url for m in matches})

    async def _run_fetch(url: str) -> tuple[str, str | None, Exception | None]:
        try:
            if fetcher is None:
                body = await _default_fetch(url, timeout=timeout, max_bytes=max_bytes)
            else:
                body = await fetcher(url)
            return url, body, None
        except Exception as e:  # noqa: BLE001 — summarized to warning below
            return url, None, e

    results = await asyncio.gather(*(_run_fetch(u) for u in unique_urls))
    url_to_body: dict[str, str | None] = {}
    for url, body, err in results:
        if err is not None:
            logger.warning(f"schema include fetch failed for {url}: {err}")
            url_to_body[url] = None
        else:
            url_to_body[url] = body

    out: list[str] = []
    cursor = 0
    for m in matches:
        out.append(markdown[cursor : m.start])
        body = url_to_body.get(m.url)
        if body is None:
            out.append(markdown[m.start : m.end])
        else:
            out.append(_format_included(m.title, m.url, body))
        cursor = m.end
    out.append(markdown[cursor:])
    return "".join(out)
