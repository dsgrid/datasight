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
import ipaddress
import os
import re
import socket
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Awaitable, Callable, overload
from urllib.parse import urlparse

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

# Opt-in env var that disables the SSRF guard so internal documentation
# servers (private IPs, ``.internal`` hostnames) become fetchable again.
# Also registered in ``datasight.settings._PROJECT_ENV_VARS``.
_ALLOW_PRIVATE_HOSTS_ENV_VAR: str = "SCHEMA_INCLUDE_ALLOW_PRIVATE_HOSTS"

# Hard ceiling on bytes pulled over the wire per URL, expressed as a
# multiple of ``max_bytes``. HTML strips to ~1/3 of its raw size typically,
# so 4x gives enough raw bytes to produce ``max_bytes`` of text without
# letting a hostile server stream megabytes into memory.
_DOWNLOAD_OVERSAMPLE: int = 4

# Per-fetch timeout. Project load is not latency-critical, so we can afford
# to wait longer than the LLM call timeout.
DEFAULT_FETCH_TIMEOUT: float = 10.0

_ALLOWED_CONTENT_PREFIXES: tuple[str, ...] = (
    "text/",
    "application/json",
    "application/xhtml",
)

# Hostnames that must never be resolved — catches the common footgun cases
# without requiring a DNS lookup.
_BLOCKED_LITERAL_HOSTNAMES: frozenset[str] = frozenset({"localhost", "ip6-localhost"})
_BLOCKED_HOSTNAME_SUFFIXES: tuple[str, ...] = (
    ".local",
    ".localhost",
    ".internal",
    ".localdomain",
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


def _allow_private_hosts() -> bool:
    """Opt-out switch for the SSRF guard. Defaults to False."""
    return os.environ.get(_ALLOW_PRIVATE_HOSTS_ENV_VAR, "").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def _is_disallowed_ip(address: str) -> bool:
    """Return True for loopback / private / link-local / reserved IPs."""
    try:
        ip = ipaddress.ip_address(address)
    except ValueError:
        return False
    return (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_reserved
        or ip.is_multicast
        or ip.is_unspecified
    )


async def _validate_url_safety(url: str) -> None:
    """Raise ``ValueError`` if ``url`` should not be fetched.

    Mitigates SSRF via a malicious ``schema_description.md`` that points at
    internal services (loopback, private IP ranges, link-local, reserved).
    Note: we only validate the supplied URL; redirects are disabled in the
    fetcher so we don't need to re-validate intermediate hops. DNS rebinding
    is NOT defended against here — a full mitigation would require resolving
    ourselves and pinning the connection to the vetted IP. Set
    ``SCHEMA_INCLUDE_ALLOW_PRIVATE_HOSTS=1`` to bypass for internal doc
    servers you trust.
    """
    if _allow_private_hosts():
        return

    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"unsupported url scheme {parsed.scheme!r}")
    hostname = (parsed.hostname or "").lower()
    if not hostname:
        raise ValueError("url has no hostname")
    if hostname in _BLOCKED_LITERAL_HOSTNAMES:
        raise ValueError(f"hostname {hostname!r} is blocked")
    if any(hostname.endswith(suffix) for suffix in _BLOCKED_HOSTNAME_SUFFIXES):
        raise ValueError(f"hostname {hostname!r} is blocked")
    if _is_disallowed_ip(hostname):
        raise ValueError(f"hostname {hostname!r} resolves to a disallowed address")

    # Resolve and reject any A/AAAA that points into a private range.
    loop = asyncio.get_running_loop()
    try:
        infos = await loop.getaddrinfo(hostname, None)
    except socket.gaierror as e:
        raise ValueError(f"failed to resolve {hostname!r}: {e}") from e
    for _family, _type, _proto, _canon, sockaddr in infos:
        addr = sockaddr[0]
        if _is_disallowed_ip(addr):
            raise ValueError(f"{hostname!r} resolves to disallowed address {addr!r}")


async def _default_fetch(url: str, *, timeout: float, max_bytes: int) -> str:
    """Fetch ``url`` and return plain text. Raises on failure.

    The body is streamed in and capped at ``max_bytes * _DOWNLOAD_OVERSAMPLE``
    raw bytes so a hostile server can't blow up project-load memory even
    when the declared ``Content-Length`` lies. HTML pages are then stripped
    to text before a final truncation to ``max_bytes`` of output.
    """
    import httpx

    await _validate_url_safety(url)

    download_cap = max_bytes * _DOWNLOAD_OVERSAMPLE
    # ``follow_redirects=False`` — a redirect target could bypass the SSRF
    # validation above (e.g. a public URL that 302s into 127.0.0.1). We'd
    # rather surface a clear error than silently chase the Location header.
    async with httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=False,
        headers={"User-Agent": "datasight schema-link resolver"},
    ) as client:
        async with client.stream("GET", url) as response:
            if 300 <= response.status_code < 400:
                location = response.headers.get("location", "")
                raise ValueError(
                    f"redirect ({response.status_code}) to {location!r} is not followed; "
                    "use the final URL directly"
                )
            response.raise_for_status()
            content_type = (
                response.headers.get("content-type", "").lower().split(";", 1)[0].strip()
            )
            if not any(content_type.startswith(p) for p in _ALLOWED_CONTENT_PREFIXES):
                raise ValueError(f"unsupported content-type {content_type!r}")

            buffer = bytearray()
            async for chunk in response.aiter_bytes():
                buffer.extend(chunk)
                if len(buffer) >= download_cap:
                    break

    body = bytes(buffer).decode("utf-8", errors="replace")
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
) -> str:
    pass


@overload
async def resolve_schema_description_links(
    markdown: None,
    *,
    fetcher: Fetcher | None = ...,
    max_bytes: int | None = ...,
    timeout: float = ...,
) -> None:
    pass


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
