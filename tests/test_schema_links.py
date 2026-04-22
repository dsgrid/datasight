"""Tests for the schema_description.md include-directive resolver."""

from __future__ import annotations

import socket
from collections.abc import Awaitable, Callable

import pytest

from datasight.schema_links import (
    DEFAULT_MAX_BYTES,
    _default_fetch,
    _html_to_text,
    _is_disallowed_ip,
    _resolve_max_bytes,
    _truncate,
    _validate_url_safety,
    resolve_schema_description_links,
)


def _stub_fetcher(mapping: dict[str, str]) -> Callable[[str], Awaitable[str]]:
    async def _fetch(url: str) -> str:
        if url not in mapping:
            raise RuntimeError(f"no stub for {url}")
        return mapping[url]

    return _fetch


async def test_none_passthrough():
    assert await resolve_schema_description_links(None, fetcher=_stub_fetcher({})) is None


async def test_no_directives_noop():
    md = "# Schema\n\nA plain description with no include directives.\n"
    result = await resolve_schema_description_links(md, fetcher=_stub_fetcher({}))
    assert result == md


async def test_single_directive_is_replaced():
    md = "See [include:Fuel codes](https://example.com/fuel) for details."
    fetch = _stub_fetcher({"https://example.com/fuel": "NG = natural gas\nSUN = solar"})
    result = await resolve_schema_description_links(md, fetcher=fetch)
    assert "[include:Fuel codes]" not in result
    assert "Included reference: Fuel codes" in result
    assert "NG = natural gas" in result
    assert "https://example.com/fuel" in result


async def test_multiple_directives_both_expand():
    md = "A [include:One](https://a.example) and [include:Two](https://b.example)."
    fetch = _stub_fetcher({"https://a.example": "content A", "https://b.example": "content B"})
    result = await resolve_schema_description_links(md, fetcher=fetch)
    assert "content A" in result
    assert "content B" in result
    assert result.count("Included reference") == 2


async def test_duplicate_url_fetched_once():
    md = "[include:X](https://same.example) then [include:Y](https://same.example)"
    calls = {"count": 0}

    async def _fetch(url: str) -> str:
        calls["count"] += 1
        return "shared body"

    result = await resolve_schema_description_links(md, fetcher=_fetch)
    assert calls["count"] == 1
    assert result.count("Included reference") == 2


async def test_fetch_failure_preserves_original_directive():
    md = "Ref: [include:Broken](https://bad.example) end."

    async def _fetch(url: str) -> str:
        raise RuntimeError("boom")

    result = await resolve_schema_description_links(md, fetcher=_fetch)
    assert "[include:Broken](https://bad.example)" in result
    assert "Included reference" not in result


async def test_partial_failure_mixes_expanded_and_preserved():
    md = "Good [include:Good](https://ok.example) and bad [include:Bad](https://fail.example)."

    async def _fetch(url: str) -> str:
        if "fail" in url:
            raise RuntimeError("network down")
        return "good body"

    result = await resolve_schema_description_links(md, fetcher=_fetch)
    assert "good body" in result
    assert "[include:Bad](https://fail.example)" in result


def test_html_to_text_strips_tags_and_scripts():
    html = (
        "<html><body><h1>Title</h1><p>hello <b>world</b></p>"
        "<script>alert('x')</script><style>.a{color:red}</style></body></html>"
    )
    text = _html_to_text(html)
    assert "Title" in text
    assert "hello" in text
    assert "world" in text
    assert "alert" not in text
    assert "color:red" not in text


def test_html_to_text_collapses_whitespace():
    html = "<p>  multiple    spaces  </p>\n\n\n<p>second</p>"
    text = _html_to_text(html)
    assert "multiple spaces" in text
    # At most one blank line between blocks.
    assert "\n\n\n" not in text


def test_truncate_preserves_small_text():
    assert _truncate("short text", 1000) == "short text"


def test_truncate_cuts_oversized_text():
    big = "a" * 5000
    out = _truncate(big, 1000)
    assert "truncated" in out
    assert len(out) < len(big)


def test_resolve_max_bytes_unset_uses_default(monkeypatch):
    monkeypatch.delenv("SCHEMA_INCLUDE_MAX_BYTES", raising=False)
    assert _resolve_max_bytes() == DEFAULT_MAX_BYTES


def test_resolve_max_bytes_reads_env(monkeypatch):
    monkeypatch.setenv("SCHEMA_INCLUDE_MAX_BYTES", "12345")
    assert _resolve_max_bytes() == 12345


def test_resolve_max_bytes_rejects_invalid(monkeypatch):
    monkeypatch.setenv("SCHEMA_INCLUDE_MAX_BYTES", "not-a-number")
    assert _resolve_max_bytes() == DEFAULT_MAX_BYTES


def test_resolve_max_bytes_zero_is_disabled_sentinel(monkeypatch):
    monkeypatch.setenv("SCHEMA_INCLUDE_MAX_BYTES", "0")
    assert _resolve_max_bytes() == 0


def test_resolve_max_bytes_rejects_negative(monkeypatch):
    monkeypatch.setenv("SCHEMA_INCLUDE_MAX_BYTES", "-5")
    assert _resolve_max_bytes() == DEFAULT_MAX_BYTES


async def test_max_bytes_zero_skips_resolution():
    md = "See [include:Foo](https://example.com/foo) for details."

    async def _fetch(url: str) -> str:
        raise AssertionError("fetcher should not run when max_bytes=0")

    result = await resolve_schema_description_links(md, fetcher=_fetch, max_bytes=0)
    assert result == md


# ---------------------------------------------------------------------------
# SSRF guard
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "address",
    [
        "127.0.0.1",
        "10.0.0.1",
        "192.168.1.1",
        "172.16.0.5",
        "169.254.169.254",  # AWS/GCP metadata service
        "::1",
        "fe80::1",
        "0.0.0.0",
    ],
)
def test_is_disallowed_ip_blocks_internal_ranges(address: str):
    assert _is_disallowed_ip(address) is True


@pytest.mark.parametrize(
    "address",
    ["1.1.1.1", "8.8.8.8", "2001:4860:4860::8888"],
)
def test_is_disallowed_ip_allows_public_addresses(address: str):
    assert _is_disallowed_ip(address) is False


async def test_validate_url_safety_rejects_localhost(monkeypatch):
    monkeypatch.delenv("SCHEMA_INCLUDE_ALLOW_PRIVATE_HOSTS", raising=False)
    with pytest.raises(ValueError, match="blocked"):
        await _validate_url_safety("http://localhost:8080/internal")


@pytest.mark.parametrize(
    "url",
    [
        "http://foo.local/docs",
        "http://bar.internal/schema",
        "http://service.localdomain/x",
    ],
)
async def test_validate_url_safety_rejects_blocked_suffixes(monkeypatch, url):
    monkeypatch.delenv("SCHEMA_INCLUDE_ALLOW_PRIVATE_HOSTS", raising=False)
    with pytest.raises(ValueError, match="blocked"):
        await _validate_url_safety(url)


async def test_validate_url_safety_rejects_ip_literal_in_private_range(monkeypatch):
    monkeypatch.delenv("SCHEMA_INCLUDE_ALLOW_PRIVATE_HOSTS", raising=False)
    with pytest.raises(ValueError, match="disallowed"):
        await _validate_url_safety("http://10.0.0.1/secret")


async def test_validate_url_safety_rejects_non_http_scheme(monkeypatch):
    monkeypatch.delenv("SCHEMA_INCLUDE_ALLOW_PRIVATE_HOSTS", raising=False)
    with pytest.raises(ValueError, match="scheme"):
        await _validate_url_safety("file:///etc/passwd")


async def test_validate_url_safety_rejects_private_dns_resolution(monkeypatch):
    """Hostname that resolves into a private range is blocked."""
    monkeypatch.delenv("SCHEMA_INCLUDE_ALLOW_PRIVATE_HOSTS", raising=False)

    async def _fake_getaddrinfo(host, port, *args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("10.0.0.5", 0))]

    import asyncio

    monkeypatch.setattr(
        asyncio.get_event_loop_policy().get_event_loop().__class__,
        "getaddrinfo",
        _fake_getaddrinfo,
    )
    with pytest.raises(ValueError, match="disallowed address"):
        await _validate_url_safety("http://rebind.example.com/x")


async def test_validate_url_safety_allows_public_host_with_mocked_dns(monkeypatch):
    monkeypatch.delenv("SCHEMA_INCLUDE_ALLOW_PRIVATE_HOSTS", raising=False)

    async def _fake_getaddrinfo(host, port, *args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("1.1.1.1", 0))]

    import asyncio

    monkeypatch.setattr(
        asyncio.get_event_loop_policy().get_event_loop().__class__,
        "getaddrinfo",
        _fake_getaddrinfo,
    )
    # Should return without raising.
    await _validate_url_safety("https://example.com/docs")


async def test_validate_url_safety_opt_out_skips_all_checks(monkeypatch):
    monkeypatch.setenv("SCHEMA_INCLUDE_ALLOW_PRIVATE_HOSTS", "1")
    # localhost would normally be rejected; opt-out bypasses the guard.
    await _validate_url_safety("http://localhost:8080/docs")


# ---------------------------------------------------------------------------
# Streaming fetch cap
# ---------------------------------------------------------------------------


async def test_default_fetch_caps_oversized_stream(monkeypatch):
    """A body larger than max_bytes * oversample is truncated during streaming."""
    import httpx

    monkeypatch.setenv("SCHEMA_INCLUDE_ALLOW_PRIVATE_HOSTS", "1")  # skip DNS

    # Serve 1 MB of text — much larger than 1 KB max_bytes * 4 oversample = 4 KB.
    big_body = b"A" * 1_000_000

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/plain"},
            content=big_body,
        )

    transport = httpx.MockTransport(_handler)
    real_client_cls = httpx.AsyncClient

    def _factory(*args, **kwargs):
        kwargs["transport"] = transport
        return real_client_cls(*args, **kwargs)

    monkeypatch.setattr("httpx.AsyncClient", _factory)

    result = await _default_fetch("https://example.com/big.txt", timeout=5.0, max_bytes=1_000)
    # Result should be truncated to max_bytes of text plus the truncation marker.
    assert "truncated" in result
    assert len(result.encode("utf-8")) < 2_000  # well under the raw body size


async def test_default_fetch_rejects_redirect(monkeypatch):
    import httpx

    monkeypatch.setenv("SCHEMA_INCLUDE_ALLOW_PRIVATE_HOSTS", "1")

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"location": "https://example.com/final"})

    transport = httpx.MockTransport(_handler)
    real_client_cls = httpx.AsyncClient

    def _factory(*args, **kwargs):
        kwargs["transport"] = transport
        return real_client_cls(*args, **kwargs)

    monkeypatch.setattr("httpx.AsyncClient", _factory)

    with pytest.raises(ValueError, match="redirect"):
        await _default_fetch("https://example.com/start", timeout=5.0, max_bytes=1_000)


async def test_default_fetch_rejects_unsupported_content_type(monkeypatch):
    import httpx

    monkeypatch.setenv("SCHEMA_INCLUDE_ALLOW_PRIVATE_HOSTS", "1")

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "application/pdf"},
            content=b"%PDF-1.4 fake",
        )

    transport = httpx.MockTransport(_handler)
    real_client_cls = httpx.AsyncClient

    def _factory(*args, **kwargs):
        kwargs["transport"] = transport
        return real_client_cls(*args, **kwargs)

    monkeypatch.setattr("httpx.AsyncClient", _factory)

    with pytest.raises(ValueError, match="content-type"):
        await _default_fetch("https://example.com/doc.pdf", timeout=5.0, max_bytes=1_000)
