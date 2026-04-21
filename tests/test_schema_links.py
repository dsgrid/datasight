"""Tests for the schema_description.md include-directive resolver."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from datasight.schema_links import (
    DEFAULT_MAX_BYTES,
    _html_to_text,
    _resolve_max_bytes,
    _truncate,
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
