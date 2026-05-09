"""
Helpers for SQL identifier quoting.

Two related rewrites land here:

1. **Mixed-case identifiers (Postgres-only).** Postgres folds unquoted
   identifiers to lowercase, so a table created as ``"Battery"`` can only be
   referenced as ``"Battery"`` (double-quoted) — a bare ``Battery`` is
   resolved to ``battery`` and errors. LLMs frequently emit the unquoted
   form, triggering a round-trip where the model sees the error and retries
   with quotes. ``quote_mixed_case_identifiers`` preemptively rewrites SQL
   to quote identifiers we know from introspection need it.

2. **Names containing whitespace (any dialect).** A CSV header like
   ``Net Generation MWh`` becomes a column whose name contains spaces.
   Smaller LLMs (Ollama) often emit ``SELECT Net Generation MWh FROM t``
   bare — the dialect parses this as ``Net AS Generation`` and a stray
   ``MWh``, so the query fails with a confusing "column not found" or
   syntax error. ``quote_special_identifiers`` finds bare references whose
   adjacent words match a known column and wraps them in double quotes.
"""

from __future__ import annotations

import re
from typing import Any

from loguru import logger


# Identifier chars that signal we're already inside an identifier and
# should not start/end a match. Includes the double-quote character so we
# don't double-quote an already-quoted name.
_IDENT_CHAR = r"[A-Za-z0-9_\"]"

# Splits SQL into chunks: capturing groups for string literals, quoted
# identifiers, and comments — anything else is "code" we may rewrite.
# Standard SQL escapes: '' inside '...' and "" inside "...".
_LITERAL_OR_COMMENT_RE = re.compile(
    r"('(?:[^']|'')*'"
    r'|"(?:[^"]|"")*"'
    r"|--[^\n]*"
    r"|/\*[\s\S]*?\*/)"
)


def configure_runner_identifier_quoting(runner: Any, schema_info: list[dict[str, Any]]) -> None:
    """Configure identifier-rewriting state on a SqlRunner.

    Walks through any wrapping layers (e.g. ``CachingSqlRunner``) to find
    the underlying runner, then populates whichever rewrite hooks it
    exposes:

    - ``special_identifiers`` (DuckDB, SQLite, Postgres) — names that must
      be double-quoted because they contain whitespace.
    - ``mixed_case_identifiers`` (Postgres only) — full case map for
      Postgres's identifier folding.

    No-op when the runner exposes neither attribute.
    """
    target = runner
    seen: set[int] = set()
    while target is not None and id(target) not in seen:
        seen.add(id(target))
        if hasattr(target, "special_identifiers") or hasattr(target, "mixed_case_identifiers"):
            break
        inner = getattr(target, "_inner", None)
        if inner is target:
            break
        target = inner

    if target is None:
        return

    if hasattr(target, "special_identifiers"):
        special = build_special_identifier_list(schema_info)
        target.special_identifiers = special or None
        if special:
            logger.info(
                f"Identifier quoting enabled for {len(special)} name(s) containing whitespace"
            )

    if hasattr(target, "mixed_case_identifiers"):
        case_map = build_identifier_case_map(schema_info)
        target.mixed_case_identifiers = case_map or None
        if case_map:
            mixed = sum(1 for v in case_map.values() if v != v.lower())
            logger.info(
                f"Postgres identifier normalization enabled "
                f"({len(case_map)} identifiers, {mixed} mixed-case)"
            )


def build_identifier_case_map(schema_info: list[dict[str, Any]]) -> dict[str, str]:
    """Return ``{lower_name: original_name}`` for **every** table and column
    in ``schema_info``.

    We include lowercase-only names (where original == lower) as well as
    mixed-case ones. The reason: the LLM sometimes quotes an all-lowercase
    column with the wrong casing (e.g. ``"Cost"`` when the real column is
    ``cost``). Knowing the true casing lets us normalize the quoted form
    back down; for mixed-case names, we upgrade bare references to quoted.
    """
    case_map: dict[str, str] = {}
    for table in schema_info:
        tname = table.get("name")
        if isinstance(tname, str):
            case_map.setdefault(tname.lower(), tname)
        for col in table.get("columns", []) or []:
            cname = col.get("name") if isinstance(col, dict) else None
            if isinstance(cname, str):
                case_map.setdefault(cname.lower(), cname)
    return case_map


def build_special_identifier_list(schema_info: list[dict[str, Any]]) -> list[str]:
    """Return identifiers from ``schema_info`` that need double-quoting.

    Currently scoped to names containing whitespace — those are the case
    the rewriter can fix safely. Names with other special characters (e.g.
    ``co-2`` or ``col.sub``) are also unquotable bare, but the LLM-emitted
    form is ambiguous to parse (``co-2`` parses as subtraction) so we
    cannot rewrite them without false positives. They're left to the
    LLM's retry loop.

    Order: longest first (by word count, then char length) so multi-word
    names match before any prefix that's also a column.
    """
    names: set[str] = set()
    for table in schema_info:
        tname = table.get("name")
        if isinstance(tname, str) and _has_whitespace(tname):
            names.add(tname)
        for col in table.get("columns", []) or []:
            cname = col.get("name") if isinstance(col, dict) else None
            if isinstance(cname, str) and _has_whitespace(cname):
                names.add(cname)
    return sorted(names, key=lambda n: (-len(n.split()), -len(n), n))


def _has_whitespace(name: str) -> bool:
    return any(c.isspace() for c in name)


def quote_special_identifiers(sql: str, special_names: list[str]) -> str:
    """Wrap bare references to ``special_names`` in double quotes.

    Each name in ``special_names`` is expected to contain whitespace (e.g.
    ``"Net Generation MWh"``). For each, find sequences of words in the
    SQL that match the name's words separated by whitespace, and replace
    with the canonical double-quoted form.

    String literals, already-quoted identifiers, and comments are left
    alone. Matching is case-insensitive; the replacement uses the schema's
    original casing.
    """
    if not special_names or not sql.strip():
        return sql

    patterns = [(_build_special_pattern(name), f'"{name}"') for name in special_names]

    parts = _LITERAL_OR_COMMENT_RE.split(sql)
    # re.split with one capturing group: even indices are code, odd are
    # literal/quoted/comment chunks to leave untouched.
    for i in range(0, len(parts), 2):
        code = parts[i]
        for pattern, replacement in patterns:
            code = pattern.sub(replacement, code)
        parts[i] = code

    return "".join(parts)


def _build_special_pattern(name: str) -> re.Pattern[str]:
    """Pattern that matches the words of ``name`` separated by whitespace,
    not preceded or followed by another identifier character."""
    words = name.split()
    body = r"\s+".join(re.escape(w) for w in words)
    return re.compile(
        rf"(?<!{_IDENT_CHAR}){body}(?!{_IDENT_CHAR})",
        re.IGNORECASE,
    )


def quote_mixed_case_identifiers(sql: str, case_map: dict[str, str]) -> str:
    """Rewrite ``sql`` so that references to identifiers listed in
    ``case_map`` are emitted with double quotes and their original casing.

    Uses sqlglot to safely walk the AST — string literals, comments, and
    already-quoted identifiers are left alone. If parsing fails, returns the
    SQL unchanged so we don't block execution.
    """
    if not case_map or not sql.strip():
        return sql

    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:
        return sql

    try:
        parsed = sqlglot.parse(sql, read="postgres")
    except sqlglot.errors.ParseError as e:
        logger.debug(f"identifier rewrite skipped — parse error: {e}")
        return sql

    changed = False
    for tree in parsed:
        if tree is None:
            continue
        for ident in tree.find_all(exp.Identifier):
            name = ident.name
            if not name:
                continue
            original = case_map.get(name.lower())
            if original is None:
                continue
            needs_quoting = original != original.lower()
            if needs_quoting:
                if ident.name == original and ident.quoted:
                    continue
                ident.set("this", original)
                ident.set("quoted", True)
                changed = True
            else:
                # Real name is all lowercase. Drop any quotes the LLM
                # added with the wrong casing; Postgres resolves the
                # unquoted form correctly.
                if ident.quoted or ident.name != original:
                    ident.set("this", original)
                    ident.set("quoted", False)
                    changed = True

    if not changed:
        return sql

    rendered = [tree.sql(dialect="postgres") for tree in parsed if tree is not None]
    return ";\n".join(rendered)
