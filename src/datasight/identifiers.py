"""
Helpers for handling Postgres identifier quoting.

Postgres folds unquoted identifiers to lowercase, so a table created as
``"Battery"`` can only be referenced as ``"Battery"`` (double-quoted) — a bare
``Battery`` is resolved to ``battery`` and errors. LLMs frequently emit the
unquoted form, triggering a round-trip where the model sees the error and
retries with quotes. This module lets us preemptively rewrite SQL to quote
identifiers we know from introspection need it, eliminating that retry.
"""

from __future__ import annotations

from typing import Any

from loguru import logger


def configure_runner_identifier_quoting(runner: Any, schema_info: list[dict[str, Any]]) -> None:
    """Set ``mixed_case_identifiers`` on an underlying ``PostgresRunner``,
    unwrapping any ``CachingSqlRunner`` layer. No-op for non-Postgres runners.
    """
    target = runner
    while target is not None and not hasattr(target, "mixed_case_identifiers"):
        inner = getattr(target, "_inner", None)
        if inner is target:
            break
        target = inner
    if target is None or not hasattr(target, "mixed_case_identifiers"):
        return
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
