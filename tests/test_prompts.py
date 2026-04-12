"""Tests for datasight.prompts."""

from datasight.prompts import build_system_prompt


def test_build_system_prompt_web_with_clarify():
    out = build_system_prompt("SCHEMA", mode="web", explain_sql=False, clarify_sql=True)
    assert "datasight" in out
    assert "Ambiguity Check" in out
    assert "SCHEMA" in out


def test_build_system_prompt_web_no_clarify():
    out = build_system_prompt("SCHEMA", mode="web", clarify_sql=False)
    assert "Ambiguity Check" not in out


def test_build_system_prompt_verify_mode():
    out = build_system_prompt("S", mode="verify", clarify_sql=True)
    # verify mode should not include the clarify section
    assert "Ambiguity Check" not in out


def test_build_system_prompt_explain_sql_adds_instruction():
    out = build_system_prompt("S", mode="web", explain_sql=True, clarify_sql=False)
    assert "explain the query" in out


def test_build_system_prompt_postgres_dialect():
    out = build_system_prompt("S", mode="web", dialect="postgres", clarify_sql=False)
    assert "PostgreSQL" in out


def test_build_system_prompt_sqlite_dialect():
    out = build_system_prompt("S", mode="web", dialect="sqlite", clarify_sql=False)
    assert "SQLite" in out


def test_build_system_prompt_unknown_dialect_falls_back_to_duckdb():
    out = build_system_prompt("S", mode="web", dialect="oracle", clarify_sql=False)
    assert "DuckDB" in out
