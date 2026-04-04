"""Tests for configuration helpers."""

from datasight.config import (
    load_schema_description,
    load_example_queries,
    format_example_queries,
)


def test_load_schema_description(tmp_path):
    desc_file = tmp_path / "schema_description.md"
    desc_file.write_text("# My Schema\nSome info.")
    result = load_schema_description(None, str(tmp_path))
    assert result == "# My Schema\nSome info."


def test_load_schema_description_explicit_path(tmp_path):
    custom = tmp_path / "custom.md"
    custom.write_text("Custom desc.")
    result = load_schema_description(str(custom), str(tmp_path))
    assert result == "Custom desc."


def test_load_schema_description_missing(tmp_path):
    result = load_schema_description(None, str(tmp_path))
    assert result is None


def test_load_example_queries(tmp_path):
    queries_file = tmp_path / "queries.yaml"
    queries_file.write_text(
        "- question: How many?\n"
        "  sql: SELECT COUNT(*) FROM t\n"
        "- question: Top 5\n"
        "  sql: SELECT * FROM t LIMIT 5\n"
    )
    result = load_example_queries(None, str(tmp_path))
    assert len(result) == 2
    assert result[0]["question"] == "How many?"
    assert "COUNT" in result[0]["sql"]


def test_load_example_queries_missing(tmp_path):
    result = load_example_queries(None, str(tmp_path))
    assert result == []


def test_load_example_queries_with_expected(tmp_path):
    queries_file = tmp_path / "queries.yaml"
    queries_file.write_text(
        "- question: Count\n  sql: SELECT COUNT(*) AS n FROM t\n  expected: n should be > 0\n"
    )
    result = load_example_queries(None, str(tmp_path))
    assert len(result) == 1
    assert "expected" in result[0]


def test_format_example_queries():
    queries = [
        {"question": "How many?", "sql": "SELECT COUNT(*) FROM t"},
        {"question": "Top 5", "sql": "SELECT * FROM t LIMIT 5"},
    ]
    formatted = format_example_queries(queries)
    assert "Q1: How many?" in formatted
    assert "Q2: Top 5" in formatted
    assert "```sql" in formatted


def test_format_example_queries_empty():
    assert format_example_queries([]) == ""
