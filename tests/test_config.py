"""Tests for configuration helpers."""

from datasight.config import (
    load_schema_description,
    load_example_queries,
    load_measure_overrides,
    format_example_queries,
)


def test_load_schema_description(tmp_path):
    desc_file = tmp_path / "schema_description.md"
    desc_file.write_text("# My Schema\nSome info.", encoding="utf-8")
    result = load_schema_description(None, str(tmp_path))
    assert result == "# My Schema\nSome info."


def test_load_schema_description_explicit_path(tmp_path):
    custom = tmp_path / "custom.md"
    custom.write_text("Custom desc.", encoding="utf-8")
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
        "  sql: SELECT * FROM t LIMIT 5\n",
        encoding="utf-8",
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
        "- question: Count\n  sql: SELECT COUNT(*) AS n FROM t\n  expected: n should be > 0\n",
        encoding="utf-8",
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


def test_load_measure_overrides(tmp_path):
    overrides_file = tmp_path / "measures.yaml"
    overrides_file.write_text(
        "- table: generation_hourly\n"
        "  column: demand_mw\n"
        "  default_aggregation: max\n"
        "  reason: Use peak demand for this project.\n"
        "- table: emissions_hourly\n"
        "  column: co2_rate_lb_per_mwh\n"
        "  weight_column: net_generation_mwh\n",
        encoding="utf-8",
    )
    result = load_measure_overrides(None, str(tmp_path))
    assert len(result) == 2
    assert result[0]["column"] == "demand_mw"
    assert result[0]["default_aggregation"] == "max"
    assert result[1]["weight_column"] == "net_generation_mwh"


def test_load_measure_overrides_supports_calculated_measures(tmp_path):
    overrides_file = tmp_path / "measures.yaml"
    overrides_file.write_text(
        "- table: generation_hourly\n"
        "  name: net_load_mw\n"
        "  expression: load_mw - renewable_generation_mw\n"
        "  default_aggregation: avg\n",
        encoding="utf-8",
    )

    result = load_measure_overrides(None, str(tmp_path))
    assert len(result) == 1
    assert result[0]["name"] == "net_load_mw"
    assert result[0]["expression"] == "load_mw - renewable_generation_mw"


def test_load_measure_overrides_loads_display_and_chart_metadata(tmp_path):
    overrides_file = tmp_path / "measures.yaml"
    overrides_file.write_text(
        "- table: generation_hourly\n"
        "  column: net_generation_mwh\n"
        "  display_name: Net generation\n"
        "  format: mwh\n"
        "  preferred_chart_types:\n"
        "    - line\n"
        "    - area\n",
        encoding="utf-8",
    )

    result = load_measure_overrides(None, str(tmp_path))
    assert result[0]["display_name"] == "Net generation"
    assert result[0]["format"] == "mwh"
    assert result[0]["preferred_chart_types"] == ["line", "area"]


def test_load_measure_overrides_missing(tmp_path):
    assert load_measure_overrides(None, str(tmp_path)) == []
