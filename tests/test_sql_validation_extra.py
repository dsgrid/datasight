"""Extra tests for datasight.sql_validation to cover remaining branches."""

from __future__ import annotations

from datasight.sql_validation import (
    build_measure_rule_map,
    validate_sql,
)


SCHEMA = {
    "generation_fuel": {"report_date", "net_generation_mwh", "plant_id_eia"},
    "plants": {"plant_id_eia", "state"},
}


# ---------------------------------------------------------------------------
# build_measure_rule_map
# ---------------------------------------------------------------------------


def test_build_measure_rule_map_empty_input():
    assert build_measure_rule_map([]) == {}


def test_build_measure_rule_map_skips_missing_required():
    rules = build_measure_rule_map(
        [
            {"table": "t", "column": "c"},  # missing default_aggregation
            {"table": "", "column": "c", "default_aggregation": "sum"},
            {"table": "t", "column": "", "default_aggregation": "sum"},
            {
                "table": "t",
                "column": "c",
                "default_aggregation": "SUM",
                # no allowed_aggregations -> defaults to (default,)
            },
        ]
    )
    assert list(rules.keys()) == [("t", "c")]
    rule = rules[("t", "c")]
    assert rule.default_aggregation == "sum"
    assert rule.allowed_aggregations == ("sum",)


def test_build_measure_rule_map_normalizes_allowed_list():
    rules = build_measure_rule_map(
        [
            {
                "table": "T",
                "column": "C",
                "default_aggregation": "avg",
                "allowed_aggregations": ["SUM", " Avg ", "", "min"],
            }
        ]
    )
    rule = rules[("t", "c")]
    assert rule.allowed_aggregations == ("sum", "avg", "min")


# ---------------------------------------------------------------------------
# validate_sql — parse error, empty parse, CTE/subquery/unknown table
# ---------------------------------------------------------------------------


def test_validate_sql_parse_error_reports_error():
    result = validate_sql("SELECT * FROM (((", SCHEMA)
    assert not result.valid
    assert any("parse error" in e.lower() for e in result.errors)


def test_validate_sql_empty_statement_is_valid():
    # A comment-only / empty SQL yields parsed[0] is None
    result = validate_sql(";", SCHEMA)
    assert result.valid
    assert result.errors == []


def test_validate_sql_unknown_table_is_flagged():
    result = validate_sql("SELECT * FROM mystery_table", SCHEMA)
    assert not result.valid
    assert any("mystery_table" in e for e in result.errors)


def test_validate_sql_cte_reference_is_allowed():
    sql = """
        WITH hourly AS (
            SELECT report_date, SUM(net_generation_mwh) AS total
            FROM generation_fuel
            GROUP BY report_date
        )
        SELECT * FROM hourly
    """
    result = validate_sql(sql, SCHEMA)
    assert result.valid


def test_validate_sql_subquery_alias_is_allowed():
    sql = "SELECT sub.state FROM (SELECT state FROM plants) AS sub"
    result = validate_sql(sql, SCHEMA)
    assert result.valid


# ---------------------------------------------------------------------------
# _find_measure_rule branches via validate_sql
# ---------------------------------------------------------------------------


def test_validate_sql_aggregation_resolves_via_table_alias():
    rules = build_measure_rule_map(
        [
            {
                "table": "generation_fuel",
                "column": "net_generation_mwh",
                "default_aggregation": "max",
                "allowed_aggregations": ["sum", "max"],
            }
        ]
    )
    # Using alias `g` forces the alias-to-table resolution path
    sql = """
        SELECT g.report_date, SUM(g.net_generation_mwh) AS v
        FROM generation_fuel g
        GROUP BY g.report_date
    """
    result = validate_sql(sql, SCHEMA, measure_rules=rules)
    assert result.valid


def test_validate_sql_aggregation_without_table_qualifier_matches_single_rule():
    rules = build_measure_rule_map(
        [
            {
                "table": "generation_fuel",
                "column": "net_generation_mwh",
                "default_aggregation": "sum",
                "allowed_aggregations": ["sum"],
            }
        ]
    )
    # No table qualifier on the column — exercises the single-match fallback
    sql = "SELECT SUM(net_generation_mwh) FROM generation_fuel"
    result = validate_sql(sql, SCHEMA, measure_rules=rules)
    assert result.valid


def test_validate_sql_aggregation_deduplicates_repeated_errors():
    rules = build_measure_rule_map(
        [
            {
                "table": "generation_fuel",
                "column": "net_generation_mwh",
                "default_aggregation": "max",
                "allowed_aggregations": ["max"],
            }
        ]
    )
    sql = "SELECT SUM(net_generation_mwh), SUM(net_generation_mwh) FROM generation_fuel"
    result = validate_sql(sql, SCHEMA, measure_rules=rules)
    assert not result.valid
    # Should dedupe the error rather than emit two identical entries
    not_allowed_errors = [e for e in result.errors if "not allowed" in e]
    assert len(not_allowed_errors) == 1


def test_validate_sql_column_without_name_is_ignored():
    # exp.Column with a wildcard / empty name branch — confirm '*' doesn't crash
    # and that star aggregations like COUNT(*) simply have no single column to
    # match a measure rule against.
    rules = build_measure_rule_map(
        [
            {
                "table": "generation_fuel",
                "column": "net_generation_mwh",
                "default_aggregation": "sum",
                "allowed_aggregations": ["sum"],
            }
        ]
    )
    result = validate_sql(
        "SELECT COUNT(*) FROM generation_fuel",
        SCHEMA,
        measure_rules=rules,
    )
    assert result.valid
