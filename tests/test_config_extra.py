"""Extra tests for datasight.config to cover remaining branches."""

from __future__ import annotations


import pytest

from datasight.config import (
    create_sql_runner,
    create_sql_runner_from_settings,
    load_example_queries,
    load_joins_config,
    load_measure_overrides,
    load_schema_description,
    load_time_series_config,
    normalize_db_mode,
)
from datasight.exceptions import ConfigurationError, ConnectionError as DsConnectionError
from datasight.runner import DuckDBRunner, SQLiteRunner
from datasight.settings import DatabaseSettings


# ---------------------------------------------------------------------------
# normalize_db_mode / create_sql_runner
# ---------------------------------------------------------------------------


def test_normalize_db_mode_local_alias():
    assert normalize_db_mode("local") == "duckdb"
    assert normalize_db_mode("duckdb") == "duckdb"
    assert normalize_db_mode("sqlite") == "sqlite"


def test_create_sql_runner_flightsql_branch():
    # FlightSqlRunner attempts to connect in its constructor; we just want
    # coverage of the flightsql branch and confirm a failure is raised cleanly.
    with pytest.raises(Exception):
        create_sql_runner(
            "flightsql",
            flight_uri="grpc://nonexistent.invalid:31337",
            flight_token="tok",
        )


def test_create_sql_runner_postgres_branch():
    # PostgresRunner connects eagerly; ensure the postgres branch is reached.
    with pytest.raises(DsConnectionError):
        create_sql_runner(
            "postgres",
            postgres_host="pg.nonexistent.invalid",
            postgres_database="db",
            postgres_user="u",
            postgres_password="p",
        )


def test_create_sql_runner_sqlite_missing_path():
    with pytest.raises(ConfigurationError, match="DB_PATH is required"):
        create_sql_runner("sqlite", db_path="")


def test_create_sql_runner_duckdb_missing_path():
    with pytest.raises(ConfigurationError, match="DB_PATH is required"):
        create_sql_runner("duckdb", db_path="")


def test_create_sql_runner_invalid_mode():
    with pytest.raises(ConfigurationError, match="Invalid database mode"):
        create_sql_runner("mongodb", db_path="x")


def test_create_sql_runner_local_alias_still_requires_path():
    with pytest.raises(ConfigurationError, match="DB_PATH is required"):
        create_sql_runner("local", db_path="")


def test_create_sql_runner_duckdb_with_path(test_duckdb_path):
    runner = create_sql_runner("duckdb", db_path=test_duckdb_path)
    assert isinstance(runner, DuckDBRunner)
    runner.close()


def test_create_sql_runner_sqlite_with_path(test_sqlite_path):
    runner = create_sql_runner("sqlite", db_path=test_sqlite_path)
    assert isinstance(runner, SQLiteRunner)
    runner.close()


# ---------------------------------------------------------------------------
# create_sql_runner_from_settings
# ---------------------------------------------------------------------------


def test_create_sql_runner_from_settings_resolves_relative_path(tmp_path, test_duckdb_path):
    # Copy the duckdb to a known relative filename in a project dir
    import shutil

    project = tmp_path / "proj"
    project.mkdir()
    rel_name = "local.duckdb"
    shutil.copy(test_duckdb_path, project / rel_name)

    settings = DatabaseSettings(mode="duckdb", path=rel_name)
    runner = create_sql_runner_from_settings(
        settings, project_dir=str(project), sql_cache_max_bytes=0
    )
    assert isinstance(runner, DuckDBRunner)
    runner.close()


def test_create_sql_runner_from_settings_absolute_path(test_duckdb_path):
    settings = DatabaseSettings(mode="duckdb", path=test_duckdb_path)
    runner = create_sql_runner_from_settings(
        settings, project_dir="/nonexistent", sql_cache_max_bytes=0
    )
    assert isinstance(runner, DuckDBRunner)
    runner.close()


def test_create_sql_runner_from_settings_postgres_ignores_project_dir():
    settings = DatabaseSettings(
        mode="postgres",
        path="",
        postgres_host="pg.nonexistent.invalid",
        postgres_database="db",
    )
    with pytest.raises(DsConnectionError):
        create_sql_runner_from_settings(settings, project_dir="/whatever")


# ---------------------------------------------------------------------------
# load_schema_description — missing explicit path branch
# ---------------------------------------------------------------------------


def test_load_schema_description_explicit_missing_path_warns(tmp_path):
    missing = tmp_path / "does-not-exist.md"
    result = load_schema_description(str(missing), str(tmp_path))
    assert result is None


# ---------------------------------------------------------------------------
# load_example_queries — invalid YAML / wrong shape / explicit missing
# ---------------------------------------------------------------------------


def test_load_example_queries_invalid_yaml(tmp_path):
    p = tmp_path / "queries.yaml"
    p.write_text("- : : unclosed\n  [broken", encoding="utf-8")
    assert load_example_queries(None, str(tmp_path)) == []


def test_load_example_queries_non_list(tmp_path):
    p = tmp_path / "queries.yaml"
    p.write_text("key: value\n", encoding="utf-8")
    assert load_example_queries(None, str(tmp_path)) == []


def test_load_example_queries_explicit_missing_path(tmp_path):
    missing = tmp_path / "nope.yaml"
    assert load_example_queries(str(missing), str(tmp_path)) == []


# ---------------------------------------------------------------------------
# load_measure_overrides — extended branches
# ---------------------------------------------------------------------------


def test_load_measure_overrides_explicit_missing_path(tmp_path):
    missing = tmp_path / "nope.yaml"
    assert load_measure_overrides(str(missing), str(tmp_path)) == []


def test_load_measure_overrides_invalid_yaml(tmp_path):
    p = tmp_path / "measures.yaml"
    p.write_text(": : bad\n  [unclosed", encoding="utf-8")
    assert load_measure_overrides(None, str(tmp_path)) == []


def test_load_measure_overrides_non_list(tmp_path):
    p = tmp_path / "measures.yaml"
    p.write_text("foo: bar\n", encoding="utf-8")
    assert load_measure_overrides(None, str(tmp_path)) == []


def test_load_measure_overrides_skips_non_dict_and_missing_required(tmp_path):
    p = tmp_path / "measures.yaml"
    p.write_text(
        "- just a string\n"
        "- 42\n"
        "- table: ''\n"
        "  column: x\n"
        "- table: orders\n"  # has table, no column, no name/expression -> skipped
        "- table: orders\n"
        "  column: qty\n"
        "  default_aggregation: sum\n"
        "  allowed_aggregations:\n"
        "    - sum\n"
        "    - max\n"
        "  forbidden_aggregations:\n"
        "    - min\n"
        "  additive_across_category: true\n"
        "  additive_across_time: false\n",
        encoding="utf-8",
    )
    result = load_measure_overrides(None, str(tmp_path))
    assert len(result) == 1
    entry = result[0]
    assert entry["column"] == "qty"
    assert entry["allowed_aggregations"] == ["sum", "max"]
    assert entry["forbidden_aggregations"] == ["min"]
    assert entry["additive_across_category"] is True
    assert entry["additive_across_time"] is False


def test_load_measure_overrides_preferred_chart_types_string(tmp_path):
    p = tmp_path / "measures.yaml"
    p.write_text(
        "- table: t\n  column: c\n  preferred_chart_types: line\n",
        encoding="utf-8",
    )
    result = load_measure_overrides(None, str(tmp_path))
    assert result[0]["preferred_chart_types"] == ["line"]


def test_load_measure_overrides_preferred_chart_types_falsy(tmp_path):
    p = tmp_path / "measures.yaml"
    p.write_text(
        "- table: t\n  column: c\n  preferred_chart_types: null\n",
        encoding="utf-8",
    )
    result = load_measure_overrides(None, str(tmp_path))
    assert "preferred_chart_types" not in result[0]


# ---------------------------------------------------------------------------
# load_time_series_config
# ---------------------------------------------------------------------------


def test_load_time_series_config_missing_default_returns_empty(tmp_path):
    assert load_time_series_config(None, str(tmp_path)) == []


def test_load_time_series_config_explicit_missing_path(tmp_path):
    missing = tmp_path / "nope.yaml"
    assert load_time_series_config(str(missing), str(tmp_path)) == []


def test_load_time_series_config_invalid_yaml(tmp_path):
    p = tmp_path / "time_series.yaml"
    p.write_text(": : bad\n  [unclosed", encoding="utf-8")
    assert load_time_series_config(None, str(tmp_path)) == []


def test_load_time_series_config_non_list(tmp_path):
    p = tmp_path / "time_series.yaml"
    p.write_text("foo: bar\n", encoding="utf-8")
    assert load_time_series_config(None, str(tmp_path)) == []


def test_load_time_series_config_valid_and_invalid_entries(tmp_path):
    p = tmp_path / "time_series.yaml"
    p.write_text(
        "- not a dict\n"
        "- table: ''\n"
        "  timestamp_column: t\n"
        "  frequency: PT1H\n"
        "- table: t\n"
        "  timestamp_column: ts\n"
        "  frequency: XYZ\n"  # unsupported
        "- table: generation\n"
        "  timestamp_column: report_date\n"
        "  frequency: pt1h\n"  # normalized upper
        "  group_columns:\n"
        "    - plant_id\n"
        "    - ''\n"
        "  time_zone: US/Eastern\n",
        encoding="utf-8",
    )
    result = load_time_series_config(None, str(tmp_path))
    assert len(result) == 1
    entry = result[0]
    assert entry["frequency"] == "PT1H"
    assert entry["group_columns"] == ["plant_id"]
    assert entry["time_zone"] == "US/Eastern"


# ---------------------------------------------------------------------------
# load_joins_config
# ---------------------------------------------------------------------------


def test_load_joins_config_missing_default_returns_empty(tmp_path):
    assert load_joins_config(None, str(tmp_path)) == []


def test_load_joins_config_explicit_missing_path(tmp_path):
    missing = tmp_path / "nope.yaml"
    assert load_joins_config(str(missing), str(tmp_path)) == []


def test_load_joins_config_invalid_yaml(tmp_path):
    p = tmp_path / "joins.yaml"
    p.write_text(": : bad\n  [unclosed", encoding="utf-8")
    assert load_joins_config(None, str(tmp_path)) == []


def test_load_joins_config_non_list(tmp_path):
    p = tmp_path / "joins.yaml"
    p.write_text("foo: bar\n", encoding="utf-8")
    assert load_joins_config(None, str(tmp_path)) == []


def test_load_joins_config_valid_with_defaults(tmp_path):
    p = tmp_path / "joins.yaml"
    p.write_text(
        "- not a dict\n"
        "- child_table: ''\n"
        "  child_column: product_id\n"
        "  parent_table: products\n"
        "- child_table: orders\n"
        "  child_column: product_id\n"
        "  parent_table: products\n"  # parent_column defaults to 'id'
        "- child_table: orders\n"
        "  child_column: customer_id\n"
        "  parent_table: customers\n"
        "  parent_column: customer_id\n",
        encoding="utf-8",
    )
    result = load_joins_config(None, str(tmp_path))
    assert len(result) == 2
    assert result[0]["parent_column"] == "id"
    assert result[1]["parent_column"] == "customer_id"
