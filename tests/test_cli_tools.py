"""Tests for CLI profiling and batch ask helpers."""

import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from click.testing import CliRunner

from datasight.cli import cli
from datasight.llm import LLMResponse, TextBlock, ToolUseBlock, Usage


def test_profile_dataset(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["profile", "--project-dir", project_dir])
    assert result.exit_code == 0
    assert "Dataset Profile" in result.output
    assert "Largest Tables" in result.output
    assert "Date Coverage" in result.output
    assert "Measure Candidates" in result.output


def test_profile_table(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["profile", "--project-dir", project_dir, "--table", "orders"])
    assert result.exit_code == 0
    assert "Table Profile" in result.output
    assert "orders" in result.output
    assert "Date Columns" in result.output
    assert "Numeric Columns" in result.output
    assert "Text Dimensions" in result.output


def test_profile_column(project_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["profile", "--project-dir", project_dir, "--column", "orders.order_date"],
    )
    assert result.exit_code == 0
    assert "Column Profile" in result.output
    assert "orders.order_date" in result.output
    assert "Date Coverage" in result.output


def test_profile_dataset_markdown_includes_sections(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["profile", "--project-dir", project_dir, "--format", "markdown"])
    assert result.exit_code == 0
    assert "# Dataset Profile" in result.output
    assert "## Largest Tables" in result.output
    assert "## Date Coverage" in result.output


def test_profile_table_markdown_includes_numeric_and_text_sections(project_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["profile", "--project-dir", project_dir, "--table", "orders", "--format", "markdown"],
    )
    assert result.exit_code == 0
    assert "# Table Profile: orders" in result.output
    assert "## Date Columns" in result.output
    assert "## Text Dimensions" in result.output


def test_profile_column_markdown_includes_date_coverage(project_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "profile",
            "--project-dir",
            project_dir,
            "--column",
            "orders.order_date",
            "--format",
            "markdown",
        ],
    )
    assert result.exit_code == 0
    assert "# Column Profile: orders.order_date" in result.output
    assert "## Date Coverage" in result.output


def test_profile_markdown_output_writes_file(project_dir, tmp_path):
    output_path = tmp_path / "profile.md"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "profile",
            "--project-dir",
            project_dir,
            "--format",
            "markdown",
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0
    assert output_path.exists()
    assert "# Dataset Profile" in output_path.read_text(encoding="utf-8")


def test_profile_json_output_writes_file(project_dir, tmp_path):
    output_path = tmp_path / "profile.json"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "profile",
            "--project-dir",
            project_dir,
            "--format",
            "json",
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0
    data = json.loads(output_path.read_text(encoding="utf-8"))
    assert data["table_count"] >= 1


def test_quality_table_output(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["quality", "--project-dir", project_dir])
    assert result.exit_code == 0
    assert "Dataset Quality Audit" in result.output
    assert "Date Coverage" in result.output
    assert "Notes" in result.output


def test_quality_table_scope(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["quality", "--project-dir", project_dir, "--table", "orders"])
    assert result.exit_code == 0
    assert "Dataset Quality Audit" in result.output
    assert "1" in result.output
    assert "orders.order_date" in result.output


def test_quality_table_scope_missing_table(project_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["quality", "--project-dir", project_dir, "--table", "missing_table"]
    )
    assert result.exit_code != 0
    assert "Table not found: missing_table" in result.output


def test_quality_markdown_output_writes_file(project_dir, tmp_path):
    output_path = tmp_path / "quality.md"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "quality",
            "--project-dir",
            project_dir,
            "--format",
            "markdown",
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0
    text = output_path.read_text(encoding="utf-8")
    assert "# Dataset Quality Audit" in text
    assert "## Date Coverage" in text


def test_quality_json_output_writes_file(project_dir, tmp_path):
    output_path = tmp_path / "quality.json"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "quality",
            "--project-dir",
            project_dir,
            "--format",
            "json",
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0
    data = json.loads(output_path.read_text(encoding="utf-8"))
    assert data["table_count"] >= 1
    assert "notes" in data


def test_dimensions_table_output(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["dimensions", "--project-dir", project_dir])
    assert result.exit_code == 0
    assert "Dimension Overview" in result.output
    assert "Dimension Candidates" in result.output
    assert "Suggested Breakdowns" in result.output


def test_dimensions_table_scope(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["dimensions", "--project-dir", project_dir, "--table", "orders"])
    assert result.exit_code == 0
    assert "Dimension Overview" in result.output
    assert "Tables scanned" in result.output
    assert "Suggested Breakdowns" in result.output


def test_dimensions_table_scope_missing_table(project_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["dimensions", "--project-dir", project_dir, "--table", "missing_table"]
    )
    assert result.exit_code != 0
    assert "Table not found: missing_table" in result.output


def test_dimensions_markdown_output_writes_file(project_dir, tmp_path):
    output_path = tmp_path / "dimensions.md"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "dimensions",
            "--project-dir",
            project_dir,
            "--format",
            "markdown",
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0
    text = output_path.read_text(encoding="utf-8")
    assert "# Dimension Overview" in text
    assert "## Suggested Breakdowns" in text


def test_measure_overview_infers_energy_aggregations():
    import asyncio

    from datasight.data_profile import build_measure_overview

    schema_info = [
        {
            "name": "generation_hourly",
            "columns": [
                {"name": "net_generation_mwh", "dtype": "DOUBLE"},
                {"name": "demand_mw", "dtype": "DOUBLE"},
                {"name": "capacity_factor_pct", "dtype": "DOUBLE"},
                {"name": "fuel_cost_per_mmbtu", "dtype": "DOUBLE"},
                {"name": "co2_rate_lb_per_mwh", "dtype": "DOUBLE"},
                {"name": "fuel_consumed_mmbtu", "dtype": "DOUBLE"},
                {"name": "plant_id", "dtype": "INTEGER"},
            ],
        }
    ]

    async def fake_run_sql(sql):  # noqa: ARG001
        raise AssertionError("measure inference should not query the database")

    data = asyncio.run(build_measure_overview(schema_info, fake_run_sql))
    measures = {item["column"]: item for item in data["measures"]}

    assert measures["net_generation_mwh"]["role"] == "energy"
    assert measures["net_generation_mwh"]["default_aggregation"] == "sum"
    assert measures["net_generation_mwh"]["additive_across_time"] is True

    assert measures["demand_mw"]["role"] == "power"
    assert measures["demand_mw"]["default_aggregation"] == "avg"
    assert "sum" in measures["demand_mw"]["forbidden_aggregations"]

    assert measures["capacity_factor_pct"]["role"] == "ratio"
    assert measures["capacity_factor_pct"]["default_aggregation"] == "avg"
    assert "sum" in measures["capacity_factor_pct"]["forbidden_aggregations"]

    assert measures["fuel_cost_per_mmbtu"]["role"] == "price"
    assert "sum" in measures["fuel_cost_per_mmbtu"]["forbidden_aggregations"]
    assert measures["fuel_cost_per_mmbtu"]["average_strategy"] == "weighted_avg"
    assert measures["fuel_cost_per_mmbtu"]["weight_column"] == "net_generation_mwh"
    assert (
        "SUM(fuel_cost_per_mmbtu * net_generation_mwh)"
        in measures["fuel_cost_per_mmbtu"]["recommended_rollup_sql"]
    )

    assert measures["co2_rate_lb_per_mwh"]["role"] == "rate"
    assert measures["co2_rate_lb_per_mwh"]["average_strategy"] == "weighted_avg"
    assert measures["co2_rate_lb_per_mwh"]["weight_column"] == "net_generation_mwh"
    assert (
        "NULLIF(SUM(net_generation_mwh), 0)"
        in measures["co2_rate_lb_per_mwh"]["recommended_rollup_sql"]
    )

    assert "plant_id" not in measures


def test_measure_overview_applies_project_override():
    import asyncio

    from datasight.data_profile import build_measure_overview

    schema_info = [
        {
            "name": "load_hourly",
            "columns": [
                {"name": "demand_mw", "dtype": "DOUBLE"},
                {"name": "net_generation_mwh", "dtype": "DOUBLE"},
            ],
        }
    ]
    overrides = [
        {
            "table": "load_hourly",
            "column": "demand_mw",
            "default_aggregation": "max",
            "reason": "This project wants peak demand by default.",
        }
    ]

    async def fake_run_sql(sql):  # noqa: ARG001
        raise AssertionError("measure inference should not query the database")

    data = asyncio.run(build_measure_overview(schema_info, fake_run_sql, overrides))
    measure = next(item for item in data["measures"] if item["column"] == "demand_mw")
    assert measure["default_aggregation"] == "max"
    assert measure["recommended_rollup_sql"] == "MAX(demand_mw) AS peak_demand_mw"
    assert measure["reason"] == "This project wants peak demand by default."


def test_measure_overview_includes_calculated_project_measure():
    import asyncio

    from datasight.data_profile import build_measure_overview

    schema_info = [
        {
            "name": "load_hourly",
            "columns": [
                {"name": "load_mw", "dtype": "DOUBLE"},
                {"name": "renewable_generation_mw", "dtype": "DOUBLE"},
            ],
        }
    ]
    overrides = [
        {
            "table": "load_hourly",
            "name": "net_load_mw",
            "expression": "load_mw - renewable_generation_mw",
            "role": "power",
            "default_aggregation": "avg",
            "reason": "Project-defined net load measure.",
        }
    ]

    async def fake_run_sql(sql):  # noqa: ARG001
        raise AssertionError("measure inference should not query the database")

    data = asyncio.run(build_measure_overview(schema_info, fake_run_sql, overrides))
    measure = next(item for item in data["measures"] if item["column"] == "net_load_mw")
    assert measure["expression"] == "load_mw - renewable_generation_mw"
    assert measure["source"] == "calculated"
    assert (
        measure["recommended_rollup_sql"]
        == "AVG(load_mw - renewable_generation_mw) AS avg_net_load_mw"
    )


def test_measure_overview_applies_display_and_chart_metadata():
    import asyncio

    from datasight.data_profile import build_measure_overview

    schema_info = [
        {
            "name": "generation_hourly",
            "columns": [
                {"name": "net_generation_mwh", "dtype": "DOUBLE"},
            ],
        }
    ]
    overrides = [
        {
            "table": "generation_hourly",
            "column": "net_generation_mwh",
            "display_name": "Net generation",
            "format": "mwh",
            "preferred_chart_types": ["line", "area"],
        }
    ]

    async def fake_run_sql(sql):  # noqa: ARG001
        raise AssertionError("measure inference should not query the database")

    data = asyncio.run(build_measure_overview(schema_info, fake_run_sql, overrides))
    measure = next(item for item in data["measures"] if item["column"] == "net_generation_mwh")
    assert measure["display_name"] == "Net generation"
    assert measure["format"] == "mwh"
    assert measure["preferred_chart_types"] == ["line", "area"]


def test_format_measure_prompt_context_includes_guardrails():
    from datasight.data_profile import format_measure_prompt_context

    text = format_measure_prompt_context(
        {
            "measures": [
                {
                    "table": "generation_hourly",
                    "column": "net_generation_mwh",
                    "role": "energy",
                    "unit": "mwh",
                    "default_aggregation": "sum",
                    "allowed_aggregations": ["sum", "avg", "min", "max"],
                    "forbidden_aggregations": [],
                    "reason": "Energy-volume metric; summing across periods is usually meaningful.",
                },
                {
                    "table": "load_hourly",
                    "column": "demand_mw",
                    "role": "power",
                    "unit": "mw",
                    "default_aggregation": "avg",
                    "allowed_aggregations": ["avg", "max", "min"],
                    "forbidden_aggregations": ["sum"],
                    "reason": "Power metric; average or peak over time rather than summing.",
                },
            ]
        }
    )

    assert "## Inferred Measure Semantics" in text
    assert "Do not SUM prices, rates, percentages, or factors" in text
    assert "generation_hourly.net_generation_mwh" in text
    assert "default=sum" in text
    assert "load_hourly.demand_mw" in text
    assert "avoid=sum" in text


def test_format_measure_prompt_context_includes_weighted_average_guidance():
    from datasight.data_profile import format_measure_prompt_context

    text = format_measure_prompt_context(
        {
            "measures": [
                {
                    "table": "generation_hourly",
                    "column": "co2_rate_lb_per_mwh",
                    "role": "rate",
                    "unit": "lb_per_mwh",
                    "default_aggregation": "avg",
                    "average_strategy": "weighted_avg",
                    "weight_column": "net_generation_mwh",
                    "allowed_aggregations": ["avg", "min", "max"],
                    "forbidden_aggregations": ["sum"],
                    "reason": "Rate metric; prefer a weighted average using `net_generation_mwh` when rolling up.",
                }
            ]
        }
    )

    assert "weighted average instead of a plain AVG" in text
    assert "weight=net_generation_mwh" in text
    assert "average=weighted_avg" in text
    assert (
        "rollup_sql=SUM(co2_rate_lb_per_mwh * net_generation_mwh) / NULLIF(SUM(net_generation_mwh), 0) AS weighted_avg_co2_rate_lb_per_mwh"
        in text
    )


def test_format_measure_prompt_context_includes_calculated_measure_formula():
    from datasight.data_profile import format_measure_prompt_context

    text = format_measure_prompt_context(
        {
            "measures": [
                {
                    "table": "load_hourly",
                    "column": "net_load_mw",
                    "name": "net_load_mw",
                    "expression": "load_mw - renewable_generation_mw",
                    "role": "power",
                    "default_aggregation": "avg",
                    "allowed_aggregations": ["avg", "max", "min"],
                    "forbidden_aggregations": ["sum"],
                    "reason": "Project-defined net load measure.",
                    "recommended_rollup_sql": "AVG(load_mw - renewable_generation_mw) AS avg_net_load_mw",
                }
            ]
        }
    )

    assert "expression=load_mw - renewable_generation_mw" in text
    assert "rollup_sql=AVG(load_mw - renewable_generation_mw) AS avg_net_load_mw" in text


def test_format_measure_prompt_context_includes_display_and_chart_metadata():
    from datasight.data_profile import format_measure_prompt_context

    text = format_measure_prompt_context(
        {
            "measures": [
                {
                    "table": "generation_hourly",
                    "column": "net_generation_mwh",
                    "role": "energy",
                    "display_name": "Net generation",
                    "format": "mwh",
                    "preferred_chart_types": ["line", "area"],
                    "default_aggregation": "sum",
                    "allowed_aggregations": ["sum", "avg", "min", "max"],
                    "forbidden_aggregations": [],
                    "reason": "Energy-volume metric.",
                    "recommended_rollup_sql": "SUM(net_generation_mwh) AS total_net_generation_mwh",
                }
            ]
        }
    )

    assert "display_name=Net generation" in text
    assert "format=mwh" in text
    assert "preferred_charts=line, area" in text


def test_measures_table_output(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["measures", "--project-dir", project_dir])
    assert result.exit_code == 0
    assert "Measure Overview" in result.output
    assert "Measure Candidates" in result.output
    assert "Aggregation Guidance" in result.output


def test_measures_table_scope(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["measures", "--project-dir", project_dir, "--table", "orders"])
    assert result.exit_code == 0
    assert "Measure Overview" in result.output
    assert "Tables scanned" in result.output
    assert "orders" in result.output


def test_measures_table_scope_missing_table(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["measures", "--project-dir", project_dir, "--table", "missing"])
    assert result.exit_code != 0
    assert "Table not found: missing" in result.output


def test_measures_markdown_output_writes_file(project_dir, tmp_path):
    output_path = tmp_path / "measures.md"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "measures",
            "--project-dir",
            project_dir,
            "--format",
            "markdown",
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0
    text = output_path.read_text(encoding="utf-8")
    assert "# Measure Overview" in text
    assert "## Measure Candidates" in text


def test_measures_json_output_writes_file(project_dir, tmp_path):
    output_path = tmp_path / "measures.json"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "measures",
            "--project-dir",
            project_dir,
            "--format",
            "json",
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0
    data = json.loads(output_path.read_text(encoding="utf-8"))
    assert data["table_count"] >= 1
    assert "measures" in data


def test_measures_scaffold_writes_template(project_dir, tmp_path):
    output_path = tmp_path / "measures.yaml"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "measures",
            "--project-dir",
            project_dir,
            "--scaffold",
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0
    text = output_path.read_text(encoding="utf-8")
    assert "datasight measure overrides" in text
    assert "table:" in text
    assert "column:" in text
    assert "default_aggregation:" in text


def test_measures_scaffold_requires_overwrite(project_dir, tmp_path):
    output_path = tmp_path / "measures.yaml"
    output_path.write_text("existing\n", encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "measures",
            "--project-dir",
            project_dir,
            "--scaffold",
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code != 0
    assert "already exists" in result.output


def test_trend_overview_uses_semantic_measure_aggregation():
    import asyncio
    import pandas as pd

    from datasight.data_profile import build_trend_overview

    schema_info = [
        {
            "name": "generation_hourly",
            "columns": [
                {"name": "report_time", "dtype": "TIMESTAMP"},
                {"name": "net_generation_mwh", "dtype": "DOUBLE"},
                {"name": "demand_mw", "dtype": "DOUBLE"},
                {"name": "balancing_authority", "dtype": "VARCHAR"},
            ],
        }
    ]

    async def fake_run_sql(sql):
        if "MIN(" in sql and "MAX(" in sql:
            return pd.DataFrame(
                [{"min_value": "2024-01-01 00:00:00", "max_value": "2024-01-02 00:00:00"}]
            )
        if "COUNT(DISTINCT" in sql:
            return pd.DataFrame([{"distinct_count": 3, "null_count": 0}])
        if "GROUP BY 1 ORDER BY COUNT(*) DESC" in sql:
            return pd.DataFrame([{"value": "BA1"}, {"value": "BA2"}])
        raise AssertionError(f"unexpected SQL in test: {sql}")

    data = asyncio.run(build_trend_overview(schema_info, fake_run_sql))
    candidates = {item["measure_column"]: item for item in data["trend_candidates"]}
    charts = {item["title"]: item for item in data["chart_recommendations"]}

    assert candidates["net_generation_mwh"]["aggregation"] == "sum"
    assert candidates["net_generation_mwh"]["measure_role"] == "energy"
    assert candidates["demand_mw"]["aggregation"] == "avg"
    assert candidates["demand_mw"]["measure_role"] == "power"
    assert "SUM net_generation_mwh over report_time" in charts
    assert charts["SUM net_generation_mwh over report_time"]["aggregation"] == "sum"


def test_trend_overview_uses_weighted_average_for_rate_metrics():
    import asyncio
    import pandas as pd

    from datasight.data_profile import build_trend_overview

    schema_info = [
        {
            "name": "emissions_hourly",
            "columns": [
                {"name": "report_time", "dtype": "TIMESTAMP"},
                {"name": "co2_rate_lb_per_mwh", "dtype": "DOUBLE"},
                {"name": "net_generation_mwh", "dtype": "DOUBLE"},
            ],
        }
    ]

    async def fake_run_sql(sql):
        if "MIN(" in sql and "MAX(" in sql:
            return pd.DataFrame(
                [{"min_value": "2024-01-01 00:00:00", "max_value": "2024-01-02 00:00:00"}]
            )
        raise AssertionError(f"unexpected SQL in test: {sql}")

    data = asyncio.run(build_trend_overview(schema_info, fake_run_sql))
    candidate = next(
        item
        for item in data["trend_candidates"]
        if item["measure_column"] == "co2_rate_lb_per_mwh"
    )
    chart = next(
        item for item in data["chart_recommendations"] if "co2_rate_lb_per_mwh" in item["title"]
    )

    assert candidate["aggregation"] == "weighted_avg"
    assert candidate["weight_column"] == "net_generation_mwh"
    assert "WEIGHTED_AVG" in candidate["recommended_query_shape"]
    assert chart["aggregation"] == "weighted_avg"
    assert "net_generation_mwh" in chart["reason"]


def test_trend_overview_prefers_configured_chart_type_and_display_name():
    import asyncio
    import pandas as pd

    from datasight.data_profile import build_trend_overview

    schema_info = [
        {
            "name": "generation_hourly",
            "columns": [
                {"name": "report_time", "dtype": "TIMESTAMP"},
                {"name": "net_generation_mwh", "dtype": "DOUBLE"},
            ],
        }
    ]
    overrides = [
        {
            "table": "generation_hourly",
            "column": "net_generation_mwh",
            "display_name": "Net generation",
            "format": "mwh",
            "preferred_chart_types": ["area", "line"],
        }
    ]

    async def fake_run_sql(sql):
        if "MIN(" in sql and "MAX(" in sql:
            return pd.DataFrame(
                [{"min_value": "2024-01-01 00:00:00", "max_value": "2024-01-02 00:00:00"}]
            )
        raise AssertionError(f"unexpected SQL in test: {sql}")

    data = asyncio.run(build_trend_overview(schema_info, fake_run_sql, overrides))
    chart = next(
        item for item in data["chart_recommendations"] if "Net generation" in item["title"]
    )
    assert chart["chart_type"] == "area"
    assert chart["preferred_chart_types"] == ["area", "line"]
    assert "format `mwh`" in chart["reason"]


def test_prompt_recipes_include_rollup_sql_for_semantic_measure():
    import asyncio

    from datasight.data_profile import build_prompt_recipes

    schema_info = [
        {
            "name": "generation_hourly",
            "columns": [
                {"name": "report_time", "dtype": "TIMESTAMP"},
                {"name": "net_generation_mwh", "dtype": "DOUBLE"},
                {"name": "co2_rate_lb_per_mwh", "dtype": "DOUBLE"},
            ],
        }
    ]

    async def fake_run_sql(sql):
        if "MIN(" in sql and "MAX(" in sql:
            import pandas as pd

            return pd.DataFrame(
                [{"min_value": "2024-01-01 00:00:00", "max_value": "2024-01-02 00:00:00"}]
            )
        raise AssertionError(f"unexpected SQL in test: {sql}")

    recipes = asyncio.run(build_prompt_recipes(schema_info, fake_run_sql))
    joined = "\n".join(item["prompt"] for item in recipes)
    assert "SUM(net_generation_mwh) AS total_net_generation_mwh" in joined


def test_trends_table_output(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["trends", "--project-dir", project_dir])
    assert result.exit_code == 0
    assert "Trend Overview" in result.output
    assert "Trend Candidates" in result.output
    assert "Chart Recommendations" in result.output


def test_trends_table_scope(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["trends", "--project-dir", project_dir, "--table", "orders"])
    assert result.exit_code == 0
    assert "Trend Overview" in result.output
    assert "orders" in result.output
    assert "order_date" in result.output


def test_trends_table_scope_missing_table(project_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["trends", "--project-dir", project_dir, "--table", "missing_table"]
    )
    assert result.exit_code != 0
    assert "Table not found: missing_table" in result.output


def test_trends_json_output_writes_file(project_dir, tmp_path):
    output_path = tmp_path / "trends.json"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["trends", "--project-dir", project_dir, "--format", "json", "--output", str(output_path)],
    )
    assert result.exit_code == 0
    data = json.loads(output_path.read_text(encoding="utf-8"))
    assert data["table_count"] >= 1
    assert "trend_candidates" in data


@pytest.fixture()
def csv_file(tmp_path):
    """Create a small CSV file for inspect tests."""
    path = tmp_path / "sales.csv"
    path.write_text(
        "date,region,amount,units\n"
        "2024-01-01,East,100.5,10\n"
        "2024-01-02,West,200.3,20\n"
        "2024-01-03,East,150.0,15\n"
        "2024-02-01,West,300.0,25\n",
        encoding="utf-8",
    )
    return str(path)


def test_inspect_table_output(csv_file):
    runner = CliRunner()
    result = runner.invoke(cli, ["inspect", csv_file])
    assert result.exit_code == 0
    assert "Dataset Profile" in result.output
    assert "Measure Candidates" in result.output
    assert "Dimension Candidates" in result.output
    assert "Trend Candidates" in result.output
    assert "Prompt Recipes" in result.output


def test_inspect_json_output(csv_file):
    runner = CliRunner()
    result = runner.invoke(cli, ["inspect", csv_file, "--format", "json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert "profile" in data
    assert "quality" in data
    assert "measures" in data
    assert "dimensions" in data
    assert "trends" in data
    assert "recipes" in data
    assert data["profile"]["table_count"] == 1


def test_inspect_markdown_output(csv_file):
    runner = CliRunner()
    result = runner.invoke(cli, ["inspect", csv_file, "--format", "markdown"])
    assert result.exit_code == 0
    assert "# Dataset Profile" in result.output


def test_inspect_writes_output_file(csv_file, tmp_path):
    output_path = tmp_path / "report.json"
    runner = CliRunner()
    result = runner.invoke(
        cli, ["inspect", csv_file, "--format", "json", "--output", str(output_path)]
    )
    assert result.exit_code == 0
    data = json.loads(output_path.read_text(encoding="utf-8"))
    assert data["profile"]["table_count"] == 1


def test_inspect_multiple_files(csv_file, tmp_path):
    second = tmp_path / "products.csv"
    second.write_text(
        "id,name,price\n1,Widget,9.99\n2,Gadget,19.99\n",
        encoding="utf-8",
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["inspect", csv_file, str(second), "--format", "json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["profile"]["table_count"] == 2


def test_inspect_duckdb_file(test_duckdb_path):
    runner = CliRunner()
    result = runner.invoke(cli, ["inspect", test_duckdb_path])
    assert result.exit_code == 0
    assert "Dataset Profile" in result.output
    assert "orders" in result.output.lower() or "products" in result.output.lower()


def test_inspect_requires_files():
    runner = CliRunner()
    result = runner.invoke(cli, ["inspect"])
    assert result.exit_code != 0


def test_recipes_table_output(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["recipes", "list", "--project-dir", project_dir])
    assert result.exit_code == 0
    assert "Prompt Recipes" in result.output
    assert "ID" in result.output
    assert "Orientation" in result.output
    assert "Why" in result.output


def test_recipes_table_scope(project_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["recipes", "list", "--project-dir", project_dir, "--table", "orders"]
    )
    assert result.exit_code == 0
    assert "Prompt Recipes" in result.output
    assert "orders" in result.output


def test_recipes_table_scope_missing_table(project_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli, ["recipes", "list", "--project-dir", project_dir, "--table", "missing_table"]
    )
    assert result.exit_code != 0
    assert "Table not found: missing_table" in result.output


def test_recipes_markdown_output_writes_file(project_dir, tmp_path):
    output_path = tmp_path / "recipes.md"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "recipes",
            "list",
            "--project-dir",
            project_dir,
            "--format",
            "markdown",
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0
    text = output_path.read_text(encoding="utf-8")
    assert "# Prompt Recipes" in text
    assert "## [1] Profile the biggest tables" in text
    assert "- Why this recipe:" in text


def test_recipes_list_json_includes_ids(project_dir, tmp_path):
    output_path = tmp_path / "recipes.json"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "recipes",
            "list",
            "--project-dir",
            project_dir,
            "--format",
            "json",
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0
    data = json.loads(output_path.read_text(encoding="utf-8"))
    assert data[0]["id"] == 1
    assert "prompt" in data[0]


def test_recipes_run_executes_selected_prompt(monkeypatch, project_dir):
    captured = {}

    async def fake_run_ask_pipeline(**kwargs):
        captured["question"] = kwargs["question"]
        return SimpleNamespace(text="recipe answer", tool_results=[])

    monkeypatch.setattr("datasight.cli._run_ask_pipeline", fake_run_ask_pipeline)

    runner = CliRunner()
    result = runner.invoke(cli, ["recipes", "run", "1", "--project-dir", project_dir])
    assert result.exit_code == 0
    assert "Running recipe [1]" in result.output
    assert captured["question"].startswith("Profile the biggest")


def test_recipes_run_missing_id(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["recipes", "run", "999", "--project-dir", project_dir])
    assert result.exit_code != 0
    assert "Recipe 999 not found." in result.output


def test_ask_file_runs_all_questions(monkeypatch, project_dir, tmp_path):
    questions_path = tmp_path / "questions.txt"
    questions_path.write_text(
        "How many orders are there?\n\nList all products.\n", encoding="utf-8"
    )

    async def fake_run_ask_pipeline(**kwargs):
        return SimpleNamespace(
            text=f"answer: {kwargs['question']}",
            tool_results=[],
        )

    monkeypatch.setattr("datasight.cli._run_ask_pipeline", fake_run_ask_pipeline)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["ask", "--project-dir", project_dir, "--file", str(questions_path)],
    )
    assert result.exit_code == 0
    assert "[1/2] How many orders are there?" in result.output
    assert "[2/2] List all products." in result.output
    assert "Batch complete: 2/2 succeeded." in result.output


def test_ask_file_output_dir_writes_artifacts(monkeypatch, project_dir, tmp_path):
    questions_path = tmp_path / "questions.txt"
    questions_path.write_text("How many orders are there?\n", encoding="utf-8")
    output_dir = tmp_path / "batch-output"

    class FakeFrame:
        empty = False

        def to_csv(self, index=False):  # noqa: ARG002
            return "count\n10\n"

        def to_string(self, index=False):  # noqa: ARG002
            return " count\n    10"

        def to_json(self, orient="records", indent=2):  # noqa: ARG002
            return '[{"count":10}]'

    async def fake_run_ask_pipeline(**kwargs):
        return SimpleNamespace(
            text=f"answer: {kwargs['question']}",
            tool_results=[
                SimpleNamespace(
                    df=FakeFrame(),
                    plotly_spec={"data": [{"type": "bar"}], "layout": {"title": "Chart"}},
                    meta={"title": "Orders"},
                )
            ],
        )

    monkeypatch.setattr("datasight.cli._run_ask_pipeline", fake_run_ask_pipeline)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "ask",
            "--project-dir",
            project_dir,
            "--file",
            str(questions_path),
            "--output-dir",
            str(output_dir),
            "--chart-format",
            "json",
            "--format",
            "csv",
        ],
    )
    assert result.exit_code == 0
    assert "Saved:" in result.output
    answer_file = output_dir / "01-how-many-orders-are-there.answer.txt"
    data_file = output_dir / "01-how-many-orders-are-there.result-1.csv"
    chart_file = output_dir / "01-how-many-orders-are-there.chart-1.json"
    assert answer_file.exists()
    assert data_file.exists()
    assert chart_file.exists()
    assert "answer: How many orders are there?" in answer_file.read_text(encoding="utf-8")
    assert "count" in data_file.read_text(encoding="utf-8")
    assert '"type": "bar"' in chart_file.read_text(encoding="utf-8")


def test_ask_yaml_file_applies_per_entry_overrides(monkeypatch, project_dir, tmp_path):
    questions_path = tmp_path / "questions.yaml"
    questions_path.write_text(
        (
            "- question: How many orders are there?\n"
            "  format: json\n"
            "  chart_format: json\n"
            "  name: orders-summary\n"
        ),
        encoding="utf-8",
    )
    output_dir = tmp_path / "yaml-batch-output"

    class FakeFrame:
        empty = False

        def to_csv(self, index=False):  # noqa: ARG002
            return "count\n10\n"

        def to_string(self, index=False):  # noqa: ARG002
            return " count\n    10"

        def to_json(self, orient="records", indent=2):  # noqa: ARG002
            return '[{"count":10}]'

    async def fake_run_ask_pipeline(**kwargs):
        return SimpleNamespace(
            text="yaml answer",
            tool_results=[
                SimpleNamespace(
                    df=FakeFrame(),
                    plotly_spec={"data": [{"type": "line"}], "layout": {"title": "Chart"}},
                    meta={"title": "Orders"},
                )
            ],
        )

    monkeypatch.setattr("datasight.cli._run_ask_pipeline", fake_run_ask_pipeline)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "ask",
            "--project-dir",
            project_dir,
            "--file",
            str(questions_path),
            "--output-dir",
            str(output_dir),
        ],
    )
    assert result.exit_code == 0
    data_file = output_dir / "01-orders-summary.result-1.json"
    chart_file = output_dir / "01-orders-summary.chart-1.json"
    assert data_file.exists()
    assert chart_file.exists()
    assert '"count":10' in data_file.read_text(encoding="utf-8")
    assert '"type": "line"' in chart_file.read_text(encoding="utf-8")


def test_ask_yaml_file_supports_output_base_override(monkeypatch, project_dir, tmp_path):
    questions_path = tmp_path / "questions.yaml"
    questions_path.write_text(
        (
            "- question: How many orders are there?\n"
            "  format: json\n"
            "  chart_format: json\n"
            "  output: reports/orders-summary\n"
        ),
        encoding="utf-8",
    )
    output_dir = tmp_path / "batch-output"

    class FakeFrame:
        empty = False

        def to_csv(self, index=False):  # noqa: ARG002
            return "count\n10\n"

        def to_string(self, index=False):  # noqa: ARG002
            return " count\n    10"

        def to_json(self, orient="records", indent=2):  # noqa: ARG002
            return '[{"count":10}]'

    async def fake_run_ask_pipeline(**kwargs):
        return SimpleNamespace(
            text="yaml answer",
            tool_results=[
                SimpleNamespace(
                    df=FakeFrame(),
                    plotly_spec={"data": [{"type": "line"}], "layout": {"title": "Chart"}},
                    meta={"title": "Orders"},
                )
            ],
        )

    monkeypatch.setattr("datasight.cli._run_ask_pipeline", fake_run_ask_pipeline)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "ask",
            "--project-dir",
            project_dir,
            "--file",
            str(questions_path),
            "--output-dir",
            str(output_dir),
        ],
    )
    assert result.exit_code == 0
    data_file = output_dir / "reports" / "orders-summary.result-1.json"
    chart_file = output_dir / "reports" / "orders-summary.chart-1.json"
    answer_file = output_dir / "reports" / "orders-summary.answer.txt"
    assert answer_file.exists()
    assert data_file.exists()
    assert chart_file.exists()


def test_ask_jsonl_file_supports_output_without_output_dir(monkeypatch, project_dir, tmp_path):
    questions_path = tmp_path / "questions.jsonl"
    target_base = tmp_path / "named-output" / "job"
    questions_path.write_text(
        json.dumps(
            {
                "question": "How many orders are there?",
                "format": "json",
                "output": str(target_base),
            }
        )
        + "\n",
        encoding="utf-8",
    )

    class FakeFrame:
        empty = False

        def to_csv(self, index=False):  # noqa: ARG002
            return "count\n10\n"

        def to_string(self, index=False):  # noqa: ARG002
            return " count\n    10"

        def to_json(self, orient="records", indent=2):  # noqa: ARG002
            return '[{"count":10}]'

    async def fake_run_ask_pipeline(**kwargs):
        return SimpleNamespace(
            text="jsonl answer",
            tool_results=[SimpleNamespace(df=FakeFrame(), plotly_spec=None, meta={})],
        )

    monkeypatch.setattr("datasight.cli._run_ask_pipeline", fake_run_ask_pipeline)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["ask", "--project-dir", project_dir, "--file", str(questions_path)],
    )
    assert result.exit_code == 0
    assert Path(str(target_base) + ".answer.txt").exists()
    assert Path(str(target_base) + ".result-1.json").exists()


def test_ask_yaml_file_rejects_invalid_format(project_dir, tmp_path):
    questions_path = tmp_path / "questions.yaml"
    questions_path.write_text(
        ("- question: How many orders are there?\n  format: parquet\n"),
        encoding="utf-8",
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["ask", "--project-dir", project_dir, "--file", str(questions_path)],
    )
    assert result.exit_code != 0
    assert "invalid format" in result.output


def test_ask_jsonl_file_rejects_invalid_chart_format(project_dir, tmp_path):
    questions_path = tmp_path / "questions.jsonl"
    questions_path.write_text(
        json.dumps(
            {
                "question": "How many orders are there?",
                "chart_format": "svg",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["ask", "--project-dir", project_dir, "--file", str(questions_path)],
    )
    assert result.exit_code != 0
    assert "invalid chart_format" in result.output


def test_doctor_reports_healthy_project(project_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["doctor", "--project-dir", project_dir])
    assert result.exit_code == 0
    assert "datasight doctor" in result.output
    assert "Database connectivity" in result.output
    assert "schema_description.md" in result.output


def test_doctor_json_output_writes_file(project_dir, tmp_path):
    output_path = tmp_path / "doctor.json"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["doctor", "--project-dir", project_dir, "--format", "json", "--output", str(output_path)],
    )
    assert result.exit_code == 0
    data = json.loads(output_path.read_text(encoding="utf-8"))
    assert data["failures"] == 0
    assert any(check["name"] == "Database connectivity" for check in data["checks"])


def test_doctor_markdown_output_writes_file(project_dir, tmp_path):
    output_path = tmp_path / "doctor.md"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "doctor",
            "--project-dir",
            project_dir,
            "--format",
            "markdown",
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0
    text = output_path.read_text(encoding="utf-8")
    assert "# datasight doctor" in text
    assert "## Checks" in text


def test_doctor_fails_when_required_files_missing(tmp_path, test_duckdb_path):
    (tmp_path / ".env").write_text(
        (
            "LLM_PROVIDER=ollama\n"
            "OLLAMA_MODEL=qwen3.5:35b-a3b\n"
            "DB_MODE=duckdb\n"
            f"DB_PATH={test_duckdb_path}\n"
        ),
        encoding="utf-8",
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["doctor", "--project-dir", str(tmp_path)])
    assert result.exit_code == 1
    assert "queries.yaml" in result.output
    assert "FAIL" in result.output


def test_generate_seeds_measure_overrides(project_dir, monkeypatch):
    class StubClient:
        async def create_message(self, **kwargs):  # noqa: ARG002
            return SimpleNamespace(
                content=[
                    TextBlock(
                        "--- schema_description.md ---\n"
                        "# Generated schema\n\n"
                        "--- queries.yaml ---\n"
                        "- question: Example\n"
                        "  sql: SELECT 1\n"
                    )
                ]
            )

    monkeypatch.setattr("datasight.cli.create_llm_client", lambda **kwargs: StubClient())

    project_path = Path(project_dir)
    (project_path / "schema_description.md").unlink()
    (project_path / "queries.yaml").unlink()

    runner = CliRunner()
    result = runner.invoke(cli, ["generate", "--project-dir", project_dir, "--overwrite"])

    assert result.exit_code == 0
    assert "Created:" in result.output
    assert "measures.yaml" in result.output
    assert (project_path / "measures.yaml").exists()
    measures_text = (project_path / "measures.yaml").read_text(encoding="utf-8")
    assert "# datasight measure overrides" in measures_text


def test_prompt_recipes_include_calculated_measure_formula():
    import asyncio

    from datasight.data_profile import build_prompt_recipes

    schema_info = [
        {
            "name": "load_hourly",
            "row_count": 24,
            "columns": [
                {"name": "report_time", "dtype": "TIMESTAMP", "nullable": False},
                {"name": "load_mw", "dtype": "DOUBLE", "nullable": False},
                {"name": "renewable_generation_mw", "dtype": "DOUBLE", "nullable": False},
            ],
        }
    ]
    overrides = [
        {
            "table": "load_hourly",
            "name": "net_load_mw",
            "expression": "load_mw - renewable_generation_mw",
            "role": "power",
            "default_aggregation": "avg",
            "reason": "Project-defined net load measure.",
        }
    ]

    async def fake_run_sql(sql):
        if "MIN(" in sql and "MAX(" in sql:
            return __import__("pandas").DataFrame(
                [{"min_value": "2024-01-01 00:00:00", "max_value": "2024-01-02 00:00:00"}]
            )
        return __import__("pandas").DataFrame()

    recipes = asyncio.run(build_prompt_recipes(schema_info, fake_run_sql, overrides))
    joined = "\n".join(recipe["prompt"] for recipe in recipes)
    assert "net_load_mw" in joined
    assert "AVG(load_mw - renewable_generation_mw) AS avg_net_load_mw" in joined


# ---------------------------------------------------------------------------
# Ask SQL helpers (--print-sql / --sql-script)
# ---------------------------------------------------------------------------


def _make_sql_result(text="answer", queries=None):
    """Build a fake AgentResult-shaped namespace for SQL helper tests."""
    queries = queries or []
    tool_results = []
    for q in queries:
        tool_results.append(
            SimpleNamespace(
                df=None,
                plotly_spec=None,
                meta={
                    "tool": q.get("tool", "run_sql"),
                    "sql": q["sql"],
                    "formatted_sql": q.get("formatted_sql", q["sql"]),
                    "error": q.get("error"),
                },
            )
        )
    return SimpleNamespace(
        text=text,
        tool_results=tool_results,
        total_input_tokens=0,
        total_output_tokens=0,
        api_calls=0,
    )


def test_sanitize_sql_identifier_basic():
    from datasight.cli import _sanitize_sql_identifier

    assert _sanitize_sql_identifier("Top 5 states by generation") == "top_5_states_by_generation"
    assert _sanitize_sql_identifier("  ??  ") == "query"
    assert _sanitize_sql_identifier("123 widgets") == "q_123_widgets"
    # Long input is capped at the 32-char slug limit
    assert len(_sanitize_sql_identifier("a" * 200)) <= 32


def test_sanitize_sql_identifier_strips_non_ascii():
    """Non-ASCII alphanumerics must be replaced — Postgres folds unquoted
    identifiers and truncates at 63 *bytes*, so multi-byte UTF-8 chars
    can blow that budget on a per-character truncation scheme.
    """
    from datasight.cli import _question_table_prefix, _sanitize_sql_identifier

    # Japanese, French, accents, emoji — all stripped to underscores then collapsed.
    assert _sanitize_sql_identifier("日本語の質問") == "query"
    assert _sanitize_sql_identifier("café revenue") == "caf_revenue"
    assert _sanitize_sql_identifier("naïve résumé") == "na_ve_r_sum"
    # Mixed: ASCII parts survive, non-ASCII parts are dropped.
    assert _sanitize_sql_identifier("top 5 日本") == "top_5"
    # The full table prefix (slug + hash) for a worst-case 200-byte
    # multi-byte question must still fit within Postgres' 63-byte limit
    # even after a `_<n>` suffix is appended.
    huge = "日" * 200
    prefix = _question_table_prefix(huge)
    assert len(f"{prefix}_999".encode("utf-8")) <= 63


def test_question_table_prefix_distinguishes_long_questions():
    """Two long questions sharing the same first sanitized chars must not collide."""
    from datasight.cli import _question_table_prefix, _sanitize_sql_identifier

    q1 = (
        "What are the top 10 products by revenue in the western region for "
        "the last fiscal quarter that we tracked"
    )
    q2 = (
        "What are the top 10 products by revenue in the western region for "
        "the entire calendar year of 2024"
    )
    # Sanity-check the precondition: the bare slugs DO collide.
    assert _sanitize_sql_identifier(q1) == _sanitize_sql_identifier(q2)
    # The full prefix must NOT collide, thanks to the hash suffix.
    assert _question_table_prefix(q1) != _question_table_prefix(q2)
    # And the prefix must be deterministic for the same question.
    assert _question_table_prefix(q1) == _question_table_prefix(q1)


def test_build_sql_script_duckdb_create_or_replace():
    from datasight.cli import _build_sql_script, _question_table_prefix

    result = _make_sql_result(
        queries=[
            {"sql": "SELECT 1 AS x", "formatted_sql": "SELECT 1 AS x"},
            {"sql": "SELECT 2 AS y", "formatted_sql": "SELECT 2 AS y"},
        ]
    )
    prefix = _question_table_prefix("Top widgets")
    script = _build_sql_script(result, "Top widgets", "duckdb")
    assert "-- Question: Top widgets" in script
    assert "-- Dialect: duckdb" in script
    assert f"CREATE OR REPLACE TABLE {prefix}_1 AS" in script
    assert "SELECT 1 AS x;" in script
    assert f"CREATE OR REPLACE TABLE {prefix}_2 AS" in script
    assert "SELECT 2 AS y;" in script
    assert "DROP TABLE" not in script


def test_build_sql_script_postgres_uses_drop_then_create():
    from datasight.cli import _build_sql_script, _question_table_prefix

    result = _make_sql_result(queries=[{"sql": "SELECT * FROM t"}])
    prefix = _question_table_prefix("list rows")
    script = _build_sql_script(result, "list rows", "postgres")
    assert f"DROP TABLE IF EXISTS {prefix}_1;" in script
    assert f"CREATE TABLE {prefix}_1 AS" in script
    assert "CREATE OR REPLACE TABLE" not in script


def test_build_sql_script_skips_errored_queries():
    from datasight.cli import _build_sql_script, _question_table_prefix

    result = _make_sql_result(
        queries=[
            {"sql": "SELECT 1", "error": "boom"},
            {"sql": "SELECT 2"},
        ]
    )
    prefix = _question_table_prefix("q")
    script = _build_sql_script(result, "q", "duckdb")
    assert "-- Skipped attempt (errored, not materialized):" in script
    assert "--   error: boom" in script
    # Failed attempts must NOT consume a table-name index — the lone
    # successful query lands on _1, not _2.
    assert f"CREATE OR REPLACE TABLE {prefix}_1 AS" in script
    assert f"CREATE OR REPLACE TABLE {prefix}_2 AS" not in script


def test_build_sql_script_table_names_stable_across_retries():
    """Same final result must land on the same table name regardless of
    how many failed attempts preceded it — otherwise rerunning the same
    question against a different agent attempt sequence leaves stale
    tables behind.
    """
    from datasight.cli import _build_sql_script, _question_table_prefix

    # Run A: agent succeeds on the first try.
    result_a = _make_sql_result(queries=[{"sql": "SELECT final"}])
    # Run B: agent retries twice before succeeding with the same query.
    result_b = _make_sql_result(
        queries=[
            {"sql": "SELECT bad1", "error": "syntax err"},
            {"sql": "SELECT bad2", "error": "missing col"},
            {"sql": "SELECT final"},
        ]
    )
    prefix = _question_table_prefix("same q")
    script_a = _build_sql_script(result_a, "same q", "duckdb")
    script_b = _build_sql_script(result_b, "same q", "duckdb")
    # Both runs must materialize the final result on _1.
    assert f"CREATE OR REPLACE TABLE {prefix}_1 AS" in script_a
    assert f"CREATE OR REPLACE TABLE {prefix}_1 AS" in script_b
    # And neither should leak a _2 / _3 from the failed attempts.
    assert f"CREATE OR REPLACE TABLE {prefix}_2 AS" not in script_b
    assert f"CREATE OR REPLACE TABLE {prefix}_3 AS" not in script_b


def test_build_sql_script_no_queries():
    from datasight.cli import _build_sql_script

    script = _build_sql_script(_make_sql_result(), "anything", "duckdb")
    assert "(no SQL queries were executed)" in script


def test_build_sql_script_escapes_newlines_in_question():
    """A newline in the question must not escape the header comment."""
    from datasight.cli import _build_sql_script

    result = _make_sql_result(queries=[{"sql": "SELECT 1"}])
    script = _build_sql_script(result, "top rows\nDROP TABLE important;", "duckdb")
    # Every non-empty line above the generated DDL must be a SQL comment —
    # in particular, the malicious second line must still be commented.
    for line in script.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        # The only non-comment lines should be the generated CREATE/SELECT.
        assert stripped.startswith(("CREATE ", "SELECT ")), f"leaked line: {line!r}"
    assert "--   DROP TABLE important;" in script


def test_build_sql_script_escapes_newlines_in_error():
    """Multi-line SQL error messages must stay commented in the script."""
    from datasight.cli import _build_sql_script

    result = _make_sql_result(
        queries=[{"sql": "SELECT 1", "error": "bad thing\nDROP TABLE users;"}]
    )
    script = _build_sql_script(result, "q", "duckdb")
    for line in script.splitlines():
        stripped = line.strip()
        assert not stripped or stripped.startswith("--"), f"leaked line: {line!r}"
    assert "--   DROP TABLE users;" in script


def test_ask_print_sql_outputs_queries_to_stderr(monkeypatch, project_dir):
    async def fake_run_ask_pipeline(**kwargs):
        return _make_sql_result(
            text="here are the rows",
            queries=[{"sql": "SELECT count(*) FROM orders"}],
        )

    monkeypatch.setattr("datasight.cli._run_ask_pipeline", fake_run_ask_pipeline)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["ask", "--project-dir", project_dir, "How many orders?", "--print-sql"],
    )
    assert result.exit_code == 0, result.output
    # SQL must appear on stderr so stdout stays clean for pipelines.
    assert "SQL queries executed" in result.stderr
    assert "SELECT count(*) FROM orders;" in result.stderr
    assert "SQL queries executed" not in result.stdout
    assert "SELECT count(*) FROM orders" not in result.stdout


def test_ask_provenance_outputs_json_to_stderr(monkeypatch, project_dir):
    async def fake_run_ask_pipeline(**kwargs):
        return _make_sql_result(
            text="here are the rows",
            queries=[
                {
                    "sql": "SELECT count(*) FROM orders",
                    "formatted_sql": "SELECT\n  count(*)\nFROM orders",
                }
            ],
        )

    monkeypatch.setattr("datasight.cli._run_ask_pipeline", fake_run_ask_pipeline)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["ask", "--project-dir", project_dir, "How many orders?", "--provenance"],
    )
    assert result.exit_code == 0, result.output
    assert "here are the rows" in result.stdout
    provenance = json.loads(result.stderr)
    assert provenance["question"] == "How many orders?"
    assert provenance["dialect"] == "duckdb"
    assert provenance["tools"][0]["formatted_sql"] == "SELECT\n  count(*)\nFROM orders"
    assert provenance["tools"][0]["validation"]["status"] == "not_run"


def test_ask_print_sql_keeps_json_stdout_parseable(monkeypatch, project_dir):
    """--print-sql must not corrupt --format json output on stdout."""

    class FakeFrame:
        empty = False
        columns = ["count"]

        def __len__(self):
            return 1

        def head(self, n):  # noqa: ARG002
            return self

        def iterrows(self):
            yield 0, [10]

        def to_csv(self, index=False):  # noqa: ARG002
            return "count\n10\n"

        def to_json(self, orient="records", indent=2):  # noqa: ARG002
            return '[\n  {\n    "count": 10\n  }\n]'

    async def fake_run_ask_pipeline(**kwargs):
        return SimpleNamespace(
            text="",
            tool_results=[
                SimpleNamespace(
                    df=FakeFrame(),
                    plotly_spec=None,
                    meta={
                        "tool": "run_sql",
                        "sql": "SELECT count(*) FROM orders",
                        "formatted_sql": "SELECT count(*) FROM orders",
                        "error": None,
                    },
                )
            ],
            suggestions=[],
            total_input_tokens=0,
            total_output_tokens=0,
            api_calls=0,
        )

    monkeypatch.setattr("datasight.cli._run_ask_pipeline", fake_run_ask_pipeline)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "ask",
            "--project-dir",
            project_dir,
            "How many orders?",
            "--format",
            "json",
            "--print-sql",
        ],
    )
    assert result.exit_code == 0, result.output
    # stdout must be valid JSON despite --print-sql being set.
    data = json.loads(result.stdout.strip())
    assert data == [{"count": 10}]
    # stderr carries the SQL diagnostics.
    assert "SELECT count(*) FROM orders;" in result.stderr


def test_ask_sql_script_writes_file(monkeypatch, project_dir, tmp_path):
    from datasight.cli import _question_table_prefix

    async def fake_run_ask_pipeline(**kwargs):
        return _make_sql_result(
            queries=[
                {"sql": "SELECT 1 AS a"},
                {"sql": "SELECT 2 AS b", "tool": "visualize_data"},
            ]
        )

    monkeypatch.setattr("datasight.cli._run_ask_pipeline", fake_run_ask_pipeline)

    script_path = tmp_path / "out" / "queries.sql"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "ask",
            "--project-dir",
            project_dir,
            "Top 5 states",
            "--sql-script",
            str(script_path),
        ],
    )
    assert result.exit_code == 0, result.output
    assert script_path.exists()
    content = script_path.read_text(encoding="utf-8")
    prefix = _question_table_prefix("Top 5 states")
    assert "-- Question: Top 5 states" in content
    assert f"CREATE OR REPLACE TABLE {prefix}_1 AS" in content
    assert f"CREATE OR REPLACE TABLE {prefix}_2 AS" in content
    # The "saved to" confirmation is a diagnostic — it must land on
    # stderr so it does not corrupt machine-readable stdout.
    assert f"SQL script saved to {script_path}" in result.stderr
    assert f"SQL script saved to {script_path}" not in result.stdout


def test_ask_sql_script_keeps_json_stdout_parseable(monkeypatch, project_dir, tmp_path):
    """--sql-script must not corrupt --format json output on stdout."""

    class FakeFrame:
        empty = False
        columns = ["count"]

        def __len__(self):
            return 1

        def head(self, n):  # noqa: ARG002
            return self

        def iterrows(self):
            yield 0, [10]

        def to_csv(self, index=False):  # noqa: ARG002
            return "count\n10\n"

        def to_json(self, orient="records", indent=2):  # noqa: ARG002
            return '[\n  {\n    "count": 10\n  }\n]'

    async def fake_run_ask_pipeline(**kwargs):
        return SimpleNamespace(
            text="",
            tool_results=[
                SimpleNamespace(
                    df=FakeFrame(),
                    plotly_spec=None,
                    meta={
                        "tool": "run_sql",
                        "sql": "SELECT count(*) FROM orders",
                        "formatted_sql": "SELECT count(*) FROM orders",
                        "error": None,
                    },
                )
            ],
            suggestions=[],
            total_input_tokens=0,
            total_output_tokens=0,
            api_calls=0,
        )

    monkeypatch.setattr("datasight.cli._run_ask_pipeline", fake_run_ask_pipeline)

    script_path = tmp_path / "queries.sql"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "ask",
            "--project-dir",
            project_dir,
            "How many orders?",
            "--format",
            "json",
            "--sql-script",
            str(script_path),
        ],
    )
    assert result.exit_code == 0, result.output
    # stdout must be valid JSON despite --sql-script being set.
    data = json.loads(result.stdout.strip())
    assert data == [{"count": 10}]
    # stderr carries the diagnostic confirmation.
    assert f"SQL script saved to {script_path}" in result.stderr
    # And the script file itself was actually written.
    assert script_path.exists()


def test_query_logger_tolerates_unwritable_parent(tmp_path):
    """A read-only project dir must not crash QueryLogger construction.

    Regression: ``datasight ask`` constructs a QueryLogger unconditionally,
    so a hard failure here would turn ask into a hard failure on read-only
    project directories.
    """
    import os
    import stat

    from datasight.query_log import QueryLogger

    readonly_dir = tmp_path / "readonly-project"
    readonly_dir.mkdir()
    os.chmod(readonly_dir, stat.S_IRUSR | stat.S_IXUSR)
    try:
        ql = QueryLogger(path=str(readonly_dir / ".datasight" / "query_log.jsonl"))
        # log() must also swallow the subsequent write failure.
        ql.log(
            session_id="s",
            user_question="q",
            tool="run_sql",
            sql="SELECT 1",
            execution_time_ms=1.0,
        )
    finally:
        os.chmod(readonly_dir, 0o700)


def test_ask_sql_script_rejects_with_file(project_dir, tmp_path):
    questions_path = tmp_path / "questions.txt"
    questions_path.write_text("How many orders?\n", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "ask",
            "--project-dir",
            project_dir,
            "--file",
            str(questions_path),
            "--sql-script",
            str(tmp_path / "out.sql"),
        ],
    )
    assert result.exit_code != 0
    assert "--sql-script cannot be combined with --file" in result.output


# ---------------------------------------------------------------------------
# Cost logging
# ---------------------------------------------------------------------------


def test_build_cost_data_known_model_returns_estimated_cost():
    from datasight.cost import build_cost_data

    data = build_cost_data(
        "claude-sonnet-4-20250514",
        api_calls=2,
        input_tokens=1_000_000,
        output_tokens=1_000_000,
    )
    assert data["api_calls"] == 2
    assert data["input_tokens"] == 1_000_000
    assert data["output_tokens"] == 1_000_000
    # 1M input @ $3 + 1M output @ $15 = $18.00
    assert data["estimated_cost"] == 18.0


def test_build_cost_data_unknown_model_returns_none_cost():
    from datasight.cost import build_cost_data

    data = build_cost_data(
        "made-up-model",
        api_calls=1,
        input_tokens=100,
        output_tokens=200,
    )
    assert data["api_calls"] == 1
    assert data["input_tokens"] == 100
    assert data["output_tokens"] == 200
    assert data["estimated_cost"] is None


def test_run_ask_pipeline_logs_cost_entry(monkeypatch, project_dir):
    """``datasight ask`` must persist a turn-level cost summary to the query log."""
    import asyncio

    from datasight import cli as cli_module
    from datasight.agent import AgentResult
    from datasight.settings import Settings

    # Stub out the LLM client and SQL runner so the pipeline does no real I/O.
    class FakeRunner:
        async def run_sql(self, sql, **kwargs):  # noqa: ARG002
            return None

    monkeypatch.setattr(cli_module, "create_llm_client", lambda **kwargs: object())
    monkeypatch.setattr(
        cli_module,
        "create_sql_runner_from_settings",
        lambda settings, project_dir: FakeRunner(),
    )

    async def fake_introspect_schema(run_sql, runner=None, allowed_tables=None):  # noqa: ARG001
        return []

    monkeypatch.setattr("datasight.schema.introspect_schema", fake_introspect_schema)

    async def fake_run_agent_loop(**kwargs):
        return AgentResult(
            text="answered",
            tool_results=[],
            total_input_tokens=1500,
            total_output_tokens=400,
            api_calls=3,
        )

    monkeypatch.setattr("datasight.agent.run_agent_loop", fake_run_agent_loop)

    env_path = Path(project_dir) / ".env"
    settings = Settings.from_env(str(env_path))
    asyncio.run(
        cli_module._run_ask_pipeline(
            question="How many orders are there?",
            settings=settings,
            resolved_model="claude-sonnet-4-20250514",
            project_dir=project_dir,
            sql_dialect="duckdb",
        )
    )

    log_path = Path(project_dir) / ".datasight" / "query_log.jsonl"
    assert log_path.exists(), "query log should be created by _run_ask_pipeline"
    entries = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]
    cost_entries = [e for e in entries if e.get("type") == "cost"]
    assert len(cost_entries) == 1
    cost = cost_entries[0]
    assert cost["api_calls"] == 3
    assert cost["input_tokens"] == 1500
    assert cost["output_tokens"] == 400
    assert cost["user_question"] == "How many orders are there?"
    # Sonnet pricing: 1500 in @ $3/M + 400 out @ $15/M = 0.0045 + 0.006 = 0.0105
    assert cost["estimated_cost"] == 0.0105


def test_run_ask_pipeline_includes_measure_guidance_in_prompt(monkeypatch, project_dir):
    """The ask pipeline should append inferred measure semantics to prompt context."""
    import asyncio

    from datasight import cli as cli_module
    from datasight.agent import AgentResult
    from datasight.settings import Settings

    captured: dict[str, str] = {}

    class FakeRunner:
        async def run_sql(self, sql, **kwargs):  # noqa: ARG002
            return None

    monkeypatch.setattr(cli_module, "create_llm_client", lambda **kwargs: object())
    monkeypatch.setattr(
        cli_module,
        "create_sql_runner_from_settings",
        lambda settings, project_dir: FakeRunner(),
    )

    async def fake_introspect_schema(run_sql, runner=None, allowed_tables=None):  # noqa: ARG001
        return [
            SimpleNamespace(
                name="generation_hourly",
                row_count=24,
                columns=[
                    SimpleNamespace(name="report_time", dtype="TIMESTAMP", nullable=False),
                    SimpleNamespace(name="net_generation_mwh", dtype="DOUBLE", nullable=True),
                    SimpleNamespace(name="demand_mw", dtype="DOUBLE", nullable=True),
                ],
            )
        ]

    monkeypatch.setattr("datasight.schema.introspect_schema", fake_introspect_schema)

    def fake_build_system_prompt(schema_text, **kwargs):
        captured["schema_text"] = schema_text
        return "PROMPT"

    monkeypatch.setattr("datasight.prompts.build_system_prompt", fake_build_system_prompt)

    async def fake_run_agent_loop(**kwargs):
        captured["system_prompt"] = kwargs["system_prompt"]
        return AgentResult(
            text="answered",
            tool_results=[],
            total_input_tokens=0,
            total_output_tokens=0,
            api_calls=0,
        )

    monkeypatch.setattr("datasight.agent.run_agent_loop", fake_run_agent_loop)

    settings = Settings.from_env(str(Path(project_dir) / ".env"))
    asyncio.run(
        cli_module._run_ask_pipeline(
            question="Show generation over time",
            settings=settings,
            resolved_model="claude-sonnet-4-20250514",
            project_dir=project_dir,
            sql_dialect="duckdb",
        )
    )

    schema_text = captured["schema_text"]
    assert "## Inferred Measure Semantics" in schema_text
    assert "generation_hourly.net_generation_mwh" in schema_text
    assert "default=sum" in schema_text
    assert "generation_hourly.demand_mw" in schema_text
    assert "avoid=sum" in schema_text
    assert captured["system_prompt"] == "PROMPT"


def test_run_ask_pipeline_uses_measure_semantics_for_energy_power_weighted_and_calculated(
    monkeypatch, project_dir
):
    import asyncio

    from datasight import cli as cli_module
    from datasight.settings import Settings

    executed_sql: list[str] = []

    class FakeRunner:
        async def run_sql(self, sql, **kwargs):  # noqa: ARG002
            executed_sql.append(sql)
            return pd.DataFrame([{"value": 1}])

    class SemanticAwareFakeClient:
        async def create_message(self, *, system, messages, **kwargs):  # noqa: ARG002
            last_message = messages[-1]["content"]
            if isinstance(last_message, str):
                question = last_message.lower()
                if "generation" in question:
                    sql = (
                        "SELECT DATE_TRUNC('day', report_time) AS day, "
                        "SUM(net_generation_mwh) AS total_net_generation_mwh "
                        "FROM generation_hourly GROUP BY 1 ORDER BY 1"
                    )
                elif "peak demand" in question:
                    sql = (
                        "SELECT DATE_TRUNC('day', report_time) AS day, "
                        "MAX(demand_mw) AS peak_demand_mw "
                        "FROM generation_hourly GROUP BY 1 ORDER BY 1"
                    )
                elif "demand" in question:
                    sql = (
                        "SELECT DATE_TRUNC('day', report_time) AS day, "
                        "AVG(demand_mw) AS avg_demand_mw "
                        "FROM generation_hourly GROUP BY 1 ORDER BY 1"
                    )
                elif "co2" in question or "emissions" in question:
                    sql = (
                        "SELECT DATE_TRUNC('day', report_time) AS day, "
                        "SUM(co2_rate_lb_per_mwh * net_generation_mwh) / "
                        "NULLIF(SUM(net_generation_mwh), 0) AS weighted_avg_co2_rate_lb_per_mwh "
                        "FROM generation_hourly GROUP BY 1 ORDER BY 1"
                    )
                elif "net load" in question:
                    sql = (
                        "SELECT DATE_TRUNC('day', report_time) AS day, "
                        "AVG(load_mw - renewable_generation_mw) AS avg_net_load_mw "
                        "FROM generation_hourly GROUP BY 1 ORDER BY 1"
                    )
                else:
                    raise AssertionError(f"unexpected question: {question}")

                if "generation" in question:
                    assert "generation_hourly.net_generation_mwh: role=energy" in system
                    assert "default=sum" in system
                elif "peak demand" in question:
                    assert "generation_hourly.demand_mw: role=power" in system
                    assert "default=max" in system
                elif "demand" in question:
                    assert "generation_hourly.demand_mw: role=power" in system
                    assert "default=avg" in system
                    assert "avoid=sum" in system
                elif "co2" in question or "emissions" in question:
                    assert "weight=net_generation_mwh, average=weighted_avg" in system
                elif "net load" in question:
                    assert "generation_hourly.net_load_mw: role=power" in system
                    assert "expression=load_mw - renewable_generation_mw" in system

                return LLMResponse(
                    content=[ToolUseBlock(id="tool-1", name="run_sql", input={"sql": sql})],
                    stop_reason="tool_use",
                    usage=Usage(),
                )

            return LLMResponse(
                content=[TextBlock("done")],
                stop_reason="end_turn",
                usage=Usage(),
            )

    monkeypatch.setattr(
        cli_module, "create_llm_client", lambda **kwargs: SemanticAwareFakeClient()
    )
    monkeypatch.setattr(
        cli_module,
        "create_sql_runner_from_settings",
        lambda settings, project_dir: FakeRunner(),
    )

    async def fake_introspect_schema(run_sql, runner=None, allowed_tables=None):  # noqa: ARG001
        return [
            SimpleNamespace(
                name="generation_hourly",
                row_count=24,
                columns=[
                    SimpleNamespace(name="report_time", dtype="TIMESTAMP", nullable=False),
                    SimpleNamespace(name="net_generation_mwh", dtype="DOUBLE", nullable=True),
                    SimpleNamespace(name="demand_mw", dtype="DOUBLE", nullable=True),
                    SimpleNamespace(name="co2_rate_lb_per_mwh", dtype="DOUBLE", nullable=True),
                    SimpleNamespace(name="load_mw", dtype="DOUBLE", nullable=True),
                    SimpleNamespace(name="renewable_generation_mw", dtype="DOUBLE", nullable=True),
                ],
            )
        ]

    monkeypatch.setattr("datasight.schema.introspect_schema", fake_introspect_schema)

    measures_path = Path(project_dir) / "measures.yaml"
    measures_path.write_text(
        (
            "- table: generation_hourly\n"
            "  name: net_load_mw\n"
            "  expression: load_mw - renewable_generation_mw\n"
            "  role: power\n"
            "  default_aggregation: avg\n"
            "  reason: Project-defined net load measure.\n"
        ),
        encoding="utf-8",
    )

    settings = Settings.from_env(str(Path(project_dir) / ".env"))

    async def run_question(question: str):
        return await cli_module._run_ask_pipeline(
            question=question,
            settings=settings,
            resolved_model="claude-sonnet-4-20250514",
            project_dir=project_dir,
            sql_dialect="duckdb",
        )

    asyncio.run(run_question("Show generation over time"))
    asyncio.run(run_question("Show demand over time"))
    asyncio.run(run_question("Show average CO2 emissions rate over time"))
    asyncio.run(run_question("Show net load over time"))

    measures_path.write_text(
        (
            "- table: generation_hourly\n"
            "  column: demand_mw\n"
            "  default_aggregation: max\n"
            "  reason: This project wants peak demand by default.\n"
        ),
        encoding="utf-8",
    )
    asyncio.run(run_question("Show peak demand over time"))

    assert any("SUM(net_generation_mwh)" in sql for sql in executed_sql)
    assert any("AVG(demand_mw)" in sql for sql in executed_sql)
    assert any("MAX(demand_mw)" in sql for sql in executed_sql)
    assert any(
        "SUM(co2_rate_lb_per_mwh * net_generation_mwh) / NULLIF(SUM(net_generation_mwh), 0)" in sql
        for sql in executed_sql
    )
    assert any("AVG(load_mw - renewable_generation_mw)" in sql for sql in executed_sql)


def test_run_agent_loop_regenerates_sql_after_measure_validation_failure():
    import asyncio

    from datasight.agent import run_agent_loop
    from datasight.sql_validation import build_measure_rule_map

    executed_sql: list[str] = []

    class FakeClient:
        async def create_message(self, *, messages, **kwargs):  # noqa: ARG002
            last_message = messages[-1]["content"]
            if isinstance(last_message, str):
                return LLMResponse(
                    content=[
                        ToolUseBlock(
                            id="tool-1",
                            name="run_sql",
                            input={
                                "sql": (
                                    "SELECT report_date, SUM(net_generation_mwh) AS wind_generation_mwh "
                                    "FROM generation_fuel "
                                    "GROUP BY report_date ORDER BY report_date"
                                )
                            },
                        )
                    ],
                    stop_reason="tool_use",
                    usage=Usage(),
                )

            tool_result_text = last_message[0]["content"]
            if "SQL validation error:" in tool_result_text:
                assert "not allowed" in tool_result_text
                return LLMResponse(
                    content=[
                        ToolUseBlock(
                            id="tool-2",
                            name="run_sql",
                            input={
                                "sql": (
                                    "SELECT report_date, MAX(net_generation_mwh) AS wind_generation_mwh "
                                    "FROM generation_fuel "
                                    "GROUP BY report_date ORDER BY report_date"
                                )
                            },
                        )
                    ],
                    stop_reason="tool_use",
                    usage=Usage(),
                )

            return LLMResponse(
                content=[TextBlock("done")],
                stop_reason="end_turn",
                usage=Usage(),
            )

    async def fake_run_sql(sql, **kwargs):  # noqa: ARG001
        executed_sql.append(sql)
        return pd.DataFrame([{"report_date": "2024-01-01", "wind_generation_mwh": 123.0}])

    schema_map = {
        "generation_fuel": {"report_date", "net_generation_mwh"},
    }
    measure_rules = build_measure_rule_map(
        [
            {
                "table": "generation_fuel",
                "column": "net_generation_mwh",
                "default_aggregation": "max",
                "allowed_aggregations": ["max"],
            }
        ]
    )

    result = asyncio.run(
        run_agent_loop(
            question="make a plot of net_generation_mwh for wind over time",
            llm_client=FakeClient(),
            model="test-model",
            system_prompt="PROMPT",
            run_sql=fake_run_sql,
            schema_map=schema_map,
            dialect="duckdb",
            measure_rules=measure_rules,
        )
    )

    assert result.text == "done"
    assert executed_sql == [
        "SELECT report_date, MAX(net_generation_mwh) AS wind_generation_mwh "
        "FROM generation_fuel "
        "GROUP BY report_date ORDER BY report_date"
    ]
    assert len(result.tool_results) == 2
    assert result.tool_results[0].meta["error"] is not None
    assert "not allowed" in result.tool_results[0].meta["error"]
    assert result.tool_results[1].meta["error"] is None


def test_run_ask_pipeline_session_ids_unique_within_second(monkeypatch, project_dir):
    """Two CLI runs started in the same wall-clock second must get
    distinct session ids — otherwise their query-log entries become
    indistinguishable in fast batch / test scenarios.
    """
    import asyncio

    from datasight import cli as cli_module
    from datasight.agent import AgentResult
    from datasight.settings import Settings

    class FakeRunner:
        async def run_sql(self, sql, **kwargs):  # noqa: ARG002
            return None

    monkeypatch.setattr(cli_module, "create_llm_client", lambda **kwargs: object())
    monkeypatch.setattr(
        cli_module,
        "create_sql_runner_from_settings",
        lambda settings, project_dir: FakeRunner(),
    )

    async def fake_introspect_schema(run_sql, runner=None, allowed_tables=None):  # noqa: ARG001
        return []

    monkeypatch.setattr("datasight.schema.introspect_schema", fake_introspect_schema)

    async def fake_run_agent_loop(**kwargs):
        return AgentResult(
            text="",
            tool_results=[],
            total_input_tokens=10,
            total_output_tokens=5,
            api_calls=1,
        )

    monkeypatch.setattr("datasight.agent.run_agent_loop", fake_run_agent_loop)

    settings = Settings.from_env(str(Path(project_dir) / ".env"))

    async def run_one(q):
        return await cli_module._run_ask_pipeline(
            question=q,
            settings=settings,
            resolved_model="claude-sonnet-4-20250514",
            project_dir=project_dir,
            sql_dialect="duckdb",
        )

    async def run_pair():
        # Back-to-back, no sleep, well within the same second.
        await run_one("first")
        await run_one("second")

    asyncio.run(run_pair())

    log_path = Path(project_dir) / ".datasight" / "query_log.jsonl"
    entries = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]
    cost_entries = [e for e in entries if e.get("type") == "cost"]
    assert len(cost_entries) == 2
    assert cost_entries[0]["session_id"] != cost_entries[1]["session_id"]


# ---------------------------------------------------------------------------
# time_series.yaml — config loading, quality checks, prompt formatting
# ---------------------------------------------------------------------------


def test_load_time_series_config_valid(tmp_path):
    from datasight.config import load_time_series_config

    yaml_text = (
        "- table: gen_hourly\n"
        "  timestamp_column: ts\n"
        "  frequency: PT1H\n"
        "  group_columns: [region, fuel]\n"
        "  time_zone: America/New_York\n"
    )
    (tmp_path / "time_series.yaml").write_text(yaml_text, encoding="utf-8")
    configs = load_time_series_config(None, str(tmp_path))
    assert len(configs) == 1
    assert configs[0]["table"] == "gen_hourly"
    assert configs[0]["timestamp_column"] == "ts"
    assert configs[0]["frequency"] == "PT1H"
    assert configs[0]["group_columns"] == ["region", "fuel"]
    assert configs[0]["time_zone"] == "America/New_York"


def test_load_time_series_config_missing_fields(tmp_path):
    from datasight.config import load_time_series_config

    yaml_text = "- table: gen_hourly\n  timestamp_column: ts\n  # no frequency\n"
    (tmp_path / "time_series.yaml").write_text(yaml_text, encoding="utf-8")
    configs = load_time_series_config(None, str(tmp_path))
    assert configs == []


def test_load_time_series_config_bad_frequency(tmp_path):
    from datasight.config import load_time_series_config

    yaml_text = "- table: gen_hourly\n  timestamp_column: ts\n  frequency: 1h\n"
    (tmp_path / "time_series.yaml").write_text(yaml_text, encoding="utf-8")
    configs = load_time_series_config(None, str(tmp_path))
    assert configs == []


def test_load_time_series_config_defaults(tmp_path):
    from datasight.config import load_time_series_config

    yaml_text = "- table: gen_hourly\n  timestamp_column: ts\n  frequency: P1D\n"
    (tmp_path / "time_series.yaml").write_text(yaml_text, encoding="utf-8")
    configs = load_time_series_config(None, str(tmp_path))
    assert len(configs) == 1
    assert configs[0]["time_zone"] == "UTC"
    assert "group_columns" not in configs[0]


def test_load_time_series_config_no_file(tmp_path):
    from datasight.config import load_time_series_config

    configs = load_time_series_config(None, str(tmp_path))
    assert configs == []


def test_format_time_series_prompt_context():
    from datasight.data_profile import format_time_series_prompt_context

    configs = [
        {
            "table": "gen_hourly",
            "timestamp_column": "datetime_utc",
            "frequency": "PT1H",
            "group_columns": ["region", "fuel"],
            "time_zone": "UTC",
        }
    ]
    text = format_time_series_prompt_context(configs)
    assert "## Time Series Structure" in text
    assert "gen_hourly.datetime_utc" in text
    assert "frequency=PT1H" in text
    assert "groups=[region, fuel]" in text
    assert "time_zone=UTC" in text


def test_format_time_series_prompt_context_empty():
    from datasight.data_profile import format_time_series_prompt_context

    assert format_time_series_prompt_context([]) == ""


def test_format_time_series_yaml_scaffold():
    from datasight.data_profile import format_time_series_yaml

    schema_info = [
        {
            "name": "gen_hourly",
            "row_count": 8760,
            "columns": [
                {"name": "datetime_utc", "dtype": "TIMESTAMP"},
                {"name": "region", "dtype": "VARCHAR"},
                {"name": "mwh", "dtype": "DOUBLE"},
            ],
        },
        {
            "name": "plants",
            "row_count": 50,
            "columns": [
                {"name": "id", "dtype": "INTEGER"},
                {"name": "name", "dtype": "VARCHAR"},
            ],
        },
    ]
    text = format_time_series_yaml(schema_info)
    assert "gen_hourly" in text
    assert "datetime_utc" in text
    assert "PT1H" in text
    # plants has <100 rows, should not appear
    assert "plants" not in text


def test_format_time_series_yaml_no_candidates():
    from datasight.data_profile import format_time_series_yaml

    schema_info = [
        {
            "name": "products",
            "row_count": 5,
            "columns": [
                {"name": "id", "dtype": "INTEGER"},
                {"name": "name", "dtype": "VARCHAR"},
            ],
        }
    ]
    text = format_time_series_yaml(schema_info)
    assert "No timestamp columns detected" in text


def test_build_time_series_quality_detects_gaps():
    import asyncio

    from datasight.data_profile import build_time_series_quality

    async def mock_run_sql(sql):
        if "MIN" in sql:
            return pd.DataFrame(
                [{"total_rows": 100, "min_ts": "2024-01-01 00:00", "max_ts": "2024-01-05 00:00"}]
            )
        if "LEAD" in sql:
            return pd.DataFrame([{"ts": "2024-01-02 03:00", "next_ts": "2024-01-02 06:00"}])
        if "HAVING" in sql:
            return pd.DataFrame()
        return pd.DataFrame()

    configs = [
        {
            "table": "gen_hourly",
            "timestamp_column": "ts",
            "frequency": "PT1H",
            "time_zone": "UTC",
        }
    ]
    result = asyncio.run(build_time_series_quality(configs, mock_run_sql))
    assert len(result["time_series_issues"]) >= 1
    gap = result["time_series_issues"][0]
    assert gap["issue"] == "gap"
    assert "2024-01-02 03:00" in gap["detail"]


def test_build_time_series_quality_detects_duplicates():
    import asyncio

    from datasight.data_profile import build_time_series_quality

    async def mock_run_sql(sql):
        if "MIN" in sql:
            return pd.DataFrame(
                [{"total_rows": 100, "min_ts": "2024-01-01 00:00", "max_ts": "2024-01-05 00:00"}]
            )
        if "LEAD" in sql:
            return pd.DataFrame()
        if "HAVING" in sql:
            return pd.DataFrame([{"ts": "2024-01-02 02:00", "n": 2}])
        return pd.DataFrame()

    configs = [
        {
            "table": "gen_hourly",
            "timestamp_column": "ts",
            "frequency": "PT1H",
            "time_zone": "UTC",
        }
    ]
    result = asyncio.run(build_time_series_quality(configs, mock_run_sql))
    dups = [i for i in result["time_series_issues"] if i["issue"] == "duplicate"]
    assert len(dups) >= 1
    assert "2 times" in dups[0]["detail"]


def test_quality_with_time_series_config(project_dir):
    yaml_text = (
        "- table: orders\n  timestamp_column: order_date\n  frequency: P1D\n  time_zone: UTC\n"
    )
    Path(project_dir, "time_series.yaml").write_text(yaml_text, encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(cli, ["quality", "--project-dir", project_dir])
    assert result.exit_code == 0
    assert "Time Series" in result.output


def test_generate_seeds_time_series(project_dir, monkeypatch):
    class StubClient:
        async def create_message(self, **kwargs):  # noqa: ARG002
            return SimpleNamespace(
                content=[
                    TextBlock(
                        "--- schema_description.md ---\n"
                        "# Generated schema\n\n"
                        "--- queries.yaml ---\n"
                        "- question: Example\n"
                        "  sql: SELECT 1\n"
                    )
                ]
            )

    monkeypatch.setattr("datasight.cli.create_llm_client", lambda **kwargs: StubClient())

    project_path = Path(project_dir)
    (project_path / "schema_description.md").unlink()
    (project_path / "queries.yaml").unlink()

    runner = CliRunner()
    result = runner.invoke(cli, ["generate", "--project-dir", project_dir, "--overwrite"])

    assert result.exit_code == 0
    assert "time_series.yaml" in result.output
    assert (project_path / "time_series.yaml").exists()
    ts_text = (project_path / "time_series.yaml").read_text(encoding="utf-8")
    assert "# datasight time series declarations" in ts_text


# ---------------------------------------------------------------------------
# time_series.yaml — config validation edge cases
# ---------------------------------------------------------------------------


def test_load_time_series_config_invalid_yaml(tmp_path):
    """Invalid YAML syntax should return empty list, not crash."""
    from datasight.config import load_time_series_config

    (tmp_path / "time_series.yaml").write_text("{{bad yaml: [", encoding="utf-8")
    configs = load_time_series_config(None, str(tmp_path))
    assert configs == []


def test_load_time_series_config_non_list_root(tmp_path):
    """A YAML file whose root is a dict (not list) should return empty list."""
    from datasight.config import load_time_series_config

    yaml_text = "table: gen_hourly\ntimestamp_column: ts\nfrequency: PT1H\n"
    (tmp_path / "time_series.yaml").write_text(yaml_text, encoding="utf-8")
    configs = load_time_series_config(None, str(tmp_path))
    assert configs == []


def test_load_time_series_config_non_dict_entry(tmp_path):
    """List entries that are strings (not dicts) should be skipped."""
    from datasight.config import load_time_series_config

    yaml_text = '- "just a string"\n- 42\n'
    (tmp_path / "time_series.yaml").write_text(yaml_text, encoding="utf-8")
    configs = load_time_series_config(None, str(tmp_path))
    assert configs == []


def test_load_time_series_config_case_normalization(tmp_path):
    """Frequency should be uppercased: 'pt1h' → 'PT1H'."""
    from datasight.config import load_time_series_config

    yaml_text = "- table: gen_hourly\n  timestamp_column: ts\n  frequency: pt1h\n"
    (tmp_path / "time_series.yaml").write_text(yaml_text, encoding="utf-8")
    configs = load_time_series_config(None, str(tmp_path))
    assert len(configs) == 1
    assert configs[0]["frequency"] == "PT1H"


def test_load_time_series_config_whitespace_only_fields(tmp_path):
    """Fields that are whitespace-only should be treated as missing."""
    from datasight.config import load_time_series_config

    yaml_text = "- table: '  '\n  timestamp_column: ts\n  frequency: PT1H\n"
    (tmp_path / "time_series.yaml").write_text(yaml_text, encoding="utf-8")
    configs = load_time_series_config(None, str(tmp_path))
    assert configs == []


def test_load_time_series_config_empty_group_columns(tmp_path):
    """Empty strings in group_columns should be filtered out."""
    from datasight.config import load_time_series_config

    yaml_text = (
        "- table: gen_hourly\n"
        "  timestamp_column: ts\n"
        "  frequency: PT1H\n"
        "  group_columns: ['', region, '']\n"
    )
    (tmp_path / "time_series.yaml").write_text(yaml_text, encoding="utf-8")
    configs = load_time_series_config(None, str(tmp_path))
    assert len(configs) == 1
    assert configs[0]["group_columns"] == ["region"]


def test_load_time_series_config_explicit_path(tmp_path):
    """Passing an explicit path should use that file, not the default location."""
    from datasight.config import load_time_series_config

    custom = tmp_path / "custom_ts.yaml"
    custom.write_text("- table: t1\n  timestamp_column: c1\n  frequency: P1D\n", encoding="utf-8")
    configs = load_time_series_config(str(custom), str(tmp_path))
    assert len(configs) == 1
    assert configs[0]["table"] == "t1"


def test_load_time_series_config_explicit_path_missing(tmp_path):
    """Explicit path that does not exist should return empty list."""
    from datasight.config import load_time_series_config

    configs = load_time_series_config(str(tmp_path / "nope.yaml"), str(tmp_path))
    assert configs == []
