"""Command-line interface for datasight."""

import asyncio
import json
import os
import shutil
import sys
from textwrap import dedent
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import rich_click as click
import yaml
from loguru import logger

from datasight import __version__
from datasight.config import create_sql_runner_from_settings
from datasight.data_profile import (
    build_column_profile,
    build_dataset_overview,
    build_dimension_overview,
    build_measure_overview,
    build_prompt_recipes,
    build_quality_overview,
    build_table_profile,
    build_trend_overview,
    find_column_info,
    find_table_info,
    format_measure_overrides_yaml,
    format_measure_prompt_context,
)
from datasight.audit_report import (
    build_audit_report,
    render_audit_report_html,
    render_audit_report_markdown,
)
from datasight.distribution import build_distribution_overview
from datasight.integrity import build_integrity_overview
from datasight.llm import create_llm_client
from datasight.settings import Settings, global_env_path, load_global_env
from datasight.validation import build_validation_report, load_validation_config


def _epilog(text: str) -> str:
    """Normalize Click epilog text defined in indented decorators."""
    # Rich Click reflows epilog paragraphs. Treat each authored line as its
    # own paragraph so examples remain scannable in terminal help.
    return "\n\n".join(line.rstrip() for line in dedent(text).strip().splitlines())


# One-line log format that shows the module name but not the function or
# line number — ``function:line`` noise rarely helps users diagnose their
# own CLI runs, and leaving it out makes each line fit comfortably.
_LOG_FORMAT = "{time:HH:mm:ss} | {level: <7} | {name} - {message}"


def _configure_logging(level: str = "INFO") -> None:
    """Replace any existing Loguru sinks with a single stderr sink.

    Call from commands that log progress so every command uses the same
    format regardless of which module emitted the log. Safe to call
    multiple times.
    """
    logger.remove()
    logger.add(sys.stderr, level=level, format=_LOG_FORMAT)


def _resolve_settings(
    project_dir: str,
    model_override: str | None = None,
) -> tuple[Settings, str]:
    """Load settings from project directory and apply any CLI overrides.

    Parameters
    ----------
    project_dir:
        Path to the project directory containing .env.
    model_override:
        Optional model name to override settings.

    Returns
    -------
    Tuple of (settings, resolved_model).
    """
    from dotenv import load_dotenv

    env_path = os.path.join(project_dir, ".env")
    if os.path.exists(env_path):
        load_dotenv(env_path, override=False)
    load_global_env(override=False)
    settings = Settings.from_env()

    # Apply model override if provided
    resolved_model = model_override if model_override else settings.llm.model

    return settings, resolved_model


def _current_db_settings_or_none():
    """Load database settings from the current directory's .env, or None.

    Used by the file-based commands (``inspect``, ``generate --files``,
    ``trends --files``) so that when the user is inside a project with
    ``DB_MODE=spark``, file operations route through Spark Connect
    instead of silently dropping the configured backend.

    Returns
    -------
    ``DatabaseSettings`` if a ``.env`` is found in the current directory,
    otherwise ``None`` (caller should fall back to plain DuckDB).
    """
    from dotenv import load_dotenv
    from loguru import logger

    env_path = os.path.join(os.getcwd(), ".env")
    if not os.path.exists(env_path):
        logger.info(
            f"No .env found in {os.getcwd()} — file commands will use an "
            "in-memory DuckDB session. Set DB_MODE in a .env to route "
            "through a configured backend (e.g. Spark)."
        )
        return None
    load_dotenv(env_path, override=False)
    load_global_env(override=False)
    db = Settings.from_env().database
    logger.info(f"Loaded .env from {env_path} — DB_MODE={db.mode}")
    return db


def _validate_settings_for_llm(settings: Settings) -> None:
    """Validate that required LLM settings are present. Exits on error."""
    errors = settings.validate()
    for error in errors:
        if "API_KEY" in error or "TOKEN" in error:
            click.echo(f"Error: {error}", err=True)
            sys.exit(1)


def _resolve_db_path(settings: Settings, project_dir: str) -> str:
    """Resolve database path, making relative paths absolute.

    Returns
    -------
    Resolved database path, or empty string for non-file databases.
    """
    if settings.database.mode not in ("duckdb", "sqlite"):
        return ""

    raw_path = settings.database.path
    if os.path.isabs(raw_path):
        return raw_path
    return str(Path(project_dir) / raw_path)


async def _run_ask_pipeline(
    *,
    question: str,
    settings: Settings,
    resolved_model: str,
    project_dir: str,
    sql_dialect: str,
):
    from datasight.agent import run_agent_loop
    from datasight.config import (
        format_example_queries,
        load_example_queries,
        load_measure_overrides,
        load_schema_config,
        load_schema_description,
        load_time_series_config,
    )
    from datasight.cost import build_cost_data, log_query_cost
    from datasight.data_profile import format_time_series_prompt_context
    from datasight.prompts import build_system_prompt
    from datasight.query_log import QueryLogger
    from datasight.schema import filter_tables, format_schema_context, introspect_schema
    from datasight.schema_links import resolve_schema_description_links
    from datasight.sql_validation import build_measure_rule_map, build_schema_map

    llm_client = create_llm_client(
        provider=settings.llm.provider,
        api_key=settings.llm.api_key,
        base_url=settings.llm.base_url,
        timeout=settings.llm.timeout,
        model=settings.llm.model,
    )
    sql_runner = create_sql_runner_from_settings(settings.database, project_dir)

    schema_config = load_schema_config(None, project_dir)
    allowed_tables: set[str] | None = None
    if schema_config is not None:
        allowed_tables = {
            e["name"] for e in schema_config.get("tables", []) if e.get("name")
        } or None
    tables = await introspect_schema(
        sql_runner.run_sql,
        runner=sql_runner,
        allowed_tables=allowed_tables,
    )
    if schema_config is not None:
        tables = filter_tables(tables, schema_config)
    from datasight.identifiers import configure_runner_identifier_quoting

    configure_runner_identifier_quoting(
        sql_runner,
        [
            {
                "name": t.name,
                "columns": [{"name": c.name} for c in t.columns],
            }
            for t in tables
        ],
    )
    user_desc = load_schema_description(None, project_dir)
    user_desc = await resolve_schema_description_links(user_desc)
    example_queries = load_example_queries(None, project_dir)
    measure_overrides = load_measure_overrides(None, project_dir)
    time_series_configs = load_time_series_config(None, project_dir)
    measure_rules = build_measure_rule_map(measure_overrides)
    schema_text = format_schema_context(tables, user_desc)
    if example_queries:
        schema_text += format_example_queries(example_queries)

    measure_text = format_measure_prompt_context(
        await build_measure_overview(
            [
                {
                    "name": t.name,
                    "row_count": t.row_count,
                    "columns": [
                        {"name": c.name, "dtype": c.dtype, "nullable": c.nullable}
                        for c in t.columns
                    ],
                }
                for t in tables
            ],
            sql_runner.run_sql,
            measure_overrides,
        )
    )
    if measure_text:
        schema_text += measure_text

    ts_text = format_time_series_prompt_context(time_series_configs)
    if ts_text:
        schema_text += ts_text

    schema_info = [
        {
            "name": t.name,
            "columns": [{"name": c.name, "dtype": c.dtype} for c in t.columns],
        }
        for t in tables
    ]
    schema_map = build_schema_map(schema_info)

    sys_prompt = build_system_prompt(
        schema_text,
        mode="web",
        clarify_sql=False,
        dialect=sql_dialect,
        headless=True,
    )

    log_path = os.environ.get(
        "QUERY_LOG_PATH",
        os.path.join(project_dir, ".datasight", "query_log.jsonl"),
    )
    query_logger = QueryLogger(path=log_path)
    # Microseconds + a short random suffix prevent collisions when several
    # `datasight ask` runs (e.g. a fast batch or test sweep) start within
    # the same wall-clock second.
    import secrets

    session_id = (
        f"cli-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}-{secrets.token_hex(3)}"
    )
    turn_id = str(uuid.uuid4())

    result = await run_agent_loop(
        question=question,
        llm_client=llm_client,
        model=resolved_model,
        system_prompt=sys_prompt,
        run_sql=sql_runner.run_sql,
        schema_map=schema_map,
        dialect=sql_dialect,
        measure_rules=measure_rules,
        query_logger=query_logger,
        session_id=session_id,
        turn_id=turn_id,
        max_cost_usd=settings.app.max_cost_usd_per_turn,
        provider=settings.llm.provider,
        max_tokens=settings.app.max_output_tokens,
    )

    # Mirror the web app: emit a turn-level cost summary so `datasight log
    # --cost` reflects CLI usage too.
    log_query_cost(
        resolved_model,
        result.api_calls,
        result.total_input_tokens,
        result.total_output_tokens,
        cache_creation_input_tokens=result.total_cache_creation_input_tokens,
        cache_read_input_tokens=result.total_cache_read_input_tokens,
        provider=settings.llm.provider,
    )
    cost_data = build_cost_data(
        resolved_model,
        result.api_calls,
        result.total_input_tokens,
        result.total_output_tokens,
        cache_creation_input_tokens=result.total_cache_creation_input_tokens,
        cache_read_input_tokens=result.total_cache_read_input_tokens,
        provider=settings.llm.provider,
    )
    query_logger.log_cost(
        session_id=session_id,
        user_question=question,
        api_calls=result.api_calls,
        input_tokens=result.total_input_tokens,
        output_tokens=result.total_output_tokens,
        cache_creation_input_tokens=result.total_cache_creation_input_tokens,
        cache_read_input_tokens=result.total_cache_read_input_tokens,
        estimated_cost=cost_data.get("estimated_cost"),
        turn_id=turn_id,
    )
    return result


def _write_or_print(text: str, output_path: str | None) -> None:
    if output_path:
        Path(output_path).write_text(text, encoding="utf-8")
        click.echo(f"Data saved to {output_path}")
    else:
        click.echo(text)


def _render_profile_markdown(scope: str, profile_data: dict[str, Any]) -> str:
    if scope == "dataset":
        lines = [
            "# Dataset Profile",
            "",
            f"- Tables: {profile_data['table_count']}",
            f"- Columns: {profile_data['total_columns']}",
            f"- Rows: {profile_data['total_rows']}",
            "",
            "## Largest Tables",
        ]
        for item in profile_data["largest_tables"]:
            lines.append(
                f"- `{item['name']}`: {item.get('row_count') or 0} rows, {item['column_count']} columns"
            )
        if profile_data["date_columns"]:
            lines.extend(["", "## Date Coverage"])
            for item in profile_data["date_columns"]:
                lines.append(
                    f"- `{item['table']}.{item['column']}`: {item.get('min') or '?'} -> {item.get('max') or '?'}"
                )
        if profile_data["measure_columns"]:
            lines.extend(["", "## Measure Candidates"])
            for item in profile_data["measure_columns"]:
                lines.append(
                    f"- `{item['table']}.{item['column']}` ({item.get('dtype') or 'unknown'})"
                )
        if profile_data["dimension_columns"]:
            lines.extend(["", "## Dimension Candidates"])
            for item in profile_data["dimension_columns"]:
                sample_values = item.get("sample_values") or []
                sample_suffix = (
                    f" — samples: {', '.join(sample_values[:3])}" if sample_values else ""
                )
                lines.append(
                    f"- `{item['table']}.{item['column']}`: "
                    f"{item.get('distinct_count') or '?'} distinct, "
                    f"{item.get('null_rate') or 0}% null{sample_suffix}"
                )
        return "\n".join(lines)

    if scope == "table":
        lines = [
            f"# Table Profile: {profile_data['table']}",
            "",
            f"- Rows: {profile_data.get('row_count') or 0}",
            f"- Columns: {profile_data['column_count']}",
        ]
        if profile_data["null_columns"]:
            lines.extend(["", "## Null-heavy Columns"])
            for item in profile_data["null_columns"]:
                lines.append(
                    f"- `{item['column']}`: {item['null_count']} nulls ({item.get('null_rate') or 0}%)"
                )
        if profile_data["date_columns"]:
            lines.extend(["", "## Date Columns"])
            for item in profile_data["date_columns"]:
                lines.append(
                    f"- `{item['column']}`: {item.get('min') or '?'} -> {item.get('max') or '?'}"
                )
        if profile_data["numeric_columns"]:
            lines.extend(["", "## Numeric Columns"])
            for item in profile_data["numeric_columns"]:
                lines.append(
                    f"- `{item['column']}`: min {item.get('min')}, max {item.get('max')}, avg {item.get('avg')}"
                )
        if profile_data["text_columns"]:
            lines.extend(["", "## Text Dimensions"])
            for item in profile_data["text_columns"]:
                sample_values = item.get("sample_values") or []
                sample_suffix = (
                    f" — samples: {', '.join(sample_values[:3])}" if sample_values else ""
                )
                lines.append(
                    f"- `{item['column']}`: {item.get('distinct_count') or '?'} distinct, "
                    f"{item.get('null_rate') or 0}% null{sample_suffix}"
                )
        return "\n".join(lines)

    lines = [
        f"# Column Profile: {profile_data['table']}.{profile_data['column']}",
        "",
        f"- Type: {profile_data.get('dtype') or 'unknown'}",
        f"- Distinct: {profile_data.get('distinct_count')}",
        f"- Nulls: {profile_data.get('null_count')}",
        f"- Null rate: {profile_data.get('null_rate')}",
    ]
    if profile_data.get("numeric_stats"):
        stats = profile_data["numeric_stats"]
        lines.extend(
            [
                "",
                "## Numeric Stats",
                f"- Min: {stats.get('min')}",
                f"- Max: {stats.get('max')}",
                f"- Avg: {stats.get('avg')}",
            ]
        )
    if profile_data.get("date_coverage"):
        coverage = profile_data["date_coverage"]
        lines.extend(
            [
                "",
                "## Date Coverage",
                f"- Min: {coverage.get('min')}",
                f"- Max: {coverage.get('max')}",
            ]
        )
    if profile_data.get("dimension_stats"):
        stats = profile_data["dimension_stats"]
        sample_values = stats.get("sample_values") or []
        lines.extend(
            [
                "",
                "## Dimension Stats",
                f"- Distinct: {stats.get('distinct_count')}",
                f"- Nulls: {stats.get('null_count')}",
                (
                    "- Samples: " + ", ".join(sample_values[:5])
                    if sample_values
                    else "- Samples: none"
                ),
            ]
        )
    if profile_data.get("sample_values"):
        lines.extend(
            [
                "",
                "## Sample Values",
                "- " + ", ".join(profile_data["sample_values"][:5]),
            ]
        )
    return "\n".join(lines)


def _render_doctor_markdown(project_dir: str, checks: list[dict[str, Any]]) -> str:
    lines = [
        "# datasight doctor",
        "",
        f"- Project: `{project_dir}`",
        "",
        "## Checks",
    ]
    for check in checks:
        status = "OK" if check["ok"] else "FAIL"
        lines.append(f"- **{check['name']}**: {status} — {check['detail']}")
    return "\n".join(lines)


def _render_quality_markdown(quality_data: dict[str, Any]) -> str:
    lines = [
        "# Dataset Quality Audit",
        "",
        f"- Tables scanned: {quality_data['table_count']}",
    ]
    if quality_data["null_columns"]:
        lines.extend(["", "## Null-heavy Columns"])
        for item in quality_data["null_columns"]:
            lines.append(
                f"- `{item['table']}.{item['column']}`: {item['null_count']} nulls "
                f"({item.get('null_rate') or 0}%)"
            )
    if quality_data["numeric_flags"]:
        lines.extend(["", "## Numeric Range Flags"])
        for item in quality_data["numeric_flags"]:
            lines.append(f"- `{item['table']}.{item['column']}`: {item['issue']}")
    if quality_data["date_columns"]:
        lines.extend(["", "## Date Coverage"])
        for item in quality_data["date_columns"]:
            lines.append(
                f"- `{item['table']}.{item['column']}`: {item.get('min') or '?'} -> {item.get('max') or '?'}"
            )
    if quality_data.get("time_series_summaries"):
        lines.extend(["", "## Time Series"])
        for s in quality_data["time_series_summaries"]:
            lines.append(
                f"- `{s['table']}.{s['timestamp_column']}`: {s.get('frequency', '')} — "
                f"{s.get('total_rows', '')} rows, {s.get('min_ts', '')} to {s.get('max_ts', '')}"
            )
    if quality_data.get("time_series_issues"):
        lines.extend(["", "## Temporal Completeness"])
        for item in quality_data["time_series_issues"]:
            lines.append(
                f"- `{item['table']}.{item['timestamp_column']}` [{item['issue']}]: {item['detail']}"
            )
    if quality_data["notes"]:
        lines.extend(["", "## Notes"])
        for item in quality_data["notes"]:
            lines.append(f"- {item}")
    return "\n".join(lines)


def _render_dimensions_markdown(dimension_data: dict[str, Any]) -> str:
    lines = [
        "# Dimension Overview",
        "",
        f"- Tables scanned: {dimension_data['table_count']}",
    ]
    if dimension_data["dimension_columns"]:
        lines.extend(["", "## Dimension Candidates"])
        for item in dimension_data["dimension_columns"]:
            samples = ", ".join((item.get("sample_values") or [])[:3])
            sample_suffix = f" — samples: {samples}" if samples else ""
            lines.append(
                f"- `{item['table']}.{item['column']}`: {item.get('distinct_count') or '?'} distinct, "
                f"{item.get('null_rate') or 0}% null{sample_suffix}"
            )
    if dimension_data["suggested_breakdowns"]:
        lines.extend(["", "## Suggested Breakdowns"])
        for item in dimension_data["suggested_breakdowns"]:
            lines.append(f"- `{item['table']}.{item['column']}`: {item['reason']}")
    if dimension_data["join_hints"]:
        lines.extend(["", "## Join Hints"])
        for item in dimension_data["join_hints"]:
            lines.append(f"- {item}")
    return "\n".join(lines)


def _render_trends_markdown(trend_data: dict[str, Any]) -> str:
    lines = [
        "# Trend Overview",
        "",
        f"- Tables scanned: {trend_data['table_count']}",
    ]
    if trend_data["trend_candidates"]:
        lines.extend(["", "## Trend Candidates"])
        for item in trend_data["trend_candidates"]:
            lines.append(
                f"- `{item['table']}`: `{str(item.get('aggregation') or '').upper()}({item['measure_column']})` "
                f"over `{item['date_column']}` ({item['date_range']})"
            )
    if trend_data["breakout_dimensions"]:
        lines.extend(["", "## Breakout Dimensions"])
        for item in trend_data["breakout_dimensions"]:
            lines.append(
                f"- `{item['table']}.{item['column']}`: {item.get('distinct_count') or '?'} distinct values"
            )
    if trend_data["chart_recommendations"]:
        lines.extend(["", "## Chart Recommendations"])
        for item in trend_data["chart_recommendations"]:
            lines.append(f"- `{item['title']}` ({item['chart_type']}): {item['reason']}")
    if trend_data["notes"]:
        lines.extend(["", "## Notes"])
        for item in trend_data["notes"]:
            lines.append(f"- {item}")
    return "\n".join(lines)


def _render_measures_markdown(measure_data: dict[str, Any]) -> str:
    lines = [
        "# Measure Overview",
        "",
        f"- Tables scanned: {measure_data['table_count']}",
    ]
    if measure_data["measures"]:
        lines.extend(["", "## Measure Candidates"])
        for item in measure_data["measures"]:
            unit = f" [{item['unit']}]" if item.get("unit") else ""
            expression = f"; expression `{item['expression']}`" if item.get("expression") else ""
            display_name = (
                f"; display `{item['display_name']}`" if item.get("display_name") else ""
            )
            fmt = f"; format `{item['format']}`" if item.get("format") else ""
            charts = (
                f"; charts {', '.join(item['preferred_chart_types'])}"
                if item.get("preferred_chart_types")
                else ""
            )
            forbidden = (
                f"; avoid {', '.join(item['forbidden_aggregations'])}"
                if item.get("forbidden_aggregations")
                else ""
            )
            weighting = (
                f"; weighted avg by {item['weight_column']}" if item.get("weight_column") else ""
            )
            lines.append(
                f"- `{item['table']}.{item['column']}`{unit}: role `{item['role']}`, "
                f"default `{item['default_aggregation']}`, allowed {', '.join(item['allowed_aggregations'])}"
                f"{display_name}{fmt}{charts}{expression}{forbidden}{weighting}; rollup SQL `{item['recommended_rollup_sql']}`. {item['reason']}"
            )
    if measure_data["notes"]:
        lines.extend(["", "## Notes"])
        for item in measure_data["notes"]:
            lines.append(f"- {item}")
    return "\n".join(lines)


def _render_recipes_markdown(recipes: list[dict[str, str]]) -> str:
    lines = ["# Prompt Recipes", ""]
    for recipe in recipes:
        lines.extend(
            [
                f"## [{recipe['id']}] {recipe['title']}",
                "",
                f"- Category: {recipe.get('category') or 'Recipe'}",
                *([f"- Why this recipe: {recipe['reason']}"] if recipe.get("reason") else []),
                f"- Prompt: {recipe['prompt']}",
                "",
            ]
        )
    return "\n".join(lines).strip()


def _render_integrity_markdown(data: dict[str, Any]) -> str:
    lines = [
        "# Referential Integrity",
        "",
        f"- Tables scanned: {data['table_count']}",
    ]
    if data["primary_keys"]:
        lines.extend(["", "## Primary Keys"])
        for item in data["primary_keys"]:
            unique = (
                "unique"
                if item["is_unique"]
                else f"NOT unique ({item['distinct_count']}/{item['row_count']})"
            )
            lines.append(f"- `{item['table']}.{item['column']}`: {unique}")
    if data["duplicate_keys"]:
        lines.extend(["", "## Duplicate Keys"])
        for item in data["duplicate_keys"]:
            lines.append(
                f"- `{item['table']}.{item['column']}`: {item['duplicate_count']} duplicate rows"
            )
    if data["orphan_foreign_keys"]:
        lines.extend(["", "## Orphan Foreign Keys"])
        for item in data["orphan_foreign_keys"]:
            lines.append(
                f"- `{item['child_table']}.{item['child_column']}` -> "
                f"`{item['parent_table']}.{item['parent_column']}`: "
                f"{item['orphan_count']} orphans out of {item['child_rows']} rows"
            )
    if data["join_explosions"]:
        lines.extend(["", "## Join Explosion Risks"])
        for item in data["join_explosions"]:
            lines.append(
                f"- {item['table_a']} x {item['table_b']} on `{item['join_column']}`: "
                f"{item['explosion_factor']}x ({item['expected_rows']} -> {item['actual_rows']} rows)"
            )
    if data["notes"]:
        lines.extend(["", "## Notes"])
        for item in data["notes"]:
            lines.append(f"- {item}")
    return "\n".join(lines)


def _render_distribution_markdown(data: dict[str, Any]) -> str:
    lines = [
        "# Distribution Profiling",
        "",
        f"- Tables scanned: {data['table_count']}",
    ]
    if data["distributions"]:
        lines.extend(["", "## Distributions"])
        for d in data["distributions"]:
            role_info = f" (role: {d['role']})" if d.get("role") else ""
            lines.append(
                f"- `{d['table']}.{d['column']}`{role_info}: "
                f"p5={_fmt_dist(d.get('p5'))}, p50={_fmt_dist(d.get('p50'))}, "
                f"p95={_fmt_dist(d.get('p95'))}, "
                f"zero={_fmt_dist(d.get('zero_rate'))}%, neg={_fmt_dist(d.get('negative_rate'))}%, "
                f"outliers={d.get('outlier_count', 0)}"
            )
    if data["energy_flags"]:
        lines.extend(["", "## Energy Flags"])
        for f in data["energy_flags"]:
            lines.append(f"- `{f['table']}.{f['column']}`: {f['detail']}")
    if data["spikes"]:
        lines.extend(["", "## Temporal Spikes"])
        for s in data["spikes"]:
            lines.append(f"- {s['detail']}")
    if data["notes"]:
        lines.extend(["", "## Notes"])
        for item in data["notes"]:
            lines.append(f"- {item}")
    return "\n".join(lines)


def _render_validation_markdown(data: dict[str, Any]) -> str:
    summary = data.get("summary", {})
    lines = [
        "# Validation Report",
        "",
        f"- Rules run: {data.get('rule_count', 0)}",
        f"- Pass: {summary.get('pass', 0)}, Fail: {summary.get('fail', 0)}, Warn: {summary.get('warn', 0)}",
    ]
    if data["results"]:
        lines.extend(["", "## Results"])
        for r in data["results"]:
            col = f" ({r['column']})" if r.get("column") else ""
            lines.append(
                f"- [{r['status'].upper()}] `{r['table']}` {r['rule']}{col}: {r['detail']}"
            )
    return "\n".join(lines)


def _fmt_dist(value: Any) -> str:
    if value is None:
        return "?"
    if isinstance(value, float):
        return f"{value:.4g}"
    return str(value)


def _format_profile_value(value: Any, default: str = "?") -> str:
    if value is None or value == "":
        return default
    return str(value)


def _load_recipe_entries(
    project_dir: str,
    settings: Settings,
    table: str | None = None,
) -> list[dict[str, Any]]:
    from datasight.config import load_measure_overrides

    resolved_db_path = _resolve_db_path(settings, project_dir)
    if settings.database.mode in ("duckdb", "sqlite") and not os.path.exists(resolved_db_path):
        click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
        sys.exit(1)

    async def _run_recipes() -> list[dict[str, Any]]:
        sql_runner, schema_info = await _load_schema_info_for_project(project_dir, settings)
        measure_overrides = load_measure_overrides(None, project_dir)
        if table:
            table_info = find_table_info(schema_info, table)
            if table_info is None:
                raise click.ClickException(f"Table not found: {table}")
            schema_info = [table_info]
        recipes = await build_prompt_recipes(schema_info, sql_runner.run_sql, measure_overrides)
        return [{"id": idx, **recipe} for idx, recipe in enumerate(recipes, start=1)]

    return asyncio.run(_run_recipes())


def _build_metric_table(title: str, rows: list[tuple[str, str]]) -> Any:
    from rich.table import Table as RichTable

    table = RichTable(title=title)
    table.add_column("Metric")
    table.add_column("Value")
    for label, value in rows:
        table.add_row(label, value)
    return table


def _build_profile_detail_table(
    title: str,
    columns: list[tuple[str, Literal["left", "center", "right", "full", "default"]]],
    rows: list[list[str]],
) -> Any:
    from rich.table import Table as RichTable

    table = RichTable(title=title)
    for label, justify in columns:
        table.add_column(label, justify=justify)
    for row in rows:
        table.add_row(*row)
    return table


async def _load_schema_info_for_project(
    project_dir: str,
    settings: Settings,
) -> tuple[Any, list[dict[str, Any]]]:
    from datasight.schema import introspect_schema

    sql_runner = create_sql_runner_from_settings(settings.database, project_dir)
    tables = await introspect_schema(sql_runner.run_sql, runner=sql_runner)
    schema_info = [
        {
            "name": t.name,
            "row_count": t.row_count,
            "columns": [
                {"name": c.name, "dtype": c.dtype, "nullable": c.nullable} for c in t.columns
            ],
        }
        for t in tables
    ]
    return sql_runner, schema_info


def _slugify_filename(value: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "-" for ch in value)
    parts = [part for part in cleaned.split("-") if part]
    return "-".join(parts)[:64] or "question"


def _sanitize_sql_identifier(value: str) -> str:
    """Convert a string into a snake_case SQL identifier.

    Keeps only ASCII alphanumerics (lowercased), joins with underscores,
    and caps length at 32 characters. The result is intentionally bounded
    and ASCII-only so callers can safely prefix/suffix it (table prefix,
    hash, query index) and stay portable across DuckDB / SQLite / Postgres
    — Postgres in particular folds unquoted identifiers via libc and
    truncates at 63 *bytes*, so non-ASCII characters could blow that
    budget on a per-character truncation scheme.
    """
    cleaned = "".join(ch.lower() if (ch.isascii() and ch.isalnum()) else "_" for ch in value)
    parts = [p for p in cleaned.split("_") if p]
    slug = "_".join(parts)[:32]
    if not slug or not slug[0].isalpha():
        slug = "query" if not slug else f"q_{slug}"
    return slug


def _question_table_prefix(question: str) -> str:
    """Build a stable, collision-resistant table-name prefix from a question.

    Combines a human-readable slug with an 8-char hash of the original
    (untruncated) question, so two long questions that happen to share the
    same first sanitized characters still get distinct tables. The full
    prefix fits within Postgres' 63-byte identifier limit even after a
    ``_<n>`` query-index suffix is appended.
    """
    import hashlib

    slug = _sanitize_sql_identifier(question)
    digest = hashlib.sha256(question.encode("utf-8")).hexdigest()[:8]
    return f"{slug}_{digest}"


def _iter_sql_tool_results(result) -> list[tuple[int, Any]]:
    """Return ``(index, tool_result)`` pairs that contain a SQL query."""
    pairs: list[tuple[int, Any]] = []
    n = 0
    for tr in result.tool_results:
        if not tr.meta or not tr.meta.get("sql"):
            continue
        n += 1
        pairs.append((n, tr))
    return pairs


def _sql_comment_lines(label: str, value: Any) -> list[str]:
    """Render ``value`` as a block of SQL line comments under ``label``.

    Any newlines in ``value`` are kept commented so that user-supplied
    questions or multi-line SQL error messages cannot escape the comment
    and become executable statements. Returns a list of ``-- ...`` lines.
    """
    text = "" if value is None else str(value)
    parts = text.splitlines() or [""]
    out = [f"-- {label}: {parts[0]}"]
    for cont in parts[1:]:
        out.append(f"--   {cont}")
    return out


def _print_sql_queries(result) -> None:
    """Print SQL queries from a result to stderr, prefixed with comments.

    stderr (not stdout) because ``--print-sql`` is a diagnostic overlay and
    must not corrupt machine-readable output on stdout (``--format json``,
    ``--format csv``, shell pipelines).
    """
    pairs = _iter_sql_tool_results(result)
    if not pairs:
        return
    click.echo(err=True)
    click.echo("-- SQL queries executed:", err=True)
    for idx, tr in pairs:
        sql = (tr.meta.get("formatted_sql") or tr.meta.get("sql") or "").rstrip()
        tool = tr.meta.get("tool", "")
        click.echo(err=True)
        click.echo(f"-- Query {idx} (tool: {tool})", err=True)
        if tr.meta.get("error"):
            for line in _sql_comment_lines("ERROR", tr.meta["error"]):
                click.echo(line, err=True)
        click.echo(sql.rstrip(";") + ";", err=True)


def _build_sql_script(result, question: str, dialect: str) -> str:
    """Build an executable SQL script that materializes results to tables.

    Each successful SQL query is wrapped in a ``CREATE OR REPLACE TABLE``
    (DuckDB) or ``DROP TABLE IF EXISTS`` + ``CREATE TABLE`` (SQLite,
    Postgres) statement that overwrites a deterministically named table.

    Tables are named ``<slug>_<hash>_<n>`` where ``slug`` is a truncated
    snake_case rendering of the question, ``hash`` is a short SHA-256
    digest of the original question, and ``n`` is the 1-based index of the
    query *among the successful ones*. Failed/intermediate attempts are
    preserved in the script as bare audit comments but do **not** consume
    a table number — that way two reruns of the same question, even if
    the agent retries differently, still land their final result on the
    same table name and the script truly overwrites in place.
    """
    prefix = _question_table_prefix(question)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    lines: list[str] = [
        "-- Generated by `datasight ask`",
        f"-- Generated at: {timestamp}",
        *_sql_comment_lines("Question", question),
        f"-- Dialect: {dialect}",
        "",
    ]
    pairs = _iter_sql_tool_results(result)
    if not pairs:
        lines.append("-- (no SQL queries were executed)")
        lines.append("")
        return "\n".join(lines)

    success_idx = 0
    for _, tr in pairs:
        if tr.meta.get("error"):
            # Audit-only: failed attempts are preserved as comments but
            # do not consume a table-name index, so retries don't shift
            # the final result onto a different table on the next run.
            lines.append("-- Skipped attempt (errored, not materialized):")
            lines.extend(_sql_comment_lines("  error", tr.meta["error"]))
            lines.append("")
            continue
        body = (tr.meta.get("formatted_sql") or tr.meta.get("sql") or "").strip()
        if not body:
            continue
        success_idx += 1
        body = body.rstrip(";")
        table = f"{prefix}_{success_idx}"
        tool = tr.meta.get("tool", "")
        lines.append(f"-- Query {success_idx} (tool: {tool})")
        if dialect == "duckdb":
            lines.append(f"CREATE OR REPLACE TABLE {table} AS")
            lines.append(f"{body};")
        else:
            lines.append(f"DROP TABLE IF EXISTS {table};")
            lines.append(f"CREATE TABLE {table} AS")
            lines.append(f"{body};")
        lines.append("")
    return "\n".join(lines)


def _default_data_extension(output_format: str) -> str:
    if output_format == "csv":
        return ".csv"
    if output_format == "json":
        return ".json"
    return ".txt"


def _default_chart_extension(chart_format: str) -> str:
    return {
        "html": ".html",
        "json": ".json",
        "png": ".png",
    }[chart_format]


def _validate_batch_entry(
    item: dict[str, Any],
    *,
    label: str,
) -> dict[str, str | None]:
    question = str(item.get("question") or "").strip()
    if not question:
        raise click.ClickException(f"{label}: expected a mapping with 'question'.")

    output_format = str(item.get("format") or "")
    if output_format and output_format not in {"table", "csv", "json"}:
        raise click.ClickException(
            f"{label}: invalid format {output_format!r}. Use table, csv, or json."
        )

    chart_format = str(item.get("chart_format") or "")
    if chart_format and chart_format not in {"html", "json", "png"}:
        raise click.ClickException(
            f"{label}: invalid chart_format {chart_format!r}. Use html, json, or png."
        )

    return {
        "question": question,
        "output_format": output_format,
        "chart_format": chart_format,
        "name": str(item.get("name") or ""),
        "output": str(item.get("output") or ""),
    }


def _load_batch_entries(questions_file: str) -> list[dict[str, str | None]]:
    path = Path(questions_file)
    suffix = path.suffix.lower()

    if suffix == ".jsonl":
        entries: list[dict[str, str | None]] = []
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            raw = line.strip()
            if not raw:
                continue
            try:
                item = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise click.ClickException(
                    f"Invalid JSONL at line {line_number}: {exc.msg}"
                ) from exc
            if not isinstance(item, dict):
                raise click.ClickException(
                    f"Invalid JSONL entry at line {line_number}: expected an object with 'question'."
                )
            entries.append(_validate_batch_entry(item, label=f"JSONL line {line_number}"))
        return entries

    if suffix in {".yaml", ".yml"}:
        try:
            loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
        except yaml.YAMLError as exc:
            raise click.ClickException(f"Invalid YAML: {exc}") from exc
        if not isinstance(loaded, list):
            raise click.ClickException("Structured YAML batch input must be a list of entries.")
        entries = []
        for idx, item in enumerate(loaded, 1):
            if not isinstance(item, dict):
                raise click.ClickException(
                    f"Invalid YAML entry #{idx}: expected a mapping with 'question'."
                )
            normalized_item = {str(key): value for key, value in item.items()}
            entries.append(_validate_batch_entry(normalized_item, label=f"YAML entry #{idx}"))
        return entries

    return [
        {
            "question": line.strip(),
            "output_format": "",
            "chart_format": "",
            "name": "",
            "output": "",
        }
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _emit_ask_result(
    result, output_format: str, chart_format: str | None, output_path: str | None
) -> None:
    from rich import box
    from rich.console import Console
    from rich.table import Table as RichTable

    console = Console()

    if result.text:
        console.print(result.text)
        console.print()

    for tr in result.tool_results:
        if tr.df is not None and not tr.df.empty:
            if output_format == "csv":
                _write_or_print(
                    tr.df.to_csv(index=False), output_path if not chart_format else None
                )
            elif output_format == "json":
                _write_or_print(
                    tr.df.to_json(orient="records", indent=2),
                    output_path if not chart_format else None,
                )
            else:
                rich_table = RichTable(box=box.ROUNDED, padding=(0, 1))
                for col in tr.df.columns:
                    rich_table.add_column(str(col))
                for _, row in tr.df.head(50).iterrows():
                    rich_table.add_row(*[str(v) for v in row])
                console.print(rich_table)
                if len(tr.df) > 50:
                    console.print(f"[dim]Showing 50 of {len(tr.df)} rows[/dim]")

        if tr.plotly_spec and chart_format and output_path:
            # Chart-export confirmations land on stderr — when both
            # `--format` and `--chart-format` are set, the data table
            # still goes to stdout (see the `if not chart_format` switch
            # above), so a stdout status line would corrupt that JSON/CSV.
            if chart_format == "json":
                Path(output_path).write_text(
                    json.dumps(tr.plotly_spec, indent=2), encoding="utf-8"
                )
                click.echo(f"Plotly spec saved to {output_path}", err=True)
            elif chart_format == "html":
                from datasight.chart import _build_artifact_html

                html = _build_artifact_html(tr.plotly_spec, tr.meta.get("title", "Chart"))
                Path(output_path).write_text(html, encoding="utf-8")
                click.echo(f"Chart HTML saved to {output_path}", err=True)
            elif chart_format == "png":
                try:
                    import plotly.graph_objects as go
                    import plotly.io as pio

                    fig = go.Figure(tr.plotly_spec)
                    pio.write_image(fig, output_path)
                    click.echo(f"Chart PNG saved to {output_path}", err=True)
                except ImportError:
                    click.echo(
                        "Error: PNG export requires kaleido. "
                        'Install the "export" extra in your current environment '
                        '(for example: pip install "datasight[export]" or '
                        'uv pip install "datasight[export]").',
                        err=True,
                    )
                    sys.exit(1)


def _build_cli_provenance(
    *,
    question: str,
    result,
    model: str,
    dialect: str,
    project_dir: str,
    provider: str | None = None,
) -> dict[str, Any]:
    from datasight.cost import build_cost_data

    cost_data = build_cost_data(
        model,
        result.api_calls,
        result.total_input_tokens,
        result.total_output_tokens,
        cache_creation_input_tokens=result.total_cache_creation_input_tokens,
        cache_read_input_tokens=result.total_cache_read_input_tokens,
        provider=provider,
    )
    tools = []
    for tr in result.tool_results:
        meta = tr.meta
        tools.append(
            {
                "turn_id": meta.get("turn_id"),
                "tool": meta.get("tool"),
                "sql": meta.get("sql"),
                "formatted_sql": meta.get("formatted_sql"),
                "validation": meta.get("validation", {"status": "not_run", "errors": []}),
                "execution": {
                    "status": "error" if meta.get("error") else "success",
                    "execution_time_ms": meta.get("execution_time_ms"),
                    "row_count": meta.get("row_count"),
                    "column_count": meta.get("column_count"),
                    "columns": meta.get("columns", []),
                    "error": meta.get("error"),
                    "timestamp": meta.get("timestamp"),
                },
            }
        )
    warnings = [
        f"{tool.get('tool') or 'tool'} failed: {tool['execution']['error']}"
        for tool in tools
        if tool["execution"]["error"]
    ]
    return {
        "turn_id": tools[0].get("turn_id") if tools else None,
        "question": question,
        "answer": result.text or "",
        "model": model,
        "dialect": dialect,
        "project_dir": project_dir,
        "tools": tools,
        "llm": {
            "api_calls": result.api_calls,
            "input_tokens": result.total_input_tokens,
            "output_tokens": result.total_output_tokens,
            "estimated_cost": cost_data.get("estimated_cost"),
        },
        "warnings": warnings,
    }


def _emit_cli_provenance(
    *,
    question: str,
    result,
    model: str,
    dialect: str,
    project_dir: str,
    provider: str | None = None,
) -> None:
    provenance = _build_cli_provenance(
        question=question,
        result=result,
        model=model,
        dialect=dialect,
        project_dir=project_dir,
        provider=provider,
    )
    click.echo(json.dumps(provenance, indent=2))


def _write_batch_result_files(
    *,
    output_dir: str | None,
    index: int,
    question: str,
    result,
    output_format: str,
    chart_format: str | None,
    name: str | None = None,
    output: str | None = None,
) -> list[str]:
    if output:
        output_base = Path(output)
        if not output_base.is_absolute() and output_dir:
            output_base = Path(output_dir) / output_base
        if output_base.suffix:
            output_base = output_base.with_suffix("")
    else:
        if name:
            base_name = f"{index:02d}-{_slugify_filename(name)}"
        else:
            base_name = f"{index:02d}-{_slugify_filename(question)}"
        output_root = Path(output_dir) if output_dir else Path(".")
        output_base = output_root / base_name

    output_base.parent.mkdir(parents=True, exist_ok=True)
    written_paths: list[str] = []

    answer_path = Path(str(output_base) + ".answer.txt")
    answer_path.write_text((result.text or "").strip() + "\n", encoding="utf-8")
    written_paths.append(str(answer_path))

    for tool_idx, tr in enumerate(result.tool_results, 1):
        if tr.df is not None and not tr.df.empty:
            data_path = Path(
                str(output_base) + f".result-{tool_idx}{_default_data_extension(output_format)}"
            )
            if output_format == "csv":
                data_path.write_text(tr.df.to_csv(index=False), encoding="utf-8")
            elif output_format == "json":
                data_path.write_text(tr.df.to_json(orient="records", indent=2), encoding="utf-8")
            else:
                data_path.write_text(tr.df.to_string(index=False), encoding="utf-8")
            written_paths.append(str(data_path))

        if tr.plotly_spec and chart_format:
            chart_path = Path(
                str(output_base) + f".chart-{tool_idx}{_default_chart_extension(chart_format)}"
            )
            if chart_format == "json":
                chart_path.write_text(json.dumps(tr.plotly_spec, indent=2), encoding="utf-8")
            elif chart_format == "html":
                from datasight.chart import _build_artifact_html

                html = _build_artifact_html(tr.plotly_spec, tr.meta.get("title", "Chart"))
                chart_path.write_text(html, encoding="utf-8")
            elif chart_format == "png":
                try:
                    import plotly.graph_objects as go
                    import plotly.io as pio

                    fig = go.Figure(tr.plotly_spec)
                    pio.write_image(fig, chart_path)
                except ImportError:
                    click.echo(
                        "Error: PNG export requires kaleido. "
                        'Install the "export" extra in your current environment '
                        '(for example: pip install "datasight[export]" or '
                        'uv pip install "datasight[export]").',
                        err=True,
                    )
                    sys.exit(1)
            written_paths.append(str(chart_path))

    return written_paths


click.rich_click.COMMAND_GROUPS = {
    "datasight": [
        {
            "name": "Quick start",
            "commands": ["inspect", "run"],
        },
        {
            "name": "Project setup",
            "commands": ["init", "generate", "doctor"],
        },
        {
            "name": "AI-powered",
            "commands": ["ask", "verify"],
        },
        {
            "name": "Data analysis (no LLM)",
            "commands": [
                "profile",
                "quality",
                "measures",
                "dimensions",
                "trends",
                "recipes",
                "templates",
            ],
        },
        {
            "name": "Data quality audit (no LLM)",
            "commands": ["integrity", "distribution", "validate", "audit-report"],
        },
        {
            "name": "Session history",
            "commands": ["log", "export", "report"],
        },
        {
            "name": "Demo datasets",
            "commands": ["demo"],
        },
    ],
}


@click.group()
@click.version_option(__version__, prog_name="datasight")
def cli():
    """datasight — AI-powered data exploration with natural language."""


def _prepare_web_runtime(
    *,
    port: int | None,
    model: str | None,
    project_dir: str | None,
    verbose: bool,
    configure_logging: bool = True,
) -> tuple[Settings, str, int]:
    from dotenv import load_dotenv

    from datasight.recent_projects import validate_project_dir

    resolved_project_dir = project_dir
    if resolved_project_dir is None:
        cwd = str(Path.cwd().resolve())
        is_valid, _ = validate_project_dir(cwd)
        if is_valid:
            resolved_project_dir = cwd

    if resolved_project_dir:
        resolved_project_dir = str(Path(resolved_project_dir).resolve())
        env_path = os.path.join(resolved_project_dir, ".env")
        if os.path.exists(env_path):
            load_dotenv(env_path, override=False)
    elif os.path.exists(".env"):
        load_dotenv(".env", override=False)

    load_global_env(override=False)

    if configure_logging:
        _configure_logging("DEBUG" if verbose else "INFO")

    settings = Settings.from_env()
    resolved_model = model if model else settings.llm.model
    resolved_port = port if port else settings.app.port

    os.environ["PORT"] = str(resolved_port)
    if model:
        match settings.llm.provider:
            case "ollama":
                os.environ["OLLAMA_MODEL"] = resolved_model
            case "github":
                os.environ["GITHUB_MODELS_MODEL"] = resolved_model
            case _:
                os.environ["ANTHROPIC_MODEL"] = resolved_model

    if resolved_project_dir:
        os.environ["DATASIGHT_AUTO_LOAD_PROJECT"] = resolved_project_dir

    return settings, resolved_model, resolved_port


@cli.command(
    epilog=_epilog(
        """
        Use this when you want to fill in .env, schema_description.md,
        queries.yaml, and time_series.yaml by hand.

        If you already have a DuckDB/SQLite database or CSV/Parquet files and
        want datasight to inspect them and draft these files, use:

            datasight generate <file>...
        """
    )
)
@click.argument("project_dir", default=".")
@click.option("--overwrite", is_flag=True, help="Overwrite existing files.")
def init(project_dir: str, overwrite: bool):
    """Create blank datasight project template files.

    PROJECT_DIR defaults to the current directory.
    """
    dest = Path(project_dir).resolve()
    dest.mkdir(parents=True, exist_ok=True)

    template_dir = Path(__file__).parent / "templates"

    files = {
        "env.template": ".env",
        "schema_description.md": "schema_description.md",
        "queries.yaml": "queries.yaml",
        "time_series.yaml": "time_series.yaml",
    }

    created = []
    skipped = []

    for src_name, dst_name in files.items():
        src = template_dir / src_name
        dst = dest / dst_name

        if dst.exists() and not overwrite:
            skipped.append(dst_name)
            continue

        shutil.copy2(src, dst)
        created.append(dst_name)

    click.echo(f"Project initialized in {dest}")
    if created:
        click.echo(f"  Created: {', '.join(created)}")
    if skipped:
        click.echo(f"  Skipped (already exist): {', '.join(skipped)}")

    click.echo()
    click.echo("Next steps:")
    click.echo("  1. Store API keys once in ~/.config/datasight/.env:")
    click.echo("     datasight config init")
    click.echo("  2. Edit .env with your database path and (optional) provider/model")
    click.echo("  3. Edit schema_description.md to describe your data")
    click.echo("  4. Edit queries.yaml with example questions")
    click.echo("  5. Or let datasight draft files from data:")
    click.echo("     datasight generate <database-or-files> --overwrite")
    click.echo("  6. Run: datasight run")


@cli.group(
    epilog=_epilog(
        """
        The user-global config file (~/.config/datasight/.env) holds API
        keys and tokens shared across every datasight project. Per-project
        .env files override its values, so each project can still pick its
        own LLM provider, model, and database.

        Examples:

            datasight config init
            datasight config show
        """
    )
)
def config():
    """Manage user-global datasight configuration."""


@config.command(name="init")
@click.option("--overwrite", is_flag=True, help="Overwrite the existing global config file.")
def config_init(overwrite: bool):
    """Create the user-global config file (~/.config/datasight/.env).

    Stores API keys and tokens in one place so per-project .env files only
    need to set provider, model, and database settings.
    """

    dest = global_env_path()
    if dest.exists() and not overwrite:
        click.echo(f"Global config already exists: {dest}")
        click.echo("Use --overwrite to replace it.")
        return

    template_path = Path(__file__).parent / "templates" / "global_env.template"
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(template_path, dest)

    click.echo(f"Created: {dest}")
    click.echo()
    click.echo("Next steps:")
    click.echo(f"  1. Edit {dest} and uncomment the API keys you use")
    click.echo("  2. In each project, .env only needs DB_MODE/DB_PATH and")
    click.echo("     (optionally) LLM_PROVIDER and the matching model variable")


@config.command(name="show")
def config_show():
    """Show the resolved datasight configuration and where it loaded from."""
    from dotenv import load_dotenv

    from datasight.recent_projects import validate_project_dir

    cwd = Path.cwd().resolve()
    is_valid, _ = validate_project_dir(str(cwd))
    project_dir = cwd if is_valid else None

    project_env = (project_dir / ".env") if project_dir else None
    global_env = global_env_path()

    if project_env and project_env.exists():
        load_dotenv(project_env, override=False)
    load_global_env(override=False)
    settings = Settings.from_env()

    def mask(secret: str) -> str:
        if not secret:
            return "(not set)"
        return f"…{secret[-4:]}" if len(secret) > 4 else "****"

    click.echo("Config files:")
    click.echo(f"  Global:  {global_env} {'(exists)' if global_env.exists() else '(missing)'}")
    if project_env:
        exists = "(exists)" if project_env.exists() else "(missing)"
        click.echo(f"  Project: {project_env} {exists}")
    else:
        click.echo("  Project: (no datasight project detected in CWD)")

    click.echo()
    click.echo("LLM:")
    click.echo(f"  provider: {settings.llm.provider}")
    click.echo(f"  model:    {settings.llm.model}")
    if settings.llm.base_url:
        click.echo(f"  base_url: {settings.llm.base_url}")
    click.echo(f"  api_key:  {mask(settings.llm.api_key)}")

    click.echo()
    click.echo("Database:")
    click.echo(f"  mode: {settings.database.mode}")
    if settings.database.mode in ("duckdb", "sqlite"):
        click.echo(f"  path: {settings.database.path}")
    elif settings.database.mode == "postgres":
        if settings.database.postgres_url:
            click.echo(f"  url:  {settings.database.postgres_url}")
        else:
            click.echo(
                f"  host: {settings.database.postgres_host}:{settings.database.postgres_port}"
            )
            click.echo(f"  db:   {settings.database.postgres_database}")
    elif settings.database.mode == "flightsql":
        click.echo(f"  uri:  {settings.database.flight_uri}")
    elif settings.database.mode == "spark":
        click.echo(f"  remote: {settings.database.spark_remote}")
        click.echo(f"  max result bytes: {settings.database.spark_max_result_bytes:,}")


@cli.group(
    epilog=_epilog(
        """
        Examples:

            datasight demo eia-generation eia-demo
            datasight demo dsgrid-tempo tempo-demo
            datasight demo time-validation time-demo
        """
    )
)
def demo():
    """Create ready-to-run demo projects with sample datasets."""


@demo.command(
    name="eia-generation",
    epilog=_epilog(
        """
        Example:

            datasight demo eia-generation eia-demo --min-year 2021
        """
    ),
)
@click.argument("project_dir", default=".")
@click.option(
    "--min-year", type=int, default=2020, help="Earliest year to include (default: 2020)."
)
def demo_eia_generation(project_dir: str, min_year: int):
    """Download an EIA energy demo dataset and create a ready-to-run project.

    Downloads cleaned EIA-923 and EIA-860 data from the PUDL project's public
    data releases. Creates a DuckDB database with generation, fuel consumption,
    and plant data, along with pre-written schema descriptions and example queries.

    PROJECT_DIR defaults to the current directory.
    """
    _configure_logging("INFO")

    dest = Path(project_dir).resolve()
    dest.mkdir(parents=True, exist_ok=True)

    click.echo(f"datasight demo eia-generation — downloading EIA energy data (>= {min_year})")
    click.echo(f"  Destination: {dest}")
    click.echo()

    from datasight.demo import download_demo_dataset, write_demo_project_files

    click.echo("Downloading from PUDL (this may take a minute)...")
    db_path = download_demo_dataset(dest, min_year=min_year)
    db_size_mb = db_path.stat().st_size / (1024 * 1024)
    click.echo(f"  Database: {db_path.name} ({db_size_mb:.1f} MB)")

    click.echo("Writing project files...")
    write_demo_project_files(dest, db_path)

    click.echo()
    click.echo("Demo project ready!")
    click.echo()
    click.echo("Next steps:")
    click.echo(f"  1. cd {dest}")
    click.echo("  2. Edit .env — set your ANTHROPIC_API_KEY")
    click.echo("  3. datasight run")


@demo.command(
    name="dsgrid-tempo",
    epilog=_epilog(
        """
        Example:

            datasight demo dsgrid-tempo tempo-demo
        """
    ),
)
@click.argument("project_dir", default=".")
def demo_dsgrid_tempo(project_dir: str):
    """Download dsgrid TEMPO EV charging demand projections.

    Downloads hourly and annual EV charging demand data from NLR's TEMPO
    project (published on OEDI). Creates a DuckDB database with charging
    profiles at census-division level, plus annual summaries by state and
    county. Covers three adoption scenarios from 2024 to 2050.

    Data source: s3://nrel-pds-dsgrid/tempo/tempo-2022/v1.0.0 (public, no credentials needed).

    PROJECT_DIR defaults to the current directory.
    """
    _configure_logging("INFO")

    dest = Path(project_dir).resolve()
    dest.mkdir(parents=True, exist_ok=True)

    click.echo("datasight demo dsgrid-tempo — downloading TEMPO EV charging data")
    click.echo(f"  Destination: {dest}")
    click.echo()

    from datasight.demo_dsgrid_tempo import (
        download_dsgrid_tempo_dataset,
        write_dsgrid_tempo_project_files,
    )

    click.echo("Downloading from OEDI S3 (this may take a minute)...")
    db_path = download_dsgrid_tempo_dataset(dest)
    db_size_mb = db_path.stat().st_size / (1024 * 1024)
    click.echo(f"  Database: {db_path.name} ({db_size_mb:.1f} MB)")

    click.echo("Writing project files...")
    write_dsgrid_tempo_project_files(dest, db_path)

    click.echo()
    click.echo("Demo project ready!")
    click.echo()
    click.echo("Next steps:")
    click.echo(f"  1. cd {dest}")
    click.echo("  2. Edit .env — set your ANTHROPIC_API_KEY")
    click.echo("  3. datasight run")


@demo.command(
    name="time-validation",
    epilog=_epilog(
        """
        Example:

            datasight demo time-validation time-demo
        """
    ),
)
@click.argument("project_dir", default=".")
def demo_time_validation(project_dir: str):
    """Generate a synthetic energy consumption dataset with planted time errors.

    Creates hourly electricity consumption data across sectors, end uses, and
    US states for future projection years (2038, 2039, 2040). The dataset
    contains intentional gaps, duplicates, and DST anomalies that datasight's
    time series quality checks can detect.

    Run "datasight quality" or "datasight run" after setup to find the errors.

    PROJECT_DIR defaults to the current directory.
    """
    _configure_logging("INFO")

    dest = Path(project_dir).resolve()
    dest.mkdir(parents=True, exist_ok=True)

    click.echo("datasight demo time-validation — generating synthetic dataset")
    click.echo(f"  Destination: {dest}")
    click.echo()

    from datasight.demo_time_validation import (
        generate_time_validation_dataset,
        write_time_validation_project_files,
    )

    click.echo("Generating hourly consumption data with planted errors...")
    db_path = generate_time_validation_dataset(dest)
    db_size_mb = db_path.stat().st_size / (1024 * 1024)
    click.echo(f"  Database: {db_path.name} ({db_size_mb:.1f} MB)")

    click.echo("Writing project files...")
    write_time_validation_project_files(dest, db_path)

    click.echo()
    click.echo("Demo project ready!")
    click.echo()
    click.echo("Next steps:")
    click.echo(f"  1. cd {dest}")
    click.echo("  2. datasight quality        # detect the planted errors")
    click.echo("  3. datasight run            # explore interactively")


@cli.command(
    epilog=_epilog(
        """
        Use datasight init for blank templates; use datasight generate to create
        project files from an existing database or data files.

        Examples:

            # Use the database configured in .env
            datasight generate

            # Reference an existing DuckDB or SQLite database directly
            datasight generate grid.duckdb
            datasight generate generation.sqlite

            # Build ./database.duckdb from CSV inputs
            datasight generate generation.csv plants.csv

            # Build ./database.duckdb from Parquet inputs
            datasight generate generation.parquet plants.parquet

            # Build a custom project DuckDB from CSV or Parquet inputs
            datasight generate generation.csv --db-path project.duckdb
            datasight generate generation.parquet --db-path project.duckdb

        FILES are input data. --db-path is only the output DuckDB path used
        when datasight needs to build a project database from CSV/Parquet or
        mixed file inputs.
        """
    )
)
@click.argument("files", nargs=-1, type=click.Path(exists=True))
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing .env.",
)
@click.option("--model", default=None, help="Model name (overrides .env).")
@click.option("--overwrite", is_flag=True, help="Overwrite existing files.")
@click.option(
    "--table",
    "-t",
    multiple=True,
    help="Table or view to include (can be specified multiple times). If omitted, all tables are included.",
)
@click.option(
    "--db-path",
    "db_path",
    type=click.Path(),
    default=None,
    help=(
        "Output DuckDB path to create from CSV/Parquet or mixed file inputs "
        "(default: database.duckdb). Do not use this with a single existing "
        "DuckDB or SQLite database; those are referenced directly."
    ),
)
@click.option(
    "--compact-schema",
    is_flag=True,
    help=(
        "Write schema.yaml with table names only. Default adds an empty "
        "'excluded_columns: []' placeholder per table so you can fill in "
        "glob patterns for columns to hide."
    ),
)
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging.")
def generate(files, project_dir, model, overwrite, table, db_path, compact_schema, verbose):
    """Generate schema_description.md, queries.yaml, measures.yaml, and time_series.yaml from your database.

    Connects to the database, inspects tables and columns, samples
    code/enum columns, and asks the LLM to produce documentation
    and example queries.
    """
    import asyncio

    project_dir = str(Path(project_dir).resolve())

    # Resolve the would-be DB path up front so we can include it in the
    # preflight check — otherwise a stale database.duckdb would abort the
    # run only after the LLM call and the doc writes, leaving behind a
    # partial, mutated project.
    use_files = bool(files)
    db_target: Path | None = None
    sqlite_source_path: Path | None = None
    duckdb_source_path: Path | None = None
    if use_files:
        from datasight.explore import detect_file_type

        resolved_file_types = [
            (Path(file_path).resolve(), detect_file_type(str(Path(file_path).resolve())))
            for file_path in files
        ]
        sqlite_files = [
            file_path for file_path, file_type in resolved_file_types if file_type == "sqlite"
        ]
        duckdb_files = [
            file_path for file_path, file_type in resolved_file_types if file_type == "duckdb"
        ]
        if sqlite_files:
            if len(files) != 1:
                click.echo(
                    "Error: SQLite input currently supports exactly one SQLite file.",
                    err=True,
                )
                sys.exit(1)
            if db_path:
                click.echo(
                    "Error: --db-path is only used when creating a project DuckDB from "
                    "CSV/Parquet or mixed inputs; omit it for an existing SQLite database.",
                    err=True,
                )
                sys.exit(1)
            sqlite_source_path = sqlite_files[0]
        elif len(duckdb_files) == 1 and len(files) == 1:
            if db_path:
                click.echo(
                    "Error: --db-path is only used when creating a project DuckDB from "
                    "CSV/Parquet or mixed inputs; omit it for an existing DuckDB database.",
                    err=True,
                )
                sys.exit(1)
            duckdb_source_path = duckdb_files[0]
        else:
            _db_target = Path(db_path or "database.duckdb")
            if not _db_target.is_absolute():
                _db_target = Path(project_dir) / _db_target
            db_target = _db_target.resolve()

    # Check for existing files early
    schema_path = Path(project_dir) / "schema_description.md"
    schema_config_path = Path(project_dir) / "schema.yaml"
    queries_path = Path(project_dir) / "queries.yaml"
    measures_path = Path(project_dir) / "measures.yaml"
    time_series_path = Path(project_dir) / "time_series.yaml"
    if not overwrite:
        existing = []
        if schema_path.exists():
            existing.append("schema_description.md")
        if schema_config_path.exists():
            existing.append("schema.yaml")
        if queries_path.exists():
            existing.append("queries.yaml")
        if measures_path.exists():
            existing.append("measures.yaml")
        if time_series_path.exists():
            existing.append("time_series.yaml")
        if db_target is not None and db_target.exists():
            existing.append(db_target.name)
        if existing:
            verb = "exists" if len(existing) == 1 else "exist"
            click.echo(
                f"Error: {', '.join(existing)} already {verb}. Use --overwrite to replace.",
                err=True,
            )
            sys.exit(1)

    # Logging
    level = "DEBUG" if verbose else "WARNING"
    _configure_logging(level)

    # Load settings and validate
    settings, resolved_model = _resolve_settings(project_dir, model)
    _validate_settings_for_llm(settings)

    if not use_files:
        resolved_db_path = _resolve_db_path(settings, project_dir)
        if settings.database.mode in ("duckdb", "sqlite") and not os.path.exists(resolved_db_path):
            click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
            sys.exit(1)

    if sqlite_source_path is not None:
        sql_dialect = "sqlite"
    elif duckdb_source_path is not None:
        sql_dialect = "duckdb"
    else:
        sql_dialect = "duckdb" if use_files else settings.database.sql_dialect

    click.echo("datasight generate")
    click.echo(f"  Model:    {resolved_model}")
    if sqlite_source_path is not None:
        click.echo(f"  Database: sqlite - {sqlite_source_path}")
    elif duckdb_source_path is not None:
        click.echo(f"  Database: duckdb - {duckdb_source_path}")
    elif use_files:
        click.echo(f"  Files:    {', '.join(files)}")
    else:
        resolved_db_path = _resolve_db_path(settings, project_dir)
        click.echo(f"  Database: {settings.database.mode} — {resolved_db_path or sql_dialect}")
    click.echo()

    async def _run():
        from datasight.generate import (
            build_generation_context,
            sample_enum_columns,
            sample_timestamp_columns,
        )
        from datasight.schema import introspect_schema

        llm_client = create_llm_client(
            provider=settings.llm.provider,
            api_key=settings.llm.api_key,
            base_url=settings.llm.base_url,
            timeout=settings.llm.timeout,
            model=settings.llm.model,
        )

        if sqlite_source_path is not None:
            from datasight.runner import SQLiteRunner

            sql_runner = SQLiteRunner(str(sqlite_source_path))
            tables_info = []
        elif duckdb_source_path is not None:
            from datasight.runner import DuckDBRunner

            sql_runner = DuckDBRunner(str(duckdb_source_path))
            tables_info = []
        elif use_files:
            from datasight.explore import create_files_session_for_settings

            sql_runner, tables_info = create_files_session_for_settings(
                list(files), settings.database
            )
        else:
            sql_runner = create_sql_runner_from_settings(settings.database, project_dir)
            tables_info = []

        # Introspect schema
        click.echo("Introspecting database schema...")
        tables = await introspect_schema(sql_runner.run_sql, runner=sql_runner)

        # Filter to specified tables if --table was provided
        if table:
            table_set = {t.lower() for t in table}
            found_lower = {t.name.lower() for t in tables}
            missing = [t for t in table if t.lower() not in found_lower]
            tables = [t for t in tables if t.name.lower() in table_set]
            if not tables:
                click.echo(f"Error: No matching tables found for: {', '.join(table)}", err=True)
                sys.exit(1)
            if missing:
                click.echo(
                    f"Warning: --table values not found: {', '.join(missing)}",
                    err=True,
                )

        click.echo(f"  Found {len(tables)} tables")

        # Sample low-cardinality string columns for enum/code detection
        click.echo("Sampling code/enum columns...")
        samples_text = await sample_enum_columns(sql_runner.run_sql, tables)

        # Sample timestamp/date columns so the LLM can infer epoch units
        # and actual time range
        click.echo("Sampling timestamp columns...")
        timestamps_text = await sample_timestamp_columns(sql_runner.run_sql, tables)

        # Build LLM prompt and call
        click.echo("Generating documentation (this may take a moment)...")
        system_prompt, user_msg = build_generation_context(
            tables, sql_dialect, samples_text, timestamps_text=timestamps_text
        )

        from datasight.llm import TextBlock

        response = await llm_client.create_message(
            model=resolved_model,
            system=system_prompt,
            messages=[{"role": "user", "content": user_msg}],
            tools=[],
            max_tokens=4096,
        )

        parts = [block.text for block in response.content if isinstance(block, TextBlock)]
        return "".join(parts), sql_runner, tables_info, tables

    text, sql_runner, tables_info, generated_tables = asyncio.run(_run())

    # Parse response into two files
    from datasight.generate import parse_generation_response

    schema_content, queries_content = parse_generation_response(text)
    if schema_content is None:
        click.echo("Warning: Could not parse LLM response.", err=True)

    # Write files
    written = []
    if schema_content:
        schema_path.write_text(schema_content + "\n", encoding="utf-8")
        written.append("schema_description.md")

    schema_yaml_lines = ["tables:"]
    for t in generated_tables:
        schema_yaml_lines.append(f"  - name: {t.name}")
        if not compact_schema:
            schema_yaml_lines.append("    excluded_columns: []")
    schema_yaml_lines.append("")
    schema_config_path.write_text("\n".join(schema_yaml_lines), encoding="utf-8")
    written.append("schema.yaml")

    if queries_content:
        queries_path.write_text(queries_content + "\n", encoding="utf-8")
        written.append("queries.yaml")

    async def _build_measure_scaffold() -> str:
        if use_files:
            from datasight.schema import introspect_schema

            tables = await introspect_schema(sql_runner.run_sql, runner=sql_runner)
            schema_info = [
                {
                    "name": t.name,
                    "row_count": t.row_count,
                    "columns": [
                        {"name": c.name, "dtype": c.dtype, "nullable": c.nullable}
                        for c in t.columns
                    ],
                }
                for t in tables
            ]
            measure_data = await build_measure_overview(
                schema_info, sql_runner.run_sql, overrides=None
            )
        else:
            _, schema_info = await _load_schema_info_for_project(project_dir, settings)
            measure_data = await build_measure_overview(
                schema_info, sql_runner.run_sql, overrides=None
            )
        return format_measure_overrides_yaml(measure_data)

    measures_path.write_text(asyncio.run(_build_measure_scaffold()), encoding="utf-8")
    written.append("measures.yaml")

    async def _build_time_series_scaffold() -> str:
        from datasight.data_profile import format_time_series_yaml

        if use_files:
            from datasight.schema import introspect_schema

            tables = await introspect_schema(sql_runner.run_sql, runner=sql_runner)
            schema_info = [
                {
                    "name": t.name,
                    "row_count": t.row_count,
                    "columns": [
                        {"name": c.name, "dtype": c.dtype, "nullable": c.nullable}
                        for c in t.columns
                    ],
                }
                for t in tables
            ]
        else:
            _, schema_info = await _load_schema_info_for_project(project_dir, settings)
        return format_time_series_yaml(schema_info)

    time_series_path.write_text(asyncio.run(_build_time_series_scaffold()), encoding="utf-8")
    written.append("time_series.yaml")

    if sqlite_source_path is not None or duckdb_source_path is not None:
        from datasight.config import set_env_vars

        db_source_path = sqlite_source_path or duckdb_source_path
        assert db_source_path is not None
        try:
            rel_db = db_source_path.relative_to(Path(project_dir).resolve())
            db_env_value = f"./{rel_db.as_posix()}"
        except ValueError:
            db_env_value = str(db_source_path)

        env_path = Path(project_dir) / ".env"
        existed = env_path.exists()
        db_mode = "sqlite" if sqlite_source_path is not None else "duckdb"
        set_env_vars(env_path, {"DB_MODE": db_mode, "DB_PATH": db_env_value})
        written.append(".env (updated)" if existed else ".env")
    elif use_files:
        from datasight.config import set_env_vars
        from datasight.explore import build_persistent_duckdb

        assert db_target is not None  # set above when use_files is True
        try:
            build_persistent_duckdb(db_target, tables_info, overwrite=overwrite)
        except FileExistsError:
            # Preflight above rejects pre-existing DBs without --overwrite,
            # so reaching here means the file appeared mid-run.
            click.echo(
                f"Error: Database file already exists: {db_target}.",
                err=True,
            )
            sys.exit(1)
        db_size_mb = db_target.stat().st_size / (1024 * 1024)
        written.append(f"{db_target.name} ({db_size_mb:.2f} MB)")

        try:
            rel_db = db_target.relative_to(Path(project_dir).resolve())
            db_env_value = f"./{rel_db.as_posix()}"
        except ValueError:
            db_env_value = str(db_target)

        env_path = Path(project_dir) / ".env"
        existed = env_path.exists()
        set_env_vars(env_path, {"DB_MODE": "duckdb", "DB_PATH": db_env_value})
        written.append(".env (updated)" if existed else ".env")

    click.echo()
    if written:
        click.echo(f"Created: {', '.join(written)}")
        click.echo()
        click.echo("Next steps:")
        click.echo("  1. Review and edit the generated files")
        click.echo("  2. Run: datasight run")
    else:
        click.echo("No files were written.", err=True)
        sys.exit(1)


@cli.command(
    epilog=_epilog(
        """
        Examples:

            datasight run
            datasight run --project-dir eia-demo
            datasight run --port 9000 --model gpt-4o
        """
    )
)
@click.option("--port", type=int, default=None, help="Web UI port (default: 8084).")
@click.option("--host", default="0.0.0.0", help="Bind address.")
@click.option("--model", default=None, help="LLM model name (overrides .env).")
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=None,
    help="Auto-load this project on startup (optional).",
)
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging.")
def run(
    port,
    host,
    model,
    project_dir,
    verbose,
):
    """Start the datasight web UI.

    If the current directory contains schema_description.md, it will be
    auto-loaded as the project. Otherwise, use the UI to select a project,
    or pass --project-dir to specify one explicitly.
    """
    _, resolved_model, resolved_port = _prepare_web_runtime(
        port=port,
        model=model,
        project_dir=project_dir,
        verbose=verbose,
    )

    if project_dir:
        project_dir = str(Path(project_dir).resolve())

    click.echo(f"datasight v{__version__}")
    click.echo(f"  Model:    {resolved_model}")
    if project_dir:
        click.echo(f"  Project:  {project_dir} (auto-load)")
    else:
        click.echo("  Project:  (none — select in UI)")
    click.echo()

    import uvicorn

    click.echo(f"Starting web UI at http://localhost:{resolved_port} ...")
    uvicorn.run(
        "datasight.web.app:app",
        host=host,
        port=resolved_port,
        log_level="warning",
    )


@cli.command(
    epilog=_epilog(
        """
        Examples:

            datasight verify
            datasight verify --queries verification.yaml
            datasight verify --model gpt-4o

        Add expected results to queries.yaml entries:

          - question: "Top 3 states by generation"
            sql: |
              SELECT state, SUM(mwh) AS total
              FROM generation GROUP BY state
              ORDER BY total DESC LIMIT 3
            expected:
              row_count: 3
              columns: [state, total]
              contains: ["CA", "TX"]
        """
    )
)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing .env and queries.yaml.",
)
@click.option("--model", default=None, help="Model name (overrides .env).")
@click.option(
    "--queries",
    "queries_path",
    type=click.Path(),
    default=None,
    help="Path to queries YAML file (default: queries.yaml in project dir).",
)
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging.")
def verify(project_dir, model, queries_path, verbose):
    """Verify LLM-generated SQL against expected results.

    Runs each question from queries.yaml through the full LLM pipeline,
    executes the generated SQL, and compares results against expected values.
    Use this to validate correctness across different models and providers.
    """
    import asyncio

    project_dir = str(Path(project_dir).resolve())

    # Logging
    level = "DEBUG" if verbose else "WARNING"
    _configure_logging(level)

    # Load queries
    from datasight.config import load_example_queries

    queries = load_example_queries(queries_path, project_dir)
    if not queries:
        click.echo("No queries found. Add questions to queries.yaml first.", err=True)
        sys.exit(1)

    # Load settings and validate
    settings, resolved_model = _resolve_settings(project_dir, model)
    _validate_settings_for_llm(settings)

    resolved_db_path = _resolve_db_path(settings, project_dir)
    if settings.database.mode in ("duckdb", "sqlite") and not os.path.exists(resolved_db_path):
        click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
        sys.exit(1)

    sql_dialect = settings.database.sql_dialect

    click.echo("datasight verify")
    click.echo(f"  Model:    {resolved_model}")
    click.echo(f"  Database: {settings.database.mode} — {resolved_db_path or sql_dialect}")
    click.echo(f"  Queries:  {len(queries)}")
    click.echo()

    async def _run():
        from datasight.config import format_example_queries, load_schema_description
        from datasight.prompts import build_system_prompt
        from datasight.schema import format_schema_context, introspect_schema
        from datasight.schema_links import resolve_schema_description_links
        from datasight.verify import run_ambiguity_analysis, run_verification

        llm_client = create_llm_client(
            provider=settings.llm.provider,
            api_key=settings.llm.api_key,
            base_url=settings.llm.base_url,
            timeout=settings.llm.timeout,
            model=settings.llm.model,
        )
        sql_runner = create_sql_runner_from_settings(settings.database, project_dir)

        # Build system prompt
        tables = await introspect_schema(sql_runner.run_sql, runner=sql_runner)
        user_desc = load_schema_description(None, project_dir)
        user_desc = await resolve_schema_description_links(user_desc)
        schema_text = format_schema_context(tables, user_desc)
        schema_text += format_example_queries(queries)

        sys_prompt = build_system_prompt(schema_text, mode="verify", dialect=sql_dialect)

        # Phase 1: Ambiguity analysis
        ambiguity_results = await run_ambiguity_analysis(
            queries=queries,
            schema_context=schema_text,
            llm_client=llm_client,
            model=resolved_model,
        )

        # Phase 2: SQL verification
        results = await run_verification(
            queries=queries,
            llm_client=llm_client,
            model=resolved_model,
            system_prompt=sys_prompt,
            run_sql=sql_runner.run_sql,
        )
        return results, ambiguity_results

    results, ambiguity_results = asyncio.run(_run())

    # Print results
    from rich import box
    from rich.console import Console
    from rich.table import Table
    from rich.text import Text

    console = Console()

    # --- Ambiguity warnings ---
    ambiguous_count = sum(1 for a in ambiguity_results if a.is_ambiguous)
    if ambiguous_count:
        amb_table = Table(
            show_lines=True,
            box=box.ROUNDED,
            title=f"Ambiguity Analysis ({ambiguous_count} warning{'s' if ambiguous_count != 1 else ''})",
            title_style="bold yellow",
        )
        amb_table.add_column("#", style="dim", no_wrap=True, width=3)
        amb_table.add_column("Question", min_width=25, overflow="fold")
        amb_table.add_column("Ambiguities", overflow="fold")
        amb_table.add_column("Suggested Revision", overflow="fold")

        for i, a in enumerate(ambiguity_results, 1):
            if not a.is_ambiguous:
                continue
            issues = "\n".join(f"- {x}" for x in a.ambiguities) if a.ambiguities else ""
            revision = a.suggested_revision or ""
            amb_table.add_row(
                str(i),
                a.question,
                Text(issues, style="yellow"),
                Text(revision, style="green") if revision else Text("—", style="dim"),
            )
        console.print(amb_table)
        console.print()

    # --- Verification results ---
    table = Table(show_lines=True, box=box.ROUNDED, title="Verification Results")
    table.add_column("#", style="dim", no_wrap=True, width=3)
    table.add_column("Question", min_width=30, overflow="fold")
    table.add_column("Status", no_wrap=True, width=6)
    table.add_column("Checks", overflow="fold")
    table.add_column("Time", justify="right", no_wrap=True, width=8)
    table.add_column("Iters", justify="right", no_wrap=True, width=5)

    total = len(results)
    passed = 0
    failed = 0

    for i, r in enumerate(results, 1):
        if r.passed:
            passed += 1
            status = Text("PASS", style="bold green")
        else:
            failed += 1
            status = Text("FAIL", style="bold red")

        if r.error:
            checks_text = Text(r.error, style="red")
        elif r.checks:
            parts = []
            for c in r.checks:
                mark = "✓" if c.passed else "✗"
                parts.append(f"{mark} {c.name}: {c.detail}")
            checks_text = "\n".join(parts)
        else:
            checks_text = Text("no checks", style="dim")

        time_str = f"{r.execution_time_ms:.0f}ms"
        question = r.question
        if len(question) > 60:
            question = question[:57] + "..."

        table.add_row(str(i), question, status, checks_text, time_str, str(r.llm_iterations))

    console.print(table)

    # SQL comparison — show diffs for failed queries (full SQL) and passed (abbreviated)
    has_diffs = False
    for i, r in enumerate(results, 1):
        if not r.generated_sql or r.generated_sql.strip() == r.reference_sql.strip():
            continue
        if not has_diffs:
            console.print()
            has_diffs = True
        label_style = "red" if not r.passed else "dim"
        console.print(f"[{label_style}]Query {i}: {r.question}[/{label_style}]")
        ref = r.reference_sql.strip()
        gen = r.generated_sql.strip()
        if r.passed:
            # Abbreviated for passing queries
            console.print(f"  [dim]Reference:[/dim] {ref[:120]}{'...' if len(ref) > 120 else ''}")
            console.print(f"  [dim]Generated:[/dim] {gen[:120]}{'...' if len(gen) > 120 else ''}")
        else:
            # Full SQL for failing queries
            console.print("  [dim]Reference:[/dim]")
            for line in ref.splitlines():
                console.print(f"    {line}")
            console.print("  [dim]Generated:[/dim]")
            for line in gen.splitlines():
                console.print(f"    {line}")
        console.print()

    # Summary
    summary_parts = []
    summary_style = "bold green" if failed == 0 else "bold red"
    summary_parts.append(
        f"[{summary_style}]{passed}/{total} passed[/{summary_style}] ({failed} failed)"
    )
    if ambiguous_count:
        summary_parts.append(f"[yellow]{ambiguous_count} ambiguous[/yellow]")
    console.print("\n" + ", ".join(summary_parts))

    sys.exit(0 if failed == 0 else 1)


@cli.command(
    epilog=_epilog(
        """
        Examples:

            datasight ask "What are the top 5 states by generation?"
            datasight ask "Show generation by year" --chart-format html -o chart.html
            datasight ask "Top 5 states" --format csv -o results.csv
            datasight ask --file questions.txt --output-dir batch-output
            datasight ask "Top 5 states" --print-sql
            datasight ask "Top 5 states" --provenance
            datasight ask "Top 5 states" --sql-script top-states.sql
        """
    )
)
@click.argument("question", required=False)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing .env and config files.",
)
@click.option("--model", default=None, help="Model name (overrides .env).")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "csv", "json"]),
    default="table",
    help="Output format for query results (default: table).",
)
@click.option(
    "--chart-format",
    type=click.Choice(["html", "json", "png"]),
    default=None,
    help="Save chart output in this format (requires --output).",
)
@click.option(
    "--output",
    "-o",
    "output_path",
    type=click.Path(),
    default=None,
    help="Output file path for chart or data export.",
)
@click.option(
    "--file",
    "questions_file",
    type=click.Path(exists=True),
    default=None,
    help="Read one question per line from a text file.",
)
@click.option(
    "--output-dir",
    type=click.Path(file_okay=False),
    default=None,
    help="Directory for per-question batch outputs (only with --file).",
)
@click.option(
    "--print-sql",
    is_flag=True,
    help="Print the SQL queries executed by the agent to the console.",
)
@click.option(
    "--provenance",
    is_flag=True,
    help="Print run provenance as JSON to stdout (suppresses human-readable answer).",
)
@click.option(
    "--sql-script",
    "sql_script_path",
    type=click.Path(),
    default=None,
    help=(
        "Write executed queries to a SQL script that materializes results "
        "into auto-named tables (CREATE OR REPLACE)."
    ),
)
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging.")
def ask(
    question,
    project_dir,
    model,
    output_format,
    chart_format,
    output_path,
    questions_file,
    output_dir,
    print_sql,
    provenance,
    sql_script_path,
    verbose,
):
    """Ask a question about your data from the command line.

    Runs the full LLM agent loop without starting a web server.
    Results are printed to the console.
    """
    project_dir = str(Path(project_dir).resolve())

    if not question and not questions_file:
        click.echo("Error: provide a QUESTION or use --file.", err=True)
        sys.exit(1)
    if question and questions_file:
        click.echo("Error: use either QUESTION or --file, not both.", err=True)
        sys.exit(1)
    if questions_file and output_path:
        click.echo(
            "Error: --file cannot be combined with --output. Use --output-dir instead.", err=True
        )
        sys.exit(1)
    if chart_format and not output_path and not questions_file:
        click.echo("Error: --chart-format requires --output.", err=True)
        sys.exit(1)
    if output_dir and not questions_file:
        click.echo("Error: --output-dir can only be used with --file.", err=True)
        sys.exit(1)
    if sql_script_path and questions_file:
        click.echo(
            "Error: --sql-script cannot be combined with --file. "
            "Run individual questions to capture per-question SQL scripts.",
            err=True,
        )
        sys.exit(1)

    # Logging
    level = "DEBUG" if verbose else "WARNING"
    _configure_logging(level)

    # Load settings and validate
    settings, resolved_model = _resolve_settings(project_dir, model)
    _validate_settings_for_llm(settings)

    click.echo(f"Using {settings.llm.provider} model: {resolved_model}", err=True)

    resolved_db_path = _resolve_db_path(settings, project_dir)
    if settings.database.mode in ("duckdb", "sqlite") and not os.path.exists(resolved_db_path):
        click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
        sys.exit(1)

    sql_dialect = settings.database.sql_dialect

    if questions_file:
        entries = _load_batch_entries(questions_file)
        if not entries:
            click.echo("Error: no questions found in file.", err=True)
            sys.exit(1)

        failures = 0
        for idx, entry in enumerate(entries, 1):
            batch_question = str(entry["question"])
            batch_output_format = str(entry.get("output_format") or output_format)
            batch_chart_format = str(entry.get("chart_format") or chart_format or "") or None
            click.echo(f"\n[{idx}/{len(entries)}] {batch_question}")
            click.echo("-" * 72)
            try:
                result = asyncio.run(
                    _run_ask_pipeline(
                        question=batch_question,
                        settings=settings,
                        resolved_model=resolved_model,
                        project_dir=project_dir,
                        sql_dialect=sql_dialect,
                    )
                )
                if not provenance:
                    _emit_ask_result(result, batch_output_format, None, None)
                if print_sql:
                    _print_sql_queries(result)
                if provenance:
                    _emit_cli_provenance(
                        question=batch_question,
                        result=result,
                        model=resolved_model,
                        dialect=sql_dialect,
                        project_dir=project_dir,
                        provider=settings.llm.provider,
                    )
                if output_dir or entry.get("output"):
                    written = _write_batch_result_files(
                        output_dir=output_dir,
                        index=idx,
                        question=batch_question,
                        result=result,
                        output_format=batch_output_format,
                        chart_format=batch_chart_format,
                        name=str(entry.get("name") or ""),
                        output=str(entry.get("output") or ""),
                    )
                    click.echo("Saved:")
                    for path in written:
                        click.echo(f"  {path}")
            except Exception as exc:
                failures += 1
                click.echo(f"Error: {exc}", err=True)

        click.echo(f"\nBatch complete: {len(entries) - failures}/{len(entries)} succeeded.")
        sys.exit(0 if failures == 0 else 1)

    result = asyncio.run(
        _run_ask_pipeline(
            question=question,
            settings=settings,
            resolved_model=resolved_model,
            project_dir=project_dir,
            sql_dialect=sql_dialect,
        )
    )
    if not provenance:
        _emit_ask_result(result, output_format, chart_format, output_path)
    if print_sql:
        _print_sql_queries(result)
    if provenance:
        _emit_cli_provenance(
            question=question,
            result=result,
            model=resolved_model,
            dialect=sql_dialect,
            project_dir=project_dir,
            provider=settings.llm.provider,
        )
    if sql_script_path:
        script_text = _build_sql_script(result, question, sql_dialect)
        script_file = Path(sql_script_path)
        script_file.parent.mkdir(parents=True, exist_ok=True)
        script_file.write_text(script_text, encoding="utf-8")
        # stderr (not stdout) — same reasoning as `_print_sql_queries`:
        # the confirmation is a diagnostic and must not corrupt
        # machine-readable output on stdout (`--format json|csv`).
        click.echo(f"SQL script saved to {script_file}", err=True)


@cli.command(
    epilog=_epilog(
        """
        Examples:

            datasight profile
            datasight profile --table generation_fuel
            datasight profile --column generation_fuel.net_generation_mwh
            datasight profile --format markdown -o profile.md
        """
    )
)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing .env and config files.",
)
@click.option("--table", default=None, help="Profile a specific table.")
@click.option(
    "--column",
    default=None,
    help="Profile a specific column as table.column.",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json", "markdown"]),
    default="table",
    help="Output format (default: table).",
)
@click.option(
    "--output",
    "-o",
    "output_path",
    type=click.Path(),
    default=None,
    help="Write the profile output to a file instead of stdout.",
)
def profile(project_dir, table, column, output_format, output_path):
    """Profile your dataset - row counts, date coverage, and column statistics.

    Use this before asking questions to understand table sizes, candidate
    measures, dimensions, null rates, and date ranges.
    """
    from rich.console import Console

    project_dir = str(Path(project_dir).resolve())
    if table and column:
        click.echo("Error: use either --table or --column, not both.", err=True)
        sys.exit(1)

    settings, _ = _resolve_settings(project_dir)
    resolved_db_path = _resolve_db_path(settings, project_dir)
    if settings.database.mode in ("duckdb", "sqlite") and not os.path.exists(resolved_db_path):
        click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
        sys.exit(1)

    async def _run_profile():
        sql_runner, schema_info = await _load_schema_info_for_project(project_dir, settings)

        if column:
            if "." not in column:
                raise click.ClickException("--column must be in table.column form.")
            table_name, column_name = column.split(".", 1)
            table_info = find_table_info(schema_info, table_name)
            if table_info is None:
                raise click.ClickException(f"Table not found: {table_name}")
            column_info = find_column_info(table_info, column_name)
            if column_info is None:
                raise click.ClickException(f"Column not found: {column}")
            return "column", await build_column_profile(
                table_info, column_info, sql_runner.run_sql
            )

        if table:
            table_info = find_table_info(schema_info, table)
            if table_info is None:
                raise click.ClickException(f"Table not found: {table}")
            return "table", await build_table_profile(table_info, sql_runner.run_sql)

        return "dataset", await build_dataset_overview(schema_info, sql_runner.run_sql)

    scope, profile_data = asyncio.run(_run_profile())

    if output_format == "json":
        _write_or_print(json.dumps(profile_data, indent=2), output_path)
        return

    if output_format == "markdown":
        _write_or_print(_render_profile_markdown(scope, profile_data), output_path)
        return

    console = Console(record=bool(output_path))
    if scope == "dataset":
        summary = _build_metric_table(
            "Dataset Profile",
            [
                ("Tables", str(profile_data["table_count"])),
                ("Columns", str(profile_data["total_columns"])),
                ("Rows", str(profile_data["total_rows"])),
            ],
        )
        console.print(summary)

        largest = _build_profile_detail_table(
            "Largest Tables",
            [("Table", "left"), ("Rows", "right"), ("Columns", "right")],
            [
                [
                    item["name"],
                    f"{item.get('row_count') or 0}",
                    str(item["column_count"]),
                ]
                for item in profile_data["largest_tables"]
            ],
        )
        console.print(largest)
        if profile_data["date_columns"]:
            date_coverage = _build_profile_detail_table(
                "Date Coverage",
                [("Column", "left"), ("Min", "left"), ("Max", "left")],
                [
                    [
                        f"{item['table']}.{item['column']}",
                        _format_profile_value(item.get("min")),
                        _format_profile_value(item.get("max")),
                    ]
                    for item in profile_data["date_columns"]
                ],
            )
            console.print(date_coverage)
        if profile_data["measure_columns"]:
            measures = _build_profile_detail_table(
                "Measure Candidates",
                [("Column", "left"), ("Type", "left")],
                [
                    [
                        f"{item['table']}.{item['column']}",
                        _format_profile_value(item.get("dtype"), "unknown"),
                    ]
                    for item in profile_data["measure_columns"]
                ],
            )
            console.print(measures)
        if profile_data["dimension_columns"]:
            dimensions = _build_profile_detail_table(
                "Dimension Candidates",
                [
                    ("Column", "left"),
                    ("Distinct", "right"),
                    ("Null %", "right"),
                    ("Samples", "left"),
                ],
                [
                    [
                        f"{item['table']}.{item['column']}",
                        _format_profile_value(item.get("distinct_count")),
                        _format_profile_value(item.get("null_rate"), "0"),
                        ", ".join((item.get("sample_values") or [])[:3]) or "none",
                    ]
                    for item in profile_data["dimension_columns"]
                ],
            )
            console.print(dimensions)
        if output_path:
            _write_or_print(console.export_text(), output_path)
        return

    if scope == "table":
        table_summary = _build_metric_table(
            f"Table Profile: {profile_data['table']}",
            [
                ("Rows", str(profile_data.get("row_count") or 0)),
                ("Columns", str(profile_data["column_count"])),
            ],
        )
        console.print(table_summary)

        if profile_data["null_columns"]:
            nulls = _build_profile_detail_table(
                "Null-heavy Columns",
                [("Column", "left"), ("Nulls", "right"), ("Null %", "right")],
                [
                    [
                        item["column"],
                        str(item["null_count"]),
                        str(item.get("null_rate") or 0),
                    ]
                    for item in profile_data["null_columns"]
                ],
            )
            console.print(nulls)
        if profile_data["date_columns"]:
            dates = _build_profile_detail_table(
                "Date Columns",
                [("Column", "left"), ("Min", "left"), ("Max", "left")],
                [
                    [
                        item["column"],
                        _format_profile_value(item.get("min")),
                        _format_profile_value(item.get("max")),
                    ]
                    for item in profile_data["date_columns"]
                ],
            )
            console.print(dates)
        if profile_data["numeric_columns"]:
            numeric = _build_profile_detail_table(
                "Numeric Columns",
                [("Column", "left"), ("Min", "left"), ("Max", "left"), ("Avg", "left")],
                [
                    [
                        item["column"],
                        _format_profile_value(item.get("min")),
                        _format_profile_value(item.get("max")),
                        _format_profile_value(item.get("avg")),
                    ]
                    for item in profile_data["numeric_columns"]
                ],
            )
            console.print(numeric)
        if profile_data["text_columns"]:
            text_dimensions = _build_profile_detail_table(
                "Text Dimensions",
                [
                    ("Column", "left"),
                    ("Distinct", "right"),
                    ("Null %", "right"),
                    ("Samples", "left"),
                ],
                [
                    [
                        item["column"],
                        _format_profile_value(item.get("distinct_count")),
                        _format_profile_value(item.get("null_rate"), "0"),
                        ", ".join((item.get("sample_values") or [])[:3]) or "none",
                    ]
                    for item in profile_data["text_columns"]
                ],
            )
            console.print(text_dimensions)
        if output_path:
            _write_or_print(console.export_text(), output_path)
        return

    column_summary = _build_metric_table(
        f"Column Profile: {profile_data['table']}.{profile_data['column']}",
        [
            ("Type", str(profile_data.get("dtype") or "unknown")),
            ("Distinct", str(profile_data.get("distinct_count"))),
            ("Nulls", str(profile_data.get("null_count"))),
            ("Null %", str(profile_data.get("null_rate"))),
        ],
    )
    console.print(column_summary)
    if profile_data.get("numeric_stats"):
        stats = profile_data["numeric_stats"]
        console.print(
            _build_profile_detail_table(
                "Numeric Stats",
                [("Min", "left"), ("Max", "left"), ("Avg", "left")],
                [
                    [
                        _format_profile_value(stats.get("min")),
                        _format_profile_value(stats.get("max")),
                        _format_profile_value(stats.get("avg")),
                    ]
                ],
            )
        )
    if profile_data.get("date_coverage"):
        stats = profile_data["date_coverage"]
        console.print(
            _build_profile_detail_table(
                "Date Coverage",
                [("Min", "left"), ("Max", "left")],
                [
                    [
                        _format_profile_value(stats.get("min")),
                        _format_profile_value(stats.get("max")),
                    ]
                ],
            )
        )
    if profile_data.get("dimension_stats"):
        stats = profile_data["dimension_stats"]
        console.print(
            _build_profile_detail_table(
                "Dimension Stats",
                [("Distinct", "right"), ("Nulls", "right"), ("Samples", "left")],
                [
                    [
                        _format_profile_value(stats.get("distinct_count")),
                        _format_profile_value(stats.get("null_count")),
                        ", ".join((stats.get("sample_values") or [])[:5]) or "none",
                    ]
                ],
            )
        )
    elif profile_data.get("sample_values"):
        console.print(
            _build_profile_detail_table(
                "Sample Values",
                [("Values", "left")],
                [[", ".join(profile_data["sample_values"][:5])]],
            )
        )
    if output_path:
        _write_or_print(console.export_text(), output_path)


@cli.command(
    epilog=_epilog(
        """
        Examples:

            datasight measures
            datasight measures --table generation_fuel
            datasight measures --scaffold
            datasight measures --format markdown -o measures.md
        """
    )
)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing .env and config files.",
)
@click.option("--table", default=None, help="Inspect measures for a specific table.")
@click.option(
    "--scaffold", is_flag=True, help="Write an editable measures.yaml scaffold and exit."
)
@click.option("--overwrite", is_flag=True, help="Overwrite an existing scaffold file.")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json", "markdown"]),
    default="table",
    help="Output format (default: table).",
)
@click.option(
    "--output",
    "-o",
    "output_path",
    type=click.Path(),
    default=None,
    help="Write the measure overview to a file instead of stdout.",
)
def measures(project_dir, table, scaffold, overwrite, output_format, output_path):
    """Surface likely measures and default aggregations.

    Measures are numeric columns that should usually be summed, averaged,
    or otherwise aggregated in generated SQL. Use --scaffold to create an
    editable measures.yaml override file.
    """
    from rich.console import Console
    from datasight.config import load_measure_overrides

    project_dir = str(Path(project_dir).resolve())
    settings, _ = _resolve_settings(project_dir)
    resolved_db_path = _resolve_db_path(settings, project_dir)
    if settings.database.mode in ("duckdb", "sqlite") and not os.path.exists(resolved_db_path):
        click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
        sys.exit(1)

    async def _run_measures():
        sql_runner, schema_info = await _load_schema_info_for_project(project_dir, settings)
        measure_overrides = load_measure_overrides(None, project_dir)
        if table:
            table_info = find_table_info(schema_info, table)
            if table_info is None:
                raise click.ClickException(f"Table not found: {table}")
            schema_info = [table_info]
        return await build_measure_overview(schema_info, sql_runner.run_sql, measure_overrides)

    measure_data = asyncio.run(_run_measures())

    if scaffold:
        scaffold_path = Path(output_path) if output_path else Path(project_dir) / "measures.yaml"
        if scaffold_path.exists() and not overwrite:
            click.echo(
                f"Error: {scaffold_path} already exists. Use --overwrite to replace.",
                err=True,
            )
            sys.exit(1)
        scaffold_path.parent.mkdir(parents=True, exist_ok=True)
        scaffold_path.write_text(
            format_measure_overrides_yaml(measure_data),
            encoding="utf-8",
        )
        click.echo(f"Measure override scaffold saved to {scaffold_path}")
        return

    if output_format == "json":
        _write_or_print(json.dumps(measure_data, indent=2), output_path)
        return

    if output_format == "markdown":
        _write_or_print(_render_measures_markdown(measure_data), output_path)
        return

    console = Console(record=bool(output_path))
    console.print(
        _build_metric_table(
            "Measure Overview",
            [("Tables scanned", str(measure_data["table_count"]))],
        )
    )
    if measure_data["measures"]:
        console.print(
            _build_profile_detail_table(
                "Measure Candidates",
                [
                    ("Column", "left"),
                    ("Role", "left"),
                    ("Unit", "left"),
                    ("Default", "left"),
                    ("Averaging", "left"),
                    ("Rollup SQL", "left"),
                    ("Allowed", "left"),
                    ("Additive", "left"),
                ],
                [
                    [
                        f"{item['table']}.{item['column']}",
                        item["role"]
                        + (f" [{item['display_name']}]" if item.get("display_name") else ""),
                        _format_profile_value(item.get("unit"), "—"),
                        item["default_aggregation"]
                        + (f" ({item['format']})" if item.get("format") else ""),
                        (
                            f"weighted by {item['weight_column']}"
                            if item.get("weight_column")
                            else item.get("average_strategy", "avg")
                        ),
                        item["recommended_rollup_sql"],
                        (
                            (", ".join(item["allowed_aggregations"]))
                            + (f" | expr: {item['expression']}" if item.get("expression") else "")
                            + (
                                f" | charts: {', '.join(item['preferred_chart_types'])}"
                                if item.get("preferred_chart_types")
                                else ""
                            )
                        ),
                        (
                            ("category" if item.get("additive_across_category") else "")
                            + (
                                ", time"
                                if item.get("additive_across_category")
                                and item.get("additive_across_time")
                                else ("time" if item.get("additive_across_time") else "")
                            )
                        )
                        or "no",
                    ]
                    for item in measure_data["measures"]
                ],
            )
        )
        console.print(
            _build_profile_detail_table(
                "Aggregation Guidance",
                [("Column", "left"), ("Avoid", "left"), ("Why", "left")],
                [
                    [
                        f"{item['table']}.{item['column']}",
                        ", ".join(item.get("forbidden_aggregations") or []) or "—",
                        item["reason"],
                    ]
                    for item in measure_data["measures"]
                ],
            )
        )
    if measure_data["notes"]:
        console.print(
            _build_profile_detail_table(
                "Notes", [("Observation", "left")], [[item] for item in measure_data["notes"]]
            )
        )
    if output_path:
        _write_or_print(console.export_text(), output_path)


@cli.command(
    epilog=_epilog(
        """
        Examples:

            datasight quality
            datasight quality --table generation_fuel
            datasight quality --format markdown -o quality.md
        """
    )
)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing .env and config files.",
)
@click.option("--table", default=None, help="Audit a specific table.")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json", "markdown"]),
    default="table",
    help="Output format (default: table).",
)
@click.option(
    "--output",
    "-o",
    "output_path",
    type=click.Path(),
    default=None,
    help="Write the quality audit to a file instead of stdout.",
)
def quality(project_dir, table, output_format, output_path):
    """Audit data quality - nulls, suspicious ranges, and date coverage.

    Also checks temporal completeness when time_series.yaml defines expected
    time series structure.
    """
    from rich.console import Console

    project_dir = str(Path(project_dir).resolve())
    settings, _ = _resolve_settings(project_dir)
    resolved_db_path = _resolve_db_path(settings, project_dir)
    if settings.database.mode in ("duckdb", "sqlite") and not os.path.exists(resolved_db_path):
        click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
        sys.exit(1)

    from datasight.config import load_time_series_config
    from datasight.data_profile import build_time_series_quality

    time_series_configs = load_time_series_config(None, project_dir)

    async def _run_quality():
        sql_runner, schema_info = await _load_schema_info_for_project(project_dir, settings)
        if table:
            table_info = find_table_info(schema_info, table)
            if table_info is None:
                raise click.ClickException(f"Table not found: {table}")
            schema_info = [table_info]
        base = await build_quality_overview(schema_info, sql_runner.run_sql)
        ts_configs = time_series_configs
        if table and ts_configs:
            ts_configs = [c for c in ts_configs if c["table"].lower() == table.lower()]
        if ts_configs:
            ts_data = await build_time_series_quality(ts_configs, sql_runner.run_sql)
            base["time_series_issues"] = ts_data.get("time_series_issues", [])
            base["time_series_summaries"] = ts_data.get("time_series_summaries", [])
        return base

    quality_data = asyncio.run(_run_quality())

    if output_format == "json":
        _write_or_print(json.dumps(quality_data, indent=2), output_path)
        return

    if output_format == "markdown":
        _write_or_print(_render_quality_markdown(quality_data), output_path)
        return

    console = Console(record=bool(output_path))
    console.print(
        _build_metric_table(
            "Dataset Quality Audit",
            [("Tables scanned", str(quality_data["table_count"]))],
        )
    )
    if quality_data["null_columns"]:
        console.print(
            _build_profile_detail_table(
                "Null-heavy Columns",
                [("Column", "left"), ("Nulls", "right"), ("Null %", "right")],
                [
                    [
                        f"{item['table']}.{item['column']}",
                        str(item["null_count"]),
                        str(item.get("null_rate") or 0),
                    ]
                    for item in quality_data["null_columns"]
                ],
            )
        )
    if quality_data["numeric_flags"]:
        console.print(
            _build_profile_detail_table(
                "Numeric Range Flags",
                [("Column", "left"), ("Issue", "left")],
                [
                    [f"{item['table']}.{item['column']}", item["issue"]]
                    for item in quality_data["numeric_flags"]
                ],
            )
        )
    if quality_data["date_columns"]:
        console.print(
            _build_profile_detail_table(
                "Date Coverage",
                [("Column", "left"), ("Min", "left"), ("Max", "left")],
                [
                    [
                        f"{item['table']}.{item['column']}",
                        _format_profile_value(item.get("min")),
                        _format_profile_value(item.get("max")),
                    ]
                    for item in quality_data["date_columns"]
                ],
            )
        )
    if quality_data.get("time_series_summaries"):
        console.print(
            _build_profile_detail_table(
                "Time Series",
                [("Column", "left"), ("Frequency", "left"), ("Rows", "right"), ("Range", "left")],
                [
                    [
                        f"{s['table']}.{s['timestamp_column']}",
                        s.get("frequency", ""),
                        str(s.get("total_rows", "")),
                        f"{s.get('min_ts', '')} — {s.get('max_ts', '')}",
                    ]
                    for s in quality_data["time_series_summaries"]
                ],
            )
        )
    if quality_data.get("time_series_issues"):
        console.print(
            _build_profile_detail_table(
                "Temporal Completeness",
                [("Column", "left"), ("Issue", "left"), ("Detail", "left")],
                [
                    [
                        f"{item['table']}.{item['timestamp_column']}",
                        item["issue"],
                        item["detail"],
                    ]
                    for item in quality_data["time_series_issues"]
                ],
            )
        )
    if quality_data["notes"]:
        console.print(
            _build_profile_detail_table(
                "Notes",
                [("Observation", "left")],
                [[item] for item in quality_data["notes"]],
            )
        )
    if output_path:
        _write_or_print(console.export_text(), output_path)


# ---------------------------------------------------------------------------
# Integrity command
# ---------------------------------------------------------------------------


@cli.command(
    epilog=_epilog(
        """
        Examples:

            datasight integrity
            datasight integrity --table plants
            datasight integrity --format json -o integrity.json
        """
    )
)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing .env and config files.",
)
@click.option("--table", default=None, help="Focus integrity checks on a specific table.")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json", "markdown"]),
    default="table",
    help="Output format (default: table).",
)
@click.option(
    "--output",
    "-o",
    "output_path",
    type=click.Path(),
    default=None,
    help="Write the integrity audit to a file instead of stdout.",
)
def integrity(project_dir, table, output_format, output_path):
    """Audit cross-table referential integrity - keys, orphans, and join risks.

    Use this to find likely primary keys, duplicate keys, orphaned foreign
    keys, and joins that may multiply rows unexpectedly.
    """
    from rich.console import Console

    project_dir = str(Path(project_dir).resolve())
    settings, _ = _resolve_settings(project_dir)
    resolved_db_path = _resolve_db_path(settings, project_dir)
    if settings.database.mode in ("duckdb", "sqlite") and not os.path.exists(resolved_db_path):
        click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
        sys.exit(1)

    from datasight.config import load_joins_config

    declared_joins = load_joins_config(None, project_dir) or None

    async def _run_integrity():
        sql_runner, schema_info = await _load_schema_info_for_project(project_dir, settings)
        if table:
            table_info = find_table_info(schema_info, table)
            if table_info is None:
                raise click.ClickException(f"Table not found: {table}")
            schema_info_filtered = [table_info]
        else:
            schema_info_filtered = schema_info
        return await build_integrity_overview(
            schema_info_filtered, sql_runner.run_sql, declared_joins
        )

    integrity_data = asyncio.run(_run_integrity())

    if output_format == "json":
        _write_or_print(json.dumps(integrity_data, indent=2), output_path)
        return

    if output_format == "markdown":
        _write_or_print(_render_integrity_markdown(integrity_data), output_path)
        return

    console = Console(record=bool(output_path))
    console.print(
        _build_metric_table(
            "Referential Integrity",
            [("Tables scanned", str(integrity_data["table_count"]))],
        )
    )
    if integrity_data["primary_keys"]:
        console.print(
            _build_profile_detail_table(
                "Primary Keys",
                [
                    ("Table", "left"),
                    ("Column", "left"),
                    ("Distinct", "right"),
                    ("Rows", "right"),
                    ("Unique", "left"),
                ],
                [
                    [
                        item["table"],
                        item["column"],
                        str(item["distinct_count"]),
                        str(item["row_count"]),
                        "yes" if item["is_unique"] else "NO",
                    ]
                    for item in integrity_data["primary_keys"]
                ],
            )
        )
    if integrity_data["duplicate_keys"]:
        console.print(
            _build_profile_detail_table(
                "Duplicate Keys",
                [("Table", "left"), ("Column", "left"), ("Duplicates", "right")],
                [
                    [item["table"], item["column"], str(item["duplicate_count"])]
                    for item in integrity_data["duplicate_keys"]
                ],
            )
        )
    if integrity_data["orphan_foreign_keys"]:
        console.print(
            _build_profile_detail_table(
                "Orphan Foreign Keys",
                [
                    ("Child", "left"),
                    ("Parent", "left"),
                    ("Orphans", "right"),
                    ("Child Rows", "right"),
                ],
                [
                    [
                        f"{item['child_table']}.{item['child_column']}",
                        f"{item['parent_table']}.{item['parent_column']}",
                        str(item["orphan_count"]),
                        str(item["child_rows"]),
                    ]
                    for item in integrity_data["orphan_foreign_keys"]
                ],
            )
        )
    if integrity_data["join_explosions"]:
        console.print(
            _build_profile_detail_table(
                "Join Explosion Risks",
                [
                    ("Table A", "left"),
                    ("Table B", "left"),
                    ("Column", "left"),
                    ("Expected", "right"),
                    ("Actual", "right"),
                    ("Factor", "right"),
                ],
                [
                    [
                        item["table_a"],
                        item["table_b"],
                        item["join_column"],
                        str(item["expected_rows"]),
                        str(item["actual_rows"]),
                        f"{item['explosion_factor']}x",
                    ]
                    for item in integrity_data["join_explosions"]
                ],
            )
        )
    if integrity_data["notes"]:
        console.print(
            _build_profile_detail_table(
                "Notes",
                [("Observation", "left")],
                [[item] for item in integrity_data["notes"]],
            )
        )
    if output_path:
        _write_or_print(console.export_text(), output_path)


# ---------------------------------------------------------------------------
# Distribution command
# ---------------------------------------------------------------------------


@cli.command(
    epilog=_epilog(
        """
        Examples:

            datasight distribution
            datasight distribution --table generation_fuel
            datasight distribution --column generation_fuel.net_generation_mwh
            datasight distribution --format markdown -o distributions.md
        """
    )
)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing .env and config files.",
)
@click.option("--table", default=None, help="Profile distributions for a specific table.")
@click.option(
    "--column",
    default=None,
    help="Focus on a specific column as table.column.",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json", "markdown"]),
    default="table",
    help="Output format (default: table).",
)
@click.option(
    "--output",
    "-o",
    "output_path",
    type=click.Path(),
    default=None,
    help="Write the distribution profile to a file instead of stdout.",
)
def distribution(project_dir, table, column, output_format, output_path):
    """Profile value distributions - percentiles, outliers, and energy flags.

    Use this to inspect numeric ranges, skew, zero/negative rates, outliers,
    and energy-domain flags before building charts or validation rules.
    """
    from rich.console import Console

    project_dir = str(Path(project_dir).resolve())
    if table and column:
        click.echo("Error: use either --table or --column, not both.", err=True)
        sys.exit(1)

    settings, _ = _resolve_settings(project_dir)
    resolved_db_path = _resolve_db_path(settings, project_dir)
    if settings.database.mode in ("duckdb", "sqlite") and not os.path.exists(resolved_db_path):
        click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
        sys.exit(1)

    from datasight.config import load_measure_overrides

    measure_overrides = load_measure_overrides(None, project_dir)

    async def _run_distribution():
        sql_runner, schema_info = await _load_schema_info_for_project(project_dir, settings)
        if table:
            table_info = find_table_info(schema_info, table)
            if table_info is None:
                raise click.ClickException(f"Table not found: {table}")
            schema_info_filtered = [table_info]
        else:
            schema_info_filtered = schema_info
        return await build_distribution_overview(
            schema_info_filtered, sql_runner.run_sql, measure_overrides, column
        )

    dist_data = asyncio.run(_run_distribution())

    if output_format == "json":
        _write_or_print(json.dumps(dist_data, indent=2), output_path)
        return

    if output_format == "markdown":
        _write_or_print(_render_distribution_markdown(dist_data), output_path)
        return

    console = Console(record=bool(output_path))
    console.print(
        _build_metric_table(
            "Distribution Profiling",
            [("Tables scanned", str(dist_data["table_count"]))],
        )
    )
    if dist_data["distributions"]:
        console.print(
            _build_profile_detail_table(
                "Distributions",
                [
                    ("Column", "left"),
                    ("p5", "right"),
                    ("p50", "right"),
                    ("p95", "right"),
                    ("Zero %", "right"),
                    ("Neg %", "right"),
                    ("Outliers", "right"),
                ],
                [
                    [
                        f"{d['table']}.{d['column']}",
                        _fmt_dist(d.get("p5")),
                        _fmt_dist(d.get("p50")),
                        _fmt_dist(d.get("p95")),
                        _fmt_dist(d.get("zero_rate")),
                        _fmt_dist(d.get("negative_rate")),
                        str(d.get("outlier_count", 0)),
                    ]
                    for d in dist_data["distributions"]
                ],
            )
        )
    if dist_data["energy_flags"]:
        console.print(
            _build_profile_detail_table(
                "Energy Flags",
                [("Column", "left"), ("Flag", "left"), ("Detail", "left")],
                [
                    [f"{f['table']}.{f['column']}", f["flag"], f["detail"]]
                    for f in dist_data["energy_flags"]
                ],
            )
        )
    if dist_data["spikes"]:
        console.print(
            _build_profile_detail_table(
                "Temporal Spikes",
                [("Column", "left"), ("Period", "left"), ("Z-score", "right"), ("Detail", "left")],
                [
                    [
                        f"{s['table']}.{s['measure_column']}",
                        s["period"],
                        str(s["z_score"]),
                        s["detail"],
                    ]
                    for s in dist_data["spikes"]
                ],
            )
        )
    if dist_data["notes"]:
        console.print(
            _build_profile_detail_table(
                "Notes",
                [("Observation", "left")],
                [[item] for item in dist_data["notes"]],
            )
        )
    if output_path:
        _write_or_print(console.export_text(), output_path)


# ---------------------------------------------------------------------------
# Validate command
# ---------------------------------------------------------------------------


@cli.command(
    epilog=_epilog(
        """
        Examples:

            datasight validate --scaffold
            datasight validate
            datasight validate --table generation_fuel
            datasight validate --format markdown -o validation.md
        """
    )
)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing .env and config files.",
)
@click.option("--table", default=None, help="Run rules for a specific table only.")
@click.option(
    "--config",
    "config_path",
    type=click.Path(),
    default=None,
    help="Path to validation.yaml (default: project_dir/validation.yaml).",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json", "markdown"]),
    default="table",
    help="Output format (default: table).",
)
@click.option(
    "--output",
    "-o",
    "output_path",
    type=click.Path(),
    default=None,
    help="Write the validation report to a file instead of stdout.",
)
@click.option(
    "--scaffold",
    is_flag=True,
    help="Write an example validation.yaml to the project directory and exit.",
)
@click.option("--overwrite", is_flag=True, help="Overwrite an existing validation.yaml.")
def validate(project_dir, table, config_path, output_format, output_path, scaffold, overwrite):
    """Run declarative validation rules against the database.

    Rules live in validation.yaml. Use --scaffold to create a starter file,
    edit it for your dataset, then run validate to produce pass/fail output.
    """
    from rich.console import Console

    project_dir = str(Path(project_dir).resolve())

    if scaffold:
        target = Path(config_path) if config_path else Path(project_dir) / "validation.yaml"
        if target.exists() and not overwrite:
            click.echo(
                f"Error: {target} already exists. Use --overwrite to replace.",
                err=True,
            )
            sys.exit(1)
        template = Path(__file__).parent / "templates" / "validation.yaml"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(template.read_text(encoding="utf-8"), encoding="utf-8")
        click.echo(f"Wrote {target}. Edit the rules to match your dataset.")
        return

    settings, _ = _resolve_settings(project_dir)
    resolved_db_path = _resolve_db_path(settings, project_dir)
    if settings.database.mode in ("duckdb", "sqlite") and not os.path.exists(resolved_db_path):
        click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
        sys.exit(1)

    rules = load_validation_config(config_path, project_dir)
    if not rules:
        click.echo(
            "No validation rules configured. Run `datasight validate --scaffold` "
            "to generate an example validation.yaml, then edit it for your dataset."
        )
        return

    if table:
        rules = [r for r in rules if r.get("table", "").lower() == table.lower()]
        if not rules:
            click.echo(f"No validation rules found for table: {table}")
            return

    async def _run_validate():
        sql_runner, schema_info = await _load_schema_info_for_project(project_dir, settings)
        return await build_validation_report(schema_info, sql_runner.run_sql, rules)

    validation_data = asyncio.run(_run_validate())

    if output_format == "json":
        _write_or_print(json.dumps(validation_data, indent=2), output_path)
        return

    if output_format == "markdown":
        _write_or_print(_render_validation_markdown(validation_data), output_path)
        return

    summary = validation_data.get("summary", {})
    console = Console(record=bool(output_path))
    console.print(
        _build_metric_table(
            "Validation Report",
            [
                ("Rules run", str(validation_data.get("rule_count", 0))),
                ("Pass", str(summary.get("pass", 0))),
                ("Fail", str(summary.get("fail", 0))),
                ("Warn", str(summary.get("warn", 0))),
            ],
        )
    )
    if validation_data["results"]:
        console.print(
            _build_profile_detail_table(
                "Results",
                [
                    ("Table", "left"),
                    ("Rule", "left"),
                    ("Column", "left"),
                    ("Status", "left"),
                    ("Detail", "left"),
                ],
                [
                    [
                        r["table"],
                        r["rule"],
                        r.get("column") or "-",
                        (
                            f"[green]{r['status'].upper()}[/green]"
                            if r["status"] == "pass"
                            else (
                                f"[red]{r['status'].upper()}[/red]"
                                if r["status"] == "fail"
                                else f"[yellow]{r['status'].upper()}[/yellow]"
                            )
                        ),
                        r["detail"],
                    ]
                    for r in validation_data["results"]
                ],
            )
        )
    if output_path:
        _write_or_print(console.export_text(), output_path)


# ---------------------------------------------------------------------------
# Audit report command
# ---------------------------------------------------------------------------


@cli.command(
    name="audit-report",
    epilog=_epilog(
        """
        Examples:

            datasight audit-report
            datasight audit-report -o audit.html
            datasight audit-report --format markdown -o audit.md
            datasight audit-report --table generation_fuel -o generation-audit.html
        """
    ),
)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing .env and config files.",
)
@click.option("--table", default=None, help="Scope the audit to a specific table.")
@click.option(
    "--output",
    "-o",
    "output_path",
    type=click.Path(),
    default="report.html",
    show_default=True,
    help="Output path (.html, .md, or .json).",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["html", "markdown", "json"]),
    default=None,
    help="Output format (default: inferred from file extension).",
)
def audit_report(project_dir, table, output_path, output_format):
    """Generate a comprehensive audit report combining all checks.

    Combines profile, measures, quality, integrity, distribution, and
    validation results into one HTML, Markdown, or JSON artifact.
    """
    _configure_logging("INFO")
    project_dir = str(Path(project_dir).resolve())
    settings, _ = _resolve_settings(project_dir)
    resolved_db_path = _resolve_db_path(settings, project_dir)
    if settings.database.mode in ("duckdb", "sqlite") and not os.path.exists(resolved_db_path):
        click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
        sys.exit(1)

    # Infer format from extension if not specified
    if output_format is None:
        ext = Path(output_path).suffix.lower()
        if ext == ".html":
            output_format = "html"
        elif ext == ".md":
            output_format = "markdown"
        elif ext == ".json":
            output_format = "json"
        else:
            output_format = "html"

    from datasight.config import load_joins_config, load_measure_overrides

    measure_overrides = load_measure_overrides(None, project_dir)
    validation_rules = load_validation_config(None, project_dir) or None
    declared_joins = load_joins_config(None, project_dir) or None

    async def _run_audit_report():
        sql_runner, schema_info = await _load_schema_info_for_project(project_dir, settings)
        if table:
            table_info = find_table_info(schema_info, table)
            if table_info is None:
                raise click.ClickException(f"Table not found: {table}")
            schema_info_filtered = [table_info]
        else:
            schema_info_filtered = schema_info
        return await build_audit_report(
            schema_info_filtered,
            sql_runner.run_sql,
            measure_overrides,
            validation_rules,
            declared_joins,
            project_name=Path(project_dir).name,
        )

    report_data = asyncio.run(_run_audit_report())

    if output_format == "json":
        _write_or_print(json.dumps(report_data, indent=2), output_path)
    elif output_format == "markdown":
        _write_or_print(render_audit_report_markdown(report_data), output_path)
    else:
        _write_or_print(render_audit_report_html(report_data), output_path)


@cli.command(
    epilog=_epilog(
        """
        Examples:

            datasight dimensions
            datasight dimensions --table generation_fuel
            datasight dimensions --format json -o dimensions.json
        """
    )
)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing .env and config files.",
)
@click.option("--table", default=None, help="Inspect dimensions for a specific table.")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json", "markdown"]),
    default="table",
    help="Output format (default: table).",
)
@click.option(
    "--output",
    "-o",
    "output_path",
    type=click.Path(),
    default=None,
    help="Write the dimension overview to a file instead of stdout.",
)
def dimensions(project_dir, table, output_format, output_path):
    """Surface likely grouping dimensions and category breakdowns.

    Use this to find text/code columns that are good GROUP BY candidates,
    such as fuel codes, states, sectors, plants, or scenario labels.
    """
    from rich.console import Console

    project_dir = str(Path(project_dir).resolve())
    settings, _ = _resolve_settings(project_dir)
    resolved_db_path = _resolve_db_path(settings, project_dir)
    if settings.database.mode in ("duckdb", "sqlite") and not os.path.exists(resolved_db_path):
        click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
        sys.exit(1)

    async def _run_dimensions():
        sql_runner, schema_info = await _load_schema_info_for_project(project_dir, settings)
        if table:
            table_info = find_table_info(schema_info, table)
            if table_info is None:
                raise click.ClickException(f"Table not found: {table}")
            schema_info = [table_info]
        return await build_dimension_overview(schema_info, sql_runner.run_sql)

    dimension_data = asyncio.run(_run_dimensions())

    if output_format == "json":
        _write_or_print(json.dumps(dimension_data, indent=2), output_path)
        return

    if output_format == "markdown":
        _write_or_print(_render_dimensions_markdown(dimension_data), output_path)
        return

    console = Console(record=bool(output_path))
    console.print(
        _build_metric_table(
            "Dimension Overview",
            [("Tables scanned", str(dimension_data["table_count"]))],
        )
    )
    if dimension_data["dimension_columns"]:
        console.print(
            _build_profile_detail_table(
                "Dimension Candidates",
                [
                    ("Column", "left"),
                    ("Distinct", "right"),
                    ("Null %", "right"),
                    ("Samples", "left"),
                ],
                [
                    [
                        f"{item['table']}.{item['column']}",
                        _format_profile_value(item.get("distinct_count")),
                        _format_profile_value(item.get("null_rate"), "0"),
                        ", ".join((item.get("sample_values") or [])[:3]) or "none",
                    ]
                    for item in dimension_data["dimension_columns"]
                ],
            )
        )
    if dimension_data["suggested_breakdowns"]:
        console.print(
            _build_profile_detail_table(
                "Suggested Breakdowns",
                [("Column", "left"), ("Reason", "left")],
                [
                    [f"{item['table']}.{item['column']}", item["reason"]]
                    for item in dimension_data["suggested_breakdowns"]
                ],
            )
        )
    if dimension_data["join_hints"]:
        console.print(
            _build_profile_detail_table(
                "Join Hints", [("Hint", "left")], [[item] for item in dimension_data["join_hints"]]
            )
        )
    if output_path:
        _write_or_print(console.export_text(), output_path)


@cli.command(
    epilog=_epilog(
        """
        Examples:

            datasight trends
            datasight trends --table generation_fuel
            datasight trends generation.parquet plants.parquet
            datasight trends --format markdown -o trends.md
        """
    )
)
@click.argument("files", nargs=-1, type=click.Path(exists=True))
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=None,
    help="Project directory containing .env and config files.",
)
@click.option("--table", default=None, help="Suggest trends for a specific table.")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json", "markdown"]),
    default="table",
    help="Output format (default: table).",
)
@click.option(
    "--output",
    "-o",
    "output_path",
    type=click.Path(),
    default=None,
    help="Write the trend overview to a file instead of stdout.",
)
def trends(files, project_dir, table, output_format, output_path):
    """Surface likely trend analyses and chart recommendations.

    Run inside a configured project, or pass one or more Parquet, CSV, or
    DuckDB files directly for a quick file-only trend scan.
    """
    from rich.console import Console
    from datasight.config import load_measure_overrides

    async def _run_trends():
        if files:
            from datasight.explore import create_files_session_for_settings
            from datasight.schema import introspect_schema

            db_settings = _current_db_settings_or_none()
            runner, _ = create_files_session_for_settings(list(files), db_settings)
            tables = await introspect_schema(runner.run_sql, runner=runner)
            schema_info = [
                {
                    "name": t.name,
                    "row_count": t.row_count,
                    "columns": [
                        {"name": c.name, "dtype": c.dtype, "nullable": c.nullable}
                        for c in t.columns
                    ],
                }
                for t in tables
            ]
            sql_runner = runner
            measure_overrides: list[dict[str, Any]] = []
        else:
            resolved_dir = str(Path(project_dir or ".").resolve())
            settings, _ = _resolve_settings(resolved_dir)
            resolved_db_path = _resolve_db_path(settings, resolved_dir)
            if settings.database.mode in ("duckdb", "sqlite") and not os.path.exists(
                resolved_db_path
            ):
                click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
                sys.exit(1)
            sql_runner, schema_info = await _load_schema_info_for_project(resolved_dir, settings)
            measure_overrides = load_measure_overrides(None, resolved_dir)

        if table:
            table_info = find_table_info(schema_info, table)
            if table_info is None:
                raise click.ClickException(f"Table not found: {table}")
            schema_info = [table_info]
        return await build_trend_overview(schema_info, sql_runner.run_sql, measure_overrides)

    trend_data = asyncio.run(_run_trends())

    if output_format == "json":
        _write_or_print(json.dumps(trend_data, indent=2), output_path)
        return

    if output_format == "markdown":
        _write_or_print(_render_trends_markdown(trend_data), output_path)
        return

    console = Console(record=bool(output_path))
    console.print(
        _build_metric_table(
            "Trend Overview",
            [("Tables scanned", str(trend_data["table_count"]))],
        )
    )
    if trend_data["trend_candidates"]:
        console.print(
            _build_profile_detail_table(
                "Trend Candidates",
                [
                    ("Table", "left"),
                    ("Date", "left"),
                    ("Aggregation", "left"),
                    ("Measure", "left"),
                    ("Range", "left"),
                ],
                [
                    [
                        item["table"],
                        item["date_column"],
                        str(item.get("aggregation") or "").upper(),
                        item["measure_column"],
                        item["date_range"],
                    ]
                    for item in trend_data["trend_candidates"]
                ],
            )
        )
    if trend_data["breakout_dimensions"]:
        console.print(
            _build_profile_detail_table(
                "Breakout Dimensions",
                [("Column", "left"), ("Distinct", "right"), ("Null %", "right")],
                [
                    [
                        f"{item['table']}.{item['column']}",
                        _format_profile_value(item.get("distinct_count")),
                        _format_profile_value(item.get("null_rate"), "0"),
                    ]
                    for item in trend_data["breakout_dimensions"]
                ],
            )
        )
    if trend_data["chart_recommendations"]:
        console.print(
            _build_profile_detail_table(
                "Chart Recommendations",
                [("Title", "left"), ("Type", "left"), ("Reason", "left")],
                [
                    [item["title"], item["chart_type"], item["reason"]]
                    for item in trend_data["chart_recommendations"]
                ],
            )
        )
    if trend_data["notes"]:
        console.print(
            _build_profile_detail_table(
                "Notes", [("Observation", "left")], [[item] for item in trend_data["notes"]]
            )
        )
    if output_path:
        _write_or_print(console.export_text(), output_path)


@cli.command(
    epilog=_epilog(
        """
        Examples:

            datasight inspect generation.parquet
            datasight inspect generation.csv plants.csv
            datasight inspect data_dir/
            datasight inspect generation.parquet --format markdown -o inspect.md
        """
    )
)
@click.argument("files", nargs=-1, required=True, type=click.Path(exists=True))
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json", "markdown"]),
    default="table",
    help="Output format (default: table).",
)
@click.option(
    "--output",
    "-o",
    "output_path",
    type=click.Path(),
    default=None,
    help="Write the full report to a file instead of stdout.",
)
def inspect(files, output_format, output_path):
    """Run all analyses on Parquet, CSV, or DuckDB files and print results.

    Creates a file-backed session and runs profile, quality, measures,
    dimensions, trends, and recipes — printing everything to the console
    without creating a project. When the current directory contains a
    ``.env`` with ``DB_MODE=spark``, the files are registered as Spark
    temp views and all queries run on the cluster; otherwise an ephemeral
    in-memory DuckDB session is used.
    """
    import time as _time

    from loguru import logger as _logger
    from rich.console import Console

    from datasight.explore import create_files_session_for_settings
    from datasight.schema import introspect_schema

    _configure_logging("INFO")
    db_settings = _current_db_settings_or_none()

    async def _run_phase(name: str, coro):
        _logger.info(f"[inspect] {name}…")
        t0 = _time.perf_counter()
        result = await coro
        _logger.info(f"[inspect] {name} done in {_time.perf_counter() - t0:.1f}s")
        return result

    async def _run_all():
        runner, tables_info = create_files_session_for_settings(list(files), db_settings)
        tables = await _run_phase(
            f"introspecting schema for {len(tables_info)} table(s)",
            introspect_schema(runner.run_sql, runner=runner),
        )
        schema_info = [
            {
                "name": t.name,
                "row_count": t.row_count,
                "columns": [
                    {"name": c.name, "dtype": c.dtype, "nullable": c.nullable} for c in t.columns
                ],
            }
            for t in tables
        ]

        profile_data = await _run_phase(
            "profiling tables", build_dataset_overview(schema_info, runner.run_sql)
        )
        quality_data = await _run_phase(
            "running quality checks", build_quality_overview(schema_info, runner.run_sql)
        )
        measure_data = await _run_phase(
            "discovering measures",
            build_measure_overview(schema_info, runner.run_sql, overrides=None),
        )
        dimension_data = await _run_phase(
            "discovering dimensions", build_dimension_overview(schema_info, runner.run_sql)
        )
        trend_data = await _run_phase(
            "scanning for trends",
            build_trend_overview(schema_info, runner.run_sql, overrides=None),
        )
        recipe_list = await _run_phase(
            "building prompt recipes",
            build_prompt_recipes(schema_info, runner.run_sql, overrides=None),
        )
        recipes_data = [{"id": idx, **r} for idx, r in enumerate(recipe_list, start=1)]

        return {
            "files": [t["name"] for t in tables_info],
            "profile": profile_data,
            "quality": quality_data,
            "measures": measure_data,
            "dimensions": dimension_data,
            "trends": trend_data,
            "recipes": recipes_data,
        }

    results = asyncio.run(_run_all())

    if output_format == "json":
        _write_or_print(json.dumps(results, indent=2), output_path)
        return

    if output_format == "markdown":
        sections = [
            _render_profile_markdown("dataset", results["profile"]),
            _render_quality_markdown(results["quality"]),
            _render_measures_markdown(results["measures"]),
            _render_dimensions_markdown(results["dimensions"]),
            _render_trends_markdown(results["trends"]),
            _render_recipes_markdown(results["recipes"]),
        ]
        _write_or_print("\n\n".join(sections), output_path)
        return

    console = Console(record=bool(output_path))
    file_label = ", ".join(str(f) for f in files)
    console.print(f"\n[bold]datasight inspect:[/bold] {file_label}\n")

    # --- Profile ---
    profile_data = results["profile"]
    console.print(
        _build_metric_table(
            "Dataset Profile",
            [
                ("Tables", str(profile_data["table_count"])),
                ("Columns", str(profile_data["total_columns"])),
                ("Rows", str(profile_data["total_rows"])),
            ],
        )
    )
    if profile_data["largest_tables"]:
        console.print(
            _build_profile_detail_table(
                "Largest Tables",
                [("Table", "left"), ("Rows", "right"), ("Columns", "right")],
                [
                    [
                        item["name"],
                        f"{item.get('row_count') or 0}",
                        str(item["column_count"]),
                    ]
                    for item in profile_data["largest_tables"]
                ],
            )
        )
    if profile_data["date_columns"]:
        console.print(
            _build_profile_detail_table(
                "Date Coverage",
                [("Column", "left"), ("Min", "left"), ("Max", "left")],
                [
                    [
                        f"{item['table']}.{item['column']}",
                        _format_profile_value(item.get("min")),
                        _format_profile_value(item.get("max")),
                    ]
                    for item in profile_data["date_columns"]
                ],
            )
        )

    # --- Quality ---
    quality_data = results["quality"]
    if quality_data["null_columns"] or quality_data["numeric_flags"]:
        console.print(
            _build_metric_table(
                "Quality Audit",
                [("Tables scanned", str(quality_data["table_count"]))],
            )
        )
        if quality_data["null_columns"]:
            console.print(
                _build_profile_detail_table(
                    "Null-heavy Columns",
                    [("Column", "left"), ("Nulls", "right"), ("Null %", "right")],
                    [
                        [
                            f"{item['table']}.{item['column']}",
                            str(item["null_count"]),
                            str(item.get("null_rate") or 0),
                        ]
                        for item in quality_data["null_columns"]
                    ],
                )
            )
        if quality_data["numeric_flags"]:
            console.print(
                _build_profile_detail_table(
                    "Numeric Range Flags",
                    [("Column", "left"), ("Issue", "left")],
                    [
                        [f"{item['table']}.{item['column']}", item["issue"]]
                        for item in quality_data["numeric_flags"]
                    ],
                )
            )
        if quality_data["notes"]:
            console.print(
                _build_profile_detail_table(
                    "Quality Notes",
                    [("Observation", "left")],
                    [[item] for item in quality_data["notes"]],
                )
            )

    # --- Measures ---
    measure_data = results["measures"]
    if measure_data["measures"]:
        console.print(
            _build_profile_detail_table(
                "Measure Candidates",
                [
                    ("Column", "left"),
                    ("Role", "left"),
                    ("Unit", "left"),
                    ("Default Agg", "left"),
                    ("Rollup SQL", "left"),
                ],
                [
                    [
                        f"{item['table']}.{item['column']}",
                        item["role"]
                        + (f" [{item['display_name']}]" if item.get("display_name") else ""),
                        _format_profile_value(item.get("unit"), "—"),
                        item["default_aggregation"],
                        item["recommended_rollup_sql"],
                    ]
                    for item in measure_data["measures"]
                ],
            )
        )

    # --- Dimensions ---
    dimension_data = results["dimensions"]
    if dimension_data["dimension_columns"]:
        console.print(
            _build_profile_detail_table(
                "Dimension Candidates",
                [
                    ("Column", "left"),
                    ("Distinct", "right"),
                    ("Null %", "right"),
                    ("Samples", "left"),
                ],
                [
                    [
                        f"{item['table']}.{item['column']}",
                        _format_profile_value(item.get("distinct_count")),
                        _format_profile_value(item.get("null_rate"), "0"),
                        ", ".join((item.get("sample_values") or [])[:3]) or "none",
                    ]
                    for item in dimension_data["dimension_columns"]
                ],
            )
        )
    if dimension_data["suggested_breakdowns"]:
        console.print(
            _build_profile_detail_table(
                "Suggested Breakdowns",
                [("Column", "left"), ("Reason", "left")],
                [
                    [f"{item['table']}.{item['column']}", item["reason"]]
                    for item in dimension_data["suggested_breakdowns"]
                ],
            )
        )

    # --- Trends ---
    trend_data = results["trends"]
    if trend_data["trend_candidates"]:
        console.print(
            _build_profile_detail_table(
                "Trend Candidates",
                [
                    ("Table", "left"),
                    ("Date", "left"),
                    ("Aggregation", "left"),
                    ("Measure", "left"),
                    ("Range", "left"),
                ],
                [
                    [
                        item["table"],
                        item["date_column"],
                        str(item.get("aggregation") or "").upper(),
                        item["measure_column"],
                        item["date_range"],
                    ]
                    for item in trend_data["trend_candidates"]
                ],
            )
        )
    if trend_data["chart_recommendations"]:
        console.print(
            _build_profile_detail_table(
                "Chart Recommendations",
                [("Title", "left"), ("Type", "left"), ("Reason", "left")],
                [
                    [item["title"], item["chart_type"], item["reason"]]
                    for item in trend_data["chart_recommendations"]
                ],
            )
        )

    # --- Recipes ---
    recipes_data = results["recipes"]
    if recipes_data:
        console.print(
            _build_profile_detail_table(
                "Prompt Recipes",
                [
                    ("ID", "right"),
                    ("Title", "left"),
                    ("Category", "left"),
                    ("Why", "left"),
                    ("Prompt", "left"),
                ],
                [
                    [
                        str(item["id"]),
                        item["title"],
                        item.get("category", ""),
                        item.get("reason", ""),
                        item["prompt"][:80] + ("…" if len(item["prompt"]) > 80 else ""),
                    ]
                    for item in recipes_data
                ],
            )
        )

    if output_path:
        _write_or_print(console.export_text(), output_path)


@cli.group(
    epilog=_epilog(
        """
        Examples:

            datasight recipes list
            datasight recipes list --table generation_fuel
            datasight recipes run 1
        """
    )
)
def recipes():
    """Generate and run reusable deterministic prompt recipes.

    Recipes are suggested natural-language questions derived from the
    schema. Listing recipes does not call an LLM; running one sends the
    recipe prompt through the normal ask pipeline.
    """


@recipes.command(
    name="list",
    epilog=_epilog(
        """
        Examples:

            datasight recipes list
            datasight recipes list --table generation_fuel
            datasight recipes list --format markdown -o recipes.md
        """
    ),
)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing .env and config files.",
)
@click.option("--table", default=None, help="Generate recipes for a specific table.")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json", "markdown"]),
    default="table",
    help="Output format (default: table).",
)
@click.option(
    "--output",
    "-o",
    "output_path",
    type=click.Path(),
    default=None,
    help="Write the recipes output to a file instead of stdout.",
)
def recipes_list(project_dir, table, output_format, output_path):
    """List reusable deterministic prompt recipes for a project."""
    from rich.console import Console

    project_dir = str(Path(project_dir).resolve())
    settings, _ = _resolve_settings(project_dir)
    recipe_data = _load_recipe_entries(project_dir, settings, table)

    if output_format == "json":
        _write_or_print(json.dumps(recipe_data, indent=2), output_path)
        return

    if output_format == "markdown":
        _write_or_print(_render_recipes_markdown(recipe_data), output_path)
        return

    console = Console(record=bool(output_path))
    console.print(
        _build_profile_detail_table(
            "Prompt Recipes",
            [
                ("ID", "right"),
                ("Title", "left"),
                ("Category", "left"),
                ("Why", "left"),
                ("Prompt", "left"),
            ],
            [
                [
                    str(item["id"]),
                    item["title"],
                    item.get("category") or "Recipe",
                    item.get("reason") or "",
                    item["prompt"],
                ]
                for item in recipe_data
            ],
        )
    )
    if output_path:
        _write_or_print(console.export_text(), output_path)


@recipes.command(
    name="run",
    epilog=_epilog(
        """
        Examples:

            datasight recipes run 1
            datasight recipes run 2 --format csv -o recipe.csv
            datasight recipes run 3 --chart-format html -o recipe.html
        """
    ),
)
@click.argument("recipe_id", type=int)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing .env and config files.",
)
@click.option("--table", default=None, help="Use recipes generated for a specific table.")
@click.option("--model", default=None, help="Model name (overrides .env).")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "csv", "json"]),
    default="table",
    help="Output format for query results (default: table).",
)
@click.option(
    "--chart-format",
    type=click.Choice(["html", "json", "png"]),
    default=None,
    help="Save chart output in this format (requires --output).",
)
@click.option(
    "--output",
    "-o",
    "output_path",
    type=click.Path(),
    default=None,
    help="Output file path for chart or data export.",
)
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging.")
def recipes_run(
    recipe_id, project_dir, table, model, output_format, chart_format, output_path, verbose
):
    """Run a generated recipe by ID through the normal ask pipeline.

    RECIPE_ID is the numeric ID shown by datasight recipes list.
    """
    import asyncio

    from rich.console import Console

    project_dir = str(Path(project_dir).resolve())
    settings, resolved_model = _resolve_settings(project_dir, model)
    _validate_settings_for_llm(settings)

    if verbose:
        _configure_logging("DEBUG")

    recipe_data = _load_recipe_entries(project_dir, settings, table)
    recipe = next((item for item in recipe_data if item["id"] == recipe_id), None)
    if recipe is None:
        click.echo(f"Recipe {recipe_id} not found.", err=True)
        raise SystemExit(1)

    sql_dialect = settings.database.sql_dialect
    console = Console()
    console.print(f"[dim]Running recipe [{recipe['id']}]: {recipe['title']}[/dim]")

    result = asyncio.run(
        _run_ask_pipeline(
            question=recipe["prompt"],
            settings=settings,
            resolved_model=resolved_model,
            project_dir=project_dir,
            sql_dialect=sql_dialect,
        )
    )
    _emit_ask_result(result, output_format, chart_format, output_path)


@cli.command(
    epilog=_epilog(
        """
        Examples:

            datasight doctor
            datasight doctor --format markdown -o doctor.md
            datasight doctor --project-dir eia-demo
        """
    )
)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing .env and config files.",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json", "markdown"]),
    default="table",
    help="Output format (default: table).",
)
@click.option(
    "--output",
    "-o",
    "output_path",
    type=click.Path(),
    default=None,
    help="Write doctor output to a file instead of stdout.",
)
def doctor(project_dir, output_format, output_path):
    """Check project configuration, local files, and database connectivity.

    Use this when a project will not load, an API key is missing, a database
    path is wrong, or the web UI cannot write state under .datasight/.
    """
    from rich.console import Console
    from rich.table import Table as RichTable

    project_path = Path(project_dir).resolve()
    console = Console(record=bool(output_path))
    checks: list[tuple[str, str, str]] = []

    def add_check(name: str, ok: bool, detail: str) -> None:
        checks.append((name, "OK" if ok else "FAIL", detail))

    env_path = project_path / ".env"
    add_check(".env", env_path.exists(), str(env_path))

    settings = _resolve_settings(str(project_path))[0]
    validation_errors = settings.validate()
    add_check(
        "LLM settings",
        not validation_errors,
        "; ".join(validation_errors) if validation_errors else settings.llm.provider,
    )

    db_detail = settings.database.mode
    db_ok = True
    resolved_db_path = _resolve_db_path(settings, str(project_path))
    if settings.database.mode in ("duckdb", "sqlite"):
        db_ok = bool(resolved_db_path) and os.path.exists(resolved_db_path)
        db_detail = resolved_db_path
    elif settings.database.mode == "postgres":
        db_ok = bool(
            settings.database.postgres_url
            or (
                settings.database.postgres_database
                and settings.database.postgres_user
                and settings.database.postgres_host
            )
        )
        db_detail = settings.database.postgres_url or (
            f"{settings.database.postgres_user}@{settings.database.postgres_host}:"
            f"{settings.database.postgres_port}/{settings.database.postgres_database}"
        )
    elif settings.database.mode == "flightsql":
        db_ok = bool(settings.database.flight_uri)
        db_detail = settings.database.flight_uri
    elif settings.database.mode == "spark":
        db_ok = bool(settings.database.spark_remote)
        db_detail = settings.database.spark_remote
    add_check("Database config", db_ok, db_detail or settings.database.mode)

    for name in ("schema_description.md", "queries.yaml"):
        path = project_path / name
        add_check(name, path.exists(), str(path))

    datasight_dir = project_path / ".datasight"
    try:
        datasight_dir.mkdir(parents=True, exist_ok=True)
        probe = datasight_dir / ".doctor-write-test"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
        add_check(".datasight writable", True, str(datasight_dir))
    except OSError as exc:
        add_check(".datasight writable", False, f"{datasight_dir}: {exc}")

    try:
        sql_runner = create_sql_runner_from_settings(settings.database, str(project_path))
        asyncio.run(sql_runner.run_sql("SELECT 1 AS ok"))
        add_check("Database connectivity", True, "SELECT 1")
    except Exception as exc:
        add_check("Database connectivity", False, str(exc))

    rendered_checks = [
        {"name": name, "ok": status == "OK", "detail": detail} for name, status, detail in checks
    ]
    failures = sum(1 for check in rendered_checks if not check["ok"])

    if output_format == "json":
        _write_or_print(
            json.dumps(
                {
                    "project_dir": str(project_path),
                    "checks": rendered_checks,
                    "failures": failures,
                },
                indent=2,
            ),
            output_path,
        )
        if failures:
            sys.exit(1)
        return

    if output_format == "markdown":
        _write_or_print(
            _render_doctor_markdown(str(project_path), rendered_checks),
            output_path,
        )
        if failures:
            sys.exit(1)
        return

    table = RichTable(title="datasight doctor")
    table.add_column("Check")
    table.add_column("Status", no_wrap=True)
    table.add_column("Detail", overflow="fold")

    for name, status, detail in checks:
        if status == "FAIL":
            status_text = "[bold red]FAIL[/bold red]"
        else:
            status_text = "[green]OK[/green]"
        table.add_row(name, status_text, detail)

    console.print(table)
    if output_path:
        _write_or_print(console.export_text(), output_path)
    if failures:
        sys.exit(1)


@cli.command(
    epilog=_epilog(
        """
        Examples:

            datasight export --list-sessions
            datasight export abc123def -o my-analysis.html
            datasight export abc123def --exclude 2,3
        """
    )
)
@click.argument("session_id", required=False)
@click.option(
    "--output",
    "-o",
    "output_path",
    type=click.Path(),
    default=None,
    help="Output file path (default: <session_id>.html).",
)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing .datasight/conversations/.",
)
@click.option(
    "--exclude",
    default=None,
    help="Comma-separated turn indices to exclude (0-based, each turn is a Q&A pair).",
)
@click.option("--list-sessions", is_flag=True, help="List available sessions and exit.")
def export(session_id, output_path, project_dir, exclude, list_sessions):
    """Export a conversation session as a self-contained HTML page.

    SESSION_ID is the conversation ID (use --list-sessions to see available IDs).
    """
    import json as json_mod

    project_dir = str(Path(project_dir).resolve())
    conv_dir = Path(project_dir) / ".datasight" / "conversations"

    if list_sessions or session_id == "list":
        if not conv_dir.exists():
            click.echo("No conversations found.")
            return
        sessions = []
        for f in sorted(conv_dir.glob("*.json")):
            try:
                data = json_mod.loads(f.read_text(encoding="utf-8"))
                events = data.get("events", [])
                msg_count = sum(1 for e in events if e.get("event") == "user_message")
                if msg_count == 0:
                    continue
                sessions.append(
                    {
                        "id": f.stem,
                        "title": data.get("title", "Untitled"),
                        "messages": msg_count,
                    }
                )
            except (json_mod.JSONDecodeError, OSError):
                continue
        if not sessions:
            click.echo("No conversations found.")
            return

        from rich.console import Console
        from rich.table import Table

        console = Console()
        table = Table(title="Available Sessions")
        table.add_column("Session ID", style="cyan", no_wrap=True)
        table.add_column("Title", overflow="fold")
        table.add_column("Messages", justify="right")
        for s in sessions:
            table.add_row(s["id"], s["title"], str(s["messages"]))
        console.print(table)
        return

    if not session_id:
        click.echo(
            "Error: provide a SESSION_ID or use --list-sessions to see available sessions.",
            err=True,
        )
        sys.exit(1)

    # Load session
    session_path = conv_dir / f"{session_id}.json"
    if not session_path.exists():
        click.echo(f"Error: Session not found: {session_id}", err=True)
        click.echo("Use 'datasight export --list-sessions' to see available sessions.", err=True)
        sys.exit(1)

    data = json_mod.loads(session_path.read_text(encoding="utf-8"))
    events = data.get("events", [])
    title = data.get("title", "datasight session")

    if not events:
        click.echo("Error: Session has no events.", err=True)
        sys.exit(1)

    exclude_indices: set[int] | None = None
    if exclude:
        try:
            exclude_indices = {int(x.strip()) for x in exclude.split(",")}
        except ValueError:
            click.echo("Error: --exclude must be comma-separated integers.", err=True)
            sys.exit(1)

    from datasight.export import export_session_html

    html = export_session_html(events, title=title, exclude_indices=exclude_indices)

    if not output_path:
        safe_id = session_id[:20]
        output_path = f"{safe_id}.html"

    Path(output_path).write_text(html, encoding="utf-8")
    click.echo(f"Session exported to {output_path}")


@cli.command(
    name="log",
    epilog=_epilog(
        """
        Examples:

            datasight log
            datasight log --tail 50 --full
            datasight log --errors
            datasight log --cost
            datasight log --sql 1
        """
    ),
)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing query_log.jsonl.",
)
@click.option("--tail", "tail_n", type=int, default=20, help="Show last N entries (default: 20).")
@click.option("--errors", is_flag=True, help="Show only failed queries.")
@click.option("--full", is_flag=True, help="Show full SQL and user question.")
@click.option("--cost", is_flag=True, help="Show LLM cost summary.")
@click.option(
    "--sql",
    "sql_index",
    type=int,
    default=None,
    help="Print raw SQL for query # (shown in the # column). Ready to copy-paste.",
)
def log_cmd(project_dir, tail_n, errors, full, cost, sql_index):
    """Display the SQL query log in a formatted table.

    Shows recent SQL queries generated by datasight. Use --sql N to print
    one raw SQL statement for copy/paste into DuckDB, SQLite, or another
    SQL client.
    """
    from rich import box
    from rich.console import Console
    from rich.table import Table
    from rich.text import Text

    from datasight.query_log import QueryLogger

    project_dir = str(Path(project_dir).resolve())
    log_path = os.path.join(project_dir, ".datasight", "query_log.jsonl")

    if not os.path.exists(log_path):
        click.echo(f"No query log found at {log_path}")
        return

    ql = QueryLogger(path=log_path)
    entries = ql.read_recent(tail_n)

    if errors:
        entries = [e for e in entries if e.get("error")]

    if not entries:
        click.echo("No matching log entries.")
        return

    # --sql N: print raw SQL for the Nth most recent query and exit
    if sql_index is not None:
        query_only = [e for e in entries if e.get("type") != "cost" and e.get("sql")]
        if not query_only:
            click.echo("No SQL queries in log.")
            return
        if sql_index < 1 or sql_index > len(query_only):
            click.echo(f"Index out of range. Use 1–{len(query_only)}.")
            return
        entry = query_only[sql_index - 1]
        sql = entry["sql"].strip()
        if not sql.endswith(";"):
            sql += ";"
        click.echo(sql)
        return

    # Separate query entries from cost entries
    query_entries = [e for e in entries if e.get("type") != "cost"]
    cost_entries = [e for e in entries if e.get("type") == "cost"]

    console = Console()
    table = Table(box=box.ROUNDED)
    table.add_column("#", justify="right", style="dim", no_wrap=True)
    table.add_column("Timestamp", style="dim", no_wrap=True)
    table.add_column("Tool", no_wrap=True)
    table.add_column("SQL", min_width=40, overflow="fold")
    table.add_column("Time", justify="right", no_wrap=True)
    table.add_column("Rows", justify="right", no_wrap=True)
    table.add_column("Status", no_wrap=True)

    if full:
        table.add_column("Question", overflow="fold")

    total = len(query_entries)
    failed = 0
    for i, entry in enumerate(query_entries):
        ts = entry.get("timestamp", "")
        # Trim to seconds, drop timezone
        if "T" in ts:
            ts = ts.replace("T", " ")[:19]

        tool = entry.get("tool", "")
        sql = entry.get("sql", "")
        if not full and len(sql) > 120:
            sql = sql[:120] + " ..."

        elapsed = entry.get("execution_time_ms")
        time_str = f"{elapsed:.0f}ms" if elapsed is not None else ""

        row_count = entry.get("row_count")
        rows_str = str(row_count) if row_count is not None else ""

        error = entry.get("error")
        if error:
            failed += 1
            status = Text("ERR", style="bold red")
        else:
            status = Text("OK", style="green")

        sql_id = str(i + 1)
        row = [sql_id, ts, tool, sql, time_str, rows_str, status]
        if full:
            row.append(entry.get("user_question", ""))
        table.add_row(*row)

    console.print(table)

    succeeded = total - failed
    summary = f"{total} queries ({succeeded} succeeded, {failed} failed)"
    console.print(f"\n[dim]{summary}[/dim]")

    # Show cost summary when --cost is used
    if cost and cost_entries:
        cost_table = Table(title="LLM Cost Summary", box=box.ROUNDED)
        cost_table.add_column("Timestamp", style="dim", no_wrap=True)
        cost_table.add_column("Question", overflow="fold")
        cost_table.add_column("API Calls", justify="right", no_wrap=True)
        cost_table.add_column("Input Tokens", justify="right", no_wrap=True)
        cost_table.add_column("Output Tokens", justify="right", no_wrap=True)
        cost_table.add_column("Est. Cost", justify="right", no_wrap=True)

        total_cost = 0.0
        total_input = 0
        total_output = 0
        for entry in cost_entries:
            ts = entry.get("timestamp", "")
            if "T" in ts:
                ts = ts.replace("T", " ")[:19]
            question = entry.get("user_question", "")
            api_calls_n = entry.get("api_calls", 0)
            inp = entry.get("input_tokens", 0)
            out = entry.get("output_tokens", 0)
            cost = entry.get("estimated_cost")
            cost_str = f"${cost:.4f}" if cost is not None else ""
            if cost:
                total_cost += cost
            total_input += inp
            total_output += out
            cost_table.add_row(
                ts,
                question,
                str(api_calls_n),
                f"{inp:,}",
                f"{out:,}",
                cost_str,
            )

        console.print()
        console.print(cost_table)
        cost_summary = f"Totals: {total_input:,} input tokens, {total_output:,} output tokens"
        if total_cost > 0:
            cost_summary += f", ${total_cost:.4f} estimated cost"
        console.print(f"\n[dim]{cost_summary}[/dim]")


@cli.group(
    epilog=_epilog(
        """
        Examples:

            datasight report list
            datasight report run 1
            datasight report run 1 --format csv -o report.csv
            datasight report delete 1
        """
    )
)
def report():
    """Manage saved reports.

    Reports are saved from the web UI and can be listed, re-run against
    fresh data, exported, or deleted from the CLI.
    """


@report.command(
    name="list",
    epilog=_epilog(
        """
        Example:

            datasight report list
        """
    ),
)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory.",
)
def report_list(project_dir):
    """List all saved reports."""
    from rich import box
    from rich.console import Console
    from rich.table import Table

    from datasight.web.app import ReportStore

    project_dir = str(Path(project_dir).resolve())
    store = ReportStore(Path(project_dir) / ".datasight" / "reports.json")
    reports = store.list_all()

    if not reports:
        click.echo("No saved reports.")
        return

    console = Console()
    table = Table(box=box.ROUNDED)
    table.add_column("ID", justify="right", no_wrap=True)
    table.add_column("Name", min_width=20)
    table.add_column("Tool", no_wrap=True)
    table.add_column("SQL", min_width=40, overflow="fold")

    for r in reports:
        sql_preview = r["sql"][:80] + ("..." if len(r["sql"]) > 80 else "")
        table.add_row(str(r["id"]), r.get("name", ""), r["tool"], sql_preview)

    console.print(table)
    click.echo(f"\n{len(reports)} report(s)")


@report.command(
    name="run",
    epilog=_epilog(
        """
        Examples:

            datasight report run 1
            datasight report run 1 --format csv -o report.csv
            datasight report run 2 --chart-format html -o chart.html
        """
    ),
)
@click.argument("report_id", type=int)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing .env and config files.",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "csv", "json"]),
    default="table",
    help="Output format for query results (default: table).",
)
@click.option(
    "--chart-format",
    type=click.Choice(["html", "json"]),
    default=None,
    help="Save chart output in this format (requires --output).",
)
@click.option(
    "--output",
    "-o",
    "output_path",
    type=click.Path(),
    default=None,
    help="Output file path for chart or data export.",
)
def report_run(report_id, project_dir, output_format, chart_format, output_path):
    """Re-execute a saved report against fresh data.

    REPORT_ID is the numeric ID shown by 'datasight report list'.
    """
    import asyncio

    from rich.console import Console

    from datasight.agent import execute_tool
    from datasight.web.app import ReportStore

    project_dir = str(Path(project_dir).resolve())
    settings, _ = _resolve_settings(project_dir)

    store = ReportStore(Path(project_dir) / ".datasight" / "reports.json")
    report_data = store.get(report_id)
    if report_data is None:
        click.echo(f"Report {report_id} not found.", err=True)
        raise SystemExit(1)

    sql_runner = create_sql_runner_from_settings(settings.database, project_dir)
    console = Console()
    console.print(f"[dim]Running report: {report_data.get('name', report_data['sql'][:60])}[/dim]")

    tool_input: dict[str, object] = {
        "sql": report_data["sql"],
        "title": report_data.get("name", "Report"),
    }
    if "plotly_spec" in report_data:
        tool_input["plotly_spec"] = report_data["plotly_spec"]

    result = asyncio.run(
        execute_tool(
            report_data["tool"],
            tool_input,
            run_sql=sql_runner.run_sql,
            dialect=settings.database.sql_dialect,
        )
    )

    if result.df is not None and not result.df.empty:
        match output_format:
            case "csv":
                click.echo(result.df.to_csv(index=False))
            case "json":
                click.echo(result.df.to_json(orient="records", indent=2))
            case _:
                from rich import box
                from rich.table import Table as RichTable

                rt = RichTable(box=box.ROUNDED)
                for col in result.df.columns:
                    rt.add_column(str(col))
                for _, row in result.df.head(50).iterrows():
                    rt.add_row(*[str(v) for v in row])
                console.print(rt)
                if len(result.df) > 50:
                    console.print(f"[dim]... showing 50 of {len(result.df)} rows[/dim]")

    if result.plotly_spec and chart_format:
        import json as json_mod

        if chart_format == "json":
            output = json_mod.dumps(result.plotly_spec, indent=2)
        else:
            from datasight.chart import build_chart_html

            output = build_chart_html(result.plotly_spec, report_data.get("name", "Report"))

        if output_path:
            Path(output_path).write_text(output, encoding="utf-8")
            click.echo(f"Chart saved to {output_path}")
        else:
            click.echo(output)
    elif result.result_text and result.df is None:
        click.echo(result.result_text, err=True)


@report.command(
    name="delete",
    epilog=_epilog(
        """
        Example:

            datasight report delete 1
        """
    ),
)
@click.argument("report_id", type=int)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory.",
)
def report_delete(report_id, project_dir):
    """Delete a saved report.

    REPORT_ID is the numeric ID shown by 'datasight report list'.
    """
    from datasight.web.app import ReportStore

    project_dir = str(Path(project_dir).resolve())
    store = ReportStore(Path(project_dir) / ".datasight" / "reports.json")
    if store.get(report_id) is None:
        click.echo(f"Report {report_id} not found.", err=True)
        raise SystemExit(1)
    store.delete(report_id)
    click.echo(f"Report {report_id} deleted.")


@cli.group(
    epilog=_epilog(
        """
        Examples:

            datasight templates save generation-dashboard
            datasight templates list
            datasight templates apply generation-dashboard --output out.html
        """
    )
)
def templates():
    """Save and re-apply dashboards as templates across datasets.

    Templates capture dashboard cards from the web UI so the same SQL and
    charts can be applied to another dataset with matching tables.
    """


def _load_project_dashboard(project_dir: str) -> dict[str, Any]:
    path = Path(project_dir).resolve() / ".datasight" / "dashboard.json"
    if not path.exists():
        raise click.ClickException(
            f"No dashboard found at {path}. Build a dashboard in the web UI first."
        )
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as err:
        raise click.ClickException(f"Dashboard JSON is invalid: {err}") from err


def _resolve_project_duckdb(project_dir: str) -> Path | None:
    """Return the project's DuckDB path if it has one configured, else None.

    Loads the project's `.env` (without polluting the global env), inspects
    DB_MODE/DB_PATH, and returns an absolute path when the project uses
    DuckDB. Returns None for non-DuckDB backends or when nothing is set —
    callers are then responsible for supplying every required table via
    --table.
    """
    from dotenv import dotenv_values

    from datasight.config import normalize_db_mode

    proj = Path(project_dir).resolve()
    env_file = proj / ".env"
    values = dotenv_values(env_file) if env_file.exists() else {}
    # Match datasight's own default: empty/missing DB_MODE means duckdb.
    mode = normalize_db_mode((values.get("DB_MODE") or "").strip() or "duckdb")
    if mode != "duckdb":
        return None
    path = (values.get("DB_PATH") or "").strip()
    if not path:
        # Fall back to the conventional location so `datasight generate`'s
        # default output is picked up even if DB_PATH wasn't written.
        fallback = proj / "database.duckdb"
        return fallback if fallback.exists() else None
    db_path = Path(path)
    if not db_path.is_absolute():
        db_path = proj / db_path
    db_path = db_path.resolve()
    return db_path if db_path.exists() else None


_PROJECT_DIR_OPT = click.option(
    "--project-dir",
    "project_dir",
    type=click.Path(exists=True, file_okay=False),
    default=".",
    help="Project directory containing .datasight/templates/ (default: cwd).",
)


@templates.command(
    name="save",
    epilog=_epilog(
        """
        Examples:

            datasight templates save generation-dashboard
            datasight templates save generation-dashboard --description "Monthly generation cards"
            datasight templates save generation-dashboard --table generation_fuel --overwrite
            datasight templates save by-scenario --var SCENARIO=reference
        """
    ),
)
@click.argument("name")
@_PROJECT_DIR_OPT
@click.option("--description", default=None, help="Template description.")
@click.option(
    "--table",
    "required_tables",
    multiple=True,
    help=(
        "Table the template requires. Repeat once per table. "
        "When omitted, tables are inferred from each card's SQL."
    ),
)
@click.option(
    "--var",
    "variables",
    multiple=True,
    help=(
        "Declare a template variable: --var NAME=VALUE. Every occurrence "
        "of VALUE in each card's SQL is rewritten to {{NAME}}, and NAME "
        "becomes a placeholder that must be resolved at apply time."
    ),
)
@click.option(
    "--var-from-filename",
    "variable_regexes",
    multiple=True,
    help=(
        "Attach a filename-extraction regex to a variable: "
        "--var-from-filename NAME=REGEX. At apply time the regex is run "
        "against each input parquet's filename and its first capture group "
        "(or whole match) becomes the variable value. Use with --var to "
        "also set the save-time literal and default."
    ),
)
@click.option("--overwrite", is_flag=True, help="Replace an existing template.")
def template_save(
    name: str,
    project_dir: str,
    description: str | None,
    required_tables: tuple[str, ...],
    variables: tuple[str, ...],
    variable_regexes: tuple[str, ...],
    overwrite: bool,
):
    """Save the current project dashboard as a reusable template.

    The dashboard must already exist in the project, usually from building
    and saving cards in the web UI.
    """
    from datasight.dashboard_template import (
        TemplateError,
        build_template,
        save_template,
    )

    var_defs: dict[str, dict[str, str]] = {}
    for raw in variables:
        if "=" not in raw:
            raise click.ClickException(f"Invalid --var value {raw!r}. Expected NAME=VALUE.")
        key, _, value = raw.partition("=")
        key = key.strip()
        value = value.strip()
        if not key or not value:
            raise click.ClickException(f"Invalid --var value {raw!r}. Expected NAME=VALUE.")
        var_defs[key] = {"name": key, "default": value}
    for raw in variable_regexes:
        if "=" not in raw:
            raise click.ClickException(
                f"Invalid --var-from-filename value {raw!r}. Expected NAME=REGEX."
            )
        key, _, regex = raw.partition("=")
        key = key.strip()
        if not key or not regex:
            raise click.ClickException(
                f"Invalid --var-from-filename value {raw!r}. Expected NAME=REGEX."
            )
        var_defs.setdefault(key, {"name": key, "default": ""})["from_filename"] = regex

    dashboard = _load_project_dashboard(project_dir)
    try:
        template_obj = build_template(
            name,
            dashboard,
            required_tables=list(required_tables) or None,
            description=description,
            variables=list(var_defs.values()) or None,
        )
        path = save_template(template_obj, project_dir, overwrite=overwrite)
    except TemplateError as err:
        raise click.ClickException(str(err)) from err

    click.echo(f"Saved template {name!r} to {path}")
    click.echo(f"  required_tables: {', '.join(template_obj['required_tables'])}")
    click.echo(f"  cards: {len(template_obj['items'])}")
    if template_obj.get("variables"):
        names = ", ".join(v["name"] for v in template_obj["variables"])
        click.echo(f"  variables: {names}")


@templates.command(
    name="list",
    epilog=_epilog(
        """
        Example:

            datasight templates list
        """
    ),
)
@_PROJECT_DIR_OPT
def template_list(project_dir: str):
    """List dashboard templates saved in this project."""
    from rich import box
    from rich.console import Console
    from rich.table import Table

    from datasight.dashboard_template import list_templates, project_template_dir

    directory = project_template_dir(project_dir)
    entries = list_templates(project_dir)
    if not entries:
        click.echo(f"No templates in {directory}.")
        return

    table = Table(box=box.ROUNDED)
    table.add_column("Name", no_wrap=True)
    table.add_column("Required tables", overflow="fold")
    table.add_column("Cards", justify="right", no_wrap=True)
    table.add_column("Description", overflow="fold")

    for entry in entries:
        table.add_row(
            entry["name"],
            ", ".join(entry["required_tables"]) or "[dim]—[/dim]",
            str(entry["cards"]),
            entry["description"] or "[dim]—[/dim]",
        )

    Console().print(table)
    click.echo(f"\n{len(entries)} template(s) in {directory}")


@templates.command(
    name="show",
    epilog=_epilog(
        """
        Example:

            datasight templates show generation-dashboard
        """
    ),
)
@click.argument("name")
@_PROJECT_DIR_OPT
def template_show(name: str, project_dir: str):
    """Print a saved template as JSON."""
    from datasight.dashboard_template import TemplateError, load_template

    try:
        data = load_template(name, project_dir)
    except TemplateError as err:
        raise click.ClickException(str(err)) from err
    click.echo(json.dumps(data, indent=2))


@templates.command(
    name="apply",
    epilog=_epilog(
        """
        Examples:

            # Render once, mapping one required table to a parquet file
            datasight templates apply generation-by-fuel \\
                --table generation_fuel=data/generation.parquet \\
                --output generation.html

            # Render once per matching parquet, writing one HTML per file
            datasight templates apply generation-by-fuel \\
                --table 'generation_fuel=data/*.parquet' \\
                --export-dir out/
        """
    ),
)
@click.argument("name")
@_PROJECT_DIR_OPT
@click.option(
    "--table",
    "table_mappings",
    multiple=True,
    help=(
        "Map a required table to a parquet file: --table NAME=PATH. "
        "Repeat per table. One mapping may use a glob to iterate the "
        "template across many files. Tables not mapped here are looked "
        "up in the project's DuckDB."
    ),
)
@click.option(
    "--output",
    "output_path",
    type=click.Path(dir_okay=False),
    default=None,
    help="HTML output path for a single-shot run (no globbing).",
)
@click.option(
    "--export-dir",
    "export_dir",
    type=click.Path(file_okay=False),
    default=None,
    help="Directory for per-file HTML output when a --table mapping globs.",
)
@click.option(
    "--var",
    "var_overrides",
    multiple=True,
    help=(
        "Override a template variable: --var NAME=VALUE. Takes precedence "
        "over the variable's filename-derived value and default."
    ),
)
@click.option(
    "--fail-fast",
    is_flag=True,
    help="Stop on the first failure instead of continuing.",
)
def template_apply(
    name: str,
    project_dir: str,
    table_mappings: tuple[str, ...],
    output_path: str | None,
    export_dir: str | None,
    var_overrides: tuple[str, ...],
    fail_fast: bool,
):
    """Apply a saved template to parquet files and export HTML dashboards.

    Each required table is registered as a view inside an in-memory DuckDB
    connection. Tables not passed via --table fall back to the project's
    own DuckDB (from .env DB_PATH) — so fixed lookup tables like ``plants``
    don't need to be re-supplied. A single --table mapping may use a shell
    glob, in which case the template is applied once per matching file and
    written to --export-dir.
    """
    import asyncio
    import glob

    from datasight.dashboard_template import (
        TemplateError,
        apply_template,
        load_template,
        resolve_variables,
    )

    try:
        template_obj = load_template(name, project_dir)
    except TemplateError as err:
        raise click.ClickException(str(err)) from err

    cli_var_overrides: dict[str, str] = {}
    for raw in var_overrides:
        if "=" not in raw:
            raise click.ClickException(f"Invalid --var value {raw!r}. Expected NAME=VALUE.")
        key, _, value = raw.partition("=")
        key = key.strip()
        if not key:
            raise click.ClickException(f"Invalid --var value {raw!r}. Expected NAME=VALUE.")
        cli_var_overrides[key] = value

    base_db = _resolve_project_duckdb(project_dir)

    parsed: dict[str, str] = {}
    for mapping in table_mappings:
        if "=" not in mapping:
            raise click.ClickException(f"Invalid --table value {mapping!r}. Expected NAME=PATH.")
        key, _, value = mapping.partition("=")
        key = key.strip()
        value = value.strip()
        if not key or not value:
            raise click.ClickException(f"Invalid --table value {mapping!r}. Expected NAME=PATH.")
        if key in parsed:
            raise click.ClickException(f"Duplicate --table mapping for {key!r}.")
        parsed[key] = value

    rotating_name: str | None = None
    rotating_paths: list[str] = []
    fixed: dict[str, str] = {}
    for key, value in parsed.items():
        is_glob = any(ch in value for ch in "*?[")
        if is_glob:
            matches = sorted(glob.glob(value))
            if not matches:
                raise click.ClickException(f"No files match --table {key}={value!r}.")
            if rotating_name is not None:
                raise click.ClickException(
                    f"Only one --table mapping may glob. Both {rotating_name!r} and {key!r} glob."
                )
            rotating_name = key
            rotating_paths = matches
            continue
        if not Path(value).exists():
            raise click.ClickException(f"File not found for --table {key}: {value}")
        fixed[key] = value

    required = list(template_obj.get("required_tables") or [])
    attached_base_tables: set[str] = set()
    if base_db:
        import duckdb

        with duckdb.connect(":memory:") as peek:
            escaped_db = str(Path(base_db).resolve()).replace("'", "''")
            peek.execute(f"ATTACH '{escaped_db}' AS peek (READ_ONLY)")
            rows = peek.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_catalog = 'peek' AND table_schema = 'main'"
            ).fetchall()
            attached_base_tables = {str(r[0]) for r in rows}

    supplied = set(fixed) | ({rotating_name} if rotating_name else set()) | attached_base_tables
    missing = [t for t in required if t not in supplied]
    if missing:
        if base_db is None:
            hint = (
                "No project DuckDB was detected (checked .env DB_PATH and "
                f"{Path(project_dir).resolve() / 'database.duckdb'}). "
                "Pass --table NAME=PATH for each missing table, or configure "
                "DB_MODE=duckdb and DB_PATH in the project's .env."
            )
        else:
            hint = (
                f"Project DuckDB {base_db} is attached but does not contain "
                f"these tables. Pass --table NAME=PATH or add them to the DB."
            )
        raise click.ClickException(
            "Required tables not provided: " + ", ".join(missing) + ". " + hint
        )

    if output_path and export_dir:
        raise click.ClickException("Pass either --output or --export-dir, not both.")

    if rotating_name is None and export_dir and not output_path:
        # No glob, but --export-dir was given. Promote a single non-globbed
        # mapping into the rotating slot so its stem names the output file.
        if len(fixed) != 1:
            raise click.ClickException(
                "--export-dir needs exactly one --table mapping (or a glob) "
                "to derive the output filename. "
                f"Got {len(fixed)} mappings — use --output PATH instead."
            )
        rotating_name, rotating_value = next(iter(fixed.items()))
        rotating_paths = [rotating_value]
        fixed.pop(rotating_name)

    if rotating_name is None:
        if not output_path:
            raise click.ClickException(
                "Single-shot runs need --output PATH (or --export-dir DIR "
                "to derive the filename from the input)."
            )
        out_path = Path(output_path).resolve()

        try:
            variable_values = resolve_variables(
                template_obj, filename=None, overrides=cli_var_overrides
            )
        except TemplateError as err:
            raise click.ClickException(str(err)) from err

        async def single_run():
            return [
                await apply_template(
                    template_obj,
                    out_path,
                    sources=fixed,
                    base_db=base_db,
                    variables=variable_values,
                )
            ]

        results = asyncio.run(single_run())
    else:
        if not export_dir and not (output_path and len(rotating_paths) == 1):
            raise click.ClickException(
                "Batch mode (a --table mapping with multiple matches) needs --export-dir DIR."
            )
        out_dir = Path(export_dir).resolve() if export_dir else None
        if out_dir:
            out_dir.mkdir(parents=True, exist_ok=True)
        fixed_output = Path(output_path).resolve() if output_path else None

        async def batch_run():
            from datasight.dashboard_template import ApplyResult

            batch_results: list[ApplyResult] = []
            for path in rotating_paths:
                stem = Path(path).stem
                if fixed_output is not None:
                    out_file = fixed_output
                else:
                    assert out_dir is not None  # guarded above
                    out_file = out_dir / f"{stem}.html"
                sources = dict(fixed)
                sources[rotating_name] = path
                try:
                    per_file_vars = resolve_variables(
                        template_obj, filename=path, overrides=cli_var_overrides
                    )
                except TemplateError as err:
                    result = ApplyResult(
                        label=Path(path).name, output=None, ok=False, error=str(err)
                    )
                    batch_results.append(result)
                    click.echo(f"  FAIL {path} — {err}", err=True)
                    if fail_fast:
                        break
                    continue
                result = await apply_template(
                    template_obj,
                    out_file,
                    sources=sources,
                    base_db=base_db,
                    variables=per_file_vars,
                )
                batch_results.append(result)
                if result.ok:
                    click.echo(f"  ok   {path} -> {out_file}")
                else:
                    reason = result.error or "; ".join(
                        f"card {c.idx} ({c.title}): {c.error}" for c in result.cards if not c.ok
                    )
                    click.echo(f"  FAIL {path} — {reason}", err=True)
                    if fail_fast:
                        break
            return batch_results

        results = asyncio.run(batch_run())

    failed = sum(1 for r in results if not r.ok)
    if rotating_name is None:
        only = results[0]
        if only.ok:
            click.echo(f"Wrote {only.output}")
        else:
            reason = only.error or "; ".join(
                f"card {c.idx} ({c.title}): {c.error}" for c in only.cards if not c.ok
            )
            click.echo(f"FAIL: {reason}", err=True)
    else:
        click.echo(
            f"Applied {name!r} to {len(results)} file(s): "
            f"{len(results) - failed} succeeded, {failed} failed."
        )
    if failed:
        raise SystemExit(1)


@templates.command(
    name="delete",
    epilog=_epilog(
        """
        Example:

            datasight templates delete generation-dashboard
        """
    ),
)
@click.argument("name")
@_PROJECT_DIR_OPT
def template_delete(name: str, project_dir: str):
    """Delete a saved template."""
    from datasight.dashboard_template import TemplateError, delete_template

    try:
        removed = delete_template(name, project_dir)
    except TemplateError as err:
        raise click.ClickException(str(err)) from err
    if not removed:
        raise click.ClickException(f"Template {name!r} not found.")
    click.echo(f"Deleted template {name!r}.")
