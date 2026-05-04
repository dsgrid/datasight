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

_SHARED_EXPORTS = (
    asyncio,
    json,
    os,
    shutil,
    sys,
    uuid,
    datetime,
    timezone,
    Path,
    Any,
    Literal,
    click,
    yaml,
    logger,
    __version__,
    create_sql_runner_from_settings,
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
    build_audit_report,
    render_audit_report_html,
    render_audit_report_markdown,
    build_distribution_overview,
    build_integrity_overview,
    create_llm_client,
    Settings,
    global_env_path,
    load_global_env,
    build_validation_report,
    load_validation_config,
)


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


@click.group()
@click.version_option(__version__, prog_name="datasight")
def cli():
    """datasight — AI-powered data exploration with natural language."""


def _register_commands() -> None:
    from datasight.cli_commands.ask import ask
    from datasight.cli_commands.audit_report import audit_report
    from datasight.cli_commands.config import config
    from datasight.cli_commands.demo import demo
    from datasight.cli_commands.dimensions import dimensions
    from datasight.cli_commands.distribution import distribution
    from datasight.cli_commands.doctor import doctor
    from datasight.cli_commands.export import export
    from datasight.cli_commands.generate import generate
    from datasight.cli_commands.init import init
    from datasight.cli_commands.inspect import inspect
    from datasight.cli_commands.integrity import integrity
    from datasight.cli_commands.log import log_cmd
    from datasight.cli_commands.measures import measures
    from datasight.cli_commands.profile import profile
    from datasight.cli_commands.quality import quality
    from datasight.cli_commands.recipes import recipes
    from datasight.cli_commands.report import report
    from datasight.cli_commands.run import run
    from datasight.cli_commands.session import session
    from datasight.cli_commands.templates import templates
    from datasight.cli_commands.trends import trends
    from datasight.cli_commands.validate import validate
    from datasight.cli_commands.verify import verify

    cli.add_command(init)
    cli.add_command(config)
    cli.add_command(demo)
    cli.add_command(generate)
    cli.add_command(run)
    cli.add_command(session)
    cli.add_command(verify)
    cli.add_command(ask)
    cli.add_command(profile)
    cli.add_command(measures)
    cli.add_command(quality)
    cli.add_command(integrity)
    cli.add_command(distribution)
    cli.add_command(validate)
    cli.add_command(audit_report)
    cli.add_command(dimensions)
    cli.add_command(trends)
    cli.add_command(inspect)
    cli.add_command(recipes)
    cli.add_command(doctor)
    cli.add_command(export)
    cli.add_command(log_cmd)
    cli.add_command(report)
    cli.add_command(templates)


_register_commands()
