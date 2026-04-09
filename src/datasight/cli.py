"""Command-line interface for datasight."""

import asyncio
import json
import os
import shutil
import sys
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
    build_prompt_recipes,
    build_quality_overview,
    build_table_profile,
    build_trend_overview,
    find_column_info,
    find_table_info,
)
from datasight.llm import create_llm_client
from datasight.settings import Settings


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
    env_path = os.path.join(project_dir, ".env")
    settings = Settings.from_env(env_path if os.path.exists(env_path) else None)

    # Apply model override if provided
    resolved_model = model_override if model_override else settings.llm.model

    return settings, resolved_model


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
        load_schema_description,
    )
    from datasight.prompts import build_system_prompt
    from datasight.schema import format_schema_context, introspect_schema
    from datasight.sql_validation import build_schema_map

    llm_client = create_llm_client(
        provider=settings.llm.provider,
        api_key=settings.llm.api_key,
        base_url=settings.llm.base_url,
    )
    sql_runner = create_sql_runner_from_settings(settings.database, project_dir)

    tables = await introspect_schema(sql_runner.run_sql, runner=sql_runner)
    user_desc = load_schema_description(None, project_dir)
    example_queries = load_example_queries(None, project_dir)
    schema_text = format_schema_context(tables, user_desc)
    if example_queries:
        schema_text += format_example_queries(example_queries)

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
    )

    return await run_agent_loop(
        question=question,
        llm_client=llm_client,
        model=resolved_model,
        system_prompt=sys_prompt,
        run_sql=sql_runner.run_sql,
        schema_map=schema_map,
        dialect=sql_dialect,
    )


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
                f"- `{item['table']}`: `{item['measure_column']}` over `{item['date_column']}` "
                f"({item['date_range']})"
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


def _format_profile_value(value: Any, default: str = "?") -> str:
    if value is None or value == "":
        return default
    return str(value)


def _load_recipe_entries(
    project_dir: str,
    settings: Settings,
    table: str | None = None,
) -> list[dict[str, Any]]:
    resolved_db_path = _resolve_db_path(settings, project_dir)
    if settings.database.mode in ("duckdb", "sqlite") and not os.path.exists(resolved_db_path):
        click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
        sys.exit(1)

    async def _run_recipes() -> list[dict[str, Any]]:
        sql_runner, schema_info = await _load_schema_info_for_project(project_dir, settings)
        if table:
            table_info = find_table_info(schema_info, table)
            if table_info is None:
                raise click.ClickException(f"Table not found: {table}")
            schema_info = [table_info]
        recipes = await build_prompt_recipes(schema_info, sql_runner.run_sql)
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
                rich_table = RichTable(show_lines=True)
                for col in tr.df.columns:
                    rich_table.add_column(str(col))
                for _, row in tr.df.head(50).iterrows():
                    rich_table.add_row(*[str(v) for v in row])
                console.print(rich_table)
                if len(tr.df) > 50:
                    console.print(f"[dim]Showing 50 of {len(tr.df)} rows[/dim]")

        if tr.plotly_spec and chart_format and output_path:
            if chart_format == "json":
                Path(output_path).write_text(
                    json.dumps(tr.plotly_spec, indent=2), encoding="utf-8"
                )
                click.echo(f"Plotly spec saved to {output_path}")
            elif chart_format == "html":
                from datasight.chart import _build_artifact_html

                html = _build_artifact_html(tr.plotly_spec, tr.meta.get("title", "Chart"))
                Path(output_path).write_text(html, encoding="utf-8")
                click.echo(f"Chart HTML saved to {output_path}")
            elif chart_format == "png":
                try:
                    import plotly.graph_objects as go
                    import plotly.io as pio

                    fig = go.Figure(tr.plotly_spec)
                    pio.write_image(fig, output_path)
                    click.echo(f"Chart PNG saved to {output_path}")
                except ImportError:
                    click.echo(
                        "Error: PNG export requires kaleido. "
                        "Install with: pip install 'datasight[export]'",
                        err=True,
                    )
                    sys.exit(1)


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
                        "Install with: pip install 'datasight[export]'",
                        err=True,
                    )
                    sys.exit(1)
            written_paths.append(str(chart_path))

    return written_paths


@click.group()
@click.version_option(__version__, prog_name="datasight")
def cli():
    """datasight — AI-powered database exploration with natural language."""


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

    if configure_logging:
        level = "DEBUG" if verbose else "INFO"
        logger.remove()
        logger.add(sys.stderr, level=level, format="{time:HH:mm:ss} {name} {level} {message}")

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


@cli.command()
@click.argument("project_dir", default=".")
@click.option("--overwrite", is_flag=True, help="Overwrite existing files.")
def init(project_dir: str, overwrite: bool):
    """Create a new datasight project with template files.

    PROJECT_DIR defaults to the current directory.
    """
    dest = Path(project_dir).resolve()
    dest.mkdir(parents=True, exist_ok=True)

    template_dir = Path(__file__).parent / "templates"

    files = {
        "env.template": ".env",
        "schema_description.md": "schema_description.md",
        "queries.yaml": "queries.yaml",
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
    click.echo("  1. Edit .env with your API key and database path")
    click.echo("  2. Edit schema_description.md to describe your data")
    click.echo("  3. Edit queries.yaml with example questions")
    click.echo("  4. Run: datasight run")


@cli.command()
@click.argument("project_dir", default=".")
@click.option(
    "--min-year", type=int, default=2020, help="Earliest year to include (default: 2020)."
)
def demo(project_dir: str, min_year: int):
    """Download an EIA energy demo dataset and create a ready-to-run project.

    Downloads cleaned EIA-923 and EIA-860 data from the PUDL project's public
    data releases. Creates a DuckDB database with generation, fuel consumption,
    and plant data, along with pre-written schema descriptions and example queries.

    PROJECT_DIR defaults to the current directory.
    """
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="{time:HH:mm:ss} {level} {message}")

    dest = Path(project_dir).resolve()
    dest.mkdir(parents=True, exist_ok=True)

    click.echo(f"datasight demo — downloading EIA energy data (>= {min_year})")
    click.echo(f"  Destination: {dest}")
    click.echo()

    from datasight.demo import download_demo_dataset, write_demo_project_files

    click.echo("Downloading from PUDL (this may take a minute)...")
    db_path = download_demo_dataset(dest, min_year=min_year)
    db_size_mb = db_path.stat().st_size / (1024 * 1024)
    click.echo(f"  Database: {db_path.name} ({db_size_mb:.1f} MB)")

    click.echo("Writing project files...")
    write_demo_project_files(dest)

    click.echo()
    click.echo("Demo project ready!")
    click.echo()
    click.echo("Next steps:")
    click.echo(f"  1. cd {dest}")
    click.echo("  2. Edit .env — set your ANTHROPIC_API_KEY")
    click.echo("  3. datasight run")


@cli.command()
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
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging.")
def generate(project_dir, model, overwrite, table, verbose):
    """Generate schema_description.md and queries.yaml from your database.

    Connects to the database, inspects tables and columns, samples
    code/enum columns, and asks the LLM to produce documentation
    and example queries.
    """
    import asyncio

    project_dir = str(Path(project_dir).resolve())

    # Check for existing files early
    schema_path = Path(project_dir) / "schema_description.md"
    queries_path = Path(project_dir) / "queries.yaml"
    if not overwrite:
        existing = []
        if schema_path.exists():
            existing.append("schema_description.md")
        if queries_path.exists():
            existing.append("queries.yaml")
        if existing:
            click.echo(
                f"Error: {', '.join(existing)} already exist. Use --overwrite to replace.",
                err=True,
            )
            sys.exit(1)

    # Logging
    level = "DEBUG" if verbose else "WARNING"
    logger.remove()
    logger.add(sys.stderr, level=level, format="{time:HH:mm:ss} {level} {message}")

    # Load settings and validate
    settings, resolved_model = _resolve_settings(project_dir, model)
    _validate_settings_for_llm(settings)

    resolved_db_path = _resolve_db_path(settings, project_dir)
    if settings.database.mode in ("duckdb", "sqlite") and not os.path.exists(resolved_db_path):
        click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
        sys.exit(1)

    sql_dialect = settings.database.sql_dialect

    click.echo("datasight generate")
    click.echo(f"  Model:    {resolved_model}")
    click.echo(f"  Database: {settings.database.mode} — {resolved_db_path or sql_dialect}")
    click.echo()

    async def _run():
        from datasight.generate import (
            build_generation_context,
            sample_enum_columns,
        )
        from datasight.schema import introspect_schema

        llm_client = create_llm_client(
            provider=settings.llm.provider,
            api_key=settings.llm.api_key,
            base_url=settings.llm.base_url,
        )
        sql_runner = create_sql_runner_from_settings(settings.database, project_dir)

        # Introspect schema
        click.echo("Introspecting database schema...")
        tables = await introspect_schema(sql_runner.run_sql, runner=sql_runner)

        # Filter to specified tables if --table was provided
        if table:
            table_set = {t.lower() for t in table}
            tables = [t for t in tables if t.name.lower() in table_set]
            if not tables:
                click.echo(f"Error: No matching tables found for: {', '.join(table)}", err=True)
                sys.exit(1)

        click.echo(f"  Found {len(tables)} tables")

        # Sample low-cardinality string columns for enum/code detection
        click.echo("Sampling code/enum columns...")
        samples_text = await sample_enum_columns(sql_runner.run_sql, tables)

        # Build LLM prompt and call
        click.echo("Generating documentation (this may take a moment)...")
        system_prompt, user_msg = build_generation_context(tables, sql_dialect, samples_text)

        from datasight.llm import TextBlock

        response = await llm_client.create_message(
            model=resolved_model,
            system=system_prompt,
            messages=[{"role": "user", "content": user_msg}],
            tools=[],
            max_tokens=4096,
        )

        parts = [block.text for block in response.content if isinstance(block, TextBlock)]
        return "".join(parts)

    text = asyncio.run(_run())

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

    if queries_content:
        queries_path.write_text(queries_content + "\n", encoding="utf-8")
        written.append("queries.yaml")

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


@cli.command()
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


@cli.command()
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

    Add expected results to queries.yaml entries:

    \b
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
    import asyncio

    project_dir = str(Path(project_dir).resolve())

    # Logging
    level = "DEBUG" if verbose else "WARNING"
    logger.remove()
    logger.add(sys.stderr, level=level, format="{time:HH:mm:ss} {level} {message}")

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
        from datasight.verify import run_ambiguity_analysis, run_verification

        llm_client = create_llm_client(
            provider=settings.llm.provider,
            api_key=settings.llm.api_key,
            base_url=settings.llm.base_url,
        )
        sql_runner = create_sql_runner_from_settings(settings.database, project_dir)

        # Build system prompt
        tables = await introspect_schema(sql_runner.run_sql, runner=sql_runner)
        user_desc = load_schema_description(None, project_dir)
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
    from rich.console import Console
    from rich.table import Table
    from rich.text import Text

    console = Console()

    # --- Ambiguity warnings ---
    ambiguous_count = sum(1 for a in ambiguity_results if a.is_ambiguous)
    if ambiguous_count:
        amb_table = Table(
            show_lines=True,
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
    table = Table(show_lines=True, title="Verification Results")
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


@cli.command()
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
    verbose,
):
    """Ask a question about your data from the command line.

    Runs the full LLM agent loop without starting a web server.
    Results are printed to the console.

    \b
    Examples:
      datasight ask "What are the top 5 states by generation?"
      datasight ask "Show generation by year" --chart-format html -o chart.html
      datasight ask "Top 5 states" --format csv -o results.csv
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

    # Logging
    level = "DEBUG" if verbose else "WARNING"
    logger.remove()
    logger.add(sys.stderr, level=level, format="{time:HH:mm:ss} {level} {message}")

    # Load settings and validate
    settings, resolved_model = _resolve_settings(project_dir, model)
    _validate_settings_for_llm(settings)

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
                _emit_ask_result(result, batch_output_format, None, None)
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
    _emit_ask_result(result, output_format, chart_format, output_path)


@cli.command()
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
    """Profile your dataset without using the LLM."""
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


@cli.command()
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
    """Run a deterministic quality audit without using the LLM."""
    from rich.console import Console

    project_dir = str(Path(project_dir).resolve())
    settings, _ = _resolve_settings(project_dir)
    resolved_db_path = _resolve_db_path(settings, project_dir)
    if settings.database.mode in ("duckdb", "sqlite") and not os.path.exists(resolved_db_path):
        click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
        sys.exit(1)

    async def _run_quality():
        sql_runner, schema_info = await _load_schema_info_for_project(project_dir, settings)
        if table:
            table_info = find_table_info(schema_info, table)
            if table_info is None:
                raise click.ClickException(f"Table not found: {table}")
            schema_info = [table_info]
        return await build_quality_overview(schema_info, sql_runner.run_sql)

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


@cli.command()
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
    """Surface likely grouping dimensions without using the LLM."""
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


@cli.command()
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
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
def trends(project_dir, table, output_format, output_path):
    """Surface likely trend analyses without using the LLM."""
    from rich.console import Console

    project_dir = str(Path(project_dir).resolve())
    settings, _ = _resolve_settings(project_dir)
    resolved_db_path = _resolve_db_path(settings, project_dir)
    if settings.database.mode in ("duckdb", "sqlite") and not os.path.exists(resolved_db_path):
        click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
        sys.exit(1)

    async def _run_trends():
        sql_runner, schema_info = await _load_schema_info_for_project(project_dir, settings)
        if table:
            table_info = find_table_info(schema_info, table)
            if table_info is None:
                raise click.ClickException(f"Table not found: {table}")
            schema_info = [table_info]
        return await build_trend_overview(schema_info, sql_runner.run_sql)

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
                [("Table", "left"), ("Date", "left"), ("Measure", "left"), ("Range", "left")],
                [
                    [
                        item["table"],
                        item["date_column"],
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


@cli.group()
def recipes():
    """Generate and run reusable deterministic prompt recipes."""


@recipes.command(name="list")
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


@recipes.command(name="run")
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
    """Run a generated recipe by ID through the normal ask pipeline."""
    import asyncio

    from rich.console import Console

    project_dir = str(Path(project_dir).resolve())
    settings, resolved_model = _resolve_settings(project_dir, model)
    _validate_settings_for_llm(settings)

    if verbose:
        logger.remove()
        logger.add(sys.stderr, level="DEBUG")

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


@cli.command()
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
    """Check project configuration, local files, and database connectivity."""
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


@cli.command()
@click.argument("session_id")
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

    \b
    Examples:
      datasight export --list-sessions
      datasight export abc123def -o my-analysis.html
      datasight export abc123def --exclude 2,3
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


@cli.command(name="log")
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
    """Display the SQL query log in a formatted table."""
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
    table = Table(show_lines=True)
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
        cost_table = Table(title="LLM Cost Summary", show_lines=True)
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


@cli.group()
def report():
    """Manage saved reports."""


@report.command(name="list")
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory.",
)
def report_list(project_dir):
    """List all saved reports."""
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
    table = Table(show_lines=True)
    table.add_column("ID", justify="right", no_wrap=True)
    table.add_column("Name", min_width=20)
    table.add_column("Tool", no_wrap=True)
    table.add_column("SQL", min_width=40, overflow="fold")

    for r in reports:
        sql_preview = r["sql"][:80] + ("..." if len(r["sql"]) > 80 else "")
        table.add_row(str(r["id"]), r.get("name", ""), r["tool"], sql_preview)

    console.print(table)
    click.echo(f"\n{len(reports)} report(s)")


@report.command(name="run")
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
                from rich.table import Table as RichTable

                rt = RichTable(show_lines=True)
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


@report.command(name="delete")
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
