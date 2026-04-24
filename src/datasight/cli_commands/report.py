# ruff: noqa: F401, F403, F405
"""CLI command module."""

from datasight import cli as cli_root
from datasight.cli import *  # noqa: F403
from datasight.cli import (
    _build_metric_table,
    _build_profile_detail_table,
    _build_sql_script,
    _configure_logging,
    _current_db_settings_or_none,
    _default_chart_extension,
    _default_data_extension,
    _emit_ask_result,
    _emit_cli_provenance,
    _epilog,
    _fmt_dist,
    _format_profile_value,
    _iter_sql_tool_results,
    _load_batch_entries,
    _load_recipe_entries,
    _load_schema_info_for_project,
    _prepare_web_runtime,
    _print_sql_queries,
    _question_table_prefix,
    _render_dimensions_markdown,
    _render_distribution_markdown,
    _render_doctor_markdown,
    _render_integrity_markdown,
    _render_measures_markdown,
    _render_profile_markdown,
    _render_quality_markdown,
    _render_recipes_markdown,
    _render_trends_markdown,
    _render_validation_markdown,
    _resolve_db_path,
    _resolve_settings,
    _sanitize_sql_identifier,
    _slugify_filename,
    _sql_comment_lines,
    _validate_batch_entry,
    _validate_settings_for_llm,
    _write_batch_result_files,
    _write_or_print,
)


def create_llm_client(*args, **kwargs):
    return cli_root.create_llm_client(*args, **kwargs)


async def _run_ask_pipeline(*args, **kwargs):
    return await cli_root._run_ask_pipeline(*args, **kwargs)


@click.group(
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


@click.command(
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


@click.command(
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


@click.command(
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


report.add_command(report_list)
report.add_command(report_run)
report.add_command(report_delete)
