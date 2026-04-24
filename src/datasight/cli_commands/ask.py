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


@click.command(
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
