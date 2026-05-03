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

            # Build ./database.duckdb from Excel inputs (one table per sheet)
            datasight generate generation.xlsx

            # Build a custom project DuckDB from CSV, Parquet, or Excel inputs
            datasight generate generation.csv --db-path project.duckdb
            datasight generate generation.parquet --db-path project.duckdb

        FILES are input data. --db-path is only the output DuckDB path used
        when datasight needs to build a project database from CSV/Parquet/Excel
        or mixed file inputs.
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
        "Output DuckDB path to create from CSV/Parquet/Excel or mixed file "
        "inputs (default: database.duckdb). Do not use this with a single "
        "existing DuckDB or SQLite database; those are referenced directly."
    ),
)
@click.option(
    "--import-mode",
    type=click.Choice(["auto", "view", "table"], case_sensitive=False),
    default="auto",
    show_default=True,
    help=(
        "When FILES are CSV/Parquet/Excel inputs, choose whether datasight "
        "creates source-backed views or materialized DuckDB tables. "
        "'auto' prefers tables for CSV and views for Parquet."
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
def generate(
    files,
    project_dir,
    model,
    overwrite,
    table,
    db_path,
    import_mode,
    compact_schema,
    verbose,
):
    """Generate schema_description.md, queries.yaml, measures.yaml, and time_series.yaml from your database.

    Connects to the database, inspects tables and columns, samples
    code/enum columns, and asks the LLM to produce documentation
    and example queries.
    """
    import asyncio

    project_dir = str(Path(project_dir).resolve())

    # Configure logging and resolve settings early so the db_target
    # preflight can respect a pre-existing DB_MODE (e.g. spark) and
    # avoid clobbering the user's backend config.
    level = "DEBUG" if verbose else "WARNING"
    _configure_logging(level)
    settings, resolved_model = _resolve_settings(project_dir, model)
    _validate_settings_for_llm(settings)

    # Resolve the would-be DB path up front so we can include it in the
    # preflight check — otherwise a stale database.duckdb would abort the
    # run only after the LLM call and the doc writes, leaving behind a
    # partial, mutated project. Skip it entirely when the project is
    # configured for a non-DuckDB backend; we're not going to touch the
    # local DuckDB in that case.
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
            if import_mode.lower() != "auto":
                click.echo(
                    "Error: --import-mode only applies when importing CSV/Parquet/Excel "
                    "or mixed file inputs into DuckDB.",
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
            if import_mode.lower() != "auto":
                click.echo(
                    "Error: --import-mode only applies when importing CSV/Parquet/Excel "
                    "or mixed file inputs into DuckDB.",
                    err=True,
                )
                sys.exit(1)
            if db_path:
                click.echo(
                    "Error: --db-path is only used when creating a project DuckDB from "
                    "CSV/Parquet or mixed inputs; omit it for an existing DuckDB database.",
                    err=True,
                )
                sys.exit(1)
            duckdb_source_path = duckdb_files[0]
        elif settings.database.mode == "duckdb":
            _db_target = Path(db_path or "database.duckdb")
            if not _db_target.is_absolute():
                _db_target = Path(project_dir) / _db_target
            db_target = _db_target.resolve()
        # Any other mode (spark, postgres, flightsql, sqlite) → no
        # local DuckDB target; we'll preserve the existing backend.

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
                list(files), settings.database, import_mode=import_mode.lower()
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
        # The "new project from parquet files" flow creates a local DuckDB
        # mirror and writes DB_MODE=duckdb to .env. When the user is already
        # inside a project configured for a different backend (Spark,
        # Postgres, Flight SQL, SQLite), doing that silently clobbers
        # their real config. Preserve whatever they already have.
        if settings.database.mode != "duckdb":
            click.echo(
                f"Existing .env has DB_MODE={settings.database.mode} — "
                "keeping it. The schema_description.md / queries.yaml "
                "files describe the same tables your configured backend "
                "serves; no local DuckDB mirror was created and .env was "
                "not modified."
            )
        else:
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
