"""Command-line interface for datasight."""

import os
import sys
import shutil
from pathlib import Path

import rich_click as click
from loguru import logger

from datasight import __version__
from datasight.config import create_sql_runner_from_settings
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


@click.group()
@click.version_option(__version__, prog_name="datasight")
def cli():
    """datasight — AI-powered database exploration with natural language."""


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
    from dotenv import load_dotenv

    from datasight.recent_projects import validate_project_dir

    # Auto-detect project in current directory if not specified
    if project_dir is None:
        cwd = str(Path.cwd().resolve())
        is_valid, _ = validate_project_dir(cwd)
        if is_valid:
            project_dir = cwd

    # Load .env from current directory or project directory if specified
    if project_dir:
        project_dir = str(Path(project_dir).resolve())
        env_path = os.path.join(project_dir, ".env")
        if os.path.exists(env_path):
            load_dotenv(env_path, override=False)
    else:
        # Try loading from current directory
        if os.path.exists(".env"):
            load_dotenv(".env", override=False)

    # Configure logging
    level = "DEBUG" if verbose else "INFO"
    logger.remove()
    logger.add(sys.stderr, level=level, format="{time:HH:mm:ss} {name} {level} {message}")

    # Load settings (API key validation deferred to project load / chat)
    settings = Settings.from_env()
    resolved_model = model if model else settings.llm.model
    resolved_port = port if port else settings.app.port

    # Set env vars for the FastAPI app
    os.environ["PORT"] = str(resolved_port)
    if model:
        # CLI override for model - set the appropriate env var
        match settings.llm.provider:
            case "ollama":
                os.environ["OLLAMA_MODEL"] = resolved_model
            case "github":
                os.environ["GITHUB_MODELS_MODEL"] = resolved_model
            case _:
                os.environ["ANTHROPIC_MODEL"] = resolved_model

    # If project-dir specified, set it for auto-load
    if project_dir:
        os.environ["DATASIGHT_AUTO_LOAD_PROJECT"] = project_dir

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
@click.argument("question")
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
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging.")
def ask(question, project_dir, model, output_format, chart_format, output_path, verbose):
    """Ask a question about your data from the command line.

    Runs the full LLM agent loop without starting a web server.
    Results are printed to the console.

    \b
    Examples:
      datasight ask "What are the top 5 states by generation?"
      datasight ask "Show generation by year" --chart-format html -o chart.html
      datasight ask "Top 5 states" --format csv -o results.csv
    """
    import asyncio
    import json as json_mod

    project_dir = str(Path(project_dir).resolve())

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

    async def _run():
        from datasight.agent import run_agent_loop
        from datasight.config import (
            format_example_queries,
            load_example_queries,
            load_schema_description,
        )
        from datasight.prompts import build_system_prompt
        from datasight.schema import format_schema_context, introspect_schema

        llm_client = create_llm_client(
            provider=settings.llm.provider,
            api_key=settings.llm.api_key,
            base_url=settings.llm.base_url,
        )
        sql_runner = create_sql_runner_from_settings(settings.database, project_dir)

        # Build system prompt
        tables = await introspect_schema(sql_runner.run_sql, runner=sql_runner)
        user_desc = load_schema_description(None, project_dir)
        example_queries = load_example_queries(None, project_dir)
        schema_text = format_schema_context(tables, user_desc)
        if example_queries:
            schema_text += format_example_queries(example_queries)

        from datasight.sql_validation import build_schema_map

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

        result = await run_agent_loop(
            question=question,
            llm_client=llm_client,
            model=resolved_model,
            system_prompt=sys_prompt,
            run_sql=sql_runner.run_sql,
            schema_map=schema_map,
            dialect=sql_dialect,
        )
        return result

    result = asyncio.run(_run())

    from rich.console import Console

    console = Console()

    # Print the assistant's text response
    if result.text:
        console.print(result.text)
        console.print()

    # Output data results
    for tr in result.tool_results:
        if tr.df is not None and not tr.df.empty:
            if output_format == "csv":
                csv_output = tr.df.to_csv(index=False)
                if output_path and not chart_format:
                    Path(output_path).write_text(csv_output, encoding="utf-8")
                    click.echo(f"Data saved to {output_path}")
                else:
                    click.echo(csv_output)
            elif output_format == "json":
                json_output = tr.df.to_json(orient="records", indent=2)
                if output_path and not chart_format:
                    Path(output_path).write_text(json_output, encoding="utf-8")
                    click.echo(f"Data saved to {output_path}")
                else:
                    click.echo(json_output)
            else:
                # Rich table
                from rich.table import Table as RichTable

                rich_table = RichTable(show_lines=True)
                for col in tr.df.columns:
                    rich_table.add_column(str(col))
                for _, row in tr.df.head(50).iterrows():
                    rich_table.add_row(*[str(v) for v in row])
                console.print(rich_table)
                if len(tr.df) > 50:
                    console.print(f"[dim]Showing 50 of {len(tr.df)} rows[/dim]")

        # Handle chart export
        if tr.plotly_spec and chart_format and output_path:
            if chart_format == "json":
                Path(output_path).write_text(
                    json_mod.dumps(tr.plotly_spec, indent=2), encoding="utf-8"
                )
                click.echo(f"Plotly spec saved to {output_path}")
            elif chart_format == "html":
                from datasight.chart import _build_artifact_html

                html = _build_artifact_html(tr.plotly_spec, tr.meta.get("title", "Chart"))
                Path(output_path).write_text(html, encoding="utf-8")
                click.echo(f"Chart HTML saved to {output_path}")
            elif chart_format == "png":
                try:
                    import plotly.io as pio
                    import plotly.graph_objects as go

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
def log_cmd(project_dir, tail_n, errors, full):
    """Display the SQL query log in a formatted table."""
    from rich.console import Console
    from rich.table import Table
    from rich.text import Text

    from datasight.query_log import QueryLogger

    project_dir = str(Path(project_dir).resolve())
    log_path = os.path.join(project_dir, "query_log.jsonl")

    if not os.path.exists(log_path):
        click.echo(f"No query log found at {log_path}")
        click.echo("Run 'datasight run --query-log' to enable logging.")
        return

    ql = QueryLogger(path=log_path, enabled=False)
    entries = ql.read_recent(tail_n)

    if errors:
        entries = [e for e in entries if e.get("error")]

    if not entries:
        click.echo("No matching log entries.")
        return

    console = Console()
    table = Table(show_lines=True)
    table.add_column("Timestamp", style="dim", no_wrap=True)
    table.add_column("Tool", no_wrap=True)
    table.add_column("SQL", min_width=40, overflow="fold")
    table.add_column("Time", justify="right", no_wrap=True)
    table.add_column("Rows", justify="right", no_wrap=True)
    table.add_column("Status", no_wrap=True)

    if full:
        table.add_column("Question", overflow="fold")

    total = len(entries)
    failed = 0
    for entry in entries:
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

        row = [ts, tool, sql, time_str, rows_str, status]
        if full:
            row.append(entry.get("user_question", ""))
        table.add_row(*row)

    console.print(table)

    succeeded = total - failed
    summary = f"{total} queries ({succeeded} succeeded, {failed} failed)"
    console.print(f"\n[dim]{summary}[/dim]")
