"""Command-line interface for datasight."""

import os
import sys
import shutil
from pathlib import Path

import rich_click as click
from loguru import logger

from datasight import __version__


@click.group()
@click.version_option(__version__, prog_name="datasight")
def cli():
    """datasight — AI-powered database exploration with natural language."""


@cli.command()
@click.argument("project_dir", default=".")
@click.option("--force", is_flag=True, help="Overwrite existing files.")
def init(project_dir: str, force: bool):
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

        if dst.exists() and not force:
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
@click.option("--port", type=int, default=None, help="Web UI port (default: 8084).")
@click.option("--host", default="0.0.0.0", help="Bind address.")
@click.option(
    "--db-mode",
    type=click.Choice(["local", "flightsql"]),
    default=None,
    help="Database mode (overrides .env).",
)
@click.option(
    "--db-path", type=click.Path(), default=None, help="Path to DuckDB file (overrides .env)."
)
@click.option("--model", default=None, help="Anthropic model name (overrides .env).")
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing .env and config files.",
)
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging.")
@click.option("--query-log", is_flag=True, help="Enable SQL query logging to query_log.jsonl.")
@click.option("--confirm-sql", is_flag=True, help="Require user approval before executing SQL.")
@click.option("--explain-sql", is_flag=True, help="Show plain-English SQL explanations.")
@click.option(
    "--no-clarify", is_flag=True, help="Disable clarifying questions for ambiguous queries."
)
def run(
    port,
    host,
    db_mode,
    db_path,
    model,
    project_dir,
    verbose,
    query_log,
    confirm_sql,
    explain_sql,
    no_clarify,
):
    """Start the datasight web UI."""
    project_dir = str(Path(project_dir).resolve())

    # Load .env from project directory
    env_path = os.path.join(project_dir, ".env")
    if os.path.exists(env_path):
        from dotenv import load_dotenv

        load_dotenv(env_path, override=False)

    # Configure logging
    level = "DEBUG" if verbose else "INFO"
    logger.remove()
    logger.add(sys.stderr, level=level, format="{time:HH:mm:ss} {name} {level} {message}")

    # Resolve settings: CLI flags > env vars > defaults
    llm_provider = os.getenv("LLM_PROVIDER", "anthropic")

    if llm_provider == "ollama":
        api_key = "ollama"  # not needed
        resolved_model = model or os.getenv("OLLAMA_MODEL", "qwen3.5:35b-a3b")
    elif llm_provider == "github":
        api_key = os.getenv("GITHUB_TOKEN", "")
        if not api_key:
            click.echo(
                "Error: GITHUB_TOKEN is not set. Add it to .env or your environment.",
                err=True,
            )
            sys.exit(1)
        resolved_model = model or os.getenv("GITHUB_MODELS_MODEL", "gpt-4o")
    else:
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            click.echo(
                "Error: ANTHROPIC_API_KEY is not set. Add it to .env or your environment.",
                err=True,
            )
            sys.exit(1)
        resolved_model = model or os.getenv("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")
    resolved_db_mode = db_mode or os.getenv("DB_MODE", "local")
    resolved_port = port or int(os.getenv("PORT", "8084"))

    # Resolve DB_PATH relative to project_dir, not CWD
    raw_db_path = db_path or os.getenv("DB_PATH", "database.duckdb")
    if resolved_db_mode == "local":
        resolved_db_path = (
            str(Path(project_dir) / raw_db_path) if not os.path.isabs(raw_db_path) else raw_db_path
        )
        if not os.path.exists(resolved_db_path):
            click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
            click.echo("  Set DB_PATH in .env or pass --db-path", err=True)
            sys.exit(1)
    else:
        resolved_db_path = raw_db_path

    # Set env vars so the FastAPI app picks them up on startup
    os.environ.setdefault("ANTHROPIC_API_KEY", api_key or "")
    os.environ["ANTHROPIC_MODEL"] = resolved_model
    if llm_provider == "github":
        os.environ["GITHUB_TOKEN"] = api_key
        os.environ["GITHUB_MODELS_MODEL"] = resolved_model
    os.environ["DB_MODE"] = resolved_db_mode
    os.environ["DB_PATH"] = resolved_db_path
    os.environ["DATASIGHT_PROJECT_DIR"] = project_dir
    if query_log:
        os.environ["QUERY_LOG_ENABLED"] = "true"
    if confirm_sql:
        os.environ["CONFIRM_SQL"] = "true"
    if explain_sql:
        os.environ["EXPLAIN_SQL"] = "true"
    if no_clarify:
        os.environ["CLARIFY_SQL"] = "false"

    flight_uri = os.getenv("FLIGHT_SQL_URI", "grpc://localhost:31337")

    click.echo(f"datasight v{__version__}")
    click.echo(f"  Model:    {resolved_model}")
    click.echo(
        f"  Database: {resolved_db_mode} — {resolved_db_path if resolved_db_mode == 'local' else flight_uri}"
    )
    click.echo(f"  Project:  {project_dir}")
    click.echo()

    import uvicorn

    os.environ["PORT"] = str(resolved_port)
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

    # Load .env
    env_path = os.path.join(project_dir, ".env")
    if os.path.exists(env_path):
        from dotenv import load_dotenv

        load_dotenv(env_path, override=False)

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

    # Resolve LLM
    from datasight.config import create_sql_runner, load_schema_description
    from datasight.llm import create_llm_client
    from datasight.schema import introspect_schema, format_schema_context

    llm_provider = os.getenv("LLM_PROVIDER", "anthropic")

    if llm_provider == "ollama":
        api_key = "ollama"
        resolved_model = model or os.getenv("OLLAMA_MODEL", "qwen3.5:35b-a3b")
    elif llm_provider == "github":
        api_key = os.getenv("GITHUB_TOKEN", "")
        if not api_key:
            click.echo("Error: GITHUB_TOKEN is not set.", err=True)
            sys.exit(1)
        resolved_model = model or os.getenv("GITHUB_MODELS_MODEL", "gpt-4o")
    else:
        api_key = os.getenv("ANTHROPIC_API_KEY", "")
        if not api_key:
            click.echo("Error: ANTHROPIC_API_KEY is not set.", err=True)
            sys.exit(1)
        resolved_model = model or os.getenv("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")

    db_mode = os.getenv("DB_MODE", "local")
    raw_db_path = os.getenv("DB_PATH", "database.duckdb")
    if db_mode == "local":
        resolved_db_path = (
            str(Path(project_dir) / raw_db_path) if not os.path.isabs(raw_db_path) else raw_db_path
        )
        if not os.path.exists(resolved_db_path):
            click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
            sys.exit(1)
    else:
        resolved_db_path = raw_db_path

    click.echo("datasight verify")
    click.echo(f"  Model:    {resolved_model}")
    click.echo(f"  Database: {db_mode} — {resolved_db_path}")
    click.echo(f"  Queries:  {len(queries)}")
    click.echo()

    async def _run():
        from datasight.config import format_example_queries
        from datasight.prompts import build_system_prompt
        from datasight.verify import run_ambiguity_analysis, run_verification

        base_url = (
            os.getenv("OLLAMA_BASE_URL")
            if llm_provider == "ollama"
            else os.getenv("GITHUB_MODELS_BASE_URL")
            if llm_provider == "github"
            else os.getenv("ANTHROPIC_BASE_URL")
        )
        llm_client = create_llm_client(
            provider=llm_provider,
            api_key=api_key,
            base_url=base_url,
        )
        sql_runner = create_sql_runner(
            db_mode=db_mode,
            db_path=resolved_db_path,
            flight_uri=os.getenv("FLIGHT_SQL_URI", "grpc://localhost:31337"),
            flight_token=os.getenv("FLIGHT_SQL_TOKEN"),
            flight_username=os.getenv("FLIGHT_SQL_USERNAME"),
            flight_password=os.getenv("FLIGHT_SQL_PASSWORD"),
        )

        # Build system prompt
        tables = await introspect_schema(sql_runner.run_sql, runner=sql_runner)
        user_desc = load_schema_description(None, project_dir)
        schema_text = format_schema_context(tables, user_desc)
        schema_text += format_example_queries(queries)

        sys_prompt = build_system_prompt(schema_text, mode="verify")

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
