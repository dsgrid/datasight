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
def run(port, host, db_mode, db_path, model, project_dir, verbose, query_log):
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
    else:
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            click.echo(
                "Error: ANTHROPIC_API_KEY is not set. Add it to .env or your environment.",
                err=True,
            )
            sys.exit(1)
        resolved_model = model or os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")
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
    os.environ.setdefault("ANTHROPIC_API_KEY", api_key)
    os.environ["ANTHROPIC_MODEL"] = resolved_model
    os.environ["DB_MODE"] = resolved_db_mode
    os.environ["DB_PATH"] = resolved_db_path
    os.environ["DATASIGHT_PROJECT_DIR"] = project_dir
    if query_log:
        os.environ["QUERY_LOG_ENABLED"] = "true"

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
