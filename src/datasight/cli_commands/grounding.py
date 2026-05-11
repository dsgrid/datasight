"""``datasight grounding`` — manage grounding-file drift independently.

Two subcommands:

- ``check``: run the static drift detector against the live database
  and print a report. Same logic as ``datasight verify --static-only``,
  exposed under a more discoverable name.
- ``repair``: run the LLM repair against an existing drift, using the
  schema snapshot persisted by the most recent ``tidy review`` /
  web-UI apply (or a CSV fallback). Supports ``--model`` to retry with
  a different model after a timeout, and ``--dry-run`` to preview the
  diff without writing.

The repair flow is deliberately decoupled from ``tidy review`` apply
so a slow/failed LLM call can be retried any time, with a different
model, without re-running the database transform.
"""

from __future__ import annotations

import asyncio
import csv
import os
import sys
from pathlib import Path
from typing import Any

import duckdb
import rich_click as click

from datasight import cli
from datasight.cli_helpers import format_epilog
from datasight.config import create_sql_runner_from_settings
from datasight.grounding import (
    build_enum_values_sync,
    build_schema_truth_sync,
    check_grounding_drift,
    format_drift_report,
)
from datasight.grounding_repair import (
    format_repair_summary,
    read_snapshot,
    repair_grounding,
    snapshot_path,
    write_repair_atomic,
)


@click.group(
    epilog=format_epilog(
        """
        Examples:

            datasight grounding check
            datasight grounding repair
            datasight grounding repair --model qwen3.6
            datasight grounding repair --from-csv load_data.csv
            datasight grounding repair --dry-run
        """
    )
)
def grounding():
    """Detect and repair drift between grounding files and the live schema.

    Grounding files (``queries.yaml``, ``schema_description.md``,
    ``time_series.yaml``) describe the database to the LLM. When the
    schema changes (typically after ``datasight tidy review``), these
    files fall out of sync and the agent silently hallucinates against
    columns that no longer exist.

    \b
    - ``check`` reports drift without changing anything.
    - ``repair`` asks the configured LLM to rewrite the stale files
      against the current schema, validates each proposed query, and
      writes atomically after you confirm the diff.
    """


@click.command(
    name="check",
    epilog=format_epilog(
        """
        Examples:

            datasight grounding check
            datasight grounding check --project-dir /path/to/project
        """
    ),
)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing .env and grounding files.",
)
def grounding_check(project_dir: str) -> None:
    """Report stale references in grounding files against the live schema.

    Static — no LLM, no query execution. Exits 0 when grounding is
    clean, 1 when drift is detected. Use ``datasight grounding
    repair`` to fix what this command finds.
    """
    project_dir = str(Path(project_dir).resolve())
    settings, _ = cli.resolve_settings(project_dir)
    if settings.database.mode != "duckdb":
        click.echo(
            f"grounding check requires DuckDB; database.mode is {settings.database.mode!r}.",
            err=True,
        )
        sys.exit(2)
    resolved_db_path = cli.resolve_db_path(settings, project_dir)
    if not resolved_db_path or not os.path.exists(resolved_db_path):
        click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
        sys.exit(1)

    conn = duckdb.connect(resolved_db_path, read_only=True)
    try:
        truth = build_schema_truth_sync(conn)
        enums = build_enum_values_sync(conn, truth)
    finally:
        conn.close()

    report = check_grounding_drift(Path(project_dir), truth, enum_values=enums)
    if report.is_clean:
        click.echo("grounding clean: no drift detected.")
        sys.exit(0)
    click.echo(format_drift_report(report), err=True)
    click.echo("", err=True)
    click.echo(
        "Run `datasight grounding repair` to rewrite the affected files.",
        err=True,
    )
    sys.exit(1)


@click.command(
    name="repair",
    epilog=format_epilog(
        """
        Examples:

            datasight grounding repair
            datasight grounding repair --model qwen3.6
            datasight grounding repair --from-csv load_data.csv
            datasight grounding repair --dry-run
        """
    ),
)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing .env and grounding files.",
)
@click.option(
    "--model",
    default=None,
    help=(
        "LLM model name to use for the repair (overrides .env). "
        "Useful for retrying with a different model after a timeout."
    ),
)
@click.option(
    "--from-csv",
    "from_csv",
    type=click.Path(exists=True, dir_okay=False),
    multiple=True,
    help=(
        "Derive the pre-tidy schema from CSV headers when no snapshot "
        "is available. Pass once per source file (e.g. the wide-format "
        "input the apply consumed). Each CSV becomes a single table "
        "named after the file stem. Combinable with the snapshot — "
        "snapshot tables win on conflict."
    ),
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Show drift + LLM proposal + diff, but don't write any files.",
)
def grounding_repair(  # noqa: C901
    project_dir: str,
    model: str | None,
    from_csv: tuple[str, ...],
    dry_run: bool,
) -> None:
    """Run the LLM grounding repair against an existing drift.

    Reads the pre-tidy schema snapshot persisted by the most recent
    apply (``.datasight/grounding_snapshot.json``). When no snapshot
    is on file, ``--from-csv`` lets you supply the wide-form schema
    by pointing at the source CSV(s).

    Shows the unified diff and prompts for confirmation before writing.
    Use ``--dry-run`` to skip the write entirely.
    """
    project_dir = str(Path(project_dir).resolve())
    settings, resolved_model = cli.resolve_settings(project_dir, model)
    if settings.database.mode != "duckdb":
        click.echo(
            f"grounding repair requires DuckDB; database.mode is {settings.database.mode!r}.",
            err=True,
        )
        sys.exit(2)
    resolved_db_path = cli.resolve_db_path(settings, project_dir)
    if not resolved_db_path or not os.path.exists(resolved_db_path):
        click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
        sys.exit(1)

    # Build old_schema from the snapshot, then merge in any --from-csv
    # entries for tables the snapshot doesn't cover. We deliberately
    # let snapshot tables win — the snapshot was captured by the actual
    # apply, the CSV is just a structural fallback.
    old_schema: dict[str, set[str]] = {}
    snapshot = read_snapshot(project_dir)
    if snapshot:
        old_schema.update(snapshot)
        click.echo(f"Loaded snapshot: {snapshot_path(project_dir)}")
    for csv_path in from_csv:
        table_name = Path(csv_path).stem
        cols = _read_csv_header(Path(csv_path))
        if table_name in old_schema:
            click.echo(
                f"  --from-csv {csv_path}: snapshot already covers "
                f"{table_name!r}; using snapshot version."
            )
            continue
        old_schema[table_name] = set(cols)
        click.echo(f"  --from-csv {csv_path}: derived {table_name} ({len(cols)} columns)")

    if not old_schema:
        click.echo(
            "No pre-tidy schema available: snapshot file missing and no "
            "--from-csv passed. Either run `datasight tidy review` first "
            "(which writes the snapshot), or pass --from-csv pointing at "
            "the wide-form source.",
            err=True,
        )
        sys.exit(1)

    conn = duckdb.connect(resolved_db_path, read_only=True)
    try:
        new_schema = build_schema_truth_sync(conn)
        enums = build_enum_values_sync(conn, new_schema)
    finally:
        conn.close()

    drift = check_grounding_drift(Path(project_dir), new_schema, enum_values=enums)
    if drift.is_clean:
        click.echo("grounding clean: no drift detected. Nothing to repair.")
        return

    click.echo("")
    click.echo(format_drift_report(drift))
    click.echo("")

    try:
        cli.validate_settings_for_llm(settings)
    except (click.UsageError, click.ClickException, SystemExit) as exc:
        click.echo(f"No LLM configured to run the repair: {exc}", err=True)
        sys.exit(1)

    click.echo(f"Running repair with model: {resolved_model}")
    try:
        result = asyncio.run(
            _run_repair(project_dir, old_schema, new_schema, drift, settings, resolved_model)
        )
    except Exception as exc:  # noqa: BLE001 — surface to user
        click.echo(f"Repair failed: {exc}", err=True)
        sys.exit(1)

    if not result.any_changes:
        click.echo("LLM proposed no changes.")
        return

    click.echo("")
    click.echo(format_repair_summary(result))
    for f in result.files:
        if not f.changed:
            continue
        click.echo("")
        click.echo(f.unified_diff(), nl=False)

    if not result.overall_ok:
        click.echo("")
        click.echo(
            "Some proposed files failed validation after retries. Skipping write; "
            "edit the files manually using the diff above as a starting point.",
            err=True,
        )
        sys.exit(1)

    if dry_run:
        click.echo("")
        click.echo("--dry-run: no files written.")
        return

    click.echo("")
    if not click.confirm("Apply this diff?", default=False):
        click.echo("Aborted; no files written.")
        return

    written = write_repair_atomic(result, Path(project_dir))
    for p in written:
        click.echo(f"Wrote {p}")


async def _run_repair(
    project_dir: str,
    old_schema: dict[str, set[str]],
    new_schema: dict[str, set[str]],
    drift: Any,
    settings: Any,
    resolved_model: str,
):
    """Wire up the LLM client + SQL runner the repair library needs."""
    llm_client = cli.create_llm_client(
        provider=settings.llm.provider,
        api_key=settings.llm.api_key,
        base_url=settings.llm.base_url,
        timeout=settings.llm.timeout,
        model=resolved_model,
    )
    try:
        sql_runner = create_sql_runner_from_settings(settings.database, project_dir)
        return await repair_grounding(
            Path(project_dir),
            old_schema,
            new_schema,
            drift,
            llm_client=llm_client,
            model=resolved_model,
            run_sql=sql_runner.run_sql,
        )
    finally:
        await llm_client.aclose()


def _read_csv_header(path: Path) -> list[str]:
    """Read the first line of a CSV as the header row.

    Uses :mod:`csv` rather than ``str.split`` so quoted fields with
    embedded commas don't get miscounted. Strips whitespace because
    real-world CSV headers are inconsistently formatted.
    """
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        try:
            row = next(reader)
        except StopIteration:
            msg = f"CSV is empty: {path}"
            raise click.ClickException(msg) from None
    return [c.strip() for c in row if c.strip()]


grounding.add_command(grounding_check)
grounding.add_command(grounding_repair)
