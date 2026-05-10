"""CLI command module."""

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

import duckdb
import rich_click as click
from rich.console import Console

from datasight import cli
from datasight.cli_helpers import format_epilog
from datasight.data_profile import find_table_info
from datasight.explore import create_files_session_for_settings
from datasight.schema import introspect_schema
from datasight.tidy import _detect_period_groups, analyze_tidy_patterns
from datasight.tidy_llm import propose_reshapes
from datasight.tidy_review import (
    apply_proposal,
    dump_plan,
    load_plan,
    resolve_source_disposition,
    update_schema_yaml_for_apply,
    validate_against_schema,
)


def _project_scope_options(func):
    """Add the --project-dir and --table options shared by all tidy subcommands."""
    func = click.option(
        "--table",
        "source_table",
        default=None,
        help="Scope tidy detection to a specific source table.",
    )(func)
    func = click.option(
        "--project-dir",
        type=click.Path(exists=True),
        default=".",
        help="Project directory containing .env and config files.",
    )(func)
    return func


async def _gather_tidy_data(project_dir: str, source_table: str | None, settings):
    sql_runner, schema_info = await cli.load_schema_info_for_project(project_dir, settings)
    try:
        if source_table:
            table_info = find_table_info(schema_info, source_table)
            if table_info is None:
                raise click.ClickException(f"Table not found: {source_table}")
            schema_info = [table_info]
        data = analyze_tidy_patterns(schema_info)
        suggestions_by_table = {t["name"]: _detect_period_groups(t) for t in schema_info}
        return data, suggestions_by_table
    finally:
        sql_runner.close()


async def _gather_tidy_data_for_files(files: tuple[str, ...]):
    """Like ``_gather_tidy_data`` but registers ``files`` in an ephemeral DuckDB.

    No project / .env required; mirrors the file-mode entry point used by
    ``datasight inspect``. The returned ``suggestions_by_table`` is empty
    on this path because ephemeral file sessions can't usefully persist a
    reshape — callers should treat file mode as suggest-only.
    """
    runner, _tables_info = create_files_session_for_settings(list(files), None)
    try:
        tables = await introspect_schema(runner.run_sql, runner=runner)
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
        return analyze_tidy_patterns(schema_info)
    finally:
        runner.close()


def _resolve_tidy_settings(project_dir: str) -> tuple[Any, str, str]:
    project_dir = str(Path(project_dir).resolve())
    settings, _ = cli.resolve_settings(project_dir)
    resolved_db_path = cli.resolve_db_path(settings, project_dir)
    if settings.database.mode in ("duckdb", "sqlite") and not os.path.exists(resolved_db_path):
        click.echo(f"Error: Database file not found: {resolved_db_path}", err=True)
        sys.exit(1)
    return settings, project_dir, resolved_db_path


def _apply_reshapes(
    suggestions_by_table: dict[str, list[Any]],
    mode: str,
    dry_run: bool,
    resolved_db_path: str,
) -> list[dict[str, Any]]:
    """Apply (or preview) tidy suggestions and return a per-statement audit log."""
    ddl_statements: list[tuple[str, Any]] = [
        (source_table, suggestion)
        for source_table, suggestions in suggestions_by_table.items()
        for suggestion in suggestions
    ]
    applied: list[dict[str, Any]] = []
    if not ddl_statements:
        click.echo("No untidy column-shape patterns detected.")
        return applied

    if dry_run:
        for source_table, suggestion in ddl_statements:
            click.echo(f"-- Would apply for {source_table}:")
            click.echo(suggestion.build_sql(mode))
    else:
        conn = duckdb.connect(resolved_db_path)
        try:
            for source_table, suggestion in ddl_statements:
                conn.execute(suggestion.build_sql(mode))
                click.echo(
                    f"Created {mode} {suggestion.target_object_name!r} "
                    f"from {source_table} ({len(suggestion.affected_columns)} columns)"
                )
        finally:
            conn.close()

    for source_table, suggestion in ddl_statements:
        applied.append(
            {
                "table": source_table,
                "target_object_name": suggestion.target_object_name,
                "object_type": mode,
                "affected_columns": suggestion.affected_columns,
                "dry_run": dry_run,
            }
        )
    return applied


@click.group(
    epilog=format_epilog(
        """
        Examples:

            datasight tidy suggest
            datasight tidy suggest --table sales_wide
            datasight tidy view --dry-run
            datasight tidy view
            datasight tidy table --table sales_wide
            datasight tidy review --from plan.json --apply-all
        """
    )
)
def tidy():
    """Detect untidy column shapes and reshape into long form.

    Two paths:

    \b
    - Deterministic: 'tidy suggest' lists candidates, 'tidy view' creates
      long-form views, 'tidy table' materializes long-form tables. These
      run on column-name pattern matching and never call an LLM.
    - LLM-augmented: 'tidy review' adds an advisor that proposes pivots
      the regex misses (fuel-type-as-column, geography-as-column,
      multi-axis pivots) for the developer to approve before applying.
    """


@click.command(
    name="suggest",
    epilog=format_epilog(
        """
        Examples:

            datasight tidy suggest                           # current project
            datasight tidy suggest monthly_generation.csv    # standalone file
            datasight tidy suggest gen.csv plants.parquet    # multiple files
            datasight tidy suggest --table sales_wide
            datasight tidy suggest --format markdown -o tidy.md
        """
    ),
)
@click.argument("files", nargs=-1, required=False, type=click.Path(exists=True))
@_project_scope_options
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
    help="Write the tidy listing to a file instead of stdout.",
)
def tidy_suggest(files, project_dir, source_table, output_format, output_path):
    """List detected untidy column shapes without changing the database.

    Pass one or more CSV / Parquet / Excel / DuckDB files as positional
    arguments to inspect them in an ephemeral session — no project setup
    required. With no files, runs against the current project's database.
    Detection is deterministic: column names plus dtypes plus row counts.
    No LLM is involved. For pivots the regex misses, see 'tidy review'.
    """
    if files:
        if source_table:
            raise click.UsageError(
                "--table cannot be combined with positional FILES; "
                "scope is implicit when files are passed."
            )
        tidy_data = asyncio.run(_gather_tidy_data_for_files(files))
    else:
        settings, project_dir, _ = _resolve_tidy_settings(project_dir)
        tidy_data, _ = asyncio.run(_gather_tidy_data(project_dir, source_table, settings))

    if output_format == "json":
        cli.write_or_print(json.dumps(tidy_data, indent=2), output_path)
        return

    if output_format == "markdown":
        cli.write_or_print(cli.render_tidy_markdown(tidy_data), output_path)
        return

    console = Console(record=bool(output_path))
    console.print(
        cli.build_metric_table(
            "Tidy Reshape Suggestions",
            [
                ("Tables scanned", str(tidy_data["table_count"])),
                ("Suggestions", str(len(tidy_data.get("suggestions") or []))),
            ],
        )
    )
    suggestions = tidy_data.get("suggestions") or []
    if suggestions:
        console.print(
            cli.build_profile_detail_table(
                "Suggestions",
                [
                    ("Source", "left"),
                    ("Target", "left"),
                    ("Pattern", "left"),
                    ("Dimensions", "left"),
                    ("Columns", "right"),
                    ("Rationale", "left"),
                ],
                [
                    [
                        item["table"],
                        item["target_object_name"],
                        item["pattern"],
                        ", ".join(d["name"] for d in item["dimensions"]),
                        str(len(item["column_mappings"])),
                        item["rationale"],
                    ]
                    for item in suggestions
                ],
            )
        )
    wide_tables = tidy_data.get("wide_tables") or []
    if wide_tables:
        console.print(
            cli.build_profile_detail_table(
                "Wide Tables",
                [("Table", "left"), ("Columns", "right"), ("Rows", "right"), ("Reason", "left")],
                [
                    [
                        item["table"],
                        str(item["column_count"]),
                        str(item.get("row_count") if item.get("row_count") is not None else "?"),
                        item["reason"],
                    ]
                    for item in wide_tables
                ],
            )
        )
    if not suggestions and not wide_tables:
        console.print("[dim]No untidy column-shape patterns detected.[/dim]")
    if output_path:
        cli.write_or_print(console.export_text(), output_path)


@click.command(
    name="view",
    epilog=format_epilog(
        """
        Examples:

            datasight tidy view
            datasight tidy view --dry-run
            datasight tidy view --table sales_wide
        """
    ),
)
@_project_scope_options
@click.option(
    "--dry-run",
    "dry_run",
    is_flag=True,
    default=False,
    help="Print the DDL without executing it.",
)
def tidy_view(project_dir, source_table, dry_run):
    """Create CREATE OR REPLACE VIEW <table>_long for each detected pattern.

    Deterministic — applies the regex detector's hits without consulting
    an LLM. For LLM-augmented proposals (fuel-type-as-column, multi-axis
    pivots), use 'tidy review'.
    """
    settings, project_dir, resolved_db_path = _resolve_tidy_settings(project_dir)
    if settings.database.mode != "duckdb":
        raise click.UsageError(
            "tidy view requires DuckDB; the apply path opens a writable DuckDB connection."
        )
    _, suggestions_by_table = asyncio.run(_gather_tidy_data(project_dir, source_table, settings))
    _apply_reshapes(suggestions_by_table, "view", dry_run, resolved_db_path)


@click.command(
    name="table",
    epilog=format_epilog(
        """
        Examples:

            datasight tidy table
            datasight tidy table --dry-run
            datasight tidy table --table sales_wide
        """
    ),
)
@_project_scope_options
@click.option(
    "--dry-run",
    "dry_run",
    is_flag=True,
    default=False,
    help="Print the DDL without executing it.",
)
def tidy_table(project_dir, source_table, dry_run):
    """Materialize CREATE OR REPLACE TABLE <table>_long for each detected pattern.

    Deterministic — applies the regex detector's hits without consulting
    an LLM. For LLM-augmented proposals (fuel-type-as-column, multi-axis
    pivots), use 'tidy review'.
    """
    settings, project_dir, resolved_db_path = _resolve_tidy_settings(project_dir)
    if settings.database.mode != "duckdb":
        raise click.UsageError(
            "tidy table requires DuckDB; the apply path opens a writable DuckDB connection."
        )
    _, suggestions_by_table = asyncio.run(_gather_tidy_data(project_dir, source_table, settings))
    _apply_reshapes(suggestions_by_table, "table", dry_run, resolved_db_path)


@click.command(
    name="review",
    epilog=format_epilog(
        """
        Examples:

            datasight tidy review --from plan.json --apply-all
            datasight tidy review --from plan.json --dry-run
            datasight tidy review --out detector.json
            datasight tidy review --from plan.json --apply-all --drop-source
            datasight tidy review --from plan.json --apply-all --replace-source
            datasight tidy review --from plan.json --apply-all --rename-source sales_wide_raw
        """
    ),
)
@_project_scope_options
@click.option(
    "--from",
    "plan_path",
    type=click.Path(exists=True),
    default=None,
    help="Load proposals from a JSON plan file (no LLM call).",
)
@click.option(
    "--out",
    "out_path",
    type=click.Path(),
    default=None,
    help=(
        "Write proposals to a JSON plan file instead of applying. "
        "Without --from, writes the deterministic detector's hits."
    ),
)
@click.option(
    "--apply-all",
    "apply_all",
    is_flag=True,
    default=False,
    help="Apply every proposal without prompting. Required for non-interactive use.",
)
@click.option(
    "--dry-run",
    "dry_run",
    is_flag=True,
    default=False,
    help="Print DDL and proposed dispositions without changing the database.",
)
@click.option(
    "--as",
    "as_mode",
    type=click.Choice(["table", "view"]),
    default="view",
    help="Materialize the long form as a table or view (default: view).",
)
@click.option(
    "--keep-source",
    "keep_source",
    is_flag=True,
    default=False,
    help="Leave the source object (table/view) unchanged after the reshape (default).",
)
@click.option(
    "--rename-source",
    "rename_source",
    default=None,
    metavar="NAME",
    help=(
        "Rename the source object (table/view) to NAME after a successful "
        "reshape. Requires '--as table' — a view's body references its source by name."
    ),
)
@click.option(
    "--replace-source",
    "replace_source",
    is_flag=True,
    default=False,
    help=(
        "Drop the source after a successful reshape and rename the long-form "
        "table to take the source's old name. Downstream code that referenced "
        "the source keeps working without edits. Requires '--as table' — a "
        "view's body references its source by name."
    ),
)
@click.option(
    "--drop-source",
    "drop_source",
    is_flag=True,
    default=False,
    help=(
        "Drop the source after a successful reshape; the long form keeps its "
        "target name. Pick this when the new shape is the canonical one going "
        "forward and you don't need to preserve the source's name. Requires "
        "'--as table'. NOTE: previously this flag carried the semantics now "
        "moved to '--replace-source'; scripts depending on the old behavior "
        "should switch to '--replace-source'."
    ),
)
@click.option(
    "--sample",
    "sample_rows",
    type=int,
    default=0,
    help=(
        "Send N sample rows per candidate to the configured LLM provider "
        "(default 0). Sample values get sent over the network — opt in only "
        "when the LLM seeing the values is acceptable."
    ),
)
def tidy_review(
    project_dir,
    source_table,
    plan_path,
    out_path,
    apply_all,
    dry_run,
    as_mode,
    keep_source,
    rename_source,
    replace_source,
    drop_source,
    sample_rows,
):
    """LLM-augmented advisor that proposes reshapes for the developer to review.

    Runs the deterministic detector first, then asks the configured LLM
    provider for additional candidates the regex misses (fuel-type-as-column,
    geography-as-column, scenario-as-column, multi-axis pivots). The
    developer approves each candidate before it is applied.

    Use ``--from PLAN`` to skip the LLM call and apply a pre-built plan.
    Use ``--out PLAN`` to write proposals to a file instead of applying;
    without ``--from`` this writes the deterministic detector's hits, giving
    a starting point that can be hand-edited and fed back via ``--from``.

    Calls the configured LLM provider whenever ``--from`` is not set.
    """
    try:
        disposition = resolve_source_disposition(
            keep_source, rename_source, replace_source, drop_source
        )
    except ValueError as exc:
        raise click.UsageError(str(exc)) from exc

    # ``--as view`` is only safe with ``--keep-source``: a view's body
    # references its source by name, so renaming/replacing/dropping the
    # source leaves the view dangling — and for ``replace`` (which also
    # renames the long form into the source's slot), DuckDB rejects
    # subsequent SELECTs with "infinite recursion detected". Catch the
    # bad combo before any work runs, including before the LLM call when
    # ``--from`` is omitted.
    if as_mode == "view" and disposition.mode != "keep":
        flag = {
            "rename": "--rename-source",
            "replace": "--replace-source",
            "drop": "--drop-source",
        }[disposition.mode]
        gerund = {
            "rename": "renaming",
            "replace": "replacing",
            "drop": "dropping",
        }[disposition.mode]
        consequence = (
            "recursively self-referencing."
            if disposition.mode == "replace"
            else "pointing at a missing object."
        )
        raise click.UsageError(
            f"{flag} requires '--as table'. A view references "
            f"its source by name, so {gerund} the source would leave "
            f"the long-form view {consequence}"
        )

    settings, project_dir, resolved_db_path = _resolve_tidy_settings(project_dir)
    if settings.database.mode != "duckdb":
        raise click.UsageError(
            "tidy review requires DuckDB; the apply path opens a writable DuckDB connection."
        )

    # Load suggestions. Three sources, in priority order:
    #   1. --from PLAN  : load that plan and skip the LLM entirely.
    #   2. --out PLAN   : when paired with no --from, this is the bootstrap
    #                     case — dump the deterministic detector's hits so
    #                     the developer can hand-edit and feed back later.
    #   3. (default)    : call the configured LLM provider for proposals.
    #                     Stubbed until the prompt loop lands.
    if plan_path is not None:
        plan = load_plan(Path(plan_path))
        suggestions = list(plan.proposals)
    elif out_path is not None:
        _, suggestions_by_table = asyncio.run(
            _gather_tidy_data(project_dir, source_table, settings)
        )
        suggestions = [s for sugs in suggestions_by_table.values() for s in sugs]
    else:
        suggestions = _propose_via_llm(project_dir, settings, source_table, sample_rows)

    if source_table:
        suggestions = [s for s in suggestions if s.table == source_table]

    # --out: write proposals to disk and exit, no apply.
    if out_path:
        dump_plan(suggestions, Path(out_path))
        click.echo(f"Wrote {len(suggestions)} proposal(s) to {out_path}")
        return

    if not suggestions:
        click.echo("No proposals to apply.")
        return

    # Cross-check every suggestion against the live schema before touching
    # anything. Stale plans (renamed tables, dropped columns) get caught
    # here with a clear error rather than mid-apply. Close the read
    # connection before the apply step opens a writable one — DuckDB
    # rejects multiple connections to the same file with conflicting
    # configurations.
    async def _load_schema():
        sql_runner, schema_info = await cli.load_schema_info_for_project(project_dir, settings)
        try:
            return list(schema_info)
        finally:
            sql_runner.close()

    schema_info = asyncio.run(_load_schema())
    invalid: list[tuple[str, list[str]]] = []
    valid: list = []
    for s in suggestions:
        problems = validate_against_schema(s, schema_info)
        if problems:
            invalid.append((s.table, problems))
        else:
            valid.append(s)
    if invalid:
        for table_name, problems in invalid:
            click.echo(f"Skipping proposal for {table_name}:", err=True)
            for problem in problems:
                click.echo(f"  - {problem}", err=True)

    if not valid:
        raise click.ClickException("No valid proposals after schema cross-check.")

    # `--apply-all` is the non-interactive code path used by tests and
    # scripted prep. Without it, walk the developer through each proposal
    # one by one and only act on the approved ones.
    if apply_all:
        approved = valid
    else:
        approved = _interactive_review(valid, disposition, as_mode)
        if not approved:
            click.echo("No proposals approved.")
            return

    _apply_review_proposals(approved, disposition, as_mode, dry_run, resolved_db_path, project_dir)


def _propose_via_llm(
    project_dir: str, settings: Any, source_table: str | None, sample_rows: int
) -> list:
    """Call the configured LLM provider for tidy-reshape proposals.

    Loads the schema once, runs the deterministic detector against it (so
    the LLM sees those hits as "already covered"), optionally fetches N
    sample rows per table, then asks the model for additional candidates
    via the ``propose_reshapes`` tool. The returned list combines the
    deterministic hits (always single-pivot, always ``source='deterministic'``)
    and the LLM's survivors (``source='llm'``) so the developer reviews
    both in one loop.
    """

    async def _gather():
        sql_runner, schema_info = await cli.load_schema_info_for_project(project_dir, settings)
        try:
            schema_list = list(schema_info)
            tidy_data = analyze_tidy_patterns(schema_list)
            deterministic_hits = tidy_data["suggestions"]
            samples: dict[str, list[dict[str, Any]]] = {}
            if sample_rows > 0:
                limit = int(sample_rows)
                for tbl in schema_list:
                    name = tbl["name"]
                    # Quote with double-quotes — the same convention used
                    # by the rest of the DuckDB-targeted code paths.
                    df = await sql_runner.run_sql(f'SELECT * FROM "{name}" LIMIT {limit}')
                    samples[name] = df.to_dict(orient="records")
            return schema_list, deterministic_hits, samples
        finally:
            sql_runner.close()

    schema_info, deterministic_hits, samples = asyncio.run(_gather())

    llm_client = cli.create_llm_client(
        provider=settings.llm.provider,
        api_key=settings.llm.api_key,
        base_url=settings.llm.base_url,
        timeout=settings.llm.timeout,
        model=settings.llm.model,
    )

    async def _call():
        return await propose_reshapes(
            llm_client,
            model=settings.llm.model,
            schema_info=schema_info,
            deterministic_hits=deterministic_hits,
            samples=samples or None,
        )

    result = asyncio.run(_call())
    for warning in result.parse_warnings:
        click.echo(f"warn: {warning}", err=True)

    # Combine deterministic + LLM proposals so the developer reviews
    # everything in one loop. Deterministic ones come first because they
    # are higher-confidence and structurally simpler.
    deterministic_suggestions = [s for tbl in schema_info for s in _detect_period_groups(tbl)]
    if source_table:
        deterministic_suggestions = [
            s for s in deterministic_suggestions if s.table == source_table
        ]
    return deterministic_suggestions + result.suggestions


def _interactive_review(
    suggestions: list,
    disposition: Any,
    mode: str,
) -> list:
    """Walk the developer through each proposal one at a time.

    Per-proposal menu:

      [a]pply / [s]kip / [e]dit / [q]uit

    ``edit`` lets the developer rename the target object, the value column,
    or trim the id columns. Renaming a dimension or remapping a column is
    deliberately not editable inline — those changes warrant editing the
    plan file directly via ``--out``.
    """
    approved: list = []
    total = len(suggestions)
    for index, suggestion in enumerate(suggestions, start=1):
        click.echo("")
        _render_proposal_summary(suggestion, index, total, disposition, mode)
        while True:
            choice = (
                click.prompt(
                    "  [a]pply / [s]kip / [e]dit / [q]uit",
                    default="s",
                    show_default=False,
                )
                .strip()
                .lower()
            )
            if choice in ("a", "apply"):
                approved.append(suggestion)
                break
            if choice in ("s", "skip", ""):
                click.echo("  Skipped.")
                break
            if choice in ("e", "edit"):
                _edit_proposal_inline(suggestion)
                # Re-render after the edit so the developer sees the new state
                # before deciding apply vs. skip.
                _render_proposal_summary(suggestion, index, total, disposition, mode)
                continue
            if choice in ("q", "quit"):
                click.echo(f"  Stopped at proposal {index} of {total}.")
                return approved
            click.echo("  Unknown choice; pick one of a / s / e / q.")
    return approved


def _render_proposal_summary(
    suggestion: Any, index: int, total: int, disposition: Any, mode: str
) -> None:
    dim_label = ", ".join(f"{d.name} ({d.kind})" for d in suggestion.dimensions)
    mapped_label = ", ".join(m.column for m in suggestion.column_mappings)
    if len(mapped_label) > 80:
        mapped_label = mapped_label[:77] + "..."
    if disposition.mode == "rename":
        disp_text = f"rename source -> {disposition.new_name}"
    elif disposition.mode == "drop":
        disp_text = f"drop source; long form takes the name {suggestion.table!r}"
    else:
        disp_text = "keep source"
    click.echo(
        f"Proposal {index} of {total} — {suggestion.table} -> {suggestion.target_object_name}"
    )
    click.echo(
        f"  Source: {suggestion.source}   Confidence: {suggestion.confidence}   Mode: {mode}"
    )
    click.echo(f"  Dimensions: {dim_label}")
    click.echo(f"  Mapped ({len(suggestion.column_mappings)}): {mapped_label}")
    click.echo(f"  Id columns: {', '.join(suggestion.id_columns) or '<none>'}")
    click.echo(f"  Value column: {suggestion.value_column}")
    click.echo(f"  Source disposition: {disp_text}")
    if suggestion.rationale:
        click.echo(f"  Rationale: {suggestion.rationale}")


def _edit_proposal_inline(suggestion: Any) -> None:
    """Allow the developer to rename target / value column / id columns.

    Mutates the suggestion in place. Dimensions and column_mappings are
    deliberately not editable — those are best handled by writing the plan
    file via ``--out``, editing it, and re-running with ``--from``.
    """
    while True:
        click.echo("  Edit which field?")
        click.echo(f"    1) target_object_name ({suggestion.target_object_name})")
        click.echo(f"    2) value_column ({suggestion.value_column})")
        click.echo(f"    3) id_columns ({', '.join(suggestion.id_columns) or '<none>'})")
        click.echo("    [b]ack")
        choice = click.prompt("  >", default="b", show_default=False).strip().lower()
        if choice in ("b", "back", ""):
            # Rebuild the SQL from the (possibly mutated) fields so it stays
            # in sync with subsequent renders and the apply step.
            suggestion.reshape_sql = suggestion.build_sql("table")
            return
        if choice == "1":
            new = click.prompt(
                "  New target_object_name",
                default=suggestion.target_object_name,
                show_default=True,
            ).strip()
            if new:
                suggestion.target_object_name = new
        elif choice == "2":
            new = click.prompt(
                "  New value_column",
                default=suggestion.value_column,
                show_default=True,
            ).strip()
            if new:
                suggestion.value_column = new
        elif choice == "3":
            new = click.prompt(
                "  New id_columns (comma-separated)",
                default=", ".join(suggestion.id_columns),
                show_default=True,
            ).strip()
            suggestion.id_columns = [c.strip() for c in new.split(",") if c.strip()]
        else:
            click.echo("  Unknown choice; pick 1, 2, 3, or b.")


def _apply_review_proposals(
    suggestions: list,
    disposition: Any,
    mode: str,
    dry_run: bool,
    resolved_db_path: str,
    project_dir: str,
) -> None:
    """Walk a validated list of suggestions through the apply pipeline.

    One DuckDB connection is opened for the whole batch. Each proposal runs
    inside its own transaction (``apply_proposal`` handles BEGIN/COMMIT) so
    a mid-batch failure leaves prior successes intact and rolls back only
    the failing one. After each successful apply, ``schema.yaml`` is synced
    so the long-form table stays visible (or, with ``--drop-source``, the
    existing entry's column filter is cleared since the shape changed).
    """
    audit: list[dict[str, Any]] = []
    conn = duckdb.connect(resolved_db_path)
    try:
        for suggestion in suggestions:
            if dry_run:
                click.echo(f"-- Would apply for {suggestion.table}:")
                click.echo(suggestion.build_sql(mode))
                click.echo(
                    f"-- Source disposition: {disposition.mode}"
                    + (f" -> {disposition.new_name}" if disposition.mode == "rename" else "")
                )
            try:
                result = apply_proposal(
                    conn,
                    suggestion,
                    mode=mode,
                    source_disposition=disposition,
                    dry_run=dry_run,
                )
            except Exception as exc:
                click.echo(f"Failed to apply {suggestion.target_object_name}: {exc}", err=True)
                continue
            if not dry_run:
                disp_suffix = ""
                if result.source_disposition == "rename":
                    disp_suffix = f"; renamed source to {result.source_renamed_to}"
                elif result.source_disposition == "drop":
                    disp_suffix = (
                        f"; dropped source and renamed long form to {result.final_target_name!r}"
                    )
                click.echo(
                    f"Created {mode} {result.final_target_name!r} from "
                    f"{result.table} ({len(result.affected_columns)} columns, "
                    f"{result.row_count_target} rows){disp_suffix}"
                )
                # Keep schema.yaml in sync so the long form remains visible
                # in `datasight run` and `datasight ask` after this run.
                try:
                    rewrote = update_schema_yaml_for_apply(
                        project_dir,
                        source_table=suggestion.table,
                        target_table=suggestion.target_object_name,
                        disposition_mode=result.source_disposition,
                        rename_to=result.source_renamed_to,
                    )
                    if rewrote:
                        click.echo(f"Updated {Path(project_dir) / 'schema.yaml'}")
                except OSError as exc:
                    click.echo(f"warn: schema.yaml not updated: {exc}", err=True)
            audit.append(result.to_dict())
    finally:
        conn.close()

    if not dry_run:
        click.echo(f"\nApplied {len(audit)} proposal(s).")


tidy.add_command(tidy_suggest)
tidy.add_command(tidy_view)
tidy.add_command(tidy_table)
tidy.add_command(tidy_review)
