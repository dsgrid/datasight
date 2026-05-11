"""CLI command module."""

import asyncio
import os
import sys
from pathlib import Path

import duckdb
import rich_click as click

from datasight.config import create_sql_runner_from_settings
from datasight.grounding import (
    build_enum_values_sync,
    build_schema_truth_sync,
    check_grounding_drift,
    format_drift_report,
)

from datasight import cli
from datasight.cli_helpers import format_epilog


@click.command(
    epilog=format_epilog(
        """
        Examples:

            datasight verify
            datasight verify --static-only
            datasight verify --queries verification.yaml
            datasight verify --model gpt-4o

        Add expected results to queries.yaml entries:

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
    )
)
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
@click.option(
    "--static-only",
    is_flag=True,
    default=False,
    help=(
        "Run only the cheap schema-drift check (no LLM, no query execution). "
        "Reports unresolved column/table references in queries.yaml, "
        "schema_description.md, and time_series.yaml against the live DB."
    ),
)
@click.option(
    "--skip-grounding-check",
    is_flag=True,
    default=False,
    help="Skip the static drift check that normally runs before the LLM phase.",
)
def verify(project_dir, model, queries_path, static_only, skip_grounding_check):  # noqa: C901
    """Verify LLM-generated SQL against expected results.

    Runs each question from queries.yaml through the full LLM pipeline,
    executes the generated SQL, and compares results against expected values.
    Use this to validate correctness across different models and providers.

    Before the LLM phase, runs a static schema-drift check that flags
    references to columns or tables that no longer exist in the live
    database. ``--static-only`` skips the LLM phase entirely;
    ``--skip-grounding-check`` skips the static check.
    """

    project_dir = str(Path(project_dir).resolve())

    # ``--static-only`` runs *only* the drift check; ``--skip-grounding-check``
    # asks to skip that very check. The combination has no coherent
    # meaning — accepting it silently would either run nothing or fall
    # through to the LLM phase (contradicting ``--static-only``). Reject
    # it up front so the user fixes their invocation.
    if static_only and skip_grounding_check:
        click.echo(
            "Error: --static-only and --skip-grounding-check are mutually "
            "exclusive (one runs the static check; the other skips it).",
            err=True,
        )
        sys.exit(2)

    # Static drift check first. Cheap, no LLM, no async — runs against a
    # direct DuckDB connection. For non-DuckDB backends we skip the
    # static check (information_schema.columns availability varies) and
    # rely on the LLM-phase query execution to surface drift.
    if not skip_grounding_check:
        settings_preflight, _ = cli.resolve_settings(project_dir, model)
        if settings_preflight.database.mode == "duckdb":
            resolved_db_path = cli.resolve_db_path(settings_preflight, project_dir)
            if resolved_db_path and os.path.exists(resolved_db_path):
                conn = duckdb.connect(resolved_db_path, read_only=True)
                try:
                    truth = build_schema_truth_sync(conn)
                    enums = build_enum_values_sync(conn, truth)
                finally:
                    conn.close()
                report = check_grounding_drift(Path(project_dir), truth, enum_values=enums)
                if not report.is_clean:
                    click.echo(format_drift_report(report), err=True)
                    click.echo("", err=True)
                    click.echo(
                        "Drift found. Run `datasight tidy review` to repair, "
                        "or pass `--skip-grounding-check` to proceed anyway.",
                        err=True,
                    )
                    if static_only:
                        sys.exit(1)
                    # Continue into the LLM phase; the user can opt out via
                    # Ctrl-C if they didn't want to burn tokens.
                elif static_only:
                    click.echo("grounding clean: no drift detected.")
                    sys.exit(0)
            elif static_only:
                # --static-only can't run without a live DB to introspect.
                # Fail loudly instead of falling through to the LLM phase,
                # which would contradict the flag's semantics.
                click.echo(
                    f"Error: Database file not found: {resolved_db_path}",
                    err=True,
                )
                sys.exit(1)
        elif static_only:
            click.echo(
                "--static-only requires DuckDB; database.mode is "
                f"{settings_preflight.database.mode!r}.",
                err=True,
            )
            sys.exit(2)

    # Load queries
    from datasight.config import load_example_queries

    queries = load_example_queries(queries_path, project_dir)
    if not queries:
        click.echo("No queries found. Add questions to queries.yaml first.", err=True)
        sys.exit(1)

    # Load settings and validate
    settings, resolved_model = cli.resolve_settings(project_dir, model)
    cli.validate_settings_for_llm(settings)

    resolved_db_path = cli.resolve_db_path(settings, project_dir)
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
        from datasight.schema_links import resolve_schema_description_links
        from datasight.verify import run_ambiguity_analysis, run_verification

        llm_client = cli.create_llm_client(
            provider=settings.llm.provider,
            api_key=settings.llm.api_key,
            base_url=settings.llm.base_url,
            timeout=settings.llm.timeout,
            model=resolved_model,
        )
        # Close the SDK's httpx pool before asyncio.run tears down the
        # event loop. See `cli.run_ask_pipeline` for the rationale.
        try:
            sql_runner = create_sql_runner_from_settings(settings.database, project_dir)

            # Build system prompt
            tables = await introspect_schema(sql_runner.run_sql, runner=sql_runner)
            user_desc = load_schema_description(None, project_dir)
            user_desc = await resolve_schema_description_links(user_desc)
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
        finally:
            await llm_client.aclose()

    results, ambiguity_results = asyncio.run(_run())

    # Print results
    from rich import box
    from rich.console import Console
    from rich.table import Table
    from rich.text import Text

    console = Console()

    # --- Ambiguity warnings ---
    ambiguous_count = sum(1 for a in ambiguity_results if a.is_ambiguous)
    if ambiguous_count:
        amb_table = Table(
            show_lines=True,
            box=box.ROUNDED,
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
    table = Table(show_lines=True, box=box.ROUNDED, title="Verification Results")
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
