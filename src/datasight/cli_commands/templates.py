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


_PROJECT_DIR_OPT = click.option(
    "--project-dir",
    "project_dir",
    type=click.Path(exists=True, file_okay=False),
    default=".",
    help="Project directory containing .datasight/templates/ (default: cwd).",
)


@click.group(
    epilog=_epilog(
        """
        Examples:

            datasight templates save generation-dashboard
            datasight templates list
            datasight templates apply generation-dashboard --output out.html
        """
    )
)
def templates():
    """Save and re-apply dashboards as templates across datasets.

    Templates capture dashboard cards from the web UI so the same SQL and
    charts can be applied to another dataset with matching tables.
    """


def _load_project_dashboard(project_dir: str) -> dict[str, Any]:
    path = Path(project_dir).resolve() / ".datasight" / "dashboard.json"
    if not path.exists():
        raise click.ClickException(
            f"No dashboard found at {path}. Build a dashboard in the web UI first."
        )
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as err:
        raise click.ClickException(f"Dashboard JSON is invalid: {err}") from err


def _resolve_project_duckdb(project_dir: str) -> Path | None:
    """Return the project's DuckDB path if it has one configured, else None.

    Loads the project's `.env` (without polluting the global env), inspects
    DB_MODE/DB_PATH, and returns an absolute path when the project uses
    DuckDB. Returns None for non-DuckDB backends or when nothing is set —
    callers are then responsible for supplying every required table via
    --table.
    """
    from dotenv import dotenv_values

    from datasight.config import normalize_db_mode

    proj = Path(project_dir).resolve()
    env_file = proj / ".env"
    values = dotenv_values(env_file) if env_file.exists() else {}
    # Match datasight's own default: empty/missing DB_MODE means duckdb.
    mode = normalize_db_mode((values.get("DB_MODE") or "").strip() or "duckdb")
    if mode != "duckdb":
        return None
    path = (values.get("DB_PATH") or "").strip()
    if not path:
        # Fall back to the conventional location so `datasight generate`'s
        # default output is picked up even if DB_PATH wasn't written.
        fallback = proj / "database.duckdb"
        return fallback if fallback.exists() else None
    db_path = Path(path)
    if not db_path.is_absolute():
        db_path = proj / db_path
    db_path = db_path.resolve()
    return db_path if db_path.exists() else None


@click.command(
    name="save",
    epilog=_epilog(
        """
        Examples:

            datasight templates save generation-dashboard
            datasight templates save generation-dashboard --description "Monthly generation cards"
            datasight templates save generation-dashboard --table generation_fuel --overwrite
            datasight templates save by-scenario --var SCENARIO=reference
        """
    ),
)
@click.argument("name")
@_PROJECT_DIR_OPT
@click.option("--description", default=None, help="Template description.")
@click.option(
    "--table",
    "required_tables",
    multiple=True,
    help=(
        "Table the template requires. Repeat once per table. "
        "When omitted, tables are inferred from each card's SQL."
    ),
)
@click.option(
    "--var",
    "variables",
    multiple=True,
    help=(
        "Declare a template variable: --var NAME=VALUE. Every occurrence "
        "of VALUE in each card's SQL is rewritten to {{NAME}}, and NAME "
        "becomes a placeholder that must be resolved at apply time."
    ),
)
@click.option(
    "--var-from-filename",
    "variable_regexes",
    multiple=True,
    help=(
        "Attach a filename-extraction regex to a variable: "
        "--var-from-filename NAME=REGEX. At apply time the regex is run "
        "against each input parquet's filename and its first capture group "
        "(or whole match) becomes the variable value. Use with --var to "
        "also set the save-time literal and default."
    ),
)
@click.option("--overwrite", is_flag=True, help="Replace an existing template.")
def template_save(
    name: str,
    project_dir: str,
    description: str | None,
    required_tables: tuple[str, ...],
    variables: tuple[str, ...],
    variable_regexes: tuple[str, ...],
    overwrite: bool,
):
    """Save the current project dashboard as a reusable template.

    The dashboard must already exist in the project, usually from building
    and saving cards in the web UI.
    """
    from datasight.dashboard_template import (
        TemplateError,
        build_template,
        save_template,
    )

    var_defs: dict[str, dict[str, str]] = {}
    for raw in variables:
        if "=" not in raw:
            raise click.ClickException(f"Invalid --var value {raw!r}. Expected NAME=VALUE.")
        key, _, value = raw.partition("=")
        key = key.strip()
        value = value.strip()
        if not key or not value:
            raise click.ClickException(f"Invalid --var value {raw!r}. Expected NAME=VALUE.")
        var_defs[key] = {"name": key, "default": value}
    for raw in variable_regexes:
        if "=" not in raw:
            raise click.ClickException(
                f"Invalid --var-from-filename value {raw!r}. Expected NAME=REGEX."
            )
        key, _, regex = raw.partition("=")
        key = key.strip()
        if not key or not regex:
            raise click.ClickException(
                f"Invalid --var-from-filename value {raw!r}. Expected NAME=REGEX."
            )
        var_defs.setdefault(key, {"name": key, "default": ""})["from_filename"] = regex

    dashboard = _load_project_dashboard(project_dir)
    try:
        template_obj = build_template(
            name,
            dashboard,
            required_tables=list(required_tables) or None,
            description=description,
            variables=list(var_defs.values()) or None,
        )
        path = save_template(template_obj, project_dir, overwrite=overwrite)
    except TemplateError as err:
        raise click.ClickException(str(err)) from err

    click.echo(f"Saved template {name!r} to {path}")
    click.echo(f"  required_tables: {', '.join(template_obj['required_tables'])}")
    click.echo(f"  cards: {len(template_obj['items'])}")
    if template_obj.get("variables"):
        names = ", ".join(v["name"] for v in template_obj["variables"])
        click.echo(f"  variables: {names}")


@click.command(
    name="list",
    epilog=_epilog(
        """
        Example:

            datasight templates list
        """
    ),
)
@_PROJECT_DIR_OPT
def template_list(project_dir: str):
    """List dashboard templates saved in this project."""
    from rich import box
    from rich.console import Console
    from rich.table import Table

    from datasight.dashboard_template import list_templates, project_template_dir

    directory = project_template_dir(project_dir)
    entries = list_templates(project_dir)
    if not entries:
        click.echo(f"No templates in {directory}.")
        return

    table = Table(box=box.ROUNDED)
    table.add_column("Name", no_wrap=True)
    table.add_column("Required tables", overflow="fold")
    table.add_column("Cards", justify="right", no_wrap=True)
    table.add_column("Description", overflow="fold")

    for entry in entries:
        table.add_row(
            entry["name"],
            ", ".join(entry["required_tables"]) or "[dim]—[/dim]",
            str(entry["cards"]),
            entry["description"] or "[dim]—[/dim]",
        )

    Console().print(table)
    click.echo(f"\n{len(entries)} template(s) in {directory}")


@click.command(
    name="show",
    epilog=_epilog(
        """
        Example:

            datasight templates show generation-dashboard
        """
    ),
)
@click.argument("name")
@_PROJECT_DIR_OPT
def template_show(name: str, project_dir: str):
    """Print a saved template as JSON."""
    from datasight.dashboard_template import TemplateError, load_template

    try:
        data = load_template(name, project_dir)
    except TemplateError as err:
        raise click.ClickException(str(err)) from err
    click.echo(json.dumps(data, indent=2))


@click.command(
    name="apply",
    epilog=_epilog(
        """
        Examples:

            # Render once, mapping one required table to a parquet file
            datasight templates apply generation-by-fuel \\
                --table generation_fuel=data/generation.parquet \\
                --output generation.html

            # Render once per matching parquet, writing one HTML per file
            datasight templates apply generation-by-fuel \\
                --table 'generation_fuel=data/*.parquet' \\
                --export-dir out/
        """
    ),
)
@click.argument("name")
@_PROJECT_DIR_OPT
@click.option(
    "--table",
    "table_mappings",
    multiple=True,
    help=(
        "Map a required table to a parquet file: --table NAME=PATH. "
        "Repeat per table. One mapping may use a glob to iterate the "
        "template across many files. Tables not mapped here are looked "
        "up in the project's DuckDB."
    ),
)
@click.option(
    "--output",
    "output_path",
    type=click.Path(dir_okay=False),
    default=None,
    help="HTML output path for a single-shot run (no globbing).",
)
@click.option(
    "--export-dir",
    "export_dir",
    type=click.Path(file_okay=False),
    default=None,
    help="Directory for per-file HTML output when a --table mapping globs.",
)
@click.option(
    "--var",
    "var_overrides",
    multiple=True,
    help=(
        "Override a template variable: --var NAME=VALUE. Takes precedence "
        "over the variable's filename-derived value and default."
    ),
)
@click.option(
    "--fail-fast",
    is_flag=True,
    help="Stop on the first failure instead of continuing.",
)
def template_apply(
    name: str,
    project_dir: str,
    table_mappings: tuple[str, ...],
    output_path: str | None,
    export_dir: str | None,
    var_overrides: tuple[str, ...],
    fail_fast: bool,
):
    """Apply a saved template to parquet files and export HTML dashboards.

    Each required table is registered as a view inside an in-memory DuckDB
    connection. Tables not passed via --table fall back to the project's
    own DuckDB (from .env DB_PATH) — so fixed lookup tables like ``plants``
    don't need to be re-supplied. A single --table mapping may use a shell
    glob, in which case the template is applied once per matching file and
    written to --export-dir.
    """
    import asyncio
    import glob

    from datasight.dashboard_template import (
        TemplateError,
        apply_template,
        load_template,
        resolve_variables,
    )

    try:
        template_obj = load_template(name, project_dir)
    except TemplateError as err:
        raise click.ClickException(str(err)) from err

    cli_var_overrides: dict[str, str] = {}
    for raw in var_overrides:
        if "=" not in raw:
            raise click.ClickException(f"Invalid --var value {raw!r}. Expected NAME=VALUE.")
        key, _, value = raw.partition("=")
        key = key.strip()
        if not key:
            raise click.ClickException(f"Invalid --var value {raw!r}. Expected NAME=VALUE.")
        cli_var_overrides[key] = value

    base_db = _resolve_project_duckdb(project_dir)

    parsed: dict[str, str] = {}
    for mapping in table_mappings:
        if "=" not in mapping:
            raise click.ClickException(f"Invalid --table value {mapping!r}. Expected NAME=PATH.")
        key, _, value = mapping.partition("=")
        key = key.strip()
        value = value.strip()
        if not key or not value:
            raise click.ClickException(f"Invalid --table value {mapping!r}. Expected NAME=PATH.")
        if key in parsed:
            raise click.ClickException(f"Duplicate --table mapping for {key!r}.")
        parsed[key] = value

    rotating_name: str | None = None
    rotating_paths: list[str] = []
    fixed: dict[str, str] = {}
    for key, value in parsed.items():
        is_glob = any(ch in value for ch in "*?[")
        if is_glob:
            matches = sorted(glob.glob(value))
            if not matches:
                raise click.ClickException(f"No files match --table {key}={value!r}.")
            if rotating_name is not None:
                raise click.ClickException(
                    f"Only one --table mapping may glob. Both {rotating_name!r} and {key!r} glob."
                )
            rotating_name = key
            rotating_paths = matches
            continue
        if not Path(value).exists():
            raise click.ClickException(f"File not found for --table {key}: {value}")
        fixed[key] = value

    required = list(template_obj.get("required_tables") or [])
    attached_base_tables: set[str] = set()
    if base_db:
        import duckdb

        with duckdb.connect(":memory:") as peek:
            escaped_db = str(Path(base_db).resolve()).replace("'", "''")
            peek.execute(f"ATTACH '{escaped_db}' AS peek (READ_ONLY)")
            rows = peek.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_catalog = 'peek' AND table_schema = 'main'"
            ).fetchall()
            attached_base_tables = {str(r[0]) for r in rows}

    supplied = set(fixed) | ({rotating_name} if rotating_name else set()) | attached_base_tables
    missing = [t for t in required if t not in supplied]
    if missing:
        if base_db is None:
            hint = (
                "No project DuckDB was detected (checked .env DB_PATH and "
                f"{Path(project_dir).resolve() / 'database.duckdb'}). "
                "Pass --table NAME=PATH for each missing table, or configure "
                "DB_MODE=duckdb and DB_PATH in the project's .env."
            )
        else:
            hint = (
                f"Project DuckDB {base_db} is attached but does not contain "
                f"these tables. Pass --table NAME=PATH or add them to the DB."
            )
        raise click.ClickException(
            "Required tables not provided: " + ", ".join(missing) + ". " + hint
        )

    if output_path and export_dir:
        raise click.ClickException("Pass either --output or --export-dir, not both.")

    if rotating_name is None and export_dir and not output_path:
        # No glob, but --export-dir was given. Promote a single non-globbed
        # mapping into the rotating slot so its stem names the output file.
        if len(fixed) != 1:
            raise click.ClickException(
                "--export-dir needs exactly one --table mapping (or a glob) "
                "to derive the output filename. "
                f"Got {len(fixed)} mappings — use --output PATH instead."
            )
        rotating_name, rotating_value = next(iter(fixed.items()))
        rotating_paths = [rotating_value]
        fixed.pop(rotating_name)

    if rotating_name is None:
        if not output_path:
            raise click.ClickException(
                "Single-shot runs need --output PATH (or --export-dir DIR "
                "to derive the filename from the input)."
            )
        out_path = Path(output_path).resolve()

        try:
            variable_values = resolve_variables(
                template_obj, filename=None, overrides=cli_var_overrides
            )
        except TemplateError as err:
            raise click.ClickException(str(err)) from err

        async def single_run():
            return [
                await apply_template(
                    template_obj,
                    out_path,
                    sources=fixed,
                    base_db=base_db,
                    variables=variable_values,
                )
            ]

        results = asyncio.run(single_run())
    else:
        if not export_dir and not (output_path and len(rotating_paths) == 1):
            raise click.ClickException(
                "Batch mode (a --table mapping with multiple matches) needs --export-dir DIR."
            )
        out_dir = Path(export_dir).resolve() if export_dir else None
        if out_dir:
            out_dir.mkdir(parents=True, exist_ok=True)
        fixed_output = Path(output_path).resolve() if output_path else None

        async def batch_run():
            from datasight.dashboard_template import ApplyResult

            batch_results: list[ApplyResult] = []
            for path in rotating_paths:
                stem = Path(path).stem
                if fixed_output is not None:
                    out_file = fixed_output
                else:
                    assert out_dir is not None  # guarded above
                    out_file = out_dir / f"{stem}.html"
                sources = dict(fixed)
                sources[rotating_name] = path
                try:
                    per_file_vars = resolve_variables(
                        template_obj, filename=path, overrides=cli_var_overrides
                    )
                except TemplateError as err:
                    result = ApplyResult(
                        label=Path(path).name, output=None, ok=False, error=str(err)
                    )
                    batch_results.append(result)
                    click.echo(f"  FAIL {path} — {err}", err=True)
                    if fail_fast:
                        break
                    continue
                result = await apply_template(
                    template_obj,
                    out_file,
                    sources=sources,
                    base_db=base_db,
                    variables=per_file_vars,
                )
                batch_results.append(result)
                if result.ok:
                    click.echo(f"  ok   {path} -> {out_file}")
                else:
                    reason = result.error or "; ".join(
                        f"card {c.idx} ({c.title}): {c.error}" for c in result.cards if not c.ok
                    )
                    click.echo(f"  FAIL {path} — {reason}", err=True)
                    if fail_fast:
                        break
            return batch_results

        results = asyncio.run(batch_run())

    failed = sum(1 for r in results if not r.ok)
    if rotating_name is None:
        only = results[0]
        if only.ok:
            click.echo(f"Wrote {only.output}")
        else:
            reason = only.error or "; ".join(
                f"card {c.idx} ({c.title}): {c.error}" for c in only.cards if not c.ok
            )
            click.echo(f"FAIL: {reason}", err=True)
    else:
        click.echo(
            f"Applied {name!r} to {len(results)} file(s): "
            f"{len(results) - failed} succeeded, {failed} failed."
        )
    if failed:
        raise SystemExit(1)


@click.command(
    name="delete",
    epilog=_epilog(
        """
        Example:

            datasight templates delete generation-dashboard
        """
    ),
)
@click.argument("name")
@_PROJECT_DIR_OPT
def template_delete(name: str, project_dir: str):
    """Delete a saved template."""
    from datasight.dashboard_template import TemplateError, delete_template

    try:
        removed = delete_template(name, project_dir)
    except TemplateError as err:
        raise click.ClickException(str(err)) from err
    if not removed:
        raise click.ClickException(f"Template {name!r} not found.")
    click.echo(f"Deleted template {name!r}.")


templates.add_command(template_save)
templates.add_command(template_list)
templates.add_command(template_show)
templates.add_command(template_apply)
templates.add_command(template_delete)
