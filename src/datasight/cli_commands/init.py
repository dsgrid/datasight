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
        Use this when you want to fill in .env, schema_description.md,
        queries.yaml, and time_series.yaml by hand.

        If you already have a DuckDB/SQLite database or CSV/Parquet/Excel
        files and want datasight to inspect them and draft these files, use:

            datasight generate <file>...
        """
    )
)
@click.argument("project_dir", default=".")
@click.option("--overwrite", is_flag=True, help="Overwrite existing files.")
def init(project_dir: str, overwrite: bool):
    """Create blank datasight project template files.

    PROJECT_DIR defaults to the current directory.
    """
    dest = Path(project_dir).resolve()
    dest.mkdir(parents=True, exist_ok=True)

    template_dir = Path(cli_root.__file__).parent / "templates"

    files = {
        "env.template": ".env",
        "schema_description.md": "schema_description.md",
        "queries.yaml": "queries.yaml",
        "time_series.yaml": "time_series.yaml",
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
    click.echo("  1. Store API keys once in ~/.config/datasight/.env:")
    click.echo("     datasight config init")
    click.echo("  2. Edit .env with your database path and (optional) provider/model")
    click.echo("  3. Edit schema_description.md to describe your data")
    click.echo("  4. Edit queries.yaml with example questions")
    click.echo("  5. Or let datasight draft files from data:")
    click.echo("     datasight generate <database-or-files> --overwrite")
    click.echo("  6. Run: datasight run")
