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


@click.group(
    epilog=_epilog(
        """
        The user-global config file (~/.config/datasight/.env) holds API
        keys and tokens shared across every datasight project. Per-project
        .env files override its values, so each project can still pick its
        own LLM provider, model, and database.

        Examples:

            datasight config init
            datasight config show
        """
    )
)
def config():
    """Manage user-global datasight configuration."""


@click.command(name="init")
@click.option("--overwrite", is_flag=True, help="Overwrite the existing global config file.")
def config_init(overwrite: bool):
    """Create the user-global config file (~/.config/datasight/.env).

    Stores API keys and tokens in one place so per-project .env files only
    need to set provider, model, and database settings.
    """

    dest = global_env_path()
    if dest.exists() and not overwrite:
        click.echo(f"Global config already exists: {dest}")
        click.echo("Use --overwrite to replace it.")
        return

    template_path = Path(cli_root.__file__).parent / "templates" / "global_env.template"
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(template_path, dest)

    click.echo(f"Created: {dest}")
    click.echo()
    click.echo("Next steps:")
    click.echo(f"  1. Edit {dest} and uncomment the API keys you use")
    click.echo("  2. In each project, .env only needs DB_MODE/DB_PATH and")
    click.echo("     (optionally) LLM_PROVIDER and the matching model variable")


@click.command(name="show")
def config_show():
    """Show the resolved datasight configuration and where it loaded from."""
    from dotenv import load_dotenv

    from datasight.recent_projects import validate_project_dir

    cwd = Path.cwd().resolve()
    is_valid, _ = validate_project_dir(str(cwd))
    project_dir = cwd if is_valid else None

    project_env = (project_dir / ".env") if project_dir else None
    global_env = global_env_path()

    if project_env and project_env.exists():
        load_dotenv(project_env, override=False)
    load_global_env(override=False)
    settings = Settings.from_env()

    def mask(secret: str) -> str:
        if not secret:
            return "(not set)"
        return f"…{secret[-4:]}" if len(secret) > 4 else "****"

    click.echo("Config files:")
    click.echo(f"  Global:  {global_env} {'(exists)' if global_env.exists() else '(missing)'}")
    if project_env:
        exists = "(exists)" if project_env.exists() else "(missing)"
        click.echo(f"  Project: {project_env} {exists}")
    else:
        click.echo("  Project: (no datasight project detected in CWD)")

    click.echo()
    click.echo("LLM:")
    click.echo(f"  provider: {settings.llm.provider}")
    click.echo(f"  model:    {settings.llm.model}")
    if settings.llm.base_url:
        click.echo(f"  base_url: {settings.llm.base_url}")
    click.echo(f"  api_key:  {mask(settings.llm.api_key)}")

    click.echo()
    click.echo("Database:")
    click.echo(f"  mode: {settings.database.mode}")
    if settings.database.mode in ("duckdb", "sqlite"):
        click.echo(f"  path: {settings.database.path}")
    elif settings.database.mode == "postgres":
        if settings.database.postgres_url:
            click.echo(f"  url:  {settings.database.postgres_url}")
        else:
            click.echo(
                f"  host: {settings.database.postgres_host}:{settings.database.postgres_port}"
            )
            click.echo(f"  db:   {settings.database.postgres_database}")
    elif settings.database.mode == "flightsql":
        click.echo(f"  uri:  {settings.database.flight_uri}")
    elif settings.database.mode == "spark":
        click.echo(f"  remote: {settings.database.spark_remote}")
        click.echo(f"  max result bytes: {settings.database.spark_max_result_bytes:,}")


config.add_command(config_init)
config.add_command(config_show)
