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
        Examples:

            datasight demo eia-generation eia-demo
            datasight demo dsgrid-tempo tempo-demo
            datasight demo time-validation time-demo
        """
    )
)
def demo():
    """Create ready-to-run demo projects with sample datasets."""


@click.command(
    name="eia-generation",
    epilog=_epilog(
        """
        Example:

            datasight demo eia-generation eia-demo --min-year 2021
        """
    ),
)
@click.argument("project_dir", default=".")
@click.option(
    "--min-year", type=int, default=2020, help="Earliest year to include (default: 2020)."
)
def demo_eia_generation(project_dir: str, min_year: int):
    """Download an EIA energy demo dataset and create a ready-to-run project.

    Downloads cleaned EIA-923 and EIA-860 data from the PUDL project's public
    data releases. Creates a DuckDB database with generation, fuel consumption,
    and plant data, along with pre-written schema descriptions and example queries.

    PROJECT_DIR defaults to the current directory.
    """
    _configure_logging("INFO")

    dest = Path(project_dir).resolve()
    dest.mkdir(parents=True, exist_ok=True)

    click.echo(f"datasight demo eia-generation — downloading EIA energy data (>= {min_year})")
    click.echo(f"  Destination: {dest}")
    click.echo()

    from datasight.demo import download_demo_dataset, write_demo_project_files

    click.echo("Downloading from PUDL (this may take a minute)...")
    db_path = download_demo_dataset(dest, min_year=min_year)
    db_size_mb = db_path.stat().st_size / (1024 * 1024)
    click.echo(f"  Database: {db_path.name} ({db_size_mb:.1f} MB)")

    click.echo("Writing project files...")
    write_demo_project_files(dest, db_path)

    click.echo()
    click.echo("Demo project ready!")
    click.echo()
    click.echo("Next steps:")
    click.echo(f"  1. cd {dest}")
    click.echo("  2. Edit .env — set your ANTHROPIC_API_KEY")
    click.echo("  3. datasight run")


@click.command(
    name="dsgrid-tempo",
    epilog=_epilog(
        """
        Example:

            datasight demo dsgrid-tempo tempo-demo
        """
    ),
)
@click.argument("project_dir", default=".")
def demo_dsgrid_tempo(project_dir: str):
    """Download dsgrid TEMPO EV charging demand projections.

    Downloads hourly and annual EV charging demand data from NLR's TEMPO
    project (published on OEDI). Creates a DuckDB database with charging
    profiles at census-division level, plus annual summaries by state and
    county. Covers three adoption scenarios from 2024 to 2050.

    Data source: s3://nrel-pds-dsgrid/tempo/tempo-2022/v1.0.0 (public, no credentials needed).

    PROJECT_DIR defaults to the current directory.
    """
    _configure_logging("INFO")

    dest = Path(project_dir).resolve()
    dest.mkdir(parents=True, exist_ok=True)

    click.echo("datasight demo dsgrid-tempo — downloading TEMPO EV charging data")
    click.echo(f"  Destination: {dest}")
    click.echo()

    from datasight.demo_dsgrid_tempo import (
        download_dsgrid_tempo_dataset,
        write_dsgrid_tempo_project_files,
    )

    click.echo("Downloading from OEDI S3 (this may take a minute)...")
    db_path = download_dsgrid_tempo_dataset(dest)
    db_size_mb = db_path.stat().st_size / (1024 * 1024)
    click.echo(f"  Database: {db_path.name} ({db_size_mb:.1f} MB)")

    click.echo("Writing project files...")
    write_dsgrid_tempo_project_files(dest, db_path)

    click.echo()
    click.echo("Demo project ready!")
    click.echo()
    click.echo("Next steps:")
    click.echo(f"  1. cd {dest}")
    click.echo("  2. Edit .env — set your ANTHROPIC_API_KEY")
    click.echo("  3. datasight run")


@click.command(
    name="time-validation",
    epilog=_epilog(
        """
        Example:

            datasight demo time-validation time-demo
        """
    ),
)
@click.argument("project_dir", default=".")
def demo_time_validation(project_dir: str):
    """Generate a synthetic energy consumption dataset with planted time errors.

    Creates hourly electricity consumption data across sectors, end uses, and
    US states for future projection years (2038, 2039, 2040). The dataset
    contains intentional gaps, duplicates, and DST anomalies that datasight's
    time series quality checks can detect.

    Run "datasight quality" or "datasight run" after setup to find the errors.

    PROJECT_DIR defaults to the current directory.
    """
    _configure_logging("INFO")

    dest = Path(project_dir).resolve()
    dest.mkdir(parents=True, exist_ok=True)

    click.echo("datasight demo time-validation — generating synthetic dataset")
    click.echo(f"  Destination: {dest}")
    click.echo()

    from datasight.demo_time_validation import (
        generate_time_validation_dataset,
        write_time_validation_project_files,
    )

    click.echo("Generating hourly consumption data with planted errors...")
    db_path = generate_time_validation_dataset(dest)
    db_size_mb = db_path.stat().st_size / (1024 * 1024)
    click.echo(f"  Database: {db_path.name} ({db_size_mb:.1f} MB)")

    click.echo("Writing project files...")
    write_time_validation_project_files(dest, db_path)

    click.echo()
    click.echo("Demo project ready!")
    click.echo()
    click.echo("Next steps:")
    click.echo(f"  1. cd {dest}")
    click.echo("  2. datasight quality        # detect the planted errors")
    click.echo("  3. datasight run            # explore interactively")


demo.add_command(demo_eia_generation)
demo.add_command(demo_dsgrid_tempo)
demo.add_command(demo_time_validation)
