"""CLI command module."""

import asyncio
import json
from pathlib import Path

import rich_click as click


from datasight import cli
from datasight.cli_helpers import format_epilog


@click.group(
    epilog=format_epilog(
        """
        Examples:

            datasight recipes list
            datasight recipes list --table generation_fuel
            datasight recipes run 1
        """
    )
)
def recipes():
    """Generate and run reusable deterministic prompt recipes.

    Recipes are suggested natural-language questions derived from the
    schema. Listing recipes does not call an LLM; running one sends the
    recipe prompt through the normal ask pipeline.
    """


@click.command(
    name="list",
    epilog=format_epilog(
        """
        Examples:

            datasight recipes list
            datasight recipes list --table generation_fuel
            datasight recipes list --format markdown -o recipes.md
        """
    ),
)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing .env and config files.",
)
@click.option("--table", default=None, help="Generate recipes for a specific table.")
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
    help="Write the recipes output to a file instead of stdout.",
)
def recipes_list(project_dir, table, output_format, output_path):
    """List reusable deterministic prompt recipes for a project."""
    from rich.console import Console

    project_dir = str(Path(project_dir).resolve())
    settings, _ = cli.resolve_settings(project_dir)
    recipe_data = cli.load_recipe_entries(project_dir, settings, table)

    if output_format == "json":
        cli.write_or_print(json.dumps(recipe_data, indent=2), output_path)
        return

    if output_format == "markdown":
        cli.write_or_print(cli.render_recipes_markdown(recipe_data), output_path)
        return

    console = Console(record=bool(output_path))
    console.print(
        cli.build_profile_detail_table(
            "Prompt Recipes",
            [
                ("ID", "right"),
                ("Title", "left"),
                ("Category", "left"),
                ("Why", "left"),
                ("Prompt", "left"),
            ],
            [
                [
                    str(item["id"]),
                    item["title"],
                    item.get("category") or "Recipe",
                    item.get("reason") or "",
                    item["prompt"],
                ]
                for item in recipe_data
            ],
        )
    )
    if output_path:
        cli.write_or_print(console.export_text(), output_path)


@click.command(
    name="run",
    epilog=format_epilog(
        """
        Examples:

            datasight recipes run 1
            datasight recipes run 2 --format csv -o recipe.csv
            datasight recipes run 3 --chart-format html -o recipe.html
        """
    ),
)
@click.argument("recipe_id", type=int)
@click.option(
    "--project-dir",
    type=click.Path(exists=True),
    default=".",
    help="Project directory containing .env and config files.",
)
@click.option("--table", default=None, help="Use recipes generated for a specific table.")
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
def recipes_run(recipe_id, project_dir, table, model, output_format, chart_format, output_path):
    """Run a generated recipe by ID through the normal ask pipeline.

    RECIPE_ID is the numeric ID shown by datasight recipes list.
    """

    from rich.console import Console

    project_dir = str(Path(project_dir).resolve())
    settings, resolved_model = cli.resolve_settings(project_dir, model)
    cli.validate_settings_for_llm(settings)

    recipe_data = cli.load_recipe_entries(project_dir, settings, table)
    recipe = next((item for item in recipe_data if item["id"] == recipe_id), None)
    if recipe is None:
        click.echo(f"Recipe {recipe_id} not found.", err=True)
        raise SystemExit(1)

    sql_dialect = settings.database.sql_dialect
    console = Console()
    console.print(f"[dim]Running recipe [{recipe['id']}]: {recipe['title']}[/dim]")

    result = asyncio.run(
        cli.run_ask_pipeline(
            question=recipe["prompt"],
            settings=settings,
            resolved_model=resolved_model,
            project_dir=project_dir,
            sql_dialect=sql_dialect,
        )
    )
    cli.emit_ask_result(result, output_format, chart_format, output_path)


recipes.add_command(recipes_list)
recipes.add_command(recipes_run)
