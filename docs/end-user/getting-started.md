# Getting started

Install datasight and start exploring data in under five minutes.

## Install

```bash
pip install git+https://github.com/dsgrid/datasight.git
```

This includes support for DuckDB, SQLite, PostgreSQL, Anthropic, GitHub
Models, and Ollama — no extras needed.

!!! note "PNG chart export"
    The `datasight ask --chart-format png` CLI command requires an additional
    package: `pip install 'datasight[export]'`. The web UI uses interactive
    HTML charts and does not need this.

## Explore your files

The fastest way to get started — no project setup required:

```bash
datasight run
```

Open <http://localhost:8084>. The landing page shows two options:

1. **Guided starters** — choose a concrete first path like **Profile this
   dataset**, **Find key dimensions**, **Build a trend chart**, or **Audit
   nulls and outliers**. datasight will run the selected starter immediately
   after your data loads.

2. **Configure your LLM** — if you haven't set environment variables, enter
   your provider and API key. (If you exported `ANTHROPIC_API_KEY` or similar
   in your shell, this step is skipped automatically.)

3. **Explore Files** — enter the path to a CSV, Parquet, or DuckDB file
   (or a directory of Parquet files) and click **Explore**.

datasight creates an in-memory database, introspects the schema, and drops
you into the chat UI. Start with the guided starter output, then continue
into freeform questions, recipes, or dashboard composition.

!!! tip "Adding more files"
    Use the input at the top of the sidebar (below **Tables**) to add more
    files to your session at any time.

### Save as a project

Once you're comfortable with your data, click **Save** in the header to
persist your session as a project. datasight will:

- Create a project directory with a DuckDB database (views pointing to your
  original files — no data copying)
- Auto-generate `schema_description.md` and `queries.yaml` using the LLM
- Load the project so future sessions remember your schema context

## Start from the CLI

### Inspect files without a project

`datasight inspect` runs every deterministic analysis in one shot — profile,
quality, measures, dimensions, trends, and recipes — directly on your files:

```bash
datasight inspect sales.parquet
datasight inspect orders.csv products.csv
datasight inspect data_dir/
```

No project setup, no LLM, no files written. Everything prints to the console.
Use `--format json` or `--format markdown` to change the output, or
`-o report.md` to save it.

### Run individual commands from a project

If you have a configured project and want to run one analysis at a time:

```bash
datasight profile
datasight quality
datasight dimensions
datasight trends
datasight recipes
```

These commands inspect the data directly and return structured output without
opening the web UI.

For a recommended progression from deterministic inspection into reusable
batch question files, see [Inspection workflows](inspection-workflows.md).

## Try the demo

```bash
datasight demo ./my-project
cd my-project
# Edit .env with your API key (see above)
datasight run
```

Open <http://localhost:8084> and start asking questions. See
[Try the demo dataset](demo-dataset.md) for details about the included
EIA energy data.

## Use your own database

If you have a DuckDB or SQLite database and want a curated project:

```bash
datasight init ./my-project
cd my-project
# Edit .env with your API key and DB_PATH
datasight generate   # auto-generate schema docs and example queries
datasight run
```

`datasight generate` connects to your database and uses the LLM to draft
`schema_description.md` and `queries.yaml` so you don't have to write them
from scratch. See the [Project setup guide](quickstart.md) for a full walkthrough.
