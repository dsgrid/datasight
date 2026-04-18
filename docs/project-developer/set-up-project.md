# Set up a project

This guide walks you through creating a datasight project for your database.

## Prerequisites

- Python 3.11+
- A database: DuckDB file, SQLite file, PostgreSQL server, or CSV/Parquet files
- An LLM provider: Anthropic API key, OpenAI API key, GitHub account (free, public), or Ollama (free, local). Not sure which? See [Choosing an LLM](../concepts/choosing-an-llm.md).

## Install datasight

```bash
uv tool install "datasight @ git+https://github.com/dsgrid/datasight.git"
```

Don't have [uv](https://docs.astral.sh/uv/) yet? See
[Install datasight](../end-user/how-to/install.md). All database
backends (DuckDB, SQLite, PostgreSQL) and LLM providers (Anthropic,
GitHub Models, Ollama) are included.

## Create a project

```bash
mkdir my-project && cd my-project
datasight init
```

This creates four template files:

`.env`
: API key and database connection settings.

`schema_description.md`
: Describe your database for the AI.

`queries.yaml`
: Example question/SQL pairs.

`time_series.yaml`
: Declare temporal structure for completeness checks. See [Declare time series](../end-user/how-to/declare-time-series.md).

## Configure

Edit `.env` with your database path and LLM settings. For guidance on
picking a provider — data sensitivity, cost tiers, local vs hosted —
see [Choosing an LLM](../concepts/choosing-an-llm.md).

**Option A — Anthropic (cloud API):**

```bash
ANTHROPIC_API_KEY=sk-ant-...
DB_MODE=duckdb
DB_PATH=./my_database.duckdb
```

**Option B — OpenAI (cloud API):**

```bash
LLM_PROVIDER=openai
OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-4o-mini
DB_MODE=duckdb
DB_PATH=./my_database.duckdb
```

**Option C — GitHub Models (free, public):**

```bash
LLM_PROVIDER=github
GITHUB_TOKEN=ghp_...
GITHUB_MODELS_MODEL=gpt-4o
DB_MODE=duckdb
DB_PATH=./my_database.duckdb
```

**Option D — Ollama (local, no API key):**

First, install and start [Ollama](https://ollama.com/), then pull a model
with tool-calling support:

```bash
ollama pull qwen3:8b
```

Then configure `.env`:

```bash
LLM_PROVIDER=ollama
OLLAMA_MODEL=qwen3:8b
DB_MODE=duckdb
DB_PATH=./my_database.duckdb
```

**Using SQLite or PostgreSQL?** Set `DB_MODE` accordingly:

```bash
# SQLite
DB_MODE=sqlite
DB_PATH=./my_database.sqlite

# PostgreSQL
DB_MODE=postgres
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=mydb
POSTGRES_USER=datasight
POSTGRES_PASSWORD=secret
# Or use a connection string instead:
# POSTGRES_URL=postgresql://user:pass@host:5432/dbname
```

See [Configuration reference](../reference/configuration.md) for all
PostgreSQL options.

### Auto-generate documentation

Instead of writing `schema_description.md` and `queries.yaml` by hand,
you can let the AI generate them from your database:

```bash
datasight generate
```

You can also pass existing database files or tabular files directly
instead of configuring a database first:

```bash
# Existing DuckDB database: referenced directly
datasight generate generation.duckdb

# Existing SQLite database: referenced directly
datasight generate generation.sqlite

# CSV inputs: datasight creates ./database.duckdb
datasight generate generation.csv plants.csv

# Parquet inputs: datasight creates ./database.duckdb
datasight generate generation.parquet plants.parquet

# CSV inputs with a custom output DuckDB path
datasight generate generation.csv plants.csv --db-path db/project.duckdb

# Parquet inputs with a custom output DuckDB path
datasight generate generation.parquet plants.parquet --db-path db/project.duckdb
```

For a single existing DuckDB or SQLite file, datasight creates or updates
`.env` to point at that database directly.

When CSV, Parquet, or mixed file inputs are passed, datasight also:

- Writes a persistent DuckDB file (`database.duckdb` by default in the
  project directory) with views pointing at each input file.
- Creates or updates `.env` with `DB_MODE=duckdb` and `DB_PATH` set to
  the new database. Existing entries are replaced in place; other env
  values (like `ANTHROPIC_API_KEY`) are preserved.

Use `--db-path <path>` to write that generated DuckDB somewhere else
(for example, `--db-path db/project.duckdb`). The path may be absolute or
relative to `--project-dir`. Do not use `--db-path` with a single existing
DuckDB or SQLite database; those inputs are referenced directly.

The command connects to your database (or creates an ephemeral one from
the given files), inspects tables and columns, samples code/enum columns
to identify their meanings, and produces draft versions of
`schema_description.md` and `queries.yaml`. Review and edit the results —
the AI gets you a solid starting point but you know your data best.

It also seeds a `measures.yaml` file for project-specific semantic
overrides and a `time_series.yaml` file for temporal completeness
declarations (see [Declare time series](../end-user/how-to/declare-time-series.md)).

To regenerate after making database changes:

```bash
datasight generate --overwrite
```

### Manual editing

You can also write these files by hand, or refine the generated versions.

Edit `schema_description.md` to explain your data — domain concepts, column
meanings, code lookups, and query tips. The AI uses this context to write
better SQL. See [Write a schema description](schema-description.md) for guidance.

Edit `queries.yaml` with example questions and their correct SQL. See
[Create example queries](example-queries.md) for guidance.

If your project contains energy metrics, rates, or project-specific formulas,
edit `measures.yaml` to lock in semantic behavior such as:

- default aggregation
- weighted-average columns
- display name and numeric format
- preferred chart types
- calculated measures such as `net_load_mw`

See [Configure semantic measures](../end-user/how-to/configure-measures.md) for the full `measures.yaml` workflow.

## Run

```bash
datasight run
```

Open <http://localhost:8084> in your browser. The sidebar shows your database
tables, recipes, example queries, and saved artifacts. The landing page also
lets you start with guided deterministic workflows such as:

- profiling the dataset
- surfacing key dimensions
- finding likely trend charts
- auditing nulls and suspicious ranges

After that first pass, type a question in plain English and the AI will write
SQL, run it, and display the results. Ask for a chart and it will generate an
interactive Plotly visualization.

### Headless mode

You can also ask questions from the command line without starting a web server:

```bash
datasight ask "What are the top 10 records by the largest numeric column?"
datasight ask "Show trends over time" --chart-format html -o chart.html
datasight ask "Top 5 states" --format csv -o results.csv
datasight ask --file questions.txt --output-dir batch-output
datasight profile
datasight quality --table generation_fuel
datasight dimensions --table generation_fuel
datasight trends --table generation_fuel
```

See [Ask questions from the CLI](../end-user/how-to/ask-from-cli.md) for batch mode, export
options, and diagnostics.

## What happens at startup

```mermaid
flowchart TB
    A[datasight run] --> B[Load .env]
    B --> C[Connect to database]
    C --> D[Introspect schema]
    D --> E[Load schema_description.md]
    E --> F[Load queries.yaml]
    F --> G[Start FastAPI server]

    style A fill:#15a8a8,stroke:#023d60,color:#fff
    style B fill:#8a7d55,stroke:#6b6040,color:#fff
    style C fill:#2e7ebb,stroke:#1a5c8a,color:#fff
    style D fill:#2e7ebb,stroke:#1a5c8a,color:#fff
    style E fill:#8a7d55,stroke:#6b6040,color:#fff
    style F fill:#8a7d55,stroke:#6b6040,color:#fff
    style G fill:#fe5d26,stroke:#023d60,color:#fff
```

datasight auto-discovers your tables, columns, and row counts, then combines
that with your description and example queries to give the AI full context
about your database.
