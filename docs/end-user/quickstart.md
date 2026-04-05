# Quickstart

This tutorial walks you through setting up datasight with your own database.

## Prerequisites

- Python 3.10+
- A database: DuckDB file, SQLite file, or PostgreSQL server
- **One of the following LLM backends:**
  - An Anthropic API key ([get one here](https://console.anthropic.com/)), **or**
  - A GitHub Copilot subscription (uses [GitHub Models](https://github.com/marketplace/models)), **or**
  - [Ollama](https://ollama.com/) installed locally (free, no API key needed)

## Install datasight

```bash
pip install git+https://github.com/dsgrid/datasight.git
```

To use GitHub Models or Ollama as the LLM backend, install with the optional
dependency:

```bash
# GitHub Models (Copilot subscription)
pip install "datasight[github] @ git+https://github.com/dsgrid/datasight.git"

# Ollama (local)
pip install "datasight[ollama] @ git+https://github.com/dsgrid/datasight.git"
```

Or install from source:

```bash
git clone https://github.com/dsgrid/datasight.git
cd datasight
pip install -e ".[github]"  # or ".[ollama]"
```

## Create a project

```bash
mkdir my-project && cd my-project
datasight init
```

This creates three template files:

`.env`
: API key and database connection settings.

`schema_description.md`
: Describe your database for the AI.

`queries.yaml`
: Example question/SQL pairs.

## Configure

Edit `.env` with your database path and LLM settings.

**Option A — Anthropic (cloud API):**

```bash
ANTHROPIC_API_KEY=sk-ant-...
DB_MODE=duckdb
DB_PATH=./my_database.duckdb
```

**Option B — GitHub Models (Copilot subscription):**

```bash
LLM_PROVIDER=github
GITHUB_TOKEN=ghp_...
GITHUB_MODELS_MODEL=gpt-4o
DB_MODE=duckdb
DB_PATH=./my_database.duckdb
```

**Option C — Ollama (local, no API key):**

First, install and start [Ollama](https://ollama.com/), then pull a model
with tool-calling support:

```bash
ollama pull qwen3.5:35b-a3b
```

Then configure `.env`:

```bash
LLM_PROVIDER=ollama
OLLAMA_MODEL=qwen3.5:35b-a3b
DB_MODE=duckdb
DB_PATH=./my_database.duckdb
```

**Using SQLite or PostgreSQL?** Set `DB_MODE` accordingly:

```bash
# SQLite
DB_MODE=sqlite
DB_PATH=./my_database.sqlite

# PostgreSQL (install with: pip install "datasight[postgres]")
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

Edit `schema_description.md` to explain your data — domain concepts, column
meanings, code lookups, and query tips. The AI uses this context to write
better SQL. See [Write a schema description](../dataset-developer/schema-description.md) for guidance.

Edit `queries.yaml` with example questions and their correct SQL. See
[Create example queries](../dataset-developer/example-queries.md) for guidance.

## Run

```bash
datasight run
```

Open <http://localhost:8084> in your browser. The sidebar shows your database
tables and example queries. Type a question in plain English and the AI will
write SQL, run it, and display the results. Ask for a chart and it will
generate an interactive Plotly visualization.

### Headless mode

You can also ask questions from the command line without starting a web server:

```bash
datasight ask "What are the top 10 records by the largest numeric column?"
datasight ask "Show trends over time" --chart-format html -o chart.html
datasight ask "Top 5 states" --format csv -o results.csv
```

See `datasight ask --help` for all options.

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
