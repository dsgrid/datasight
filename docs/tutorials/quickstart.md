# Quickstart

This tutorial walks you through setting up datasight with your own DuckDB
database.

## Prerequisites

- Python 3.10+
- A DuckDB database file
- **One of the following LLM backends:**
  - An Anthropic API key ([get one here](https://console.anthropic.com/)), **or**
  - [Ollama](https://ollama.com/) installed locally (free, no API key needed)

## Install datasight

```bash
pip install git+https://github.com/dsgrid/datasight.git
```

To use Ollama as the LLM backend, install with the optional dependency:

```bash
pip install "datasight[ollama] @ git+https://github.com/dsgrid/datasight.git"
```

Or install from source:

```bash
git clone https://github.com/dsgrid/datasight.git
cd datasight
pip install -e ".[ollama]"
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
DB_MODE=local
DB_PATH=./my_database.duckdb
```

**Option B — Ollama (local, no API key):**

First, install and start [Ollama](https://ollama.com/), then pull a model
with tool-calling support:

```bash
ollama pull qwen3.5:35b-a3b
```

Then configure `.env`:

```bash
LLM_PROVIDER=ollama
OLLAMA_MODEL=qwen3.5:35b-a3b
DB_MODE=local
DB_PATH=./my_database.duckdb
```

Edit `schema_description.md` to explain your data — domain concepts, column
meanings, code lookups, and query tips. The AI uses this context to write
better SQL. See [](../how-to/schema-description.md) for guidance.

Edit `queries.yaml` with example questions and their correct SQL. See
[](../how-to/example-queries.md) for guidance.

## Run

```bash
datasight run
```

Open <http://localhost:8084> in your browser. The sidebar shows your database
tables and example queries. Type a question in plain English and the AI will
write SQL, run it, and display the results. Ask for a chart and it will
generate an interactive Plotly visualization.

## What happens at startup

```{mermaid}
flowchart LR
    A[datasight run] --> B[Load .env]
    B --> C[Connect to database]
    C --> D[Introspect schema]
    D --> E[Load schema_description.md]
    E --> F[Load queries.yaml]
    F --> G[Start FastAPI server]

    style A fill:#15a8a8,stroke:#023d60,color:#fff
    style B fill:#e7e1cf,stroke:#023d60,color:#023d60
    style C fill:#023d60,stroke:#023d60,color:#fff
    style D fill:#023d60,stroke:#023d60,color:#fff
    style E fill:#e7e1cf,stroke:#023d60,color:#023d60
    style F fill:#e7e1cf,stroke:#023d60,color:#023d60
    style G fill:#fe5d26,stroke:#023d60,color:#fff
```

datasight auto-discovers your tables, columns, and row counts, then combines
that with your description and example queries to give the AI full context
about your database.
