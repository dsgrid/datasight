# datasight

> **Status: early and evolving.** This project is in active development and the
> code is changing rapidly — APIs, CLI flags, and behavior may shift between
> commits. Feedback and bug reports from users are very welcome; please open an
> issue on GitHub.

AI-powered data exploration with natural language.

datasight connects an AI agent to your database and provides a web UI
where you can ask questions in natural language. The agent writes SQL, runs
queries, and generates interactive Plotly visualizations.

Supports **DuckDB**, **PostgreSQL**, **SQLite**, and **Flight SQL** databases.
Also queries local **CSV**, **Parquet**, and **Excel** (.xlsx) files directly —
no database setup required. Supports **Anthropic Claude** (default),
**GitHub Models** (open source), and **Ollama** (local) as LLM backends.

## Quick start

```bash
uv tool install datasight

# Create a new project
mkdir my-project && cd my-project
datasight init

# Edit .env with your API key and database path
# Edit schema_description.md to describe your data
# Edit queries.yaml with example questions

# Run the web UI
datasight run
```

Open http://localhost:8084 and start asking questions.

### Explore CSV, Parquet, or Excel files with no setup

```bash
# Launch the web UI with no project, then paste a file or directory path
# into "Explore Files" to create views automatically
datasight run

# Or inspect a file from the command line (schema, row count, column stats)
datasight inspect generation.parquet

# Or build a persistent project from CSV/Parquet inputs
datasight generate generation.csv plants.csv --db-path grid.duckdb
```

Or ask from the command line without starting a server:

```bash
datasight ask "What are the top 10 records?"
datasight ask "Show trends by year" --chart-format html -o chart.html
datasight profile
datasight quality --format markdown -o quality.md
datasight ask --file questions.txt --output-dir batch-output
```

## Features

- **Natural language queries** — ask questions in English, get SQL + results
- **Interactive charts** — Plotly visualizations with chart-type switching
- **Multiple databases** — DuckDB, PostgreSQL, SQLite, and Flight SQL
- **Query files directly** — point at a local CSV, Parquet, or Excel file (or directory) and start asking questions; datasight creates DuckDB views (or one table per Excel sheet) on the fly
- **Headless CLI** — `datasight ask` runs queries without a web server
- **Deterministic CLI workflows** — profile, quality, dimension, trend, and recipe commands that do not require an LLM
- **Schema browser** — sidebar with tables, columns, and example queries
- **Schema auto-discovery** — tables, columns, and types detected automatically
- **Domain context** — describe your data in Markdown for better AI understanding
- **Example queries** — seed the AI with question/SQL pairs
- **Reusable prompt recipes** — project-specific analysis prompts derived from the schema
- **Multi-chart dashboard** — pin results, filter cards, and configure layouts
- **Session export** — export conversations as shareable HTML pages
- **Keyboard shortcuts** — `?` to see all shortcuts, `/` to focus input
- **Streaming responses** — real-time SSE streaming from the LLM

## Architecture

datasight pairs a FastAPI backend with a Svelte 5 + TypeScript + Tailwind CSS
frontend built with Vite. It supports multiple LLM backends — Anthropic
(default), OpenAI, GitHub Models, and Ollama — selectable via `LLM_PROVIDER` in `.env`.

```
datasight run / datasight ask / datasight profile / datasight quality
  → LLM provider (Anthropic / OpenAI / GitHub Models / Ollama)
    → DuckDB / PostgreSQL / SQLite / Flight SQL (or CSV/Parquet via DuckDB views)
    → Plotly chart generator
  → Web UI (SSE streaming) or CLI output
```

## Documentation

```bash
uv sync --extra dev
. .venv/bin/activate
zensical serve
zensical build
python scripts/generate_cli_reference.py
```

## Development Tests

```bash
# Build frontend assets for FastAPI serving after a clean checkout
bash scripts/build-frontend.sh

# Python test suite
pytest

# CI-safe Python test suite, excluding tests that need local Ollama
pytest -m "not integration"

# Frontend unit tests (Vitest)
cd frontend && npm test

# Frontend E2E tests (Playwright, requires datasight run)
cd frontend && npm run test:e2e

# Rebuild frontend for FastAPI serving after frontend changes
bash scripts/build-frontend.sh
```

Generated web assets under `src/datasight/web/static/` and
`src/datasight/web/templates/index.html` are not checked in. Run
`bash scripts/build-frontend.sh` before using `datasight run` from a clean
checkout when you want FastAPI to serve the production UI.

Ollama-backed CLI tests are marked `integration` because they require a running
local Ollama server with the `qwen2.5:7b` model available. CI runs `pytest -m "not
integration"`; run `pytest -m integration` locally when you want to exercise the
live LLM path.

## Software Record

datasight is developed under NLR Software Record SWR-26-045.
