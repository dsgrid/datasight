# datasight

AI-powered database exploration with natural language.

datasight connects an AI agent to your database and provides a web UI
where you can ask questions in natural language. The agent writes SQL, runs
queries, and generates interactive Plotly visualizations.

Supports **DuckDB**, **PostgreSQL**, **SQLite**, and **Flight SQL** databases.
Supports **Anthropic Claude** (default), **GitHub Models** (Copilot subscription),
and **Ollama** (local) as LLM backends.

## Quick start

```bash
pip install git+https://github.com/dsgrid/datasight.git

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
- **Headless CLI** — `datasight ask` runs queries without a web server
- **Deterministic CLI workflows** — profile, quality, dimension, trend, and recipe commands that do not require an LLM
- **Schema browser** — sidebar with tables, columns, and example queries
- **Schema auto-discovery** — tables, columns, and types detected automatically
- **Domain context** — describe your data in Markdown for better AI understanding
- **Example queries** — seed the AI with question/SQL pairs
- **Reusable prompt recipes** — project-specific analysis prompts derived from the schema
- **Multi-chart dashboard** — pin results to a dashboard with configurable layouts
- **Session export** — export conversations as shareable HTML pages
- **Keyboard shortcuts** — `?` to see all shortcuts, `/` to focus input
- **Streaming responses** — real-time SSE streaming from the LLM

## Architecture

datasight uses the Anthropic SDK directly with a FastAPI backend and a
lightweight HTML/JS frontend. Swap in GitHub Models or Ollama by setting
`LLM_PROVIDER` in `.env`. No heavy frameworks — just Python, SQL, and
Plotly.

```
datasight run / datasight ask / datasight profile / datasight quality
  → LLM provider (Anthropic / GitHub Models / Ollama)
    → DuckDB / PostgreSQL / SQLite / Flight SQL
    → Plotly chart generator
  → Web UI (SSE streaming) or CLI output
```

## Documentation

```bash
pip install "datasight[dev] @ git+https://github.com/dsgrid/datasight.git"
uv run zensical serve
uv run zensical build
uv run python scripts/generate_cli_reference.py
```

## Development Tests

```bash
# Full Python test suite
pytest

# Frontend structure tests for the split vanilla JS modules
node --test tests/test_web_helpers.js tests/test_web_ui_refactor.js

# FastAPI-backed web UI smoke tests
pytest -q tests/test_web_ui_smoke.py
```
