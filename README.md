# datasight

AI-powered database exploration with natural language.

datasight connects an AI agent (Claude) to your DuckDB database and provides a
web UI where you can ask questions in natural language. The agent writes SQL, runs
queries, and generates interactive Plotly visualizations.

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

## Features

- **Natural language queries** — ask questions in English, get SQL + results
- **Interactive charts** — Plotly visualizations with chart-type switching
- **Schema browser** — sidebar with tables, columns, and example queries
- **Schema auto-discovery** — tables, columns, and types detected automatically
- **Domain context** — describe your data in Markdown for better AI understanding
- **Example queries** — seed the AI with question/SQL pairs
- **Local or remote** — connect to local DuckDB files or remote Flight SQL servers
- **Streaming responses** — real-time SSE streaming from the LLM

## Architecture

datasight uses the Anthropic SDK directly with a FastAPI backend and a
lightweight HTML/JS frontend. No heavy frameworks — just Python, SQL, and
Plotly.

```
datasight run
  → FastAPI + uvicorn
    → Anthropic Claude (tool use)
      → DuckDB or Flight SQL
      → Plotly chart generator
    → SSE streaming to browser
```

## Documentation

```bash
pip install "datasight[docs] @ git+https://github.com/dsgrid/datasight.git"
cd docs && make html
```
