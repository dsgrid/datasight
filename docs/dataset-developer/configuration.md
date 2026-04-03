# Configuration reference

datasight is configured via environment variables, typically in a `.env` file
in the project directory. CLI flags override `.env` values.

## Environment variables

### LLM provider

| Variable | Default | Description |
|----------|---------|-------------|
| `LLM_PROVIDER` | `anthropic` | LLM backend: `anthropic` or `ollama` |

### Anthropic settings (when `LLM_PROVIDER=anthropic`)

| Variable | Default | Description |
|----------|---------|-------------|
| `ANTHROPIC_API_KEY` | *(required)* | Anthropic API key |
| `ANTHROPIC_MODEL` | `claude-sonnet-4-20250514` | Model name. Sonnet is recommended for most use cases; Opus is unnecessary for SQL generation and significantly more expensive. |
| `ANTHROPIC_BASE_URL` | — | Custom API endpoint (e.g. Azure AI Foundry) |

### Ollama settings (when `LLM_PROVIDER=ollama`)

| Variable | Default | Description |
|----------|---------|-------------|
| `OLLAMA_MODEL` | `qwen3.5:35b-a3b` | Ollama model name (must support tool calling) |
| `OLLAMA_BASE_URL` | `http://localhost:11434/v1` | Ollama API endpoint |

### Database settings

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_MODE` | `local` | `local` for DuckDB file, `flightsql` for remote |
| `DB_PATH` | `./database.duckdb` | Path to local DuckDB file (ignored when `DB_MODE=flightsql`) |
| `FLIGHT_SQL_URI` | `grpc://localhost:31337` | Flight SQL server URI |
| `FLIGHT_SQL_TOKEN` | — | Bearer token for Flight SQL auth |
| `FLIGHT_SQL_USERNAME` | — | Username for Flight SQL basic auth |
| `FLIGHT_SQL_PASSWORD` | — | Password for Flight SQL basic auth |

### Other settings

| Variable | Default | Description |
|----------|---------|-------------|
| `SCHEMA_DESCRIPTION_PATH` | `./schema_description.md` | Schema description file |
| `EXAMPLE_QUERIES_PATH` | `./queries.yaml` | Example queries file |
| `PORT` | `8084` | Web UI port |
| `QUERY_LOG_ENABLED` | `false` | Enable SQL query logging ([guide](../end-user/query-log.md)) |
| `QUERY_LOG_PATH` | `./query_log.jsonl` | Path to query log file |
| `CLARIFY_SQL` | `true` | Ask clarifying questions for ambiguous queries ([guide](../end-user/query-confidence.md)) |
| `CONFIRM_SQL` | `false` | Require user approval before executing SQL ([guide](../end-user/query-confidence.md)) |
| `EXPLAIN_SQL` | `false` | Show plain-English SQL explanations ([guide](../end-user/query-confidence.md)) |

## Project files

A datasight project directory contains:

| File | Required | Description |
|------|----------|-------------|
| `.env` | Yes | API key and connection settings |
| `schema_description.md` | No | Domain context for the AI ([guide](schema-description.md)). Always a local file, even when using Flight SQL. |
| `queries.yaml` | No | Example question/SQL pairs ([guide](example-queries.md)). Always a local file, even when using Flight SQL. |
| `query_log.jsonl` | No | SQL query log, created when logging is enabled ([guide](../end-user/query-log.md)) |
| `.datasight/` | No | Auto-created directory for app state (see below) |

### `.datasight/` directory

datasight stores persistent state in a `.datasight/` directory inside the
project directory. This is created automatically and should be added to
`.gitignore`.

| Path | Description |
|------|-------------|
| `.datasight/conversations/` | Saved chat conversations as JSON files. Each file contains the message history and UI event log for replay. |
| `.datasight/bookmarks.json` | Bookmarked SQL queries with names. |

## Precedence

Settings are resolved in this order (highest priority first):

1. CLI flags (`--port`, `--db-mode`, `--db-path`, `--model`)
2. Environment variables
3. `.env` file in the project directory
4. Built-in defaults
