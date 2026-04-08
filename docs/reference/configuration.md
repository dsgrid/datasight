# Configuration reference

datasight is configured via environment variables, typically in a `.env` file
in the project directory. CLI flags override `.env` values.

## Environment variables

### LLM provider

| Variable | Default | Description |
|----------|---------|-------------|
| `LLM_PROVIDER` | `anthropic` | LLM backend: `anthropic`, `github`, or `ollama` |

### Anthropic settings (when `LLM_PROVIDER=anthropic`)

| Variable | Default | Description |
|----------|---------|-------------|
| `ANTHROPIC_API_KEY` | *(required)* | Anthropic API key |
| `ANTHROPIC_MODEL` | `claude-haiku-4-5-20251001` | Model name. Haiku is recommended for most use cases; it handles SQL generation well at a fraction of the cost of larger models. |
| `ANTHROPIC_BASE_URL` | — | Custom API endpoint (e.g. Azure AI Foundry) |

### GitHub Models settings (when `LLM_PROVIDER=github`)

| Variable | Default | Description |
|----------|---------|-------------|
| `GITHUB_TOKEN` | *(required)* | GitHub personal access token. Run `gh auth token` if you use the GitHub CLI. |
| `GITHUB_MODELS_MODEL` | `gpt-4o` | Model name available on [GitHub Models](https://github.com/marketplace/models) |
| `GITHUB_MODELS_BASE_URL` | `https://models.inference.ai.azure.com` | GitHub Models API endpoint |

### Ollama settings (when `LLM_PROVIDER=ollama`)

| Variable | Default | Description |
|----------|---------|-------------|
| `OLLAMA_MODEL` | `qwen3.5:35b-a3b` | Ollama model name (must support tool calling) |
| `OLLAMA_BASE_URL` | `http://localhost:11434/v1` | Ollama API endpoint |

### Database settings

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_MODE` | `duckdb` | Database type: `duckdb`, `sqlite`, `postgres`, or `flightsql` |
| `DB_PATH` | `./database.duckdb` | Path to DuckDB or SQLite file (used when `DB_MODE=duckdb` or `sqlite`) |

#### PostgreSQL settings (when `DB_MODE=postgres`)

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_URL` | — | Connection string (takes precedence over individual fields). Example: `postgresql://user:pass@host:5432/dbname` |
| `POSTGRES_HOST` | `localhost` | Database host |
| `POSTGRES_PORT` | `5432` | Database port |
| `POSTGRES_DATABASE` | — | Database name |
| `POSTGRES_USER` | — | Username |
| `POSTGRES_PASSWORD` | — | Password |
| `POSTGRES_SSLMODE` | `prefer` | SSL mode: `disable`, `prefer`, `require`, `verify-ca`, `verify-full` |

For production, use `POSTGRES_SSLMODE=verify-full` and consider using a
`.pgpass` file or environment variables rather than storing passwords in `.env`.

#### Flight SQL settings (when `DB_MODE=flightsql`)

| Variable | Default | Description |
|----------|---------|-------------|
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
| `schema_description.md` | No | Domain context for the AI ([guide](../project-developer/schema-description.md)). Always a local file, even when using Flight SQL. |
| `queries.yaml` | No | Example question/SQL pairs ([guide](../project-developer/example-queries.md)). Always a local file, even when using Flight SQL. |
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
| `.datasight/reports.json` | Saved reports — rerunnable queries with optional chart specs. |
| `.datasight/dashboard.json` | Pinned dashboard items and layout. |

## Precedence

Settings are resolved in this order (highest priority first):

1. CLI flags (`--port`, `--db-mode`, `--db-path`, `--model`)
2. Environment variables
3. `.env` file in the project directory
4. Built-in defaults
