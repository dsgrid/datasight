# Configuration reference

datasight is configured via environment variables, loaded from two `.env`
files: a per-project `.env` in the project directory, and an optional
**user-global** `.env` shared across every project. CLI flags override both.

## Global vs project config

Most users want to store API keys and tokens **once**, not in every project.
Run:

```bash
datasight config init
```

…to create `~/.config/datasight/.env` (honors `XDG_CONFIG_HOME`) from a
template. Put credentials such as `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, and
`GITHUB_TOKEN` there. Then each project's `.env` only needs to set provider,
model, and database — for example:

```bash
# project .env
LLM_PROVIDER=openai
OPENAI_MODEL=gpt-4o
DB_MODE=duckdb
DB_PATH=./my_database.duckdb
```

Per-project values **override** the global file, so you can still pin a
specific API key or model on a single project when needed.

To inspect which provider, model, and database you'd connect to right now —
and which config files were loaded — run:

```bash
datasight config show
```

## Environment variables

### LLM provider

| Variable | Default | Description |
|----------|---------|-------------|
| `LLM_PROVIDER` | `anthropic` | LLM backend: `anthropic`, `openai`, `github`, or `ollama` |

For help picking a provider, see [Choosing an LLM](../concepts/choosing-an-llm.md).

### Anthropic settings (when `LLM_PROVIDER=anthropic`)

| Variable | Default | Description |
|----------|---------|-------------|
| `ANTHROPIC_API_KEY` | *(required)* | Anthropic API key |
| `ANTHROPIC_MODEL` | `claude-haiku-4-5-20251001` | Model name. Haiku is recommended for most use cases; it handles SQL generation well at a fraction of the cost of larger models. |
| `ANTHROPIC_BASE_URL` | — | Custom API endpoint (e.g. Azure AI Foundry, AWS Bedrock gateway) |

### OpenAI settings (when `LLM_PROVIDER=openai`)

| Variable | Default | Description |
|----------|---------|-------------|
| `OPENAI_API_KEY` | *(required)* | OpenAI API key |
| `OPENAI_MODEL` | `gpt-4o-mini` | Model name. `gpt-4o-mini` handles most SQL generation well; step up to `gpt-4o` for harder schemas. |
| `OPENAI_BASE_URL` | `https://api.openai.com/v1` | Custom API endpoint (e.g. Azure OpenAI, corporate gateway) |

### GitHub Models settings (when `LLM_PROVIDER=github`)

| Variable | Default | Description |
|----------|---------|-------------|
| `GITHUB_TOKEN` | *(required)* | Token with GitHub Models access. Either the output of `gh auth token` (if you use the GitHub CLI) or a fine-grained PAT with the `Models: read` permission — *not* a classic PAT or git push credential. |
| `GITHUB_MODELS_MODEL` | `gpt-4o` | Model name available on [GitHub Models](https://github.com/marketplace/models) |
| `GITHUB_MODELS_BASE_URL` | `https://models.inference.ai.azure.com` | GitHub Models API endpoint |

### Ollama settings (when `LLM_PROVIDER=ollama`)

| Variable | Default | Description |
|----------|---------|-------------|
| `OLLAMA_MODEL` | `qwen2.5:7b` | Ollama model name (must support tool calling). `qwen2.5:7b` works well for CLI queries; for the web UI with visualizations, try `qwen2.5:14b`. |
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
| `SCHEMA_INCLUDE_MAX_BYTES` | `20000` | Per-URL size cap for `[include:…](url)` directives inside the schema description. Set to `0` to skip include resolution entirely — useful when fetched pages push the prompt past a small-context model's token limit. |
| `SCHEMA_INCLUDE_ALLOW_PRIVATE_HOSTS` | `false` | Opt-in switch that disables the SSRF guard on `[include:…](url)` directives, allowing fetches from `localhost`, private IP ranges, and `.internal`/`.local` hostnames. Leave off unless a project intentionally references an internal documentation server. |
| `PORT` | `8084` | Web UI port |
| `QUERY_LOG_ENABLED` | `false` | Enable SQL query logging ([guide](../end-user/how-to/review-query-log.md)) |
| `QUERY_LOG_PATH` | `./query_log.jsonl` | Path to query log file |
| `CLARIFY_SQL` | `true` | Ask clarifying questions for ambiguous queries ([guide](../end-user/reference/query-confidence-toggles.md)) |
| `CONFIRM_SQL` | `false` | Require user approval before executing SQL ([guide](../end-user/reference/query-confidence-toggles.md)) |
| `EXPLAIN_SQL` | `false` | Show plain-English SQL explanations ([guide](../end-user/reference/query-confidence-toggles.md)) |
| `SHOW_PROVENANCE` | `false` | Show copyable run details in the web UI |
| `SQL_CACHE_MAX_BYTES` | `1073741824` (1 GiB) | In-memory SQL result cache budget ([concept](../concepts/sql-result-cache.md)). Set to `0` to disable. |
| `MAX_COST_USD_PER_TURN` | `1.0` | Per-question LLM spend cap (USD). The agent aborts with a visible stop message when the running estimated cost exceeds this value. Set to `none`, `off`, or `disabled` to turn off the check. |

## Project files

A datasight project directory contains:

| File | Required | Description |
|------|----------|-------------|
| `.env` | Yes | API key and connection settings |
| `schema_description.md` | No | Domain context for the AI ([guide](../project-developer/schema-description.md)). Always a local file, even when using Flight SQL. |
| `queries.yaml` | No | Example question/SQL pairs ([guide](../project-developer/example-queries.md)). Always a local file, even when using Flight SQL. |
| `query_log.jsonl` | No | SQL query log, created when logging is enabled ([guide](../end-user/how-to/review-query-log.md)) |
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
2. Environment variables (shell exports)
3. `.env` file in the project directory
4. User-global `.env` (`~/.config/datasight/.env`)
5. Built-in defaults
