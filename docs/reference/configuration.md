# Configuration reference

datasight is configured via environment variables, typically in a `.env` file
in the project directory. CLI flags override `.env` values.

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ANTHROPIC_API_KEY` | *(required)* | Anthropic API key |
| `ANTHROPIC_MODEL` | `claude-sonnet-4-20250514` | Model name |
| `ANTHROPIC_BASE_URL` | — | Custom API endpoint (e.g. Azure AI Foundry) |
| `DB_MODE` | `local` | `local` for DuckDB file, `flightsql` for remote |
| `DB_PATH` | `./database.duckdb` | Path to local DuckDB file (ignored when `DB_MODE=flightsql`) |
| `FLIGHT_SQL_URI` | `grpc://localhost:31337` | Flight SQL server URI |
| `FLIGHT_SQL_TOKEN` | — | Bearer token for Flight SQL auth |
| `FLIGHT_SQL_USERNAME` | — | Username for Flight SQL basic auth |
| `FLIGHT_SQL_PASSWORD` | — | Password for Flight SQL basic auth |
| `SCHEMA_DESCRIPTION_PATH` | `./schema_description.md` | Schema description file |
| `EXAMPLE_QUERIES_PATH` | `./queries.yaml` | Example queries file |
| `PORT` | `8084` | Web UI port |

## Project files

A datasight project directory contains:

| File | Required | Description |
|------|----------|-------------|
| `.env` | Yes | API key and connection settings |
| `schema_description.md` | No | Domain context for the AI ([guide](../how-to/schema-description.md)). Always a local file, even when using Flight SQL. |
| `queries.yaml` | No | Example question/SQL pairs ([guide](../how-to/example-queries.md)). Always a local file, even when using Flight SQL. |

## Precedence

Settings are resolved in this order (highest priority first):

1. CLI flags (`--port`, `--db-mode`, `--db-path`, `--model`)
2. Environment variables
3. `.env` file in the project directory
4. Built-in defaults
