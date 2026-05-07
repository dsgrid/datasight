# Glossary

Short definitions for the terms you'll encounter throughout these docs.

## API key

A password-like string you get from an AI provider (Anthropic, OpenAI, etc.) that lets
datasight call their service on your behalf. It looks like `sk-ant-...` (Anthropic) or
`sk-...` (OpenAI). Keep it secret — anyone who has it can make calls charged to your
account. datasight reads it from your `.env` file and never uploads it anywhere.

## Context window

The maximum amount of text an LLM can read in a single call, measured in *tokens*
(roughly one token per word). If your database has many tables or very wide tables, the
schema description can exceed this limit — you'll see a "request too large" error.
See [Limit schema sent to the LLM](project-setup/how-to/schema-config.md) for how to
trim it down.

## Deterministic

A command or step that always produces the same result and does not call an LLM. In the
datasight docs this usually means "runs without calling the AI" — commands like
`datasight profile`, `datasight quality`, and `datasight tidy suggest` fall into
this category. Contrast with `datasight ask`, which sends a request to the LLM.

## Dimension

A categorical column useful for grouping or filtering — fuel type, state, plant name,
scenario. datasight infers likely dimensions from column names and distinct-value counts.

## DuckDB

A fast in-process analytical database. It's the default backend for datasight because
it needs no server setup and handles CSV, Parquet, and Excel files natively. If you
don't have an existing database, DuckDB is the right choice.

## .env file

A plain text file containing key=value pairs that configure datasight for a project.
A typical project `.env` looks like:

```
ANTHROPIC_API_KEY=sk-ant-...
DB_MODE=duckdb
DB_PATH=./my_data.duckdb
```

datasight reads this file at startup. Never commit credentials to version control.

## LLM (Large Language Model)

The AI model datasight uses to translate your natural-language questions into SQL.
Examples: Claude (Anthropic), GPT-4o (OpenAI), Llama (Meta, run locally via Ollama).
datasight is not tied to a specific model — you configure which one to use in `.env`.

## Measure / metric

A numeric column you'd typically sum or average — net generation in MWh, capacity in
MW, cost in dollars. datasight infers measure candidates from column names, types, and
value distributions.

## Ollama

Software for running AI models locally on your own computer. Use it when your data is
too sensitive to send to a cloud API, or when you want zero per-query cost and are
willing to run a GPU. See [Choosing an LLM](use/concepts/choosing-an-llm.md).

## Parquet

A compact binary file format for tabular data — like a highly compressed CSV. Parquet
files are significantly smaller and faster to load than CSV for large datasets.
datasight reads them without any conversion step.

## Project

A directory that datasight treats as a named workspace. At minimum it contains a
`schema_description.md`. A full project also has `.env` (database connection and API
keys), `queries.yaml` (example questions), and optional YAML files for semantic
measures and time series. See [Set up your first project](project-setup/tutorials/set-up-project.md).

## Schema

The structure of a database — its tables, column names, and data types. datasight
introspects your schema automatically. You supplement it with a `schema_description.md`
that explains what the columns mean in plain English, which helps the AI write
better SQL.

## SQL

Structured Query Language — the language databases use to answer questions like "sum
generation by fuel type." datasight writes SQL for you. You don't need to know SQL to
use datasight, but being able to read it helps you verify that the AI's answers are
correct.

## SQLite

A lightweight file-based database engine. Like DuckDB it needs no server, but it lacks
DuckDB's analytical query speed and native Parquet support. Useful if your data already
lives in a `.sqlite` file.

## Tidy data

A tabular shape where each variable is one column and each observation is one row.
A tidy `generation` table has a `month` column and a `value` column — not 12 separate
columns named `jan`, `feb`, … `dec`. Tidy data is much easier for both SQL and the
LLM agent to query. See [Tidy a wide-month spreadsheet](use/tutorials/tidy.md).

## Token

The unit AI providers use to measure and bill for LLM usage. Roughly one token per
word (or ~4 characters). A database schema with many tables can easily reach thousands
of tokens.
