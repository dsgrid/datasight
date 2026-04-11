# CLI reference

This page is generated from the Click command tree in `datasight.cli`.
Update it with `python scripts/generate_cli_reference.py`.

## Common workflows

### Run batch questions

```bash
datasight ask --file questions.txt --output-dir batch-output
datasight ask --file questions.yaml --output-dir batch-output
datasight ask --file questions.jsonl --output-dir batch-output
```

### Inspect a project without the LLM

```bash
datasight profile
datasight profile --table orders
datasight profile --column orders.order_date
```

### Run deterministic audits and suggestions

```bash
datasight quality --table orders
datasight dimensions --table orders
datasight trends --table orders
datasight recipes --table orders
```

### Check project health

```bash
datasight doctor
datasight doctor --format markdown -o doctor.md
```

## `datasight`

datasight — AI-powered database exploration with natural language.

```bash
datasight [OPTIONS] COMMAND [ARGS]...
```

**Parameters**

| Name | Details |
| --- | --- |
| `--version` | Show the version and exit. |

**Subcommands**

- `init`: Create a new datasight project with template files.
- `demo`: Download an EIA energy demo dataset and create a ready-to-run project.
- `generate`: Generate schema_description.md, queries.yaml, and measures.yaml from your database.
- `run`: Start the datasight web UI.
- `verify`: Verify LLM-generated SQL against expected results.
- `ask`: Ask a question about your data from the command line.
- `profile`: Profile your dataset without using the LLM.
- `measures`: Surface likely measures and default aggregations without using the LLM.
- `quality`: Run a deterministic quality audit without using the LLM.
- `dimensions`: Surface likely grouping dimensions without using the LLM.
- `trends`: Surface likely trend analyses without using the LLM.
- `recipes`: Generate and run reusable deterministic prompt recipes.
- `doctor`: Check project configuration, local files, and database connectivity.
- `export`: Export a conversation session as a self-contained HTML page.
- `log`: Display the SQL query log in a formatted table.
- `report`: Manage saved reports.

### `datasight init`

Create a new datasight project with template files.

PROJECT_DIR defaults to the current directory.

```bash
datasight init [OPTIONS] [PROJECT_DIR]
```

**Parameters**

| Name | Details |
| --- | --- |
| `PROJECT_DIR` |   |
| `--overwrite` | Overwrite existing files. |

### `datasight demo`

Download an EIA energy demo dataset and create a ready-to-run project.

Downloads cleaned EIA-923 and EIA-860 data from the PUDL project's public
data releases. Creates a DuckDB database with generation, fuel consumption,
and plant data, along with pre-written schema descriptions and example queries.

PROJECT_DIR defaults to the current directory.

```bash
datasight demo [OPTIONS] [PROJECT_DIR]
```

**Parameters**

| Name | Details |
| --- | --- |
| `PROJECT_DIR` |   |
| `--min-year` | Earliest year to include (default: 2020). Default: `2020`. |

### `datasight generate`

Generate schema_description.md, queries.yaml, and measures.yaml from your database.

Connects to the database, inspects tables and columns, samples
code/enum columns, and asks the LLM to produce documentation
and example queries.

Optionally pass one or more Parquet, CSV, or DuckDB files directly:

    datasight generate sales.parquet returns.csv

```bash
datasight generate [OPTIONS] [FILES]...
```

**Parameters**

| Name | Details |
| --- | --- |
| `FILES` |   |
| `--project-dir` | Project directory containing .env. Default: `.`. |
| `--model` | Model name (overrides .env). |
| `--overwrite` | Overwrite existing files. |
| `--table`, `-t` | Table or view to include (can be specified multiple times). If omitted, all tables are included. |
| `-v`, `--verbose` | Enable debug logging. |

### `datasight run`

Start the datasight web UI.

If the current directory contains schema_description.md, it will be
auto-loaded as the project. Otherwise, use the UI to select a project,
or pass --project-dir to specify one explicitly.

```bash
datasight run [OPTIONS]
```

**Parameters**

| Name | Details |
| --- | --- |
| `--port` | Web UI port (default: 8084). |
| `--host` | Bind address. Default: `0.0.0.0`. |
| `--model` | LLM model name (overrides .env). |
| `--project-dir` | Auto-load this project on startup (optional). |
| `-v`, `--verbose` | Enable debug logging. |

### `datasight verify`

Verify LLM-generated SQL against expected results.

Runs each question from queries.yaml through the full LLM pipeline,
executes the generated SQL, and compares results against expected values.
Use this to validate correctness across different models and providers.

Add expected results to queries.yaml entries:

  - question: "Top 3 states by generation"
    sql: |
      SELECT state, SUM(mwh) AS total
      FROM generation GROUP BY state
      ORDER BY total DESC LIMIT 3
    expected:
      row_count: 3
      columns: [state, total]
      contains: ["CA", "TX"]

```bash
datasight verify [OPTIONS]
```

**Parameters**

| Name | Details |
| --- | --- |
| `--project-dir` | Project directory containing .env and queries.yaml. Default: `.`. |
| `--model` | Model name (overrides .env). |
| `--queries` | Path to queries YAML file (default: queries.yaml in project dir). |
| `-v`, `--verbose` | Enable debug logging. |

### `datasight ask`

Ask a question about your data from the command line.

Runs the full LLM agent loop without starting a web server.
Results are printed to the console.

Examples:
  datasight ask "What are the top 5 states by generation?"
  datasight ask "Show generation by year" --chart-format html -o chart.html
  datasight ask "Top 5 states" --format csv -o results.csv
  datasight ask "Top 5 states" --print-sql
  datasight ask "Top 5 states" --sql-script top-states.sql

```bash
datasight ask [OPTIONS] [QUESTION]
```

**Parameters**

| Name | Details |
| --- | --- |
| `QUESTION` |   |
| `--project-dir` | Project directory containing .env and config files. Default: `.`. |
| `--model` | Model name (overrides .env). |
| `--format` | Output format for query results (default: table). Default: `table`. |
| `--chart-format` | Save chart output in this format (requires --output). |
| `--output`, `-o` | Output file path for chart or data export. |
| `--file` | Read one question per line from a text file. |
| `--output-dir` | Directory for per-question batch outputs (only with --file). |
| `--print-sql` | Print the SQL queries executed by the agent to the console. |
| `--sql-script` | Write executed queries to a SQL script that materializes results into auto-named tables (CREATE OR REPLACE). |
| `-v`, `--verbose` | Enable debug logging. |

### `datasight profile`

Profile your dataset without using the LLM.

```bash
datasight profile [OPTIONS]
```

**Parameters**

| Name | Details |
| --- | --- |
| `--project-dir` | Project directory containing .env and config files. Default: `.`. |
| `--table` | Profile a specific table. |
| `--column` | Profile a specific column as table.column. |
| `--format` | Output format (default: table). Default: `table`. |
| `--output`, `-o` | Write the profile output to a file instead of stdout. |

### `datasight measures`

Surface likely measures and default aggregations without using the LLM.

```bash
datasight measures [OPTIONS]
```

**Parameters**

| Name | Details |
| --- | --- |
| `--project-dir` | Project directory containing .env and config files. Default: `.`. |
| `--table` | Inspect measures for a specific table. |
| `--scaffold` | Write an editable measures.yaml scaffold and exit. |
| `--overwrite` | Overwrite an existing scaffold file. |
| `--format` | Output format (default: table). Default: `table`. |
| `--output`, `-o` | Write the measure overview to a file instead of stdout. |

### `datasight quality`

Run a deterministic quality audit without using the LLM.

```bash
datasight quality [OPTIONS]
```

**Parameters**

| Name | Details |
| --- | --- |
| `--project-dir` | Project directory containing .env and config files. Default: `.`. |
| `--table` | Audit a specific table. |
| `--format` | Output format (default: table). Default: `table`. |
| `--output`, `-o` | Write the quality audit to a file instead of stdout. |

### `datasight dimensions`

Surface likely grouping dimensions without using the LLM.

```bash
datasight dimensions [OPTIONS]
```

**Parameters**

| Name | Details |
| --- | --- |
| `--project-dir` | Project directory containing .env and config files. Default: `.`. |
| `--table` | Inspect dimensions for a specific table. |
| `--format` | Output format (default: table). Default: `table`. |
| `--output`, `-o` | Write the dimension overview to a file instead of stdout. |

### `datasight trends`

Surface likely trend analyses without using the LLM.

Optionally pass one or more Parquet, CSV, or DuckDB files directly:

    datasight trends sales.parquet returns.parquet

```bash
datasight trends [OPTIONS] [FILES]...
```

**Parameters**

| Name | Details |
| --- | --- |
| `FILES` |   |
| `--project-dir` | Project directory containing .env and config files. |
| `--table` | Suggest trends for a specific table. |
| `--format` | Output format (default: table). Default: `table`. |
| `--output`, `-o` | Write the trend overview to a file instead of stdout. |

### `datasight recipes`

Generate and run reusable deterministic prompt recipes.

```bash
datasight recipes [OPTIONS] COMMAND [ARGS]...
```

**Subcommands**

- `list`: List reusable deterministic prompt recipes for a project.
- `run`: Run a generated recipe by ID through the normal ask pipeline.

#### `datasight recipes list`

List reusable deterministic prompt recipes for a project.

```bash
datasight recipes list [OPTIONS]
```

**Parameters**

| Name | Details |
| --- | --- |
| `--project-dir` | Project directory containing .env and config files. Default: `.`. |
| `--table` | Generate recipes for a specific table. |
| `--format` | Output format (default: table). Default: `table`. |
| `--output`, `-o` | Write the recipes output to a file instead of stdout. |

#### `datasight recipes run`

Run a generated recipe by ID through the normal ask pipeline.

```bash
datasight recipes run [OPTIONS] RECIPE_ID
```

**Parameters**

| Name | Details |
| --- | --- |
| `RECIPE_ID` |   |
| `--project-dir` | Project directory containing .env and config files. Default: `.`. |
| `--table` | Use recipes generated for a specific table. |
| `--model` | Model name (overrides .env). |
| `--format` | Output format for query results (default: table). Default: `table`. |
| `--chart-format` | Save chart output in this format (requires --output). |
| `--output`, `-o` | Output file path for chart or data export. |
| `-v`, `--verbose` | Enable debug logging. |

### `datasight doctor`

Check project configuration, local files, and database connectivity.

```bash
datasight doctor [OPTIONS]
```

**Parameters**

| Name | Details |
| --- | --- |
| `--project-dir` | Project directory containing .env and config files. Default: `.`. |
| `--format` | Output format (default: table). Default: `table`. |
| `--output`, `-o` | Write doctor output to a file instead of stdout. |

### `datasight export`

Export a conversation session as a self-contained HTML page.

SESSION_ID is the conversation ID (use --list-sessions to see available IDs).

Examples:
  datasight export --list-sessions
  datasight export abc123def -o my-analysis.html
  datasight export abc123def --exclude 2,3

```bash
datasight export [OPTIONS] SESSION_ID
```

**Parameters**

| Name | Details |
| --- | --- |
| `SESSION_ID` |   |
| `--output`, `-o` | Output file path (default: <session_id>.html). |
| `--project-dir` | Project directory containing .datasight/conversations/. Default: `.`. |
| `--exclude` | Comma-separated turn indices to exclude (0-based, each turn is a Q&A pair). |
| `--list-sessions` | List available sessions and exit. |

### `datasight log`

Display the SQL query log in a formatted table.

```bash
datasight log [OPTIONS]
```

**Parameters**

| Name | Details |
| --- | --- |
| `--project-dir` | Project directory containing query_log.jsonl. Default: `.`. |
| `--tail` | Show last N entries (default: 20). Default: `20`. |
| `--errors` | Show only failed queries. |
| `--full` | Show full SQL and user question. |
| `--cost` | Show LLM cost summary. |
| `--sql` | Print raw SQL for query # (shown in the # column). Ready to copy-paste. |

### `datasight report`

Manage saved reports.

```bash
datasight report [OPTIONS] COMMAND [ARGS]...
```

**Subcommands**

- `list`: List all saved reports.
- `run`: Re-execute a saved report against fresh data.
- `delete`: Delete a saved report.

#### `datasight report list`

List all saved reports.

```bash
datasight report list [OPTIONS]
```

**Parameters**

| Name | Details |
| --- | --- |
| `--project-dir` | Project directory. Default: `.`. |

#### `datasight report run`

Re-execute a saved report against fresh data.

REPORT_ID is the numeric ID shown by 'datasight report list'.

```bash
datasight report run [OPTIONS] REPORT_ID
```

**Parameters**

| Name | Details |
| --- | --- |
| `REPORT_ID` |   |
| `--project-dir` | Project directory containing .env and config files. Default: `.`. |
| `--format` | Output format for query results (default: table). Default: `table`. |
| `--chart-format` | Save chart output in this format (requires --output). |
| `--output`, `-o` | Output file path for chart or data export. |

#### `datasight report delete`

Delete a saved report.

REPORT_ID is the numeric ID shown by 'datasight report list'.

```bash
datasight report delete [OPTIONS] REPORT_ID
```

**Parameters**

| Name | Details |
| --- | --- |
| `REPORT_ID` |   |
| `--project-dir` | Project directory. Default: `.`. |
