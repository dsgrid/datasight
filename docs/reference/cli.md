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
datasight profile --table generation_fuel
datasight profile --column generation_fuel.report_date
```

### Run deterministic audits and suggestions

```bash
datasight quality --table generation_fuel
datasight dimensions --table generation_fuel
datasight trends --table generation_fuel
datasight recipes list --table generation_fuel
```

### Check project health

```bash
datasight doctor
datasight doctor --format markdown -o doctor.md
```

## `datasight`

datasight — AI-powered data exploration with natural language.

```bash
datasight [OPTIONS] COMMAND [ARGS]...
```

**Parameters**

| Name | Details |
| --- | --- |
| `--version` | Show the version and exit. |

**Subcommands**

- `init`: Create blank datasight project template files.
- `config`: Manage user-global datasight configuration.
- `demo`: Create ready-to-run demo projects with sample datasets.
- `generate`: Generate schema_description.md, queries.yaml, measures.yaml, and time_series.yaml from your database.
- `run`: Start the datasight web UI.
- `verify`: Verify LLM-generated SQL against expected results.
- `ask`: Ask a question about your data from the command line.
- `profile`: Profile your dataset - row counts, date coverage, and column statistics.
- `measures`: Surface likely measures and default aggregations.
- `quality`: Audit data quality - nulls, suspicious ranges, and date coverage.
- `integrity`: Audit cross-table referential integrity - keys, orphans, and join risks.
- `distribution`: Profile value distributions - percentiles, outliers, and measure flags.
- `validate`: Run declarative validation rules against the database.
- `audit-report`: Generate a comprehensive audit report combining all checks.
- `dimensions`: Surface likely grouping dimensions and category breakdowns.
- `trends`: Surface likely trend analyses and chart recommendations.
- `inspect`: Run all analyses on Parquet, CSV, Excel, or DuckDB files and print results.
- `recipes`: Generate and run reusable deterministic prompt recipes.
- `doctor`: Check project configuration, local files, and database connectivity.
- `export`: Export a conversation session as a self-contained HTML page or Python script.
- `log`: Display the SQL query log in a formatted table.
- `report`: Manage saved reports.
- `templates`: Save and re-apply dashboards as templates across datasets.

### `datasight init`

Create blank datasight project template files.

PROJECT_DIR defaults to the current directory.

Use this when you want to fill in .env, schema_description.md,

queries.yaml, and time_series.yaml by hand.

If you already have a DuckDB/SQLite database or CSV/Parquet/Excel

files and want datasight to inspect them and draft these files, use:

```
datasight generate <file>...
```

```bash
datasight init [OPTIONS] [PROJECT_DIR]
```

**Parameters**

| Name | Details |
| --- | --- |
| `PROJECT_DIR` |   |
| `--overwrite` | Overwrite existing files. |

### `datasight config`

Manage user-global datasight configuration.

The user-global config file (~/.config/datasight/.env) holds API

keys and tokens shared across every datasight project. Per-project

.env files override its values, so each project can still pick its

own LLM provider, model, and database.

Examples:

```
datasight config init
datasight config show
```

```bash
datasight config [OPTIONS] COMMAND [ARGS]...
```

**Subcommands**

- `init`: Create the user-global config file (~/.config/datasight/.env).
- `show`: Show the resolved datasight configuration and where it loaded from.

#### `datasight config init`

Create the user-global config file (~/.config/datasight/.env).

Stores API keys and tokens in one place so per-project .env files only
need to set provider, model, and database settings.

```bash
datasight config init [OPTIONS]
```

**Parameters**

| Name | Details |
| --- | --- |
| `--overwrite` | Overwrite the existing global config file. |

#### `datasight config show`

Show the resolved datasight configuration and where it loaded from.

```bash
datasight config show [OPTIONS]
```

### `datasight demo`

Create ready-to-run demo projects with sample datasets.

Examples:

```
datasight demo eia-generation eia-demo
datasight demo dsgrid-tempo tempo-demo
datasight demo time-validation time-demo
```

```bash
datasight demo [OPTIONS] COMMAND [ARGS]...
```

**Subcommands**

- `eia-generation`: Download an EIA energy demo dataset and create a ready-to-run project.
- `dsgrid-tempo`: Download dsgrid TEMPO EV charging demand projections.
- `time-validation`: Generate a synthetic energy consumption dataset with planted time errors.

#### `datasight demo eia-generation`

Download an EIA energy demo dataset and create a ready-to-run project.

Downloads cleaned EIA-923 and EIA-860 data from the PUDL project's public
data releases. Creates a DuckDB database with generation, fuel consumption,
and plant data, along with pre-written schema descriptions and example queries.

PROJECT_DIR defaults to the current directory.

Example:

```
datasight demo eia-generation eia-demo --min-year 2021
```

```bash
datasight demo eia-generation [OPTIONS] [PROJECT_DIR]
```

**Parameters**

| Name | Details |
| --- | --- |
| `PROJECT_DIR` |   |
| `--min-year` | Earliest year to include (default: 2020). Default: `2020`. |

#### `datasight demo dsgrid-tempo`

Download dsgrid TEMPO EV charging demand projections.

Downloads hourly and annual EV charging demand data from NLR's TEMPO
project (published on OEDI). Creates a DuckDB database with charging
profiles at census-division level, plus annual summaries by state and
county. Covers three adoption scenarios from 2024 to 2050.

Data source: s3://nrel-pds-dsgrid/tempo/tempo-2022/v1.0.0 (public, no credentials needed).

PROJECT_DIR defaults to the current directory.

Example:

```
datasight demo dsgrid-tempo tempo-demo
```

```bash
datasight demo dsgrid-tempo [OPTIONS] [PROJECT_DIR]
```

**Parameters**

| Name | Details |
| --- | --- |
| `PROJECT_DIR` |   |

#### `datasight demo time-validation`

Generate a synthetic energy consumption dataset with planted time errors.

Creates hourly electricity consumption data across sectors, end uses, and
US states for future projection years (2038, 2039, 2040). The dataset
contains intentional gaps, duplicates, and DST anomalies that datasight's
time series quality checks can detect.

Run "datasight quality" or "datasight run" after setup to find the errors.

PROJECT_DIR defaults to the current directory.

Example:

```
datasight demo time-validation time-demo
```

```bash
datasight demo time-validation [OPTIONS] [PROJECT_DIR]
```

**Parameters**

| Name | Details |
| --- | --- |
| `PROJECT_DIR` |   |

### `datasight generate`

Generate schema_description.md, queries.yaml, measures.yaml, and time_series.yaml from your database.

Connects to the database, inspects tables and columns, samples
code/enum columns, and asks the LLM to produce documentation
and example queries.

Use datasight init for blank templates; use datasight generate to create

project files from an existing database or data files.

Examples:

```
# Use the database configured in .env
datasight generate
# Reference an existing DuckDB or SQLite database directly
datasight generate grid.duckdb
datasight generate generation.sqlite
# Build ./database.duckdb from CSV inputs
datasight generate generation.csv plants.csv
# Build ./database.duckdb from Parquet inputs
datasight generate generation.parquet plants.parquet
# Build ./database.duckdb from Excel inputs (one table per sheet)
datasight generate generation.xlsx
# Build a custom project DuckDB from CSV, Parquet, or Excel inputs
datasight generate generation.csv --db-path project.duckdb
datasight generate generation.parquet --db-path project.duckdb
```

FILES are input data. --db-path is only the output DuckDB path used

when datasight needs to build a project database from CSV/Parquet/Excel

or mixed file inputs.

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
| `--db-path` | Output DuckDB path to create from CSV/Parquet/Excel or mixed file inputs (default: database.duckdb). Do not use this with a single existing DuckDB or SQLite database; those are referenced directly. |
| `--compact-schema` | Write schema.yaml with table names only. Default adds an empty 'excluded_columns: []' placeholder per table so you can fill in glob patterns for columns to hide. |
| `-v`, `--verbose` | Enable debug logging. |

### `datasight run`

Start the datasight web UI.

If the current directory contains schema_description.md, it will be
auto-loaded as the project. Otherwise, use the UI to select a project,
or pass --project-dir to specify one explicitly.

Examples:

```
datasight run
datasight run --project-dir eia-demo
datasight run --port 9000 --model gpt-4o
datasight run --unix-socket /tmp/datasight.sock
```

```bash
datasight run [OPTIONS]
```

**Parameters**

| Name | Details |
| --- | --- |
| `--port` | Web UI port (default: 8084). |
| `--host` | Bind address for TCP mode. Default: `127.0.0.1`. |
| `--unix-socket` | Listen on this UNIX domain socket instead of TCP. |
| `--model` | LLM model name (overrides .env). |
| `--project-dir` | Auto-load this project on startup (optional). |
| `-v`, `--verbose` | Enable debug logging. |

### `datasight verify`

Verify LLM-generated SQL against expected results.

Runs each question from queries.yaml through the full LLM pipeline,
executes the generated SQL, and compares results against expected values.
Use this to validate correctness across different models and providers.

Examples:

```
datasight verify
datasight verify --queries verification.yaml
datasight verify --model gpt-4o
```

Add expected results to queries.yaml entries:

```
- question: "Top 3 states by generation"
  sql: |
    SELECT state, SUM(mwh) AS total
    FROM generation GROUP BY state
    ORDER BY total DESC LIMIT 3
  expected:
    row_count: 3
    columns: [state, total]
    contains: ["CA", "TX"]
```

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

```
datasight ask "What are the top 5 states by generation?"
datasight ask "Show generation by year" --chart-format html -o chart.html
datasight ask "Top 5 states" --format csv -o results.csv
datasight ask --file questions.txt --output-dir batch-output
datasight ask "Top 5 states" --print-sql
datasight ask "Top 5 states" --provenance
datasight ask "Top 5 states" --sql-script top-states.sql
```

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
| `--provenance` | Print run provenance as JSON to stdout (suppresses human-readable answer). |
| `--sql-script` | Write executed queries to a SQL script that materializes results into auto-named tables (CREATE OR REPLACE). |
| `-v`, `--verbose` | Enable debug logging. |

### `datasight profile`

Profile your dataset - row counts, date coverage, and column statistics.

Use this before asking questions to understand table sizes, candidate
measures, dimensions, null rates, and date ranges.

Examples:

```
datasight profile
datasight profile --table generation_fuel
datasight profile --column generation_fuel.net_generation_mwh
datasight profile --format markdown -o profile.md
```

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

Surface likely measures and default aggregations.

Measures are numeric columns that should usually be summed, averaged,
or otherwise aggregated in generated SQL. Use --scaffold to create an
editable measures.yaml override file.

Examples:

```
datasight measures
datasight measures --table generation_fuel
datasight measures --scaffold
datasight measures --format markdown -o measures.md
```

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

Audit data quality - nulls, suspicious ranges, and date coverage.

Also checks temporal completeness when time_series.yaml defines expected
time series structure.

Examples:

```
datasight quality
datasight quality --table generation_fuel
datasight quality --format markdown -o quality.md
```

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

### `datasight integrity`

Audit cross-table referential integrity - keys, orphans, and join risks.

Use this to find likely primary keys, duplicate keys, orphaned foreign
keys, and joins that may multiply rows unexpectedly.

Examples:

```
datasight integrity
datasight integrity --table plants
datasight integrity --format json -o integrity.json
```

```bash
datasight integrity [OPTIONS]
```

**Parameters**

| Name | Details |
| --- | --- |
| `--project-dir` | Project directory containing .env and config files. Default: `.`. |
| `--table` | Focus integrity checks on a specific table. |
| `--format` | Output format (default: table). Default: `table`. |
| `--output`, `-o` | Write the integrity audit to a file instead of stdout. |

### `datasight distribution`

Profile value distributions - percentiles, outliers, and measure flags.

Use this to inspect numeric ranges, skew, zero/negative rates, outliers,
and measure-semantic flags before building charts or validation rules.

Examples:

```
datasight distribution
datasight distribution --table generation_fuel
datasight distribution --column generation_fuel.net_generation_mwh
datasight distribution --format markdown -o distributions.md
```

```bash
datasight distribution [OPTIONS]
```

**Parameters**

| Name | Details |
| --- | --- |
| `--project-dir` | Project directory containing .env and config files. Default: `.`. |
| `--table` | Profile distributions for a specific table. |
| `--column` | Focus on a specific column as table.column. |
| `--format` | Output format (default: table). Default: `table`. |
| `--output`, `-o` | Write the distribution profile to a file instead of stdout. |

### `datasight validate`

Run declarative validation rules against the database.

Rules live in validation.yaml. Use --scaffold to create a starter file,
edit it for your dataset, then run validate to produce pass/fail output.

Examples:

```
datasight validate --scaffold
datasight validate
datasight validate --table generation_fuel
datasight validate --format markdown -o validation.md
```

```bash
datasight validate [OPTIONS]
```

**Parameters**

| Name | Details |
| --- | --- |
| `--project-dir` | Project directory containing .env and config files. Default: `.`. |
| `--table` | Run rules for a specific table only. |
| `--config` | Path to validation.yaml (default: project_dir/validation.yaml). |
| `--format` | Output format (default: table). Default: `table`. |
| `--output`, `-o` | Write the validation report to a file instead of stdout. |
| `--scaffold` | Write an example validation.yaml to the project directory and exit. |
| `--overwrite` | Overwrite an existing validation.yaml. |

### `datasight audit-report`

Generate a comprehensive audit report combining all checks.

Combines profile, measures, quality, integrity, distribution, and
validation results into one HTML, Markdown, or JSON artifact.

Examples:

```
datasight audit-report
datasight audit-report -o audit.html
datasight audit-report --format markdown -o audit.md
datasight audit-report --table generation_fuel -o generation-audit.html
```

```bash
datasight audit-report [OPTIONS]
```

**Parameters**

| Name | Details |
| --- | --- |
| `--project-dir` | Project directory containing .env and config files. Default: `.`. |
| `--table` | Scope the audit to a specific table. |
| `--output`, `-o` | Output path (.html, .md, or .json). Default: `report.html`. |
| `--format` | Output format (default: inferred from file extension). |

### `datasight dimensions`

Surface likely grouping dimensions and category breakdowns.

Use this to find text/code columns that are good GROUP BY candidates,
such as fuel codes, states, sectors, plants, or scenario labels.

Examples:

```
datasight dimensions
datasight dimensions --table generation_fuel
datasight dimensions --format json -o dimensions.json
```

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

Surface likely trend analyses and chart recommendations.

Run inside a configured project, or pass one or more Parquet, CSV, Excel,
or DuckDB files directly for a quick file-only trend scan.

Examples:

```
datasight trends
datasight trends --table generation_fuel
datasight trends generation.parquet plants.parquet
datasight trends --format markdown -o trends.md
```

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

### `datasight inspect`

Run all analyses on Parquet, CSV, Excel, or DuckDB files and print results.

Creates a file-backed session and runs profile, quality, measures,
dimensions, trends, and recipes — printing everything to the console
without creating a project. When the current directory contains a
``.env`` with ``DB_MODE=spark``, the files are registered as Spark
temp views and all queries run on the cluster; otherwise an ephemeral
in-memory DuckDB session is used.

Examples:

```
datasight inspect generation.parquet
datasight inspect generation.csv plants.csv
datasight inspect data_dir/
datasight inspect generation.parquet --format markdown -o inspect.md
```

```bash
datasight inspect [OPTIONS] FILES...
```

**Parameters**

| Name | Details |
| --- | --- |
| `FILES` |   |
| `--format` | Output format (default: table). Default: `table`. |
| `--output`, `-o` | Write the full report to a file instead of stdout. |

### `datasight recipes`

Generate and run reusable deterministic prompt recipes.

Recipes are suggested natural-language questions derived from the
schema. Listing recipes does not call an LLM; running one sends the
recipe prompt through the normal ask pipeline.

Examples:

```
datasight recipes list
datasight recipes list --table generation_fuel
datasight recipes run 1
```

```bash
datasight recipes [OPTIONS] COMMAND [ARGS]...
```

**Subcommands**

- `list`: List reusable deterministic prompt recipes for a project.
- `run`: Run a generated recipe by ID through the normal ask pipeline.

#### `datasight recipes list`

List reusable deterministic prompt recipes for a project.

Examples:

```
datasight recipes list
datasight recipes list --table generation_fuel
datasight recipes list --format markdown -o recipes.md
```

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

RECIPE_ID is the numeric ID shown by datasight recipes list.

Examples:

```
datasight recipes run 1
datasight recipes run 2 --format csv -o recipe.csv
datasight recipes run 3 --chart-format html -o recipe.html
```

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

Use this when a project will not load, an API key is missing, a database
path is wrong, or the web UI cannot write state under .datasight/.

Examples:

```
datasight doctor
datasight doctor --format markdown -o doctor.md
datasight doctor --project-dir eia-demo
```

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

Export a conversation session as a self-contained HTML page or Python script.

SESSION_ID is the conversation ID (use --list-sessions to see available IDs).

Examples:

```
datasight export --list-sessions
datasight export abc123def -o my-analysis.html
datasight export abc123def --format py -o my-analysis.py
datasight export abc123def --exclude 2,3
```

```bash
datasight export [OPTIONS] [SESSION_ID]
```

**Parameters**

| Name | Details |
| --- | --- |
| `SESSION_ID` |   |
| `--output`, `-o` | Output file path. Defaults to <session_id>.<format> with the session ID truncated to 20 characters. |
| `--format` | html (self-contained viewer, default) or py (runnable Python script). Default: `html`. |
| `--project-dir` | Project directory containing .datasight/conversations/. Default: `.`. |
| `--exclude` | Comma-separated turn indices to exclude (0-based, each turn is a Q&A pair). |
| `--list-sessions` | List available sessions and exit. |

### `datasight log`

Display the SQL query log in a formatted table.

Shows recent SQL queries generated by datasight. Use --sql N to print
one raw SQL statement for copy/paste into DuckDB, SQLite, or another
SQL client.

Examples:

```
datasight log
datasight log --tail 50 --full
datasight log --errors
datasight log --cost
datasight log --sql 1
```

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

Reports are saved from the web UI and can be listed, re-run against
fresh data, exported, or deleted from the CLI.

Examples:

```
datasight report list
datasight report run 1
datasight report run 1 --format csv -o report.csv
datasight report delete 1
```

```bash
datasight report [OPTIONS] COMMAND [ARGS]...
```

**Subcommands**

- `list`: List all saved reports.
- `run`: Re-execute a saved report against fresh data.
- `delete`: Delete a saved report.

#### `datasight report list`

List all saved reports.

Example:

```
datasight report list
```

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

Examples:

```
datasight report run 1
datasight report run 1 --format csv -o report.csv
datasight report run 2 --chart-format html -o chart.html
```

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

Example:

```
datasight report delete 1
```

```bash
datasight report delete [OPTIONS] REPORT_ID
```

**Parameters**

| Name | Details |
| --- | --- |
| `REPORT_ID` |   |
| `--project-dir` | Project directory. Default: `.`. |

### `datasight templates`

Save and re-apply dashboards as templates across datasets.

Templates capture dashboard cards from the web UI so the same SQL and
charts can be applied to another dataset with matching tables.

Examples:

```
datasight templates save generation-dashboard
datasight templates list
datasight templates apply generation-dashboard --output out.html
```

```bash
datasight templates [OPTIONS] COMMAND [ARGS]...
```

**Subcommands**

- `save`: Save the current project dashboard as a reusable template.
- `list`: List dashboard templates saved in this project.
- `show`: Print a saved template as JSON.
- `apply`: Apply a saved template to parquet files and export HTML dashboards.
- `delete`: Delete a saved template.

#### `datasight templates save`

Save the current project dashboard as a reusable template.

The dashboard must already exist in the project, usually from building
and saving cards in the web UI.

Examples:

```
datasight templates save generation-dashboard
datasight templates save generation-dashboard --description "Monthly generation cards"
datasight templates save generation-dashboard --table generation_fuel --overwrite
datasight templates save by-scenario --var SCENARIO=reference
```

```bash
datasight templates save [OPTIONS] NAME
```

**Parameters**

| Name | Details |
| --- | --- |
| `NAME` |   |
| `--project-dir` | Project directory containing .datasight/templates/ (default: cwd). Default: `.`. |
| `--description` | Template description. |
| `--table` | Table the template requires. Repeat once per table. When omitted, tables are inferred from each card's SQL. |
| `--var` | Declare a template variable: --var NAME=VALUE. Every occurrence of VALUE in each card's SQL is rewritten to {{NAME}}, and NAME becomes a placeholder that must be resolved at apply time. |
| `--var-from-filename` | Attach a filename-extraction regex to a variable: --var-from-filename NAME=REGEX. At apply time the regex is run against each input parquet's filename and its first capture group (or whole match) becomes the variable value. Use with --var to also set the save-time literal and default. |
| `--overwrite` | Replace an existing template. |

#### `datasight templates list`

List dashboard templates saved in this project.

Example:

```
datasight templates list
```

```bash
datasight templates list [OPTIONS]
```

**Parameters**

| Name | Details |
| --- | --- |
| `--project-dir` | Project directory containing .datasight/templates/ (default: cwd). Default: `.`. |

#### `datasight templates show`

Print a saved template as JSON.

Example:

```
datasight templates show generation-dashboard
```

```bash
datasight templates show [OPTIONS] NAME
```

**Parameters**

| Name | Details |
| --- | --- |
| `NAME` |   |
| `--project-dir` | Project directory containing .datasight/templates/ (default: cwd). Default: `.`. |

#### `datasight templates apply`

Apply a saved template to parquet files and export HTML dashboards.

Each required table is registered as a view inside an in-memory DuckDB
connection. Tables not passed via --table fall back to the project's
own DuckDB (from .env DB_PATH) — so fixed lookup tables like ``plants``
don't need to be re-supplied. A single --table mapping may use a shell
glob, in which case the template is applied once per matching file and
written to --export-dir.

Examples:

```
# Render once, mapping one required table to a parquet file
datasight templates apply generation-by-fuel \
    --table generation_fuel=data/generation.parquet \
    --output generation.html
# Render once per matching parquet, writing one HTML per file
datasight templates apply generation-by-fuel \
    --table 'generation_fuel=data/*.parquet' \
    --export-dir out/
```

```bash
datasight templates apply [OPTIONS] NAME
```

**Parameters**

| Name | Details |
| --- | --- |
| `NAME` |   |
| `--project-dir` | Project directory containing .datasight/templates/ (default: cwd). Default: `.`. |
| `--table` | Map a required table to a parquet file: --table NAME=PATH. Repeat per table. One mapping may use a glob to iterate the template across many files. Tables not mapped here are looked up in the project's DuckDB. |
| `--output` | HTML output path for a single-shot run (no globbing). |
| `--export-dir` | Directory for per-file HTML output when a --table mapping globs. |
| `--var` | Override a template variable: --var NAME=VALUE. Takes precedence over the variable's filename-derived value and default. |
| `--fail-fast` | Stop on the first failure instead of continuing. |

#### `datasight templates delete`

Delete a saved template.

Example:

```
datasight templates delete generation-dashboard
```

```bash
datasight templates delete [OPTIONS] NAME
```

**Parameters**

| Name | Details |
| --- | --- |
| `NAME` |   |
| `--project-dir` | Project directory containing .datasight/templates/ (default: cwd). Default: `.`. |
