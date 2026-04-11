# Use the CLI

datasight provides command-line tools for querying data, reviewing SQL logs,
and exporting conversations — all without opening the web UI.

## Ask questions from the terminal

`datasight ask` runs the full LLM agent loop and prints results to the
terminal. It uses the same AI, schema context, and example queries as the
web UI.

```bash
datasight ask "What are the top 5 states by total generation?"
```

```
The top 5 states by total electricity generation are:

┌───────┬──────────────────┐
│ state │ total_generation  │
├───────┼──────────────────┤
│ TX    │  574,327,891.2   │
│ FL    │  287,654,321.0   │
│ PA    │  243,876,543.8   │
│ IL    │  198,234,567.1   │
│ CA    │  187,654,321.4   │
└───────┴──────────────────┘
```

The AI's text explanation is printed first, followed by a formatted table
of the query results.

### Export results as CSV or JSON

```bash
datasight ask "Top 10 plants by generation" --format csv
```

```
plant_name,total_mwh
Palo Verde,32541876.5
West County Energy Center,28764532.1
Scherer,27654321.0
Martin,25432198.7
Navajo,24321098.4
Grand Coulee,23456789.0
Oconee,22345678.9
Amos,21234567.8
Monroe,20987654.3
Gibson,20123456.7
```

Save directly to a file:

```bash
datasight ask "Monthly generation trend" --format csv -o generation.csv
datasight ask "Top 10 plants" --format json -o plants.json
```

### Export charts

Ask for a visualization and save it as an HTML file with an interactive
Plotly chart:

```bash
datasight ask "Show monthly wind generation as a line chart" \
    --chart-format html -o wind-trend.html
```

```
Chart HTML saved to wind-trend.html
```

Open `wind-trend.html` in a browser to see the interactive chart. Other
export formats:

```bash
# Plotly JSON spec (for embedding in notebooks or other tools)
datasight ask "Generation by fuel type" --chart-format json -o chart.json

# Static PNG image (requires: pip install "datasight[export]")
datasight ask "Solar generation by state" --chart-format png -o solar-map.png
```

### Inspect or replay the SQL the agent runs

Print every SQL query the agent executes to the console alongside the
answer:

```bash
datasight ask "Top 5 states by generation" --print-sql
```

Save the executed queries as a re-runnable SQL script that materializes
each result into an auto-named table (using `CREATE OR REPLACE TABLE` on
DuckDB, or `DROP TABLE IF EXISTS` + `CREATE TABLE` on SQLite/Postgres):

```bash
datasight ask "Top 5 states by generation" --sql-script top-states.sql
```

The generated script names each table `<question_slug>_<short_hash>_<n>`,
where `<short_hash>` is an 8-character digest of the original question that
keeps two different questions from colliding on the same table when their
slugs happen to share the same prefix. Re-running the same question reuses
the same names, so the script overwrites in place and you can inspect the
results with normal SQL tooling. SQL queries are also appended to
`.datasight/query_log.jsonl` just like the web UI — review them later with
`datasight log`.

When datasight detects likely measures such as MWh generation, MW demand,
capacity, or rate-style columns, `ask` also uses those inferred semantics
to steer SQL aggregation choices toward safer defaults.

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--format` | `table` | Output format: `table`, `csv`, or `json` |
| `--chart-format` | — | Chart export format: `html`, `json`, or `png` |
| `-o` / `--output` | — | Save output to a file instead of printing |
| `--print-sql` | off | Print executed SQL queries to the console |
| `--sql-script` | — | Write executed queries to a re-runnable SQL script |
| `--model` | from `.env` | Override the model for this query |
| `--project-dir` | `.` | Project directory containing `.env` |
| `-v` / `--verbose` | off | Show debug logging (LLM requests, SQL, timing) |

### Scripting examples

Chain `datasight ask` with other tools:

```bash
# Pipe CSV output to another program
datasight ask "All plants in Texas" --format csv | head -20

# Use in a shell script
datasight ask "Count of plants by state" --format json -o counts.json
python analyze.py counts.json

# Quick data check before a meeting
datasight ask "Total generation by fuel type for 2024" --format table
```

## Run batch question files

`datasight ask --file` executes multiple prompts in one run. Plain text
files use one question per non-empty line:

```bash
datasight ask --file questions.txt
```

```text
How many rows are in the largest table?
What are the main date columns?
Show the top 10 fuel types by total generation.
```

Write per-question artifacts to a directory:

```bash
datasight ask --file questions.txt \
  --output-dir batch-output \
  --format json \
  --chart-format json
```

Structured YAML or JSONL input supports per-entry overrides:

```yaml
- question: How many power plants are there?
  format: json
  name: plant-summary
- question: Show monthly generation as a line chart.
  chart_format: html
  output: reports/monthly-generation
```

```bash
datasight ask --file questions.yaml --output-dir batch-output
datasight ask --file questions.jsonl --output-dir batch-output
```

JSONL uses one object per line:

```json
{"question":"How many power plants are there?","format":"json","name":"plant-summary"}
{"question":"Show monthly generation as a line chart.","chart_format":"html","output":"reports/monthly-generation"}
```

## Use deterministic inspection commands

These commands do not require an LLM. They inspect the connected data
directly and return structured output in `table`, `json`, or `markdown`
formats.

### Inspect files without a project

`datasight inspect` runs all deterministic analyses — profile, quality,
measures, dimensions, trends, and recipes — in a single command directly on
Parquet, CSV, or DuckDB files:

```bash
datasight inspect generation.parquet
datasight inspect generation.csv plants.csv
datasight inspect data_dir/
```

Nothing is written to disk. Use `--format json` or `--format markdown` to
change the output format, or `-o report.md` to save it to a file.

This is the fastest way to get a complete overview of unfamiliar data from
the command line.

### Profile the dataset

```bash
# Whole dataset
datasight profile

# One table
datasight profile --table generation_fuel

# One column
datasight profile --column generation_fuel.report_date
```

Useful when you need row counts, date coverage, numeric ranges, candidate
dimensions, and representative values before writing prompts.

### Audit data quality

```bash
datasight quality
datasight quality --table generation_fuel
datasight quality --format markdown -o quality.md
```

This surfaces null-heavy columns, suspicious numeric ranges, date coverage,
and quick notes from a deterministic quality pass.

### Find likely dimensions and trend analyses

```bash
datasight measures
datasight measures --table generation_fuel
datasight dimensions
datasight dimensions --table generation_fuel
datasight trends
datasight trends --table generation_fuel
```

Use these to discover:

- likely measures and sensible default aggregations
- likely grouping columns
- suggested category breakdowns
- candidate date/measure pairs for time-series analysis
- lightweight chart recommendations

`datasight measures` is energy-aware. It tries to distinguish additive
energy volumes such as `net_generation_mwh`, power or demand signals such
as `demand_mw`, capacity metrics, and non-additive rates or factors, then
suggests whether `SUM`, `AVG`, or `MAX` is the safer default.

The output includes:

- a semantic role such as `energy`, `power`, `capacity`, `rate`, or `ratio`
- the default aggregation datasight will prefer
- forbidden aggregations such as avoiding `SUM` on non-additive metrics
- weighted-average guidance when a valid denominator is available
- a suggested SQL rollup shape
- any configured display metadata such as display name, format, and preferred chart types

`datasight generate` also seeds a `measures.yaml` scaffold alongside
`schema_description.md` and `queries.yaml`, so new projects start with an
editable semantic-measure config.

If the defaults are wrong for your project, add a `measures.yaml` file in
the project root to override them. Each entry should include at least
`table` and `column`, plus any semantic fields you want to override such as
`default_aggregation`, `weight_column`, or `reason`.

You can also define calculated measures in `measures.yaml` by using
`table`, `name`, and `expression`, for example a project-specific
`net_load_mw` or `capacity_factor` formula. Datasight will surface these
alongside physical columns in the measure overview, trend suggestions, and
prompt guidance.

For project-defined physical measures, datasight also validates generated SQL
before execution. If the model uses a non-default or disallowed aggregation for
an overridden column, the SQL is rejected and the model must regenerate unless
the user explicitly asked for that aggregation.

You can generate a starting template from the current inferred measures:

```bash
datasight measures --scaffold
```

### Edit `measures.yaml`

Use `measures.yaml` when you want datasight to treat a metric differently
from what the heuristics inferred.

For physical columns, these overrides now influence both prompt guidance and
pre-execution SQL validation.

Common override fields:

- `default_aggregation`
- `average_strategy`
- `weight_column`
- `display_name`
- `format`
- `preferred_chart_types`
- `reason`

Example physical measure override:

```yaml
- table: generation_hourly
  column: demand_mw
  display_name: System demand
  default_aggregation: max
  format: mw
  preferred_chart_types:
    - line
  reason: This project usually wants peak demand, not average demand.
```

Example calculated measure:

```yaml
- table: generation_hourly
  name: net_load_mw
  expression: load_mw - renewable_generation_mw
  display_name: Net load
  default_aggregation: avg
  format: mw
  preferred_chart_types:
    - line
    - area
  reason: Project-defined net load measure.
```

Calculated measures appear in:

- `datasight measures`
- prompt guidance for `datasight ask`
- trend suggestions
- web Measure Overview and Measure Overrides

### When To Use Which File

Use:

- `schema_description.md` for narrative business/domain context
- `queries.yaml` for example questions and correct SQL
- `measures.yaml` for semantic metric behavior and calculated measures

See [Semantic measures](measures.md) for a fuller walkthrough and more
examples.

### Generate reusable prompt recipes

```bash
datasight recipes list
datasight recipes list --table generation_fuel
datasight recipes list --format markdown -o recipes.md
datasight recipes run 1
```

`datasight recipes list` builds reusable analysis prompts like “profile the
biggest tables” or “trend `net_generation_mwh` over `report_date`” from the actual schema
and detected columns. Each recipe includes an `id`.

Use `datasight recipes run <id>` to execute a selected recipe through the
normal `ask` workflow without retyping the prompt.

See [Inspection workflows](inspection-workflows.md) for a more opinionated
way to combine `profile`, `quality`, `dimensions`, `trends`, `recipes`, and
batch `ask --file` runs.

## Run project diagnostics

`datasight doctor` checks the local project setup without opening the web UI.

```bash
datasight doctor
datasight doctor --format markdown -o doctor.md
```

It validates:

- `.env`
- LLM settings
- database configuration
- `schema_description.md`
- `queries.yaml`
- `.datasight` writability
- live database connectivity

## Review the query log

`datasight log` shows a formatted table of recent SQL queries from the
query log file. Logging must be enabled first — see
[Log and review SQL queries](query-log.md).

```bash
datasight log
```

```
 Timestamp            Tool     SQL                                  Time  Rows  Status
─────────────────────────────────────────────────────────────────────────────────────────
 2026-04-04 09:15:03  run_sql  SELECT state, SUM(net_generation_     84ms     5  OK
                               mwh) AS total FROM generation_fuel
                               GROUP BY state ORDER BY total DESC
                               LIMIT 5
 2026-04-04 09:15:47  run_sql  SELECT DATE_TRUNC('month',           142ms    48  OK
                               report_date) AS month, SUM(net_
                               generation_mwh) AS total FROM
                               generation_fuel WHERE
                               energy_source_code = 'WND'
                               GROUP BY month ORDER BY month
 2026-04-04 09:17:22  run_sql  SELECT * FROM nonexistent_table        2ms        ERR
─────────────────────────────────────────────────────────────────────────────────────────
3 queries (2 succeeded, 1 failed)
```

### Filter and format

```bash
# Show only failed queries
datasight log --errors
```

```
 Timestamp            Tool     SQL                                Time  Rows  Status
────────────────────────────────────────────────────────────────────────────────────────
 2026-04-04 09:17:22  run_sql  SELECT * FROM nonexistent_table      2ms        ERR
────────────────────────────────────────────────────────────────────────────────────────
1 query (0 succeeded, 1 failed)
```

```bash
# Show full SQL and the original natural-language question
datasight log --full --tail 5
```

```
 Timestamp            Question              Tool     SQL                       Time  Rows  Status
──────────────────────────────────────────────────────────────────────────────────────────────────
 2026-04-04 09:15:03  Top 5 states by       run_sql  SELECT state,              84ms     5  OK
                      generation                       SUM(net_generation_mwh)
                                                       AS total
                                                     FROM generation_fuel
                                                     GROUP BY state
                                                     ORDER BY total DESC
                                                     LIMIT 5
──────────────────────────────────────────────────────────────────────────────────────────────────
```

## Export conversations

`datasight export` converts a saved web UI conversation into a
self-contained HTML page.

```bash
# List available conversations
datasight export --list-sessions
```

```
 Session   Title                         Messages
──────────────────────────────────────────────────
 a1b2c3d4  Top plants by generation             4
 e5f6g7h8  Wind generation trends               7
 i9j0k1l2  State comparison analysis            12
```

```bash
# Export a conversation to HTML
datasight export a1b2c3d4 -o analysis.html

# Exclude specific turns by index (0-based, each turn is a Q&A pair)
datasight export e5f6g7h8 --exclude 0,3 -o wind-report.html
```

The exported HTML page includes all messages, SQL queries with syntax
highlighting, data tables, and interactive Plotly charts — ready to share
with colleagues or embed in a report.

## Run saved reports

Reports saved in the web UI can be re-executed from the CLI. No AI is
involved — the saved SQL runs directly against the database.

### List reports

```bash
datasight report list
```

```
 ID  Name                Tool            SQL
─────────────────────────────────────────────────────────────────
  1  Top states          run_sql         SELECT state, SUM(net_generation_mwh)...
  2  Monthly wind trend  visualize_data  SELECT DATE_TRUNC('month', report_dat...
```

### Run a report

```bash
# Table output (default)
datasight report run 1

# CSV or JSON output
datasight report run 1 --format csv
datasight report run 1 --format json -o results.json

# Export a chart report as HTML or Plotly JSON
datasight report run 2 --chart-format html -o trend.html
datasight report run 2 --chart-format json -o trend.json
```

### Delete a report

```bash
datasight report delete 1
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--format` | `table` | Output format: `table`, `csv`, or `json` |
| `--chart-format` | — | Chart export format: `html` or `json` |
| `-o` / `--output` | — | Save output to a file |
| `--project-dir` | `.` | Project directory |
