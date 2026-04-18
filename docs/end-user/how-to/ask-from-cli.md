# Ask questions from the CLI

`datasight ask` runs the full LLM agent loop and prints results to the
terminal. It uses the same AI, schema context, and example queries as the
web UI.

```bash
datasight ask "What are the top 5 states by total generation?"
```

```
The top 5 states by total electricity generation are:

+-------+------------------+
| state | total_generation  |
+-------+------------------+
| TX    |  574,327,891.2   |
| FL    |  287,654,321.0   |
| PA    |  243,876,543.8   |
| IL    |  198,234,567.1   |
| CA    |  187,654,321.4   |
+-------+------------------+
```

## Export results as CSV or JSON

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

## Export charts

Ask for a visualization and save it as an interactive HTML file:

```bash
datasight ask "Show monthly wind generation as a line chart" \
    --chart-format html -o wind-trend.html
```

Other export formats:

```bash
# Plotly JSON spec (for embedding in notebooks or other tools)
datasight ask "Generation by fuel type" --chart-format json -o chart.json

# Static PNG image (requires the [export] extra — see the install guide)
datasight ask "Solar generation by state" --chart-format png -o solar-map.png
```

## Inspect the SQL

Print every SQL query the agent executes:

```bash
datasight ask "Top 5 states by generation" --print-sql
```

Save the executed queries as a re-runnable SQL script:

```bash
datasight ask "Top 5 states by generation" --sql-script top-states.sql
```

The generated script names each table `<question_slug>_<short_hash>_<n>`,
using `CREATE OR REPLACE TABLE` on DuckDB (or `DROP TABLE IF EXISTS` +
`CREATE TABLE` on SQLite/Postgres). Re-running the same question reuses
the same names so the script overwrites in place.

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `--format` | `table` | Output format: `table`, `csv`, or `json` |
| `--chart-format` | -- | Chart export format: `html`, `json`, or `png` |
| `-o` / `--output` | -- | Save output to a file instead of printing |
| `--print-sql` | off | Print executed SQL queries to the console |
| `--sql-script` | -- | Write executed queries to a re-runnable SQL script |
| `--model` | from `.env` | Override the model for this query |
| `--project-dir` | `.` | Project directory containing `.env` |
| `-v` / `--verbose` | off | Show debug logging (LLM requests, SQL, timing) |

## Scripting examples

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

## Export conversations

`datasight export` converts a saved web UI conversation into a
self-contained HTML page:

```bash
# List available conversations
datasight export --list-sessions

# Export a conversation to HTML
datasight export a1b2c3d4 -o analysis.html

# Exclude specific turns by index (0-based)
datasight export e5f6g7h8 --exclude 0,3 -o wind-report.html
```

## Run saved reports

Reports saved in the web UI can be re-executed from the CLI. No AI is
involved — the saved SQL runs directly against the database.

```bash
# List reports
datasight report list

# Run a report
datasight report run 1
datasight report run 1 --format csv
datasight report run 2 --chart-format html -o trend.html

# Delete a report
datasight report delete 1
```

## Project diagnostics

`datasight doctor` checks your project setup:

```bash
datasight doctor
datasight doctor --format markdown -o doctor.md
```

It validates `.env`, LLM settings, database configuration,
`schema_description.md`, `queries.yaml`, `.datasight` writability, and
live database connectivity.

## Troubleshooting

### "Maximum context length exceeded" or "request too large"

Your database schema is too large for the LLM's context window. This
commonly happens on the free GitHub Models tier (capped at 8,000 tokens)
when the database has more than ~20 tables. Ask your project developer to
[limit the schema sent to the LLM](../../project-developer/schema-config.md)
via a `schema.yaml` file, or switch to a provider with a larger context
window (OpenAI, Anthropic, or a local Ollama model).

## Review the query log

`datasight log` shows recent SQL queries. Logging must be enabled first —
see [Review the SQL query log](review-query-log.md).

```bash
datasight log
datasight log --errors        # Only failed queries
datasight log --full --tail 5 # Full SQL with natural-language question
datasight log --sql 1         # Print raw SQL for query #1
datasight log --cost          # Show LLM cost summary
```
