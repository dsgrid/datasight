# Inspection workflows

Use these workflows when you want a deterministic first pass on your data
before asking freeform LLM questions.

## Quick start: inspect files directly

If you have Parquet, CSV, or DuckDB files and want everything at once,
`datasight inspect` runs all analyses in a single command — no project setup
required:

```bash
datasight inspect sales.parquet returns.csv
datasight inspect data_dir/
datasight inspect --format markdown -o overview.md sales.parquet
```

This prints profile, quality, measures, dimensions, trends, and recipes to
the console. Skip to [Move into batch mode](#move-into-batch-mode) once you
know what questions to ask.

The sections below describe how to run each analysis individually from a
configured project directory.

## Start with a profile

`datasight profile` gives you a high-level map of the project:

```bash
datasight profile
datasight profile --table orders
datasight profile --column orders.order_date
```

Use it to answer:

- which tables are largest
- which columns look like dates or measures
- which text columns look like useful grouping dimensions
- what representative values appear in important columns

## Run a quality pass

`datasight quality` surfaces lightweight QA issues without using the LLM:

```bash
datasight quality
datasight quality --table orders
datasight quality --format markdown -o quality.md
```

Use it to spot:

- null-heavy columns
- suspicious numeric ranges
- basic date coverage
- quick notes worth turning into follow-up questions

## Find dimensions and trends

`datasight dimensions` and `datasight trends` help you decide what to analyze
next:

```bash
datasight dimensions
datasight dimensions --table orders

datasight trends
datasight trends --table orders
```

Use them when you need:

- candidate grouping columns
- likely category breakdowns
- date/measure pairs for time-series analysis
- simple chart recommendations

## Generate prompt recipes

`datasight recipes list` builds reusable prompts from the schema and
deterministic profiling output:

```bash
datasight recipes list
datasight recipes list --table orders
datasight recipes list --format markdown -o recipes.md
```

These are useful when you want a stronger starting prompt than a blank chat
box, but still want the LLM to do the final analysis.

Each recipe includes an `id`, so you can execute it directly:

```bash
datasight recipes run 1
datasight recipes run 2 --format json
```

That runs the selected recipe through the normal `datasight ask` workflow.

## Move into batch mode

Once you know the questions you want to ask, switch to `datasight ask --file`
to run a set of prompts consistently.

Plain text:

```text
How many rows are in the largest table?
What are the main date columns?
Show monthly order volume as a line chart.
```

```bash
datasight ask --file questions.txt --output-dir batch-output
```

Structured YAML:

```yaml
- question: How many orders are there?
  format: json
  name: orders-summary
- question: Show monthly volume as a line chart.
  chart_format: html
  output: reports/monthly-volume
```

Structured JSONL:

```json
{"question":"How many orders are there?","format":"json","name":"orders-summary"}
{"question":"Show monthly volume as a line chart.","chart_format":"html","output":"reports/monthly-volume"}
```

```bash
datasight ask --file questions.yaml --output-dir batch-output
datasight ask --file questions.jsonl --output-dir batch-output
```

## Check project health before larger runs

Use `datasight doctor` before a larger batch or a shared team workflow:

```bash
datasight doctor
datasight doctor --format markdown -o doctor.md
```

This checks project files, LLM configuration, local state writability, and
database connectivity.
