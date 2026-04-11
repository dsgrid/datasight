# Audit data quality

datasight provides deterministic commands that inspect your data directly —
no LLM needed. Use them to find nulls, suspicious ranges, date gaps,
dimension breakdowns, and trend candidates before asking AI-powered
questions.

These commands work with both configured projects and files passed directly
to `datasight inspect`.

## Quick audit: `datasight inspect`

For a single-command overview that includes quality checks alongside
everything else:

```bash
datasight inspect generation.parquet
```

This runs profile, quality, measures, dimensions, trends, and recipes in
one pass. See [Inspect files](inspect-files.md) for details.

The sections below describe how to run each analysis individually for more
focused work from a project directory.

## Profile the dataset

`datasight profile` gives you a high-level map of the data:

```bash
# Whole dataset
datasight profile

# One table
datasight profile --table generation_fuel

# One column
datasight profile --column generation_fuel.report_date
```

Use it to answer:

- which tables are largest
- which columns look like dates or measures
- which text columns look like useful grouping dimensions
- what representative values appear in important columns

## Check data quality

`datasight quality` surfaces lightweight QA issues:

```bash
datasight quality
datasight quality --table generation_fuel
datasight quality --format markdown -o quality.md
```

Use it to spot:

- null-heavy columns (10%+ nulls)
- suspicious numeric ranges (constant values, averages at boundaries)
- date coverage gaps
- quick notes worth turning into follow-up questions
- temporal completeness issues when [`time_series.yaml`](time-series.md) is present

## Find measures

`datasight measures` infers likely metrics and their aggregation semantics:

```bash
datasight measures
datasight measures --table generation_fuel
```

The output includes:

- a semantic role such as `energy`, `power`, `capacity`, `rate`, or `ratio`
- the default aggregation datasight will prefer
- forbidden aggregations such as avoiding `SUM` on non-additive metrics
- weighted-average guidance when a valid denominator is available
- a suggested SQL rollup shape

Generate a starting `measures.yaml` template:

```bash
datasight measures --scaffold
```

See [Semantic measures](measures.md) for the full override workflow.

## Find dimensions

`datasight dimensions` identifies likely grouping columns:

```bash
datasight dimensions
datasight dimensions --table generation_fuel
```

Use it to discover:

- candidate grouping columns with distinct counts and sample values
- suggested category breakdowns with reasons
- join hints between tables

## Find trends

`datasight trends` surfaces date/measure pairs for time-series analysis:

```bash
datasight trends
datasight trends --table generation_fuel
```

It reports:

- candidate date/measure pairs
- date ranges
- suggested aggregations
- lightweight chart recommendations

## Generate prompt recipes

`datasight recipes list` builds reusable prompts from the schema and
profiling output:

```bash
datasight recipes list
datasight recipes list --table generation_fuel
datasight recipes list --format markdown -o recipes.md
```

Each recipe includes an `id`. Execute one directly through the AI:

```bash
datasight recipes run 1
datasight recipes run 2 --format json
```

## Recommended workflow

For a thorough data quality audit, run the commands in this order:

1. **Profile** — understand the shape of the data
2. **Quality** — find nulls, range issues, and date gaps
3. **Measures** — identify metrics and verify aggregation defaults
4. **Dimensions** — find grouping columns for breakdowns
5. **Trends** — discover time-series candidates

Or skip straight to `datasight inspect` to get all of this in one pass.

Once you know what questions to ask, move into batch mode:

```bash
datasight ask --file questions.txt --output-dir batch-output
```

See [Ask questions from the CLI](ask-questions.md) for batch workflows.

## Output formats

All commands support `--format` and `--output`:

```bash
datasight quality --format json
datasight quality --format markdown -o quality.md
datasight profile --format json -o profile.json
```

Available formats: `table` (default), `json`, `markdown`.
