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
one pass. See [Explore files without a project](explore-files.md) for details.

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
- temporal completeness issues when [`time_series.yaml`](../../project-setup/how-to/declare-time-series.md) is present

## Detect untidy column shapes

`datasight quality` also flags tables whose **column names encode dimension
values** — a common spreadsheet shape that confuses both DuckDB and the LLM
agent. To list, preview, or apply reshapes, use the dedicated
`datasight tidy` command described below.

### What "tidy" means

A *tidy* dataset, as defined by Hadley Wickham in [Tidy
Data](https://www.jstatsoft.org/article/view/v059i10) (J. Stat. Softw., 2014),
follows three rules:

1. each variable forms a column,
2. each observation forms a row,
3. each type of observational unit forms a table.

In a tidy `generation_fuel` table, that means one row per *(plant, date, fuel)*
observation, with `net_generation_mwh` as a single column — not 12 columns
named `jan`, `feb`, `mar`, … or 24 columns named `hour_00`, `hour_01`, ….
When dimension values (year, month, quarter, hour, day) live in the column
headers, the data is *untidy* and queries become awkward (e.g. "average across
months" turns into a SUM across 12 columns instead of a `GROUP BY month`).

For a longer treatment with worked examples, the R for Data Science chapter
[Data tidying](https://r4ds.hadley.nz/data-tidy) walks through the same ideas
with concrete reshapes.

### What datasight detects

The `quality` command flags two structural patterns from the schema alone — no
extra SQL is issued:

- **Period values in column names** — three or more columns whose names match
  a recognized year, year-month, year-quarter, quarter, month, hour, or day
  token. Both shared-prefix shapes (`sales_2020`, `sales_2021`, `sales_2022`)
  and bare-token shapes (`q1`, `q2`, `q3`, `q4` or `hour_00` … `hour_23`) are
  recognized.
- **Wide tables with low row counts** — tables with 30 or more columns where
  the row count is comparable to the column count. This is a softer note,
  emitted only when no period pattern is detected.

### List suggestions: `datasight tidy suggest`

To inspect the suggestions on their own, use:

```bash
datasight tidy suggest                        # current project
datasight tidy suggest sales_wide.csv         # standalone file, no project
datasight tidy suggest gen.csv plants.parquet # multiple files
datasight tidy suggest --table sales_wide
datasight tidy suggest --format markdown -o tidy.md
```

When you pass file arguments, datasight registers them in an ephemeral
DuckDB session and skips project loading entirely — useful for the "is
this spreadsheet untidy?" question before committing to project setup.

Example output:

```
$ datasight tidy suggest --table sales_wide --format markdown
# Tidy Reshape Suggestions

- Tables scanned: 1

## Suggestions
- `sales_wide` (repeated_prefix_period, year, 4 columns): Columns share prefix
  `sales` with year suffixes — 4 columns look like a single measure spread
  across year values.

  ```sql
  CREATE OR REPLACE VIEW "sales_wide_long" AS
  SELECT "region", '2020' AS "year", "sales_2020" AS "sales" FROM "sales_wide"
  UNION ALL SELECT "region", '2021', "sales_2021" FROM "sales_wide"
  UNION ALL SELECT "region", '2022', "sales_2022" FROM "sales_wide"
  UNION ALL SELECT "region", '2023', "sales_2023" FROM "sales_wide";
  ```
```

### Apply a reshape: `datasight tidy view` / `tidy table`

Two sibling subcommands write the long-form object directly to the project's
DuckDB database. Both accept `--dry-run` to preview the DDL without executing
it:

```bash
# Preview the view DDL
datasight tidy view --dry-run

# Create one long-form view per suggestion (CREATE OR REPLACE VIEW)
datasight tidy view

# Materialize as physical tables instead (CREATE OR REPLACE TABLE)
datasight tidy table

# Scope to a single source table
datasight tidy view --table sales_wide
```

The default target name is `<source_table>_long`; existing objects with that
name are replaced. After the view or table exists, reference it in your
`schema_description.md` so the LLM agent prefers tidy queries like
`SELECT year, SUM(sales) FROM sales_wide_long GROUP BY year` over wide-column
arithmetic.

The inner `SELECT … UNION ALL …` body is portable to SQLite and PostgreSQL,
but the surrounding `CREATE OR REPLACE VIEW` / `CREATE OR REPLACE TABLE`
wrapper is DuckDB-specific (PostgreSQL has no `CREATE OR REPLACE TABLE`,
SQLite has no `CREATE OR REPLACE VIEW`). `tidy view` and `tidy table`
correspondingly only execute against DuckDB project databases — paste the
`SELECT` body into the equivalent `CREATE VIEW` / `CREATE TABLE AS` syntax
yourself if your project runs on a different backend.

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

See [Configure semantic measures](../../project-setup/how-to/configure-measures.md) for the full override workflow.

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

## Check referential integrity

`datasight integrity` looks across tables for primary-key and foreign-key
issues:

```bash
datasight integrity
datasight integrity --table generation_fuel
datasight integrity --format markdown -o integrity.md
```

It reports:

- **Primary keys** — columns named `id` or `{table}_id` whose values are 100%
  distinct, so they can be safely used as PKs
- **Duplicate keys** — inferred PKs that actually have duplicates
- **Orphan foreign keys** — columns ending in `_id` whose values don't exist
  in the referenced parent table
- **Join explosion risks** — FK relationships where joining multiplies the
  row count (a one-to-many where you expected one-to-one)

By default, datasight infers foreign keys by naming convention. When that
convention doesn't fit your schema, declare the relationships explicitly in
[`joins.yaml`](../../project-setup/how-to/declare-joins.md).

## Profile distributions

`datasight distribution` computes percentile summaries for each numeric
non-identifier column:

```bash
datasight distribution
datasight distribution --table generation_fuel
datasight distribution --column generation_fuel.net_generation_mwh
datasight distribution --format markdown -o distribution.md
```

For each column it reports:

- **Percentiles**: p1, p5, p50, p95, p99
- **Spread**: Q1, Q3, IQR, mean, stddev, coefficient of variation
- **Rates**: zero rate and negative rate
- **IQR outliers**: count of values outside 1.5 × IQR of the quartiles
- **Temporal spikes**: per-month averages more than 3σ from the mean

### Energy-domain flags

When a column is recognized as an energy-domain measure (via [semantic
measure inference](../../project-setup/concepts/semantic-measures.md)), the command raises targeted warnings:

- **Negative generation** — an `energy`-role column with negative values
- **Capacity factor > 1** — a `ratio` column whose p99 exceeds 1.0
- **Implausible heat rate** — a `rate` column in MMBtu/MWh outside 3–30
- **Zero values in rate** — rate/ratio/price columns containing zeros that
  may indicate zero-denominator bugs upstream

## Run declarative validation

`datasight validate` runs declarative rules defined in `validation.yaml`.
Use this to lock in expectations about your data that should hold across
every refresh — required columns, allowed values, freshness, and so on.

```bash
datasight validate
datasight validate --table generation_fuel
datasight validate --format markdown -o validation.md
```

Each result is reported as `pass`, `fail`, or `warn`, with a short detail
string explaining the violation. See
[Define validation rules](../../project-setup/how-to/validation-rules.md) for
authoring `validation.yaml` and the full list of supported rule types.

## Combined audit report

`datasight audit-report` runs the dataset overview, quality, integrity,
distribution, and (if configured) validation checks together and writes a
single self-contained file:

```bash
# HTML (default output: report.html)
datasight audit-report

# Choose a different output
datasight audit-report -o audit.md
datasight audit-report -o audit.json --format json

# Scope to a single table
datasight audit-report --table generation_fuel -o plant_audit.html
```

The HTML report is self-contained (inline CSS, no external assets) so you
can email it, attach it to a PR, or drop it into a wiki. The project
directory name appears in the report title.

## Recommended workflow

For a thorough data quality audit, run the commands in this order:

1. **Profile** — understand the shape of the data
2. **Quality** — find nulls, range issues, date gaps, and untidy column shapes
3. **Integrity** — verify primary keys, foreign keys, and join behavior
4. **Distribution** — inspect percentiles, outliers, and temporal spikes
5. **Measures** — identify metrics and verify aggregation defaults
6. **Dimensions** — find grouping columns for breakdowns
7. **Trends** — discover time-series candidates
8. **Validate** — codify expectations in [`validation.yaml`](../../project-setup/how-to/validation-rules.md) and run them on every refresh

Or skip straight to `datasight inspect` for profile/quality/measures in one
pass, or `datasight audit-report` to roll all of the audit checks into a
single shareable document.

Once you know what questions to ask, move into batch mode:

```bash
datasight ask --file questions.txt --output-dir batch-output
```

See [Ask questions from the CLI](ask-from-cli.md) for batch workflows.

## Output formats

All commands support `--format` and `--output`:

```bash
datasight quality --format json
datasight quality --format markdown -o quality.md
datasight profile --format json -o profile.json
```

Available formats: `table` (default), `json`, `markdown`.
