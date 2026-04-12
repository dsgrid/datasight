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
- temporal completeness issues when [`time_series.yaml`](declare-time-series.md) is present

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

See [Configure semantic measures](configure-measures.md) for the full override workflow.

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

### Declaring joins explicitly

datasight infers foreign keys by naming convention, but you can declare them
explicitly in a `joins.yaml` file in your project directory:

```yaml
- child_table: generation_fuel
  child_column: plant_id
  parent_table: plants
  parent_column: id   # optional, defaults to "id"

- child_table: generation_fuel
  child_column: energy_source_code
  parent_table: energy_sources
  parent_column: code
```

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
measure inference](../explanation/semantic-measures.md)), the command raises targeted warnings:

- **Negative generation** — an `energy`-role column with negative values
- **Capacity factor > 1** — a `ratio` column whose p99 exceeds 1.0
- **Implausible heat rate** — a `rate` column in MMBtu/MWh outside 3–30
- **Zero values in rate** — rate/ratio/price columns containing zeros that
  may indicate zero-denominator bugs upstream

## Declarative validation rules

`datasight validate` runs declarative rules defined in `validation.yaml`.
Use this to lock in expectations about your data that should hold across
every refresh.

Generate a starting template, then edit it:

```bash
datasight validate --scaffold
```

This writes `validation.yaml` into the project directory. Example:

```yaml
- table: generation_fuel
  rules:
    - type: required_columns
      columns: [plant_id, report_date, energy_source_code, net_generation_mwh]

    - type: max_null_rate
      column: net_generation_mwh
      threshold: 0.05

    - type: numeric_range
      column: net_generation_mwh
      min: 0

    - type: allowed_values
      column: energy_source_code
      values: [NG, COL, NUC, SUN, WND, WAT, OTH, PET, GEO, BIO, WDS, OOG]

    - type: uniqueness
      columns: [plant_id, report_date, energy_source_code]

    - type: row_count
      min: 100

    - type: freshness
      column: report_date
      max_age_days: 90
```

### Supported rule types

| Rule | Purpose |
|------|---------|
| `required_columns` | Columns that must exist in the schema |
| `max_null_rate` | Maximum allowed fraction of NULL values (0.0–1.0) |
| `numeric_range` | `min` and/or `max` bounds for a numeric column |
| `allowed_values` | Whitelist of accepted category values |
| `regex` | Pattern that all non-null values must match |
| `uniqueness` | Composite key that must be unique across rows |
| `monotonic` | Column must be non-decreasing or strictly increasing |
| `row_count` | Table row count must fall within `min`/`max` bounds |
| `freshness` | Latest date must be within `max_age_days` of today |

Run the rules:

```bash
datasight validate
datasight validate --table generation_fuel
datasight validate --format markdown -o validation.md
```

Each result is reported as `pass`, `fail`, or `warn`, with a short detail
string explaining the violation.

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
2. **Quality** — find nulls, range issues, and date gaps
3. **Integrity** — verify primary keys, foreign keys, and join behavior
4. **Distribution** — inspect percentiles, outliers, and temporal spikes
5. **Measures** — identify metrics and verify aggregation defaults
6. **Dimensions** — find grouping columns for breakdowns
7. **Trends** — discover time-series candidates
8. **Validate** — codify expectations in `validation.yaml` and run them on every refresh

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
