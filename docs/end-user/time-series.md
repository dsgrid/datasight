# Time Series Declarations

Energy research datasets often contain hourly time arrays — for example,
8,760 rows per year for each combination of region, fuel type, and metric.
These arrays frequently have issues that are hard to catch with generic
quality checks:

- missing hours from incomplete data ingestion
- DST spring-forward gaps (a missing hour when clocks jump ahead)
- DST fall-back duplicates (a repeated hour when clocks fall back)
- leap-year inconsistencies (some groups include February 29, others do not)

`time_series.yaml` lets you declare the temporal structure of your tables so
datasight can check completeness automatically.

## How It Works

1. You create a `time_series.yaml` file in your project directory.
2. `datasight quality` runs temporal completeness checks against the
   declarations — no LLM needed.
3. The AI agent receives the declarations as context, so it understands the
   timestamp column, expected frequency, and group structure when answering
   time-series questions.

## `time_series.yaml`

The file lives in the project root alongside `measures.yaml` and
`queries.yaml`.

### Minimal example

```yaml
- table: generation_hourly
  timestamp_column: datetime_utc
  frequency: PT1H
```

### Full example

```yaml
- table: generation_hourly
  timestamp_column: datetime_utc
  frequency: PT1H
  group_columns: [region, energy_source_code]
  time_zone: UTC

- table: load_forecast
  timestamp_column: forecast_hour
  frequency: PT1H
  group_columns: [zone_id]
  time_zone: America/New_York
```

### Required fields

| Field | Description |
|-------|-------------|
| `table` | Table name |
| `timestamp_column` | The column that defines the time axis |
| `frequency` | Expected interval as an ISO 8601 duration |

### Optional fields

| Field | Default | Description |
|-------|---------|-------------|
| `group_columns` | none | Columns that define independent time arrays. Each unique combination of these values should have a complete series. |
| `time_zone` | `UTC` | IANA time zone name. Important for DST-aware datasets. |

## Frequency values

Frequencies use the [ISO 8601 duration](https://en.wikipedia.org/wiki/ISO_8601#Durations)
format:

| Duration | Meaning | Typical row count per year |
|----------|---------|---------------------------|
| `PT15M` | 15 minutes | 35,040 (non-leap) / 35,136 (leap) |
| `PT30M` | 30 minutes | 17,520 / 17,568 |
| `PT1H` | 1 hour | 8,760 / 8,784 |
| `P1D` | 1 day | 365 / 366 |
| `P1M` | 1 month | 12 |

## Quality checks

When `time_series.yaml` exists, `datasight quality` adds two new sections
to its output:

### Time Series

A summary of each declared time series showing row count, frequency, and
date range.

### Temporal Completeness

Issues found in the data:

- **gap** — an interval between consecutive timestamps that is larger than
  the declared frequency. This catches missing hours, dropped days, and
  DST spring-forward gaps.
- **duplicate** — a timestamp that appears more than once within a group.
  This catches DST fall-back duplicates and accidental re-ingestion.

```bash
datasight quality
datasight quality --table generation_hourly
datasight quality --format json -o quality.json
```

!!! tip "DST and leap-year detection"
    For datasets stored in local time (such as `America/New_York`),
    set the `time_zone` field so the quality report context is clear.
    A spring-forward gap in Eastern time is expected to produce a
    missing hour on the second Sunday of March. A fall-back duplicate
    produces an extra hour on the first Sunday of November.

    For datasets stored in UTC, DST is not an issue — but leap-year
    completeness still matters. A 2024 dataset should have 8,784 hourly
    rows, not 8,760.

## Creating the file

### With `datasight generate`

`datasight generate` automatically scaffolds `time_series.yaml` alongside
`schema_description.md`, `queries.yaml`, and `measures.yaml`. It detects
tables with timestamp columns and creates entries with a default `PT1H`
frequency.

```bash
datasight generate
```

Review and edit the generated file — you will likely need to adjust the
frequency, add group columns, and set the correct time zone.

### With `datasight init`

`datasight init` copies a commented template:

```bash
datasight init my-project
```

### Manually

Create `time_series.yaml` in the project root:

```yaml
- table: generation_hourly
  timestamp_column: datetime_utc
  frequency: PT1H
  group_columns: [region, energy_source_code]
  time_zone: UTC
```

## How it helps the AI

When you use `datasight ask` or the web UI, the time series declarations
are included in the system prompt. This means the AI already knows:

- which column is the time axis
- the expected frequency
- which columns define independent groups
- the time zone

So when you ask "are there any gaps in the wind generation data?", the
agent can write targeted SQL using the correct timestamp column and group
structure without guessing.

## Which file should you edit?

| File | Purpose |
|------|---------|
| `schema_description.md` | Narrative domain context |
| `queries.yaml` | Example questions and correct SQL |
| `measures.yaml` | Metric semantics and calculated measures |
| `time_series.yaml` | Temporal structure and completeness expectations |
