# Define validation rules

`validation.yaml` declares expectations about your data that should hold
across every refresh — required columns, allowed values, freshness, row
counts, uniqueness, and so on. `datasight validate` checks each rule and
reports `pass`, `fail`, or `warn`. Lock these in alongside the schema so
that data drift is caught the moment it appears, not after a stakeholder
spots a bad number.

## Scaffold a starting file

```bash
datasight validate --scaffold
```

This writes a commented `validation.yaml` into the project directory.
Edit it to match your data:

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

## Supported rule types

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

## Run the rules

```bash
datasight validate
datasight validate --table generation_fuel
datasight validate --format markdown -o validation.md
```

Each result is reported as `pass`, `fail`, or `warn`, with a short detail
string explaining the violation. See
[Audit data quality](../../use/how-to/audit-data-quality.md#run-declarative-validation)
for how `validate` fits with the other deterministic audits.

## Which file should you edit?

| File | Purpose |
|------|---------|
| `schema_description.md` | Narrative domain context |
| `queries.yaml` | Example questions and correct SQL |
| `measures.yaml` | Metric semantics and calculated measures |
| `time_series.yaml` | Temporal structure and completeness expectations |
| `joins.yaml` | Foreign-key relationships when naming convention doesn't apply |
| `validation.yaml` | Declarative data-quality rules |
