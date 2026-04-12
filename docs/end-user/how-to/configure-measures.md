# Configure semantic measures

`measures.yaml` lets you override the default aggregation datasight uses
for each measure and define calculated measures (like `net_load_mw`) that
behave like first-class columns. For the concept behind measures and how
they influence the AI, see
[What semantic measures are](../explanation/semantic-measures.md).

## Inspect what datasight inferred

### CLI

```bash
datasight measures
datasight measures --table generation_hourly
datasight measures --format json
```

This shows likely measures, their semantic roles, default and forbidden
aggregations, weighted-average hints, suggested SQL rollup formulas, and
configured display/chart metadata.

### Web UI

1. Load or explore a dataset.
2. Use **Key measures** from the landing starter or the **Inspect**
   sidebar section.
3. Review the inferred measure cards and SQL rollup guidance.
4. Open **Measure Overrides** if you want to change measure behavior.

## Create `measures.yaml`

`measures.yaml` lives in the project root. Datasight can create it for
you:

- `datasight generate`
- `datasight measures --scaffold`
- saving an explore session as a project in the web UI
- saving from the web **Measure Overrides** editor

### Override a physical measure

Use `column` when you want to override an existing physical column:

```yaml
- table: generation_hourly
  column: demand_mw
  display_name: System demand
  default_aggregation: max
  format: mw
  preferred_chart_types:
    - line
  reason: This project usually wants peak demand.
```

### Define a calculated measure

Use `name` and `expression` for a measure that doesn't exist as a physical
column:

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

### Weighted averages

For rates, prices, and similar non-additive measures, use `weight_column`
to prefer a weighted average over a plain `AVG`:

```yaml
- table: emissions_hourly
  column: co2_rate_lb_per_mwh
  default_aggregation: avg
  average_strategy: weighted_avg
  weight_column: net_generation_mwh
  format: float
```

That tells datasight to prefer rollups like:

```sql
SUM(co2_rate_lb_per_mwh * net_generation_mwh)
/ NULLIF(SUM(net_generation_mwh), 0)
```

### Display metadata

`display_name`, `format`, and `preferred_chart_types` influence
deterministic measure overviews, trend recommendations, starter recipes,
and web measure editing flows. Examples:

- `display_name: Net generation`
- `format: mwh`
- `preferred_chart_types: [line, area]`

## Supported fields

Common fields in `measures.yaml`:

- `table`
- `column` (for physical measures)
- `name` + `expression` (for calculated measures)
- `role`
- `unit`
- `default_aggregation`
- `average_strategy`
- `weight_column`
- `display_name`
- `format`
- `preferred_chart_types`
- `allowed_aggregations`
- `forbidden_aggregations`
- `reason`

## Use the web editor

The **Measure Overrides** sidebar section in the web UI lets you edit
`measures.yaml` without leaving the app.

For a physical measure:

1. Pick **physical measure**.
2. Choose an inferred measure.
3. Adjust aggregation, weighting, display name, format, or chart types.
4. Save overrides.

For a calculated measure:

1. Pick **calculated measure**.
2. Choose the target table.
3. Enter the measure name and SQL expression.
4. Set aggregation, display name, format, and preferred chart types.
5. Save overrides.

Datasight reloads semantic measure state after save, so the updated config
immediately affects inspect flows, prompt guidance, and SQL validation.

## Which file should you edit?

- `schema_description.md` — narrative domain context
- `queries.yaml` — example questions and correct SQL
- `measures.yaml` — metric semantics and calculated measures
- [`time_series.yaml`](declare-time-series.md) — temporal structure and
  completeness expectations

## Suggested workflow

1. Start with `datasight measures` or **Key measures**.
2. Identify any wrong aggregation defaults.
3. Add or edit entries in `measures.yaml`.
4. Define calculated measures for project-specific business logic.
5. Re-run **Key measures**, **Trend ideas**, or `datasight ask`.

If a query still uses the wrong aggregation, check whether the user
prompt explicitly asked for something like a total or average. Explicit
user intent can override the configured default when that aggregation is
allowed.
