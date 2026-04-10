# Semantic Measures

`datasight` can infer likely measures from your schema and let you override
their behavior in a project-level `measures.yaml` file.

This is most useful for energy datasets where:

- energy volumes such as `net_generation_mwh` should usually be summed
- power signals such as `demand_mw` should usually be averaged or peaked
- rates, prices, and factors should usually not be summed
- project-specific formulas such as `net_load_mw` should behave like
  first-class measures

## What A Measure Includes

A semantic measure can include:

- a role such as `energy`, `power`, `capacity`, `rate`, `price`, or `ratio`
- a default aggregation
- optional weighted-average behavior through `weight_column`
- optional display metadata such as `display_name` and `format`
- preferred chart types
- a calculated SQL expression

Datasight uses this information in:

- `datasight measures`
- the web **Key measures** inspect flow
- prompt guidance for `datasight ask`
- pre-execution SQL validation for project-defined physical measures
- trend recommendations
- reusable recipes

## How Measures Influence The AI

`measures.yaml` now affects query behavior in two different ways:

1. Prompt guidance
   Datasight includes semantic measure context in the schema prompt so the model
   sees default aggregations, weighting hints, display metadata, and suggested
   rollup SQL before it writes a query.

2. Pre-execution validation
   Before `run_sql` or `visualize_data` executes, datasight validates the
   generated SQL against project-defined physical measure rules from
   `measures.yaml`.

For physical measures with a `column`:

- `default_aggregation` is enforced unless the user explicitly asks for a
  different aggregation such as `sum`, `total`, `average`, `max`, or `minimum`
- `allowed_aggregations` limits which rollups are accepted
- invalid aggregations are rejected before the SQL runs, so the model must
  regenerate

For calculated measures defined with `name` and `expression`:

- datasight uses them in prompt guidance, inspect flows, and suggestions
- current SQL enforcement is focused on physical columns, not calculated
  expressions embedded in arbitrary generated SQL

This means semantic measures are no longer just hints. For project-defined
physical measures, they are part of the execution contract.

## Inspect Measures

### CLI

Use the deterministic measure overview:

```bash
datasight measures
datasight measures --table generation_hourly
datasight measures --format json
```

This shows:

- likely measures
- the semantic role for each measure
- default and forbidden aggregations
- weighted-average hints
- suggested SQL rollup formulas
- configured display/chart metadata

### Web UI

In the web UI:

1. Load or explore a dataset.
2. Use **Key measures** from the landing starter or the **Inspect** section.
3. Review the inferred measure cards and SQL rollup guidance.
4. Open **Measure Overrides** if you want to change measure behavior.

## `measures.yaml`

`measures.yaml` lives in the project root and stores project-specific measure
semantics.

Datasight can create it for you in several ways:

- `datasight generate`
- `datasight measures --scaffold`
- saving an explore session as a project in the web UI
- saving from the web **Measure Overrides** editor

### Physical measure override

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

### Calculated measure

Use `name` and `expression` when you want to define a semantic measure that
does not exist as a physical column:

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

## Supported Fields

Common fields in `measures.yaml`:

- `table`
- `column`
- `name`
- `expression`
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

Use:

- `column` for physical measures
- `name` + `expression` for calculated measures

## Weighted Averages

For rates, prices, and similar non-additive measures, use `weight_column` to
prefer a weighted average over a plain `AVG`.

Example:

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

## Display Metadata

`display_name`, `format`, and `preferred_chart_types` help datasight produce
more useful defaults.

Examples:

- `display_name: Net generation`
- `format: mwh`
- `preferred_chart_types: [line, area]`

These fields influence:

- deterministic measure overviews
- trend recommendations
- starter recipes
- web measure editing flows

## Use The Web Editor

The **Measure Overrides** section in the web UI lets you edit `measures.yaml`
without leaving the app.

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

Datasight reloads semantic measure state after save, so the updated measure
config immediately affects inspect flows, prompt guidance, and SQL validation.

## Which File Should You Edit?

Use:

- `schema_description.md` for narrative domain context
- `queries.yaml` for example questions and correct SQL
- `measures.yaml` for metric semantics and calculated measures

## Suggested Workflow

1. Start with `datasight measures` or **Key measures**.
2. Identify any wrong aggregation defaults.
3. Add or edit entries in `measures.yaml`.
4. Define calculated measures for project-specific business logic.
5. Re-run **Key measures**, **Trend ideas**, or `datasight ask`.

If a query still uses the wrong aggregation, check whether the user prompt
explicitly asked for something like a total or average. Explicit user intent
can override the configured default when that aggregation is allowed.
