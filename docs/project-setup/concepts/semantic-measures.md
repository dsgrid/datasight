# What semantic measures are

datasight can infer likely measures from your schema and let you override
their behavior in a project-level `measures.yaml` file. This page explains
what measures are and how they shape AI behavior. For the editing
workflow, see [Configure semantic measures](../how-to/configure-measures.md).

## Why measures matter

Measures are especially important for energy datasets where:

- energy volumes such as `net_generation_mwh` should usually be summed
- power signals such as `demand_mw` should usually be averaged or peaked
- rates, prices, and factors should usually not be summed
- project-specific formulas such as `net_load_mw` should behave like
  first-class measures

A semantic measure can include:

- a **role** — `energy`, `power`, `capacity`, `rate`, `price`, or `ratio`
- a **default aggregation**
- optional **weighted-average behavior** through `weight_column`
- optional **display metadata** — `display_name`, `format`
- **preferred chart types**
- a **calculated SQL expression**

## How measures influence the AI

`measures.yaml` affects query behavior in two ways.

### 1. Prompt guidance

Datasight includes semantic measure context in the schema prompt so the
model sees default aggregations, weighting hints, display metadata, and
suggested rollup SQL before it writes a query.

### 2. Pre-execution validation

Before `run_sql` or `visualize_data` executes, datasight validates the
generated SQL against project-defined physical measure rules from
`measures.yaml`.

For physical measures with a `column`:

- `default_aggregation` is enforced unless the user explicitly asks for a
  different aggregation such as `sum`, `total`, `average`, `max`, or
  `minimum`
- `allowed_aggregations` limits which rollups are accepted
- invalid aggregations are rejected before the SQL runs, so the model
  must regenerate

For calculated measures defined with `name` and `expression`:

- datasight uses them in prompt guidance, inspect flows, and suggestions
- current SQL enforcement is focused on physical columns, not calculated
  expressions embedded in arbitrary generated SQL

This means semantic measures are not just hints. For project-defined
physical measures, they are part of the execution contract.

## Where measures show up

datasight uses measure configuration in:

- `datasight measures`
- the web **Key measures** inspect flow
- prompt guidance for `datasight ask`
- pre-execution SQL validation for project-defined physical measures
- trend recommendations
- reusable recipes

## Related

- [Configure semantic measures](../how-to/configure-measures.md) — how to
  author `measures.yaml`, including weighted averages, calculated
  measures, and the web editor.
- [Declare time series](../how-to/declare-time-series.md) — the
  complementary `time_series.yaml` for temporal structure.
