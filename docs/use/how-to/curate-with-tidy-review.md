# Curate datasets with `tidy review`

`datasight tidy review` is an LLM-augmented advisor for the project
developer. It proposes reshapes that the regex-based detector
(`tidy suggest`) cannot see — fuel-type-as-column, geography-as-column,
scenario-as-column, multi-axis pivots — and lets you approve each one
before it changes the database.

This is a **developer-side curation tool**. The intended workflow is:
the project developer runs `tidy review` while preparing a dataset, then
end users ask questions through the read-only web UI against the tidy
form. End users never see this command.

## When to reach for `tidy review`

Use the deterministic commands first:

- `datasight tidy suggest` — list candidates.
- `datasight tidy view` / `datasight tidy table` — apply the obvious ones.

Use `tidy review` when those leave gaps. The regex detector recognizes
period-shaped pivots (`gen_2020_01`, `mwh_q1`, `hour_00`). It does not
recognize:

- **Category-as-column** — `coal_mwh`, `gas_mwh`, `nuclear_mwh`,
  `solar_mwh`, `wind_mwh` should pivot into a `fuel_type` dimension.
- **Geography-as-column** — `ca_capacity`, `tx_capacity`, `ny_capacity`
  should pivot into a `state` (or `region`) dimension.
- **Scenario-as-column** — `base_case_load`, `high_growth_load`,
  `low_load` should pivot into a `scenario` dimension.
- **Multi-axis pivots** — `coal_2020`, `coal_2021`, `gas_2020`,
  `gas_2021` is one reshape with two dimensions (`fuel_type` × `year`),
  not two separate single-axis reshapes.

The LLM sees the schema (column names, dtypes, row counts), the regex
detector's existing hits, and optionally a few sample rows per table,
and proposes structured reshapes you can apply, edit, or skip.

## Prerequisites

`tidy review` opens a writable connection to the project's DuckDB
database, so it requires:

- A project with `DB_MODE=duckdb` and a `DB_PATH`.
- A configured LLM provider (`LLM_PROVIDER=anthropic` with a real API
  key, GitHub Models, or a local Ollama instance). The deterministic
  `tidy {suggest,view,table}` commands work without an API key;
  `tidy review` does not.

## The interactive review loop

Run with no flags and walk through proposals one at a time:

```bash
datasight tidy review
```

For each proposal you'll see:

```
Proposal 1 of 2 — generation_fuel_wide -> generation_fuel_long
  Source: llm   Confidence: high   Mode: view
  Dimensions: fuel_type (category)
  Mapped (5): coal_mwh, gas_mwh, nuclear_mwh, solar_mwh, wind_mwh
  Id columns: plant_id, report_date
  Value column: net_generation_mwh
  Source disposition: keep source
  Rationale: Fuel-type categories encoded across column suffixes.
  [a]pply / [s]kip / [e]dit / [q]uit:
```

The four actions:

- **`a` apply** — wraps the reshape in a DuckDB transaction, runs the
  DDL, verifies that the target row count equals
  `len(source) × len(column_mappings)`, applies the source disposition,
  and commits.
- **`s` skip** — moves on without changing the database.
- **`e` edit** — lets you rename the target object, the value column,
  or trim the id columns. Dimensions and column-to-value mappings are
  not editable inline; for those, use `--out` to dump a plan, edit the
  JSON, and re-run with `--from`.
- **`q` quit** — stops the loop. Already-applied proposals stay
  committed; remaining ones are skipped.

## Choosing a target shape

Two flags control how the long form is materialized:

```bash
# Default: a view that re-evaluates against the source on every query
datasight tidy review --as view

# A physical table — recommended when the source rarely changes
datasight tidy review --as table
```

Tables are usually the better choice for curated datasets because
DuckDB's Python binding (1.5.2) has a regression that breaks
`UNPIVOT` inside views. `tidy review` works around that automatically
by emitting `UNION ALL` for views, but the SQL is more verbose.

## Source disposition

After a successful reshape you have three options for what happens to
the wide source table:

```bash
# (default) Leave the source untouched alongside the new long form.
datasight tidy review --keep-source

# Rename the source — useful when end users only need the tidy form.
datasight tidy review --rename-source generation_fuel_wide_raw

# Drop the source entirely.
datasight tidy review --drop-source
```

The flags are mutually exclusive. The disposition runs *only after* the
verify step passes; if anything fails, the entire transaction rolls back
and the source remains in place.

## Sample rows: opt-in data exposure

By default, `tidy review` sends only the schema (column names, dtypes,
row counts) to the LLM. To improve the model's judgment on ambiguous
cases, you can opt in to sending a few sample rows per candidate:

```bash
datasight tidy review --sample 5
```

The values in those rows go to your configured LLM provider over the
network. Use this only when the data is not sensitive — research
datasets, public datasets, or your own test data.

## Non-interactive workflows

Three flags compose for scripting and CI:

### `--apply-all`: apply everything without prompting

```bash
datasight tidy review --apply-all --as table
```

Combine with `--dry-run` to print the DDL each proposal would run
without changing the database:

```bash
datasight tidy review --apply-all --dry-run
```

### `--out`: dump proposals to a plan file

`--out` writes a JSON plan describing every proposal — without applying
anything. The plan is a structured snapshot of the dimensions, column
mappings, id columns, value column, and target name for each reshape.

```bash
datasight tidy review --out reshape_plan.json
```

Without `--from`, this dumps the deterministic detector's hits as a
starting point. Hand-edit the JSON to add LLM-style proposals or
fine-tune what the regex found.

### `--from`: replay a plan deterministically

```bash
datasight tidy review --from reshape_plan.json --apply-all
```

`--from` skips the LLM call entirely and applies the plan as written.
This is the path that CI-friendly dataset prep uses: hand-curate the
plan once, commit it to the repo, run `--from` from a build step.

The structural validator catches stale plans (column names that no
longer exist, target names that already exist) before any DDL runs, so
you get a clean error rather than a half-applied reshape.

## Plan file format

```json
{
  "version": 1,
  "proposals": [
    {
      "table": "generation_fuel_wide",
      "dimensions": [
        {"name": "fuel_type", "kind": "category"}
      ],
      "id_columns": ["plant_id", "report_date"],
      "value_column": "net_generation_mwh",
      "target_object_name": "generation_fuel_long",
      "column_mappings": [
        {"column": "coal_mwh",    "dimension_values": {"fuel_type": "coal"}},
        {"column": "gas_mwh",     "dimension_values": {"fuel_type": "gas"}},
        {"column": "nuclear_mwh", "dimension_values": {"fuel_type": "nuclear"}}
      ],
      "confidence": "high",
      "source": "user",
      "rationale": "Fuel-type-as-column pivot."
    }
  ]
}
```

For a multi-axis pivot, add more entries to `dimensions` and one
`dimension_values` entry per dimension on each mapping:

```json
{
  "dimensions": [
    {"name": "fuel_type", "kind": "category"},
    {"name": "year", "kind": "date_period"}
  ],
  "column_mappings": [
    {"column": "coal_2020", "dimension_values": {"fuel_type": "coal", "year": "2020"}},
    {"column": "coal_2021", "dimension_values": {"fuel_type": "coal", "year": "2021"}},
    {"column": "gas_2020",  "dimension_values": {"fuel_type": "gas",  "year": "2020"}},
    {"column": "gas_2021",  "dimension_values": {"fuel_type": "gas",  "year": "2021"}}
  ]
}
```

Allowed `dimensions[].kind` values: `date_period`, `category`,
`geography`, `scenario`, `other`. Allowed `confidence` values: `high`,
`medium`, `low`.

## Verify-before-dispose

Every applied proposal goes through a four-step transaction:

1. Build the long-form target (`CREATE OR REPLACE TABLE|VIEW`).
2. Verify `count(target) == count(source) × len(column_mappings)`.
3. Apply the source disposition (`keep`, `rename`, `drop`).
4. Commit.

If the count check fails — usually because `id_columns` omits a column
whose values would duplicate or drop rows — the transaction rolls back
in step 2, before the source is touched. The error message names the
table and the count it expected.

## Recipes

### Curate one wide table for end users

```bash
# 1. Inspect what the regex finds.
datasight tidy suggest --table generation_fuel_wide

# 2. Walk through LLM proposals. Apply the good ones, edit names if needed.
datasight tidy review --table generation_fuel_wide --as table --drop-source

# 3. Mention the new tidy table in your schema_description.md so the agent
#    prefers it.
```

### Snapshot the curation plan into the repo

```bash
# Capture the current set of proposals.
datasight tidy review --out reshape_plan.json

# Review and edit reshape_plan.json by hand.

# Commit it. Future builds replay deterministically:
datasight tidy review --from reshape_plan.json --apply-all
```

### Audit-only run (no changes)

```bash
datasight tidy review --apply-all --dry-run
```

Prints every DDL statement and the disposition each proposal would
take. Useful for code review before merging a curation PR.

## See also

- [Tidy a wide-month spreadsheet](../tutorials/tidy.md) — period-shaped
  pivots that the deterministic detector handles end-to-end.
- [Audit data quality](audit-data-quality.md) — `datasight quality`
  surfaces tidy suggestions alongside null/range/date-coverage checks.
- [Write a schema description](../../project-setup/how-to/schema-description.md)
  — point the agent at the long-form tables once they exist.
