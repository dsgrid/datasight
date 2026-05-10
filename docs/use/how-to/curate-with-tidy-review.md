# Curate datasets with Tidy review

!!! warning "Experimental"
    Tidy review is experimental and its interface is subject to change.
    Review every proposal carefully before applying, and back up your
    database before running an apply for the first time. The CLI's
    `--dry-run` is the safest way to preview changes.

Tidy review is an LLM-augmented advisor for the project developer. It
proposes reshapes that the regex-based detector (`tidy suggest`) cannot
see — fuel-type-as-column, geography-as-column, scenario-as-column,
multi-axis pivots — and lets you approve each one before it changes the
database.

This is a **developer-side curation tool**. The intended workflow is:
the project developer runs Tidy review while preparing a dataset, then
end users ask questions through the read-only web UI against the tidy
form. End users never see this surface.

Two ways to drive it:

- **Web UI drawer** (`datasight run`) — point-and-click flow with a
  visual melt diagram, per-card edits, live SQL preview, and inline row
  preview. Best for interactive curation.
- **CLI** (`datasight tidy review`) — scriptable, plan-file based.
  Best for CI and reproducible dataset prep.

Both surfaces share the same underlying engine, so a proposal you tweak
in the drawer produces the same DDL the CLI would.

## From the web UI

Open a project with `datasight run`, click a table in the schema
sidebar to expand it, and click the **Tidy** chip in the per-table
action row. The chip is DuckDB-only — other backends don't get it
because the apply pipeline opens a writable DuckDB connection.

### What the drawer does

The drawer slides in from the right and immediately runs the
deterministic detector, showing any regex-matched proposals as cards.
The LLM does not run automatically — the **Run agent** button at the
top of the drawer fires the LLM advisor and appends its proposals to
the list. This split is intentional: the deterministic step is free
and instant, the LLM step costs a model call, and you should opt in
explicitly.

Per proposal you get:

- A small SVG **melt diagram** showing source columns folding into the
  long-form schema (id columns straight across, measure columns curved
  into the value column, dimension columns annotated with their dtype).
- Editable **target name**, **value column**, and **id columns**.
- A **Keep rows with NULL values** checkbox (off by default — see
  [Null handling](#null-handling) below).
- A **Preview rows** button that samples 50 rows of the would-be long
  form against the live database, no DDL.
- A **Show SQL** toggle that renders the DDL the apply step would run,
  reflecting your edits and the current view/table mode in real time.
- A **Skip** toggle to exclude one proposal from the next apply (useful
  when the agent returns alternatives you don't want).

### Apply

The footer holds the materialization controls and the Apply button:

- **Materialize as** — `view` (default) or `table`. Views re-evaluate
  against the source on every query; tables are physical copies.
- **Source** — `Keep` (default), `Rename`, `Replace`, or `Drop`. See
  [Source disposition](#source-disposition) below for what each one
  means.
- **Apply** — runs every non-skipped, non-already-applied proposal in
  one batch. Each proposal goes through its own DuckDB transaction with
  a row-count verify, so a mid-batch failure leaves prior successes
  intact and rolls back only the failing one.

After a successful apply, the schema sidebar refreshes so the new
long-form objects show up immediately. If the project keeps a
`schema.yaml` allowlist, the apply rewrites it so the new objects stay
visible across restarts. If the project doesn't have one yet, the
drawer creates it (the CLI keeps the no-op-when-absent default).

### Drawer agent panel

The **Sample rows sent to LLM** input in the agent panel controls
how many rows of values per table go to the configured LLM provider.
Default is 0 (schema-only). Bump it when column names are ambiguous
and the agent isn't picking up obvious patterns from names alone (a
typical sweet spot is 5–10). Values go over the network, so leave it
at 0 for sensitive data — see [Sample rows: opt-in data
exposure](#sample-rows-opt-in-data-exposure) for the rationale.

## From the CLI

The CLI exposes the same engine for scripting and CI. Reach for it when:

- The web UI isn't running (e.g. headless dataset prep on a build box).
- You want to capture the curation as a checked-in plan file that
  replays deterministically across environments.
- You need `--dry-run` to print every DDL statement without touching
  the database.

### When to reach for `tidy review`

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

After a successful reshape you have four options for what happens to
the wide source table. They differ on the *final name* of the long
form:

```bash
# (default) Leave the source untouched alongside the new long form.
datasight tidy review --keep-source

# Rename the source — useful when end users only need the tidy form.
# Long form lives at its target name (e.g. `generation_fuel_long`);
# old wide table moves to `generation_fuel_wide_raw`.
datasight tidy review --rename-source generation_fuel_wide_raw

# Drop the source AND rename the long form into its slot.
# Downstream code that referenced the source name keeps working —
# `SELECT * FROM generation_fuel_wide` now returns the long form.
datasight tidy review --replace-source

# Drop the source. The long form keeps its target name
# (`generation_fuel_long`). Downstream code referencing the source
# name will break — pick this when the new shape is canonical.
datasight tidy review --drop-source
```

!!! warning "Breaking change in `--drop-source`"
    `--drop-source` previously meant *replace* (drop the source and
    rename the long form into its slot). The flag now means *bare drop*
    (long form keeps its target name); use `--replace-source` for the
    old behavior. Scripts that depended on the old `--drop-source`
    should switch.

The flags are mutually exclusive. The disposition runs *only after* the
verify step passes; if anything fails, the entire transaction rolls back
and the source remains in place.

In the web UI these map to the **Source** radio group in the drawer
footer: Keep, Rename, Replace, Drop.

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

## Null handling

Each proposal carries an `include_nulls` flag (default `false`). When
false, rows where the source value is `NULL` are dropped from the long
form — matching DuckDB's native `UNPIVOT` behavior. When true, those
rows survive and the long form has exactly
`len(source) × len(column_mappings)` rows.

The default is `false` because most NULLs in wide tables are
*structural*: a NULL means "this combination doesn't apply" (e.g.,
`lpg_lighting` in a fuel × end-use pivot, where LPG isn't typically
used for lighting). Carrying those rows into the long form just inflates
cardinality with no information value — analysts would filter them out
as their first move anyway.

Flip the toggle on for data where a NULL is a *real* missing observation
you want to keep visible:

- **Sensor data** with hourly columns where some hours had no reading.
  NULL = "we tried to measure but no reading came in" — dropping loses
  the gap.
- **Survey data** with optional questions. NULL = "respondent didn't
  answer" — dropping loses the non-response signal.

In the web UI: the **Keep rows with NULL values** checkbox on each
proposal card. In the CLI: edit the `include_nulls` field in a plan
file and feed it back via `--from`.

!!! info "Mode parity"
    DuckDB 1.5.2 has no `INCLUDE NULLS` clause for `UNPIVOT`, so
    applying the same proposal as a view vs. a table used to silently
    produce different row counts. The current dispatcher avoids this by
    routing through UNPIVOT only when `include_nulls=false` AND mode is
    `table` AND it's a single-pivot proposal — every other combination
    uses `UNION ALL` (with a `WHERE … IS NOT NULL` filter per branch
    when `include_nulls=false`) so the two modes always line up.

## Dimension dtypes

Each dimension column carries a `dtype` so the long-form column doesn't
inherit `VARCHAR` from the literal. The deterministic detector picks
sensible defaults per period kind:

| Kind                                   | Default dtype |
|----------------------------------------|---------------|
| `year`, `hour`, `day`, `month_num`     | `INTEGER`     |
| `year_month`, `year_quarter`, `quarter`, `month` | `VARCHAR` |

LLM-proposed and user-supplied dimensions default to `VARCHAR` unless
the agent picks otherwise. The drawer shows the dtype next to each
dimension name on the proposal card; in plan files it's the
`dimensions[].dtype` field.

Allowed values: `VARCHAR`, `INTEGER`, `BIGINT`, `SMALLINT`, `DOUBLE`,
`DATE`, `TIMESTAMP`. Anything else is rejected at parse time so a
plan can't smuggle SQL into the generated `CAST(...)` clause.

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
        {"name": "fuel_type", "kind": "category", "dtype": "VARCHAR"}
      ],
      "id_columns": ["plant_id", "report_date"],
      "value_column": "value",
      "target_object_name": "generation_fuel_long",
      "column_mappings": [
        {"column": "coal_mwh",    "dimension_values": {"fuel_type": "coal"}},
        {"column": "gas_mwh",     "dimension_values": {"fuel_type": "gas"}},
        {"column": "nuclear_mwh", "dimension_values": {"fuel_type": "nuclear"}}
      ],
      "include_nulls": false,
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
    {"name": "fuel_type", "kind": "category", "dtype": "VARCHAR"},
    {"name": "year",      "kind": "date_period", "dtype": "INTEGER"}
  ],
  "column_mappings": [
    {"column": "coal_2020", "dimension_values": {"fuel_type": "coal", "year": "2020"}},
    {"column": "coal_2021", "dimension_values": {"fuel_type": "coal", "year": "2021"}},
    {"column": "gas_2020",  "dimension_values": {"fuel_type": "gas",  "year": "2020"}},
    {"column": "gas_2021",  "dimension_values": {"fuel_type": "gas",  "year": "2021"}}
  ]
}
```

Field reference:

- `dimensions[].kind` — one of `date_period`, `category`, `geography`,
  `scenario`, `other`.
- `dimensions[].dtype` — one of `VARCHAR`, `INTEGER`, `BIGINT`,
  `SMALLINT`, `DOUBLE`, `DATE`, `TIMESTAMP`. Defaults to `VARCHAR` if
  omitted.
- `include_nulls` — `false` (default) drops rows where the value is
  NULL; `true` keeps them. See [Null handling](#null-handling).
- `value_column` — defaults to `value`. The agent and the deterministic
  detector both prefer this generic name for predictability across
  long-form tables.
- `confidence` — `high`, `medium`, or `low`.

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

### Curate one wide table from the web UI

1. `datasight run` and open the project.
2. Click the table in the schema sidebar, then click **Tidy**.
3. Review the deterministic proposals; click **Run agent** to ask the
   LLM for additional candidates.
4. Skip anything you don't want, edit target/value names if needed.
5. Footer: pick **Materialize as Table** + **Source: Replace** if you
   want the long form to take over the source's name (downstream
   queries keep working). Click **Apply**.
6. Mention the new tidy table in your `schema_description.md` so the
   agent prefers it for future questions.

### Curate one wide table from the CLI

```bash
# 1. Inspect what the regex finds.
datasight tidy suggest --table generation_fuel_wide

# 2. Walk through LLM proposals. Apply the good ones, edit names if needed.
#    --replace-source = drop source, long form takes over its name.
datasight tidy review --table generation_fuel_wide --as table --replace-source

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
