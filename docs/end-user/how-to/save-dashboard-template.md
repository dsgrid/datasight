# Save a dashboard as a template

A dashboard template captures the cards, filters, and layout of a
dashboard so you can re-apply it to other tables or datasets. This is
useful when you iterate on one generation dataset and then want to run
the same analysis across many more without rebuilding the dashboard by
hand.

Templates are project-scoped: they live in
`<project>/.datasight/templates/<name>.json` and are loaded relative
to a `--project-dir` (default: current directory). That means they
ship with the project in git and can rely on the project's own
database for fixed lookup tables.

## Save the current dashboard

Build a dashboard in the web UI the usual way — pin cards, add
filters, arrange the layout. Then, from the project directory:

```bash
datasight templates save generation-by-fuel \
    --description "Monthly generation share by fuel type"
```

The template is written to `.datasight/templates/generation-by-fuel.json`
inside the project. Pass `--project-dir PATH` to target a different
project.

### How required tables are chosen

By default, datasight parses every pinned card's SQL and records the
full set of referenced tables as the template's `required_tables`. A
dashboard whose cards join `generation_fuel` to `plants` ends up with
`required_tables: ["generation_fuel", "plants"]`.

When datasight can't infer them (for example, a notes-only dashboard),
pass them explicitly with `--table` (repeatable):

```bash
datasight templates save fuel-mix \
    --table generation_fuel \
    --table plants
```

## Inspect saved templates

```bash
datasight templates list
datasight templates show generation-by-fuel
```

`list` prints one row per template with the required tables and card
count. `show` emits the full JSON — useful for piping into `jq` or
checking which filters were captured.

## Replace or remove a template

```bash
# Refresh an existing template with the current dashboard
datasight templates save generation-by-fuel --overwrite

# Remove a template you no longer need
datasight templates delete generation-by-fuel
```

## Apply a template to other datasets

`templates apply` registers each required table as a view inside an
in-memory DuckDB connection and then re-runs every card. Each
required table is satisfied either by:

- `--table NAME=PATH` — maps the table to a parquet file. Repeat
  the flag once per rotating table.
- The project's own DuckDB — any required table that is not passed
  via `--table` is looked up in the database referenced by the
  project's `.env` (`DB_MODE=duckdb`, `DB_PATH=...`). This lets
  fixed lookup tables like `plants` stay where they already live.

### Single-shot

Apply the template once and write the rendered HTML to a file:

```bash
datasight templates apply generation-by-fuel \
    --table generation_fuel=data/2020.parquet \
    --output out/2020.html
```

If `--output` is omitted but `--export-dir DIR` is given, the output
filename is derived from the rotating parquet's stem (here:
`out/2020.html`).

### Batch over many files

If exactly one `--table` mapping uses a shell glob, datasight applies
the template once per matched file and writes one HTML per input into
`--export-dir`:

```bash
datasight templates apply generation-by-fuel \
    --table 'generation_fuel=data/*.parquet' \
    --export-dir out/
```

For each matched file, datasight:

1. Opens an in-memory DuckDB connection.
2. ATTACHes the project's DuckDB and exposes its tables as views.
3. Registers the rotating parquet as the named view (overriding any
   same-named table in the project DB).
4. Runs every card's SQL.
5. Writes `<export-dir>/<parquet-stem>.html`.

Cards that reference columns missing from a particular parquet fail
with an error recorded in the output, while the remaining cards still
render. Pass `--fail-fast` to stop on the first failure.

### Projects without a DuckDB

If the project doesn't have `DB_MODE=duckdb` configured, every
required table must be supplied with `--table`. Datasight prints the
missing names when a required table can't be resolved.

## Templated filters (variables)

A common pattern is capturing a dashboard that filters to one year (or
region, or fuel type) and then running the same dashboard across many
others. Templates support variables for this.

Declare a variable at save time with `--var NAME=VALUE`. Every
occurrence of `VALUE` in each card's SQL is rewritten to `{{NAME}}`,
and `NAME` is recorded as a required placeholder:

```bash
datasight templates save generation-by-fuel \
    --var year=2020 \
    --var-from-filename 'year=(\d{4})'
```

`--var-from-filename` attaches a regex that extracts the variable's
value from each rotating parquet's filename at apply time. The first
capture group (or whole match) is used.

At apply time, variables resolve in this precedence:

1. `--var NAME=VALUE` passed to `templates apply` (highest — one value
   applied to every file in a batch).
2. The `from_filename` regex, evaluated against each input's filename.
3. The variable's default (the `VALUE` supplied at save time).

**Failure behavior**: if a variable has a `from_filename` regex and a
rotating file's name doesn't match, that file fails — datasight does
**not** fall back to the default. This is deliberate: silently rendering
the wrong year against the wrong parquet produces plausible-looking but
incorrect dashboards, which is worse than a clear error.

### Example

Save a dashboard whose SQL filters `WHERE year = 2020`:

```bash
datasight templates save generation-by-fuel \
    --var year=2020 \
    --var-from-filename 'year=(\d{4})'
```

The card's SQL now reads `WHERE year = {{year}}` in the JSON.

Apply it across yearly parquets:

```bash
datasight templates apply generation-by-fuel \
    --table 'generation_fuel=data/gen_*.parquet' \
    --export-dir out/
```

Each input's filename (`gen_2020.parquet`, `gen_2021.parquet`, …)
provides the `year` value, and each output HTML reflects the correct
year.

Override for a single run:

```bash
datasight templates apply generation-by-fuel \
    --table generation_fuel=data/gen_2020.parquet \
    --output out/2099.html \
    --var year=2099
```

## Template file format

Templates are plain JSON. A minimal example:

```json
{
  "name": "generation-by-fuel",
  "version": 2,
  "description": "Monthly generation share by fuel type",
  "required_tables": ["generation_fuel", "plants"],
  "required_columns": [],
  "variables": [
    {"name": "year", "default": "2020", "from_filename": "(\\d{4})"}
  ],
  "items": [
    {
      "id": 1,
      "type": "chart",
      "title": "Generation by fuel",
      "sql": "SELECT energy_source_code, SUM(net_generation_mwh) AS total_mwh FROM generation_fuel GROUP BY 1",
      "plotly_spec": { "data": [], "layout": {} }
    }
  ],
  "columns": 2,
  "filters": []
}
```

`items`, `columns`, and `filters` share the shape of a project's
`.datasight/dashboard.json`, so saving a template is effectively
copying that file and tagging it with a name and required tables.

Older v1 templates with a single `source_table` field are migrated
on load — no re-save is required.
