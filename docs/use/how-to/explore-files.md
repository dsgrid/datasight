# Inspect files

You have CSV, Parquet, Excel, or DuckDB files and want to understand what's
in them. datasight can profile the data, surface measures and dimensions,
suggest trends, and generate starter prompts — all without setting up a
project or calling an LLM.

## One command: `datasight inspect`

`datasight inspect` runs every deterministic analysis in one shot:

```bash
datasight inspect generation.parquet
datasight inspect generation.csv plants.csv
datasight inspect generation.xlsx
datasight inspect data_dir/
```

You can pass any mix of file types; they are loaded into an ephemeral
DuckDB session for the duration of the command.

This prints:

- **Profile** — table and column counts, row counts, largest tables, date coverage
- **Quality** — null-heavy columns, suspicious numeric ranges, notes
- **Measures** — inferred metrics with roles, default aggregations, rollup SQL
- **Dimensions** — grouping candidates with distinct counts and sample values
- **Trends** — date/measure pairs with chart recommendations
- **Recipes** — suggested prompts for deeper exploration

Nothing is written to disk. Use `--format json` or `--format markdown` to
change the output, or `-o report.md` to save it to a file:

```bash
datasight inspect generation.parquet --format json
datasight inspect generation.parquet --format markdown -o overview.md
```

## Explore files in the web UI

If you prefer a visual interface, the web UI can also work with files
directly — no project setup required:

```bash
datasight run
```

Open <http://localhost:8084>. The landing page shows:

1. **Guided starters** — choose a workflow like **Profile this dataset**,
   **Find key dimensions**, **Build a trend chart**, or **Audit nulls and
   outliers**. datasight runs the selected starter as soon as your data loads.

2. **Configure your LLM** — if needed, enter your provider and API key.
   (If you exported `ANTHROPIC_API_KEY` or similar in your shell, this step
   is skipped.)

3. **Explore Files** — enter the path to your CSV, Parquet, Excel, or
   DuckDB file (or a directory of CSV/Parquet/Excel files) and click
   **Explore**.

datasight creates an in-memory database, introspects the schema, and drops
you into the chat UI.

!!! tip "Adding more files"
    Use the input at the top of the sidebar (below **Tables**) to add more
    files to your session at any time.

### Save as a project

Once you're comfortable with your data, click **Save** in the header to
persist your session as a project. datasight will:

- Create a project directory with a DuckDB database (views pointing to your
  original files — no data copying)
- Auto-generate `schema_description.md` and `queries.yaml` using the LLM
- Seed a `measures.yaml` scaffold from the inferred semantic measures

See [Set up a project](../../project-setup/tutorials/set-up-project.md) for the full project workflow.

## Generate project files from files

`datasight generate` can also work directly with files to auto-generate
schema documentation:

```bash
datasight generate generation.parquet plants.csv
```

Examples:

```bash
# Reference an existing DuckDB database directly
datasight generate generation.duckdb

# Reference an existing SQLite database directly
datasight generate generation.sqlite

# Create ./database.duckdb from CSV inputs
datasight generate generation.csv plants.csv

# Create ./database.duckdb from Parquet inputs
datasight generate generation.parquet plants.parquet

# Create ./database.duckdb from Excel inputs (one table per sheet)
datasight generate generation.xlsx plants.xlsx

# Create a custom project DuckDB from CSV inputs
datasight generate generation.csv plants.csv --db-path db/project.duckdb

# Create a custom project DuckDB from Parquet inputs
datasight generate generation.parquet plants.parquet --db-path db/project.duckdb
```

`--db-path` is an output path. Use it only when datasight is creating a
DuckDB project database from CSV, Parquet, Excel, or mixed file inputs.
Do not use `--db-path` with a single existing DuckDB or SQLite database;
those files are referenced directly in `.env`.

This creates `schema_description.md`, `queries.yaml`, `measures.yaml`,
and `time_series.yaml` in the current directory using the LLM. See
[Set up a project](../../project-setup/tutorials/set-up-project.md) for details.

## Supported file types

| Type | Example | How it's handled |
|------|---------|-----------------|
| CSV | `data.csv` | Loaded via DuckDB's `read_csv_auto` |
| Parquet | `data.parquet` | Loaded via DuckDB's `read_parquet` |
| Excel | `data.xlsx` | Each sheet materialized into DuckDB via pandas + openpyxl |
| DuckDB | `data.duckdb` | Referenced directly when it is the only input |
| SQLite | `data.sqlite` | Referenced directly when it is the only input |
| Parquet directory | `data_dir/` | Hive-partitioned parquet with `read_parquet` glob |
| CSV directory | `data_dir/` | All CSVs loaded via `read_csv_auto` glob |

For CSV and Parquet inputs, each file becomes a view in an ephemeral
in-memory DuckDB database. The view name is derived from the filename
(e.g. `generation.parquet` becomes the `generation` table).

### Excel workbooks

DuckDB has no native Excel reader, so Excel workbooks are read through
pandas (with the `openpyxl` engine) and each sheet is inserted as a full
DuckDB table — not a view.

- A **single-sheet** workbook produces one table named after the file
  (e.g. `plants.xlsx` → `plants`).
- A **multi-sheet** workbook produces one table per sheet, named after
  the sheet (e.g. sheets `generation` and `plants` → tables `generation`
  and `plants`). Sheets whose names already exist in the session get a
  numeric suffix (`generation_2`).
- Changes to the underlying `.xlsx` file are not picked up automatically
  — unlike CSV/Parquet views, the data has been copied into DuckDB.
  Reload the session (or run `datasight generate` again) after editing
  the workbook.
- Data is read in full into memory, so very large workbooks may be slow.
  For large tabular data, convert to CSV or Parquet first.
