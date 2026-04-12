# Inspect files

You have CSV, Parquet, or DuckDB files and want to understand what's in
them. datasight can profile the data, surface measures and dimensions,
suggest trends, and generate starter prompts — all without setting up a
project or calling an LLM.

## One command: `datasight inspect`

`datasight inspect` runs every deterministic analysis in one shot:

```bash
datasight inspect generation.parquet
datasight inspect generation.csv plants.csv
datasight inspect data_dir/
```

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

3. **Explore Files** — enter the path to your CSV, Parquet, or DuckDB file
   (or a directory of Parquet files) and click **Explore**.

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

See [Set up a project](../../project-developer/set-up-project.md) for the full project workflow.

## Generate project files from files

`datasight generate` can also work directly with files to auto-generate
schema documentation:

```bash
datasight generate generation.parquet plants.csv
```

This creates `schema_description.md`, `queries.yaml`, and `measures.yaml`
in the current directory using the LLM. See [Set up a project](../../project-developer/set-up-project.md)
for details.

## Supported file types

| Type | Example | How it's handled |
|------|---------|-----------------|
| CSV | `data.csv` | Loaded via DuckDB's `read_csv_auto` |
| Parquet | `data.parquet` | Loaded via DuckDB's `read_parquet` |
| DuckDB | `data.duckdb` | Opened directly with all tables/views |
| Parquet directory | `data_dir/` | Hive-partitioned parquet with `read_parquet` glob |
| CSV directory | `data_dir/` | All CSVs loaded via `read_csv_auto` glob |

Each file becomes a view in an ephemeral in-memory DuckDB database. The
view name is derived from the filename (e.g. `generation.parquet` becomes
the `generation` table).
