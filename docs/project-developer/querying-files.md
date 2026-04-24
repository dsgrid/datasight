# Query files directly

DuckDB can query CSV and Parquet files using SQL — without importing or
copying any data. datasight creates lightweight **views** that point at your
files and treats them like regular database tables. Excel workbooks are also
supported, with each sheet materialized as a DuckDB table (see
[Excel files](#excel-files) below for the caveats).

## Quick explore (no setup)

The fastest way to query files is the **Explore Files** feature on the
landing page. Run `datasight run`, enter a file or directory path, and click
**Explore**. datasight creates views automatically and you can start asking
questions immediately.

You can add more files at any time from the sidebar. When you're ready to
save your work, click **Save** in the header to create a project.

See the [EIA generation tutorial](../end-user/tutorials/getting-started.md) for a walkthrough.

## Manual setup

For full control over view definitions, create a DuckDB file and define
views yourself.

## How it works

A DuckDB view over a file is just a pointer. The data stays on disk in
its original format, and DuckDB reads it on the fly when a query runs.
For Parquet files this is especially efficient because DuckDB can skip
entire row groups and read only the columns a query needs.

## Set up views

Create a `.duckdb` file and define views that point at your files:

```bash
duckdb my_project.duckdb
```

### Parquet files

```sql
CREATE VIEW measurements AS
SELECT * FROM read_parquet('data/measurements.parquet');
```

### CSV files

```sql
CREATE VIEW events AS
SELECT * FROM read_csv('data/events.csv', auto_detect=true);
```

DuckDB's `auto_detect` option infers column names, types, delimiters,
and date formats automatically. If auto-detection gets something wrong,
you can specify options explicitly:

```sql
CREATE VIEW events AS
SELECT * FROM read_csv(
    'data/events.csv',
    header=true,
    delim=',',
    dateformat='%Y-%m-%d',
    columns={
        'event_id': 'INTEGER',
        'event_date': 'DATE',
        'category': 'VARCHAR',
        'value': 'DOUBLE'
    }
);
```

### Excel files

Unlike CSV and Parquet, DuckDB cannot read `.xlsx` lazily. datasight reads
Excel workbooks through pandas (with the `openpyxl` engine) and inserts
each sheet as a full DuckDB **table** — not a view:

- A **single-sheet** workbook produces one table named after the file
  (e.g. `plants.xlsx` → `plants`).
- A **multi-sheet** workbook produces one table per sheet, named after
  the sheet (e.g. sheets `generation` and `plants` → tables `generation`
  and `plants`). Collisions with existing tables are deduped with a
  numeric suffix (`generation_2`).

Excel is handled automatically by `datasight run`'s **Explore Files** flow
and by `datasight generate` / `datasight inspect` when you pass `.xlsx`
paths — there is no SQL syntax to write yourself. If you want to rebuild
the project DuckDB from Excel inputs, point `datasight generate` at the
workbooks:

```bash
datasight generate generation.xlsx plants.xlsx
```

!!! warning "Excel data is copied, not referenced"
    Because sheets are materialized, edits to the underlying `.xlsx` file
    are **not** picked up on the next query the way CSV/Parquet view
    changes are. Re-run `datasight generate` (or reload the Explore
    session) after editing the workbook.

!!! tip "Convert large workbooks to Parquet"
    Excel sheets are read in full into memory during load. If the
    workbook is large or queried often, convert it to Parquet once so
    DuckDB can read it lazily with predicate and column pushdown.

### Multiple files with globs

You can point a single view at many files using glob patterns:

```sql
-- All Parquet files in a directory
CREATE VIEW sensor_data AS
SELECT * FROM read_parquet('data/sensors/*.parquet');

-- Recursive glob
CREATE VIEW all_logs AS
SELECT * FROM read_parquet('data/**/logs_*.parquet');
```

DuckDB also exposes the `filename` column so you can tell which file
each row came from:

```sql
CREATE VIEW sensor_data AS
SELECT *, filename FROM read_parquet('data/sensors/*.parquet');
```

### Hive-partitioned datasets

If your files are organized in a Hive-style directory layout
(`year=2024/month=01/data.parquet`), DuckDB can read the partition keys
as columns:

```sql
CREATE VIEW generation AS
SELECT * FROM read_parquet('data/generation/**/*.parquet', hive_partitioning=true);
```

This avoids scanning partitions that a query doesn't need.

### Remote files on S3

DuckDB can read Parquet files directly from S3 (and S3-compatible stores
like GCS and MinIO) — no download step required. This is especially
powerful for large datasets that you don't want to copy locally.

```sql
-- Public bucket (no credentials needed)
CREATE VIEW measurements AS
SELECT * FROM read_parquet('s3://my-bucket/data/measurements.parquet');

-- Hive-partitioned dataset on S3
CREATE VIEW generation AS
SELECT * FROM read_parquet('s3://my-bucket/data/generation/**/*.parquet', hive_partitioning=true);
```

For private buckets, configure credentials before creating views:

```sql
-- Set S3 credentials
SET s3_region = 'us-west-2';
SET s3_access_key_id = 'AKIA...';
SET s3_secret_access_key = '...';

-- Or use environment-based credentials (IAM roles, SSO, etc.)
CALL load_aws_credentials();

CREATE VIEW private_data AS
SELECT * FROM read_parquet('s3://private-bucket/data/*.parquet');
```

DuckDB handles predicate pushdown and column pruning over HTTP range
requests, so only the data needed by each query is transferred. See the
[DuckDB S3 documentation](https://duckdb.org/docs/extensions/httpfs/s3api)
for all configuration options.

## Point datasight at the database

Once your views are defined, configure your `.env` to use the DuckDB file:

```bash
DB_MODE=duckdb
DB_PATH=./my_project.duckdb
```

Run `datasight run` and your views will appear in the sidebar alongside
any regular tables. The AI can query them with natural language just like
any other table.

## Write a schema description

Even though datasight auto-discovers view names, columns, and types, a
`schema_description.md` file helps the AI understand what the data
means. This is especially useful for file-backed views because column
names in CSV and Parquet files are often terse or ambiguous.

See [Write a schema description](schema-description.md) for guidance.

## Tips

!!! tip "Use Parquet when you can"
    Parquet files are columnar and compressed, so DuckDB can read only
    the columns and row groups a query touches. CSV files must be fully
    scanned for every query. If your data is large and you query it
    often, converting to Parquet once will make every subsequent query
    faster.

!!! tip "Relative paths are relative to where you start datasight"
    File paths in `read_parquet()` and `read_csv()` are resolved relative
    to the working directory when datasight starts — not relative to the
    `.duckdb` file. Keep your data files alongside your project directory,
    or use absolute paths.

!!! tip "Views update automatically"
    Because views read the file on every query, changes to the underlying
    file are picked up immediately. There is nothing to reload or
    re-import.
