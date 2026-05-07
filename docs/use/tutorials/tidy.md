# Tidy a wide-month spreadsheet

Many spreadsheets arrive in this shape:

| plant_id | fuel_type | jan | feb | mar | … | dec |
|---|---|---|---|---|---|---|
| 1 | coal | 180 | 165 | 140 | … | 200 |

Each month is its own column. That makes "average generation by month" an
awkward sum across twelve columns. The *tidy* shape datasight prefers looks like:

| plant_id | fuel_type | month | value |
|---|---|---|---|
| 1 | coal | jan | 180 |
| 1 | coal | feb | 165 |

Each row is one observation. Aggregations become simple `GROUP BY` queries.

This tutorial walks through detecting the first shape and reshaping it into
the second. Allow about five minutes. **No API key required** — none of the
steps call the AI.

!!! warning "Experimental"
    The `datasight tidy` commands are experimental and their interface is
    subject to change. `tidy suggest` is read-only and safe to run.
    `tidy table` and `tidy view` write to your database — use `--dry-run`
    first to preview the DDL before applying.

!!! note "Background reading"
    The tidy-data concept comes from Hadley Wickham's
    [Tidy Data](https://www.jstatsoft.org/article/view/v059i10) (J. Stat. Softw., 2014).
    The [R for Data Science chapter on data tidying](https://r4ds.hadley.nz/data-tidy)
    has more worked examples if you want the full picture.

## 1. Install datasight

```bash
uv tool install datasight
```

Don't have [uv](https://docs.astral.sh/uv/) yet? See
[Install datasight](../how-to/install.md) for the one-line installer.

## 2. Save a wide CSV

The CSV below mirrors a common spreadsheet shape: one row per plant, with
twelve numeric columns named `jan` through `dec` holding monthly net
generation (MWh).

```bash
cat > monthly_generation.csv <<'EOF'
plant_id,fuel_type,jan,feb,mar,apr,may,jun,jul,aug,sep,oct,nov,dec
1,coal,180,165,140,120,110,100,95,105,130,160,175,200
2,gas,220,200,180,170,160,175,200,220,210,200,215,230
3,wind,150,145,160,175,190,200,195,180,170,155,145,140
4,solar,60,90,130,175,215,240,245,225,180,130,90,55
EOF
```

This is the *untidy* shape: the `month` dimension is hidden in the column
headers, so a question like "average generation by month across plants"
becomes an awkward sum across twelve columns instead of a `GROUP BY`.

## 3. Detect untidy column shapes — no setup required

Point `datasight tidy suggest` straight at the CSV. It runs in an
ephemeral DuckDB session, so no project directory or `.env` is needed:

```bash
datasight tidy suggest monthly_generation.csv
```

```
                                  Suggestions
┏━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━┓
┃ Source                 ┃ Target                      ┃ Pattern             ┃ Period ┃ Columns ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━┩
│ monthly_generation_mwh │ monthly_generation_mwh_long │ date_in_column_…    │ month  │      12 │
└────────────────────────┴─────────────────────────────┴─────────────────────┴────────┴─────────┘
```

datasight has spotted that twelve column names look like month tokens —
`jan`, `feb`, … `dec` — and proposes to reshape the table into a long
form named `monthly_generation_mwh_long`.

You can also pass several files at once, or `parquet` / `xlsx` / `duckdb`
sources:

```bash
datasight tidy suggest data/*.csv
datasight tidy suggest hourly.parquet plants.duckdb
```

## 4. Set up a project to apply the reshape

`tidy suggest` is read-only and ephemeral, but applying the reshape
(`tidy view` or `tidy table`) needs a writable database. That means
loading the CSV into a DuckDB file and pointing a `.env` at it.

```bash
mkdir tidy-tutorial && mv monthly_generation.csv tidy-tutorial && cd tidy-tutorial

uv run --with duckdb python -c "
import duckdb
conn = duckdb.connect('generation.duckdb')
conn.execute('CREATE TABLE monthly_generation_mwh AS SELECT * FROM read_csv_auto(\"monthly_generation.csv\")')
"

cat > .env <<'EOF'
DB_MODE=duckdb
DB_PATH=generation.duckdb
LLM_PROVIDER=anthropic
ANTHROPIC_API_KEY=stub
EOF
```

`uv run --with duckdb` runs Python with DuckDB available without
installing it system-wide. The `tidy` command doesn't call the AI,
so the `ANTHROPIC_API_KEY` value is a placeholder here — replace it with a real key only when you're ready to start asking
questions.

## 5. Preview the reshape SQL

Before writing anything to the database, see exactly what would run:

```bash
datasight tidy table --dry-run
```

You'll see a `CREATE OR REPLACE TABLE` statement built around a DuckDB
`UNPIVOT`. The id columns (`plant_id`, `fuel_type`) come through
automatically; the twelve wide measure columns are stacked into a
two-column `(month, value)` pair.

## 6. Apply the reshape

When the preview looks right, drop the `--dry-run` flag:

```bash
datasight tidy table
```

```
Created table 'monthly_generation_mwh_long' from monthly_generation_mwh (12 columns)
```

`datasight tidy view` is also available if you want a view that
re-evaluates against the source on every query (useful when the source
table is updated periodically). Because of a regression in the Python
`duckdb` 1.5.2 binding that breaks UNPIVOT inside views, `tidy view`
emits a `UNION ALL` form instead — the result is identical but the SQL
is more verbose. Prefer `tidy table` unless you specifically need view
semantics.

## 7. Query the tidy form

```bash
uv run --with duckdb python -c "
import duckdb
conn = duckdb.connect('generation.duckdb', read_only=True)
rows = conn.execute('''
  SELECT month, ROUND(AVG(value), 1) AS avg_mwh
  FROM monthly_generation_mwh_long
  GROUP BY month
  ORDER BY month
''').fetchall()
for month, avg in rows:
    print(f'{month}: {avg}')
"
```

The same question on the wide table would have required summing twelve
named columns by hand. With the long form, monthly aggregation is a
plain `GROUP BY`.

## What's next

- **Point the agent at the long form.** Mention `monthly_generation_mwh_long`
  and its columns in your `schema_description.md` so the LLM agent prefers
  tidy queries. See [Write a schema description](../../project-setup/how-to/schema-description.md).
- **Audit the rest of your data.** [Audit data quality](../how-to/audit-data-quality.md)
  covers `datasight quality`, which surfaces tidy suggestions alongside
  null/range/date-coverage checks during routine audits.
- **Reshape pivots the regex misses.** This tutorial covered period
  pivots (`jan` … `dec`). For category pivots like `coal_mwh`,
  `gas_mwh`, `nuclear_mwh`, or multi-axis pivots like `coal_2020`,
  `gas_2020`, `coal_2021`, `gas_2021`, see
  [Curate datasets with `tidy review`](../how-to/curate-with-tidy-review.md).
  That command asks an LLM for proposals and lets you approve each one.
- **Try a real dataset.** [Explore US electricity generation (EIA)](getting-started.md)
  walks through the same loop on the PUDL EIA dataset.
