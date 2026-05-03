# Declare table joins

datasight infers foreign keys from naming convention — a column named
`plant_id` is treated as a likely FK to a `plants` table with an `id`
column. When that convention doesn't match your schema, declare the
relationships explicitly in `joins.yaml`. The declarations are used by
`datasight integrity` and inform the AI agent when it writes joins.

## Create `joins.yaml`

`joins.yaml` lives in the project root. Each entry maps a child column to
a parent table and column:

```yaml
- child_table: generation_fuel
  child_column: plant_id
  parent_table: plants
  parent_column: id   # optional, defaults to "id"

- child_table: generation_fuel
  child_column: energy_source_code
  parent_table: energy_sources
  parent_column: code
```

`parent_column` defaults to `id`, so most entries reduce to three lines.

## Verify the declarations

`datasight integrity` reports primary keys, duplicate keys, orphan FKs,
and join-explosion risks. Once `joins.yaml` is in place it follows the
declared relationships instead of guessing:

```bash
datasight integrity
datasight integrity --table generation_fuel
```

See [Audit data quality](../../use/how-to/audit-data-quality.md#check-referential-integrity)
for the full integrity workflow.
