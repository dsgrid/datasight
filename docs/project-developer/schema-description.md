# Write a schema description

The `schema_description.md` file provides domain context that helps the AI
write better SQL. datasight auto-discovers your table structure (names,
columns, types, row counts) — this file is for everything it *can't*
introspect.

## What to include

**What the data represents**
: A brief description of the dataset and its source.

**Column meanings**
: Explain non-obvious columns, especially codes and enums.

**Relationships**
: Which tables join on which keys.

**Query tips**
: Gotchas, preferred views, date formats, NULL handling.

## Example

```markdown
# EIA Power Plant Generation

Monthly electricity generation data from the U.S. Energy Information
Administration (EIA), covering fuel types, capacity, and state-level
reporting.

## Key Columns

- **plant_id**: Unique plant identifier (joins to plants table)
- **energy_source_code**: Fuel type code — NG (natural gas), SUN (solar), WND (wind), COL (coal), NUC (nuclear), etc.
- **prime_mover_code**: Generator technology — ST (steam turbine), CT (combustion turbine), PV (photovoltaic), etc.

## Relationships

- `generation_fuel.plant_id` → `plants.plant_id`
- `plant_details.plant_id` → `plants.plant_id`

## Tips

- Use `net_generation_mwh` for total output; `total_fuel_consumption_mmbtu` for fuel input
- Dates are monthly — `report_date` is the first of each month
- Negative `net_generation_mwh` is valid (pumped storage plants consume more than they produce)
- Filter to `energy_source_code IN ('SUN', 'WND')` for renewable-only analysis
```

## What NOT to include

Don't repeat what introspection discovers:

- Table names and column lists (auto-discovered)
- Column data types (auto-discovered)
- Row counts (auto-discovered)

Focus on the *meaning* behind the schema, not the schema itself.

## Pull in external references

Use `[include:Title](https://…)` anywhere in the file to fetch a web page
at project-load time and splice its content into the system prompt. Useful
for pointing the LLM at fuel-code glossaries, data-source documentation,
or anything else that lives elsewhere and changes occasionally.

```markdown
Fuel code meanings come from
[include:EIA fuel codes](https://www.eia.gov/electricity/monthly/pdf/technotes.pdf).
```

- HTML is stripped to plain text; non-text content types are skipped.
- Each URL is capped at 20 KB (override with the `SCHEMA_INCLUDE_MAX_BYTES`
  env var) and fetched once per project load.
- If a fetch fails, the original `[include:…](url)` markdown link is left
  in place so the LLM still sees the pointer.
- Set `SCHEMA_INCLUDE_MAX_BYTES=0` to skip resolution entirely. The
  directives stay in the prompt as plain markdown links, which is the
  escape hatch when a linked page pushes the system prompt past a
  small-context model's token limit (common on the free GitHub Models
  tier).

## File location

By default, datasight looks for `schema_description.md` in the project
directory. Override with the `SCHEMA_DESCRIPTION_PATH` environment variable.
