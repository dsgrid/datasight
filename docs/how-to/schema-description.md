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
# Customer Orders Database

Sales data from our e-commerce platform, updated daily.

## Key Columns

- **customer_id**: Unique customer identifier (joins to customers table)
- **order_status**: PENDING, CONFIRMED, SHIPPED, DELIVERED, CANCELLED
- **channel**: web, mobile, api, pos

## Relationships

- `orders.customer_id` → `customers.id`
- `line_items.order_id` → `orders.id`
- `line_items.product_id` → `products.id`

## Tips

- Use `orders_summary` view for aggregated metrics (faster than raw tables)
- Dates are in UTC; use `AT TIME ZONE` for local time
- Revenue = `line_items.quantity * line_items.unit_price` (no discount column)
- Cancelled orders still appear — filter with `order_status != 'CANCELLED'`
```

## What NOT to include

Don't repeat what introspection discovers:

- Table names and column lists (auto-discovered)
- Column data types (auto-discovered)
- Row counts (auto-discovered)

Focus on the *meaning* behind the schema, not the schema itself.

## File location

By default, datasight looks for `schema_description.md` in the project
directory. Override with the `SCHEMA_DESCRIPTION_PATH` environment variable.
