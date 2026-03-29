# My Database

Describe your database here so the AI understands the domain context.

## Key Concepts

- **important_column**: Explain what this column means
- **status codes**: List any codes or enums the AI should know about

## Relationships

- `orders` links to `customers` via `customer_id`
- `line_items` links to `orders` via `order_id`

## Tips for Queries

- Use the `v_summary` view for aggregated results
- Dates are stored as DATE type in YYYY-MM-DD format
- This is a DuckDB database — use DuckDB SQL syntax
