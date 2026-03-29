# Create example queries

The `queries.yaml` file teaches the AI how to write correct SQL for your
database. Each entry pairs a natural language question with its SQL answer.

## How examples are used

```{mermaid}
flowchart TD
    A[queries.yaml] --> B[System prompt<br>few-shot examples]
    A --> C[Sidebar<br>clickable queries]
    B --> D[AI writes SQL]

    style A fill:#e7e1cf,stroke:#023d60,color:#023d60
    style B fill:#15a8a8,stroke:#023d60,color:#fff
    style C fill:#fe5d26,stroke:#023d60,color:#fff
    style D fill:#023d60,stroke:#023d60,color:#fff
```

At startup, datasight:

1. Appends formatted examples to the AI system prompt as few-shot context
2. Displays them in the sidebar, filterable by selected table

When a user asks a question, the AI references these examples to write
accurate SQL. Users can also click any example query in the sidebar to
send it directly to the chat.

## File format

```yaml
# queries.yaml
- question: What are the top 10 customers by revenue?
  sql: |
    SELECT customer_name, SUM(amount) AS revenue
    FROM orders
    GROUP BY customer_name
    ORDER BY revenue DESC
    LIMIT 10

- question: Monthly order trend
  sql: |
    SELECT DATE_TRUNC('month', order_date) AS month,
           COUNT(*) AS order_count
    FROM orders
    GROUP BY month
    ORDER BY month
```

## Writing good examples

**Cover common questions.** What will your users ask most often?

**Show tricky patterns.** Joins, enum filters, and aggregations specific to
your schema are where examples help the most.

**Keep SQL readable.** The AI adapts the pattern — it doesn't copy verbatim.
Clear SQL teaches better than clever SQL.

**Include chart-friendly queries.** Two or three columns (one category/date +
one numeric) produce the cleanest visualizations.

**Aim for 5-15 examples.** Too few gives the AI little to work with. Too many
dilutes the signal and bloats the system prompt.

## File location

By default, datasight looks for `queries.yaml` in the project directory.
Override with the `EXAMPLE_QUERIES_PATH` environment variable.
