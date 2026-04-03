# Query confidence features

datasight generates SQL from natural language, which means different runs
can produce different queries. These features help you build confidence
that the results are correct. All are toggleable from the header toolbar.

## Clarify ambiguous queries (on by default)

When enabled, the AI asks clarifying questions before writing SQL if your
question could be interpreted in multiple ways. It checks for:

- **Temporal granularity** -- "over time" without specifying monthly/yearly
- **Aggregation scope** -- "top states" without a count
- **Metric choice** -- "largest" when multiple numeric columns exist
- **Filter boundaries** -- "recent" without a date range
- **Grouping level** -- when multiple grouping columns are available

Clarifying questions appear with clickable option buttons so you can respond
with one click instead of typing.

To disable, click the crosshair button in the header toolbar. This trades
determinism for convenience -- the AI will make reasonable assumptions
instead of asking.

## SQL approval mode (off by default)

Shows each SQL query in an editable text area before execution. You can:

- **Approve** -- run the query as-is
- **Approve with edits** -- modify the SQL and run your version
- **Reject** -- cancel the query and tell the AI what to change

Enable by clicking the checkmark button in the header toolbar.

## SQL explanations (off by default)

When enabled, the AI explains each query in plain English before executing
it -- what tables are queried, what joins and filters are applied, and what
the output represents. This helps you verify the logic without reading SQL.

Enable by clicking the question mark button in the header toolbar.

## Tips for getting consistent results

Even without these features, you can improve consistency by being specific:

1. **Specify temporal granularity.** "Monthly generation over time" not
   "generation over time."
2. **Include counts.** "Top 10 states" not "top states."
3. **Name the metric.** "Largest plants by total MWh" not "largest plants."
4. **Use explicit date ranges.** "From 2020 to 2024" not "recent years."
