# Why clarifying questions appear

datasight generates SQL from natural language, which means different runs
can produce different queries. Three features — clarifying questions, SQL
approval mode, and SQL explanations — work together to help you build
confidence that the results are correct. This page explains what each one
does and why it exists. For how to toggle them, see
[Query confidence toggles](../../reference/query-confidence-toggles.md).

## Clarifying questions

When your question could be interpreted in multiple ways, the AI asks a
clarifying question before writing SQL. It checks for:

- **Temporal granularity** — "over time" without specifying
  monthly/yearly
- **Aggregation scope** — "top states" without a count
- **Metric choice** — "largest" when multiple numeric columns exist
- **Filter boundaries** — "recent" without a date range
- **Grouping level** — when multiple grouping columns are available

Clarifying questions appear with clickable option buttons so you can
respond with one click instead of typing.

Disabling clarify trades determinism for convenience — the AI will make
reasonable assumptions instead of asking.

## SQL approval

When approval mode is on, each SQL query appears in an editable text area
before execution. You can approve as-is, approve with edits (modifying the
SQL and running your version), or reject with feedback.

Approval is the highest-confidence mode. Use it when the cost of a wrong
query is high — for example when you're about to publish a number or hand
results to a stakeholder.

## SQL explanations

With explanations on, the AI explains each query in plain English before
executing it: what tables are queried, what joins and filters are applied,
and what the output represents. This helps you verify the logic without
reading SQL.

Explanations are lighter-weight than approval. Use them when you want
visibility into the AI's reasoning without adding a click to every query.

## Tips for getting consistent results

Even without these features, you can improve consistency by being
specific:

1. **Specify temporal granularity.** "Monthly generation over time" not
   "generation over time."
2. **Include counts.** "Top 10 states" not "top states."
3. **Name the metric.** "Largest plants by total MWh" not "largest
   plants."
4. **Use explicit date ranges.** "From 2020 to 2024" not "recent years."

## Related

- [Trusting AI-generated results](trusting-ai-results.md) — caveats that
  apply even with all three features enabled.
- [Query confidence toggles](../../reference/query-confidence-toggles.md) —
  how to turn each feature on or off.
