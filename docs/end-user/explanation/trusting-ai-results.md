# Trusting AI-generated results

datasight uses AI to translate natural language into SQL. The query
confidence features reduce the risk of errors, but **AI-generated queries
and their results should always be treated as unverified drafts.** Keep the
following in mind before relying on a result.

## Queries may be incorrect

The AI can misinterpret your question, choose the wrong columns, apply
incorrect filters, or use flawed aggregation logic — even when the results
look plausible.

## Results can be misleading

A query that runs without errors is not necessarily a correct query.
Spot-check row counts, totals, and edge cases before drawing conclusions.

## Verify before publishing

Never include AI-generated queries or results in reports, dashboards, or
decision-making without independent verification by someone who
understands the underlying data.

## Different runs may produce different SQL

The same natural-language question can yield different queries across
sessions, potentially with different results.

## The AI has no domain expertise

It relies on column names and schema descriptions to infer meaning. If the
schema is ambiguous or incomplete, the AI may make wrong assumptions about
what the data represents.

## What to do about it

- Enable [SQL approval and explanations](../reference/query-confidence-toggles.md)
  to review every query before it runs.
- For high-stakes analyses, use the
  [query verification](../../project-developer/verification.md) workflow to
  validate results against known expected outputs.
- Be specific when asking questions — specify temporal granularity, include
  counts, name the metric, use explicit date ranges. See
  [Why clarifying questions appear](query-confidence.md#tips-for-getting-consistent-results).
