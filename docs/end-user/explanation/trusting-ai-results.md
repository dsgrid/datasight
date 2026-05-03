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
- [Export a finished analysis as a Python script](../how-to/save-and-rerun.md#export-a-session-as-a-python-script)
  to lock in the exact SQL, audit it line-by-line, and re-run it deterministically
  outside the AI loop.

## Reproducing a result without the AI

A chat session is a working draft. Once you have an answer worth keeping,
**export it to a Python script**. The exported file contains the literal
SQL the AI wrote, the chart spec it produced, and a hardcoded path to the
project's database — no LLM, no datasight install required to run it.

This converts an AI-assisted exploration into:

- **An auditable artifact.** Every SQL statement is visible, named
  (`SQL_1`, `SQL_2`, ...), and easy to read. A reviewer can sign off on
  the queries without knowing how datasight works.
- **A reproducible result.** Running `python datasight-session.py`
  re-executes the same queries against the same database and writes the
  same charts. There is no model temperature, no prompt drift, no
  variation between runs.
- **A starting point you control.** Edit any `SQL_N` constant to change a
  filter or group-by and re-run. The AI's contribution is now version-
  controllable code that you own, not a chat transcript that has to be
  re-executed in the app.

If you publish, share, or hand off a result, exporting first is the
single most effective way to demonstrate that the analysis is reproducible
and that the SQL has been seen by a human.
