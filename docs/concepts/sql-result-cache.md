# SQL result cache

datasight keeps an in-memory cache of SQL query results so that repeated or
near-repeated questions don't re-run the same query against the database.
This page explains what it caches, when entries are dropped, and how to tune
or disable it.

## The problem it solves

A common interaction pattern in the web UI looks like this:

> **User:** *What was total net generation by fuel in 2023?*
>
> *(datasight runs the query, shows a table.)*
>
> **User:** *Make a plot of that.*

Without a cache, the agent's follow-up produces the same SQL (or a trivially
different one that compiles to the same plan) and the database re-scans the
same rows. For large generation or load tables, this can turn a one-second
follow-up into a multi-second round trip.

The same problem shows up whenever a user scrolls back and re-asks, tries a
question twice with slightly different wording, or opens the same dataset
overview twice in one session.

## What gets cached

The cache lives in the `CachingSqlRunner` wrapper around the active
[`SqlRunner`](../tool-developer/architecture.md). It caches **pandas
DataFrames keyed on normalized SQL text** — whitespace is collapsed, trailing
semicolons are stripped, and the key is lowercased. Two queries that differ
only in formatting share a cache entry.

The cache is not per-user and not per-session: it lives for the lifetime of
the loaded project. Every query run through `state.sql_runner` — agent
queries, schema introspection, dataset/measure/trend overviews, table
previews — is eligible.

## Eviction and bounds

The cache is **byte-bounded**, not entry-bounded. Each stored DataFrame's
size is estimated with
[`df.memory_usage(deep=True)`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.memory_usage.html),
and the total across all entries is kept below the budget (default 1 GiB).
When a new result would push the total over the budget, least-recently-used
entries are evicted until it fits.

One special case: **if a single result is larger than the entire budget, it
is not cached at all**, rather than evicting everything else to make room
for something that can't stay long. Small frequently-reused queries
therefore aren't thrashed out by one pathologically large scan.

## When the cache is cleared

The cache is dropped in these situations:

- **Loading a different project** — the runner itself is rebuilt, so the
  cache dies with it.
- **Adding new files** to an exploration session (via the UI's
  file-upload flow) — cached schema-introspection queries would otherwise
  miss the new tables.
- **Disconnecting or switching database connections.**

It is **not** cleared when the system prompt is rebuilt (e.g. toggling
`CLARIFY_SQL`) — the underlying data hasn't changed, so prior results are
still correct.

!!! warning "External writes are not detected"
    datasight opens DuckDB and SQLite connections read-only and does not
    monitor the backing files or external databases for changes. If a
    project's data is mutated by another process while datasight is running,
    reload the project (or restart `datasight run`) to drop stale results.
    Disable the cache entirely with `SQL_CACHE_MAX_BYTES=0` if you need
    every query to hit the database.

## Tuning

The cache size is controlled by the `SQL_CACHE_MAX_BYTES` environment
variable:

```bash
# Default: 1 GiB
SQL_CACHE_MAX_BYTES=1073741824

# Larger — a workstation with plenty of RAM exploring a big schema
SQL_CACHE_MAX_BYTES=8589934592   # 8 GiB

# Disable
SQL_CACHE_MAX_BYTES=0
```

Set `SQL_CACHE_MAX_BYTES=0` when you want every question to hit the live
database — for instance, when you are actively ingesting data, or when
you're running performance benchmarks against the SQL engine itself.

## What it does *not* do

- **It is not a persistent cache.** Results live in the server process only.
  Restarting `datasight run` clears everything.
- **It does not cache LLM output.** Prompt reuse savings come from
  Anthropic's prompt cache (visible in the cost panel as cache-read tokens),
  not from this cache. A separate first-turn response cache in the web
  server replays whole agent turns for verbatim-repeated first questions.
- **It does not rewrite follow-up questions.** If a user asks a question
  that produces *different* SQL from what's cached — e.g. the same data
  filtered further — the agent still executes that SQL. The cache helps
  when SQL matches; making follow-ups reuse prior result sets would require
  the agent to target a materialized view of the previous result, which
  datasight does not currently do.
