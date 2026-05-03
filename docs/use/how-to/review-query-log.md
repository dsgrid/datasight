# Log and review SQL queries

datasight logs every SQL query the AI generates to a structured file,
capturing timing, results, and errors. This is useful for auditing,
debugging, and learning SQL.

Logging is on by default whenever a project is loaded. The log lives at
`<project>/.datasight/query_log.jsonl` and grows append-only across
sessions. Ephemeral file-explore sessions (started without a project) do
not write a log.

Each entry is a JSON object on its own line:

```json
{
  "timestamp": "2026-04-01T14:02:03.123456+00:00",
  "session_id": "a1b2c3",
  "user_question": "What are the top 10 plants?",
  "tool": "run_sql",
  "sql": "SELECT plant_name, SUM(gen) AS total FROM gen GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
  "execution_time_ms": 42.15,
  "row_count": 10,
  "column_count": 2,
  "error": null
}
```

## View the query log

Use `datasight log` to display recent queries in a formatted table:

```bash
datasight log
```

```
 #  Timestamp            Tool     SQL                                  Time  Rows  Status
──────────────────────────────────────────────────────────────────────────────────────────────
 1  2026-04-01 14:02:03  run_sql  SELECT plant_name, SUM(gen)           42ms    10  OK
                                  AS total FROM gen GROUP BY 1
                                  ORDER BY 2 DESC LIMIT 10
 2  2026-04-01 14:04:01  run_sql  SELECT * FROM nonexistent_table        3ms        ERR
──────────────────────────────────────────────────────────────────────────────────────────────
2 queries (1 succeeded, 1 failed)
```

Long SQL queries wrap across multiple lines within each row.

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--tail N` | `20` | Show the last N entries |
| `--errors` | off | Show only failed queries |
| `--full` | off | Show complete SQL and add a Question column |
| `--cost` | off | Show LLM cost summary (token counts and estimated cost) |
| `--sql N` | — | Print raw SQL for query `#N` (shown in the `#` column), ready to copy-paste |
| `--project-dir` | `.` | Project directory containing `query_log.jsonl` |

### Examples

```bash
# Show last 50 entries
datasight log --tail 50

# Show only errors
datasight log --errors

# Full detail including the user's natural-language question
datasight log --full

# Copy a specific query's SQL (use the # shown in the table)
datasight log --sql 1

# Pipe directly into DuckDB
datasight log --sql 1 | duckdb database.duckdb

# Show LLM cost summary
datasight log --cost

# Point to a different project
datasight log --project-dir ./my-project
```

## Change the log file path

Override the default path with the `QUERY_LOG_PATH` environment variable:

```bash
QUERY_LOG_PATH=/var/log/datasight/queries.jsonl datasight run
```
