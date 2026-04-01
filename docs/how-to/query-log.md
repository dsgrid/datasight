# Log and review SQL queries

datasight can log every SQL query the AI generates to a structured file,
capturing timing, results, and errors. This is useful for auditing, debugging,
and learning SQL.

## Enable query logging

**From the CLI:**

```bash
datasight run --query-log
```

**From the web UI:** click the log button in the header toolbar (between
the SQL panel toggle and New chat). The button highlights in teal when
logging is active. Click again to disable.

**Via environment variable:** set `QUERY_LOG_ENABLED=true` in `.env` or
your shell.

Queries are written to `query_log.jsonl` in the project directory. Each line
is a JSON object:

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
┏━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━┳━━━━━━┳━━━━━━━━┓
┃ Timestamp           ┃ Tool    ┃ SQL                                ┃  Time ┃ Rows ┃ Status ┃
┡━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━╇━━━━━━╇━━━━━━━━┩
│ 2026-04-01 14:02:03 │ run_sql │ SELECT plant_name, SUM(gen)        │  42ms │   10 │ OK     │
│                     │         │ AS total FROM gen GROUP BY 1       │       │      │        │
│                     │         │ ORDER BY 2 DESC LIMIT 10           │       │      │        │
├─────────────────────┼─────────┼────────────────────────────────────┼───────┼──────┼────────┤
│ 2026-04-01 14:04:01 │ run_sql │ SELECT * FROM nonexistent_table    │   3ms │      │ ERR    │
└─────────────────────┴─────────┴────────────────────────────────────┴───────┴──────┴────────┘
2 queries (1 succeeded, 1 failed)
```

Long SQL queries wrap across multiple lines within each row.

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--tail N` | `20` | Show the last N entries |
| `--errors` | off | Show only failed queries |
| `--full` | off | Show complete SQL and add a Question column |
| `--project-dir` | `.` | Project directory containing `query_log.jsonl` |

### Examples

```bash
# Show last 50 entries
datasight log --tail 50

# Show only errors
datasight log --errors

# Full detail including the user's natural-language question
datasight log --full

# Point to a different project
datasight log --project-dir ./my-project
```

## Change the log file path

By default the log writes to `query_log.jsonl` in the project directory.
Override with the `QUERY_LOG_PATH` environment variable:

```bash
QUERY_LOG_PATH=/var/log/datasight/queries.jsonl datasight run --query-log
```

## Toggle logging at runtime

The web UI button toggles logging without restarting the server. You can
also call the API directly:

```bash
# Toggle on/off
curl -X POST http://localhost:8084/api/query-log/toggle

# Read recent entries as JSON
curl http://localhost:8084/api/query-log?n=10
```
