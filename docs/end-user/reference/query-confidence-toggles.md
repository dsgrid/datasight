# Query confidence toggles

Three toggles control how much the AI checks in with you before and after
writing SQL. All three are on the header toolbar. For the reasoning behind
them, see [Why clarifying questions appear](../explanation/query-confidence.md).

| Toggle | Button | Default | Effect |
|--------|--------|---------|--------|
| **Clarify ambiguous queries** | Crosshair | On | AI asks clarifying questions when your prompt is ambiguous |
| **SQL approval mode** | Checkmark | Off | Shows each query in an editable text area before execution |
| **SQL explanations** | Question mark | Off | AI explains each query in plain English before running it |

## Environment variables

You can also set the defaults via `.env`:

| Variable | Default | Purpose |
|----------|---------|---------|
| `CLARIFY_SQL` | `true` | Ask clarifying questions for ambiguous queries |
| `CONFIRM_SQL` | `false` | Require user approval before executing SQL |
| `EXPLAIN_SQL` | `false` | Show plain-English SQL explanations |

See [Configuration reference](../../reference/configuration.md) for the
full list of environment variables.
