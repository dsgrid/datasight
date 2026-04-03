# Verify queries across models

datasight generates SQL from natural language, which means the same question
could produce different queries depending on the model, provider, or even
the run. The `datasight verify` command helps you validate that your example
queries produce correct results regardless of which LLM is used.

## The verification workflow

```{mermaid}
flowchart LR
    A[queries.yaml<br>+ expected] --> B[datasight verify]
    B --> C[Ambiguity<br>analysis]
    B --> D[SQL generation<br>+ result checks]
    C --> E[Warnings:<br>ambiguous questions]
    D --> F[Pass/fail<br>report]

    style A fill:#e7e1cf,stroke:#023d60,color:#023d60
    style B fill:#15a8a8,stroke:#023d60,color:#fff
    style C fill:#fe5d26,stroke:#023d60,color:#fff
    style D fill:#fe5d26,stroke:#023d60,color:#fff
    style E fill:#023d60,stroke:#023d60,color:#fff
    style F fill:#023d60,stroke:#023d60,color:#fff
```

`datasight verify` runs in two phases:

1. **Ambiguity analysis** -- checks each question for structural ambiguities
   that could produce different SQL (temporal granularity, missing counts, etc.)
2. **SQL verification** -- sends each question through the LLM, executes the
   generated SQL, and compares results against the reference SQL or explicit
   expected values

## Add expected results to queries.yaml

Add an `expected` block to any entry in `queries.yaml` to define what
correct output looks like:

```yaml
- question: What are the top 5 states by solar generation?
  sql: |
    SELECT p.state, SUM(g.net_generation_mwh) AS solar_mwh
    FROM generation_fuel g
    JOIN plants p USING (plant_id_eia)
    WHERE g.energy_source_code = 'SUN'
    GROUP BY p.state
    ORDER BY solar_mwh DESC
    LIMIT 5
  expected:
    row_count: 5
    columns: [state, solar_mwh]
    contains: ["CA", "TX"]
```

### Available checks

| Field | Description |
|-------|-------------|
| `row_count` | Exact number of rows expected |
| `min_row_count` | Minimum number of rows |
| `max_row_count` | Maximum number of rows |
| `columns` | Exact column names in order |
| `contains` | Values that must appear somewhere in the results |
| `not_contains` | Values that must NOT appear in the results |

When no `expected` block is present, `datasight verify` falls back to
comparing the LLM-generated SQL output against the reference SQL output
(row count and column names).

## Run verification

```bash
datasight verify
```

This runs all queries in `queries.yaml` against the configured model and
database. Output includes an ambiguity analysis table (if any questions are
flagged) followed by a pass/fail verification table:

```
           Ambiguity Analysis (2 warnings)
┃ # ┃ Question                          ┃ Ambiguities              ┃ Suggested Revision          ┃
│ 4 │ Compare coal vs gas over time     │ - "over time" does not   │ Compare coal vs gas monthly │
│   │                                   │   specify granularity    │ generation from 2020-2024   │

                        Verification Results
┃ # ┃ Question                          ┃ Status ┃ Checks               ┃
│ 1 │ Top 10 plants by generation       │ PASS   │ ✓ row_count: 10      │
│ 4 │ Compare coal vs gas over time     │ FAIL   │ ✗ row_count: ...     │

8/9 passed (1 failed), 2 ambiguous
```

The exit code is 0 if all queries pass, 1 if any fail -- making it suitable
for CI pipelines.

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--model` | from `.env` | Override the model for this run |
| `--queries` | `queries.yaml` | Path to queries file |
| `--project-dir` | `.` | Project directory |
| `-v` / `--verbose` | off | Enable debug logging |

### Test across models

Run the same suite against different models to compare reliability:

```bash
datasight verify --model claude-sonnet-4-20250514
datasight verify --model claude-haiku-4-5-20251001
LLM_PROVIDER=github datasight verify --model gpt-4o
LLM_PROVIDER=ollama datasight verify --model qwen3.5:35b-a3b
```

## Writing deterministic queries

The ambiguity analysis will flag common issues, but here are guidelines for
writing questions that produce consistent results across models:

1. **Specify temporal granularity.** "Monthly generation over time" not
   "generation over time."
2. **Include counts.** "Top 10 states" not "top states."
3. **Name the metric.** "Largest plants by total MWh" not "largest plants."
4. **Use explicit date ranges.** "From 2020 to 2024" not "recent years."
5. **Name the grouping column.** "By state" not "by region" when multiple
   geographic columns exist.
