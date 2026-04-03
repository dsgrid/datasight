# Try the demo dataset

datasight ships with a command that downloads a real EIA energy dataset so you
can try it out immediately — no database setup required.

## What you get

The demo downloads cleaned US power plant data from the
[PUDL project](https://catalyst.coop/pudl/) (Public Utility Data Liberation),
sourced from EIA Forms 923 and 860:

| Table | Description | Rows (2020+) |
|-------|-------------|-------------|
| `generation_fuel` | Monthly generation and fuel consumption by plant/fuel/prime mover | ~1M |
| `plants` | Plant names, locations, coordinates | ~19K |
| `plant_details` | Balancing authority, NERC region, sector | ~50K |

Plus pre-built views for common aggregations and 8 example queries tuned for
energy analysis.

## Download the demo

```bash
datasight demo ./eia-project
```

This takes about a minute to download and creates:

```
eia-project/
├── .env                     # Connection settings
├── eia_demo.duckdb          # DuckDB database (~50 MB)
├── schema_description.md    # EIA data documentation
└── queries.yaml             # Example energy queries
```

To include more historical data (back to 2001):

```bash
datasight demo ./eia-project --min-year 2001
```

## Configure and run

```bash
cd eia-project

# Edit .env — add your API key (Anthropic, GitHub token, or Ollama)
datasight run
```

Open <http://localhost:8084> and try questions like:

- "What are the top 10 power plants by total generation?"
- "Show me the monthly trend of wind generation"
- "Compare coal vs natural gas generation over time"
- "What is the generation mix for Texas?"
- "Which states generate the most solar power?"

Use the sidebar to browse tables and click example queries to run them
directly.

## Data source

The data comes from [PUDL's nightly releases](https://catalyst.coop/pudl/) on
AWS S3. PUDL cleans and standardizes data from EIA Forms 923 (generation and
fuel consumption) and 860 (plant characteristics). The data is publicly
accessible with no credentials required.
