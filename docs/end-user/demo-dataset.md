# Try the demo datasets

datasight ships with commands that download real energy datasets so you can try
it out immediately — no database setup required.

## Available demos

| Command | Dataset | Size | Source |
|---------|---------|------|--------|
| `datasight demo eia-generation` | US power plant generation and fuel data | ~50 MB | [PUDL](https://catalyst.coop/pudl/) |
| `datasight demo dsgrid-tempo` | EV charging demand projections (2024–2050) | ~19 MB | [NLR TEMPO / OEDI](https://github.com/dsgrid/dsgrid-project-StandardScenarios/blob/main/tempo_project/README.md) |
| `datasight demo time-validation` | Synthetic data with planted time errors | ~70 MB | Generated locally |

---

## EIA generation

Downloads cleaned US power plant data from the
[PUDL project](https://catalyst.coop/pudl/) (Public Utility Data Liberation),
sourced from EIA Forms 923 and 860:

| Table | Description | Rows (2020+) |
|-------|-------------|-------------|
| `generation_fuel` | Monthly generation and fuel consumption by plant/fuel/prime mover | ~1M |
| `plants` | Plant names, locations, coordinates | ~19K |
| `plant_details` | Balancing authority, NERC region, sector | ~50K |

Plus pre-built views for common aggregations and 8 example queries tuned for
energy analysis.

### Download

```bash
datasight demo eia-generation ./eia-project
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
datasight demo eia-generation ./eia-project --min-year 2001
```

### Configure and run

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

### Data source

The data comes from [PUDL's nightly releases](https://catalyst.coop/pudl/) on
AWS S3. PUDL cleans and standardizes data from EIA Forms 923 (generation and
fuel consumption) and 860 (plant characteristics). The data is publicly
accessible with no credentials required.

---

## dsgrid TEMPO — EV charging demand

Downloads projected electric vehicle charging demand from NLR's
[TEMPO project](https://github.com/dsgrid/dsgrid-project-StandardScenarios/blob/main/tempo_project/README.md)
(Transportation Energy & Mobility Pathway Options), part of the
[dsgrid](https://www.nrel.gov/analysis/dsgrid.html) framework. The dataset
models hourly county-level charging load for light-duty passenger EVs across the
contiguous United States from 2024 to 2050.

| Table | Description | Rows |
|-------|-------------|------|
| `charging_profiles` | Hourly demand by census division, scenario, and model year | ~1.4M |
| `annual_state` | Annual demand by state, vehicle type, and scenario | ~16K |
| `annual_county` | Annual demand by county (FIPS), vehicle type, and scenario | ~448K |

Three EV adoption scenarios model different market trajectories:

- **reference** — AEO Reference Case baseline
- **efs_high_ldv** — High electrification of light-duty vehicles
- **ldv_sales_evs_2035** — All new LDV sales are electric by 2035

All energy values are in MWh. Vehicle types are split into BEV and PHEV
variants across four body styles (compact, midsize, SUV, pickup).

### Download

```bash
datasight demo dsgrid-tempo ./tempo-project
```

Downloads three aggregated datasets from OEDI's public S3 bucket in about 10
seconds and creates:

```
tempo-project/
├── .env                     # Connection settings
├── dsgrid_tempo.duckdb      # DuckDB database (~19 MB)
├── schema_description.md    # TEMPO data documentation
└── queries.yaml             # Example EV charging queries
```

### Configure and run

```bash
cd tempo-project
# Edit .env — add your API key
datasight run
```

Open <http://localhost:8084> and try questions like:

- "Total projected EV charging demand by scenario and year"
- "Which states have the highest projected EV charging demand?"
- "Show the hourly charging profile for a summer day in the Pacific division"
- "Compare BEV vs PHEV charging demand over time"
- "How does the all-EV-by-2035 scenario compare to reference for California?"
- "Average daily charging pattern by season"

### Data source

The data comes from [OEDI](https://data.openei.org/) (Open Energy Data
Initiative) on AWS S3 at `s3://nrel-pds-dsgrid/tempo/tempo-2022/v1.0.0`.
Published by NLR based on the 2023 technical report by Yip, Hoehne, Jadun et
al. The data is publicly accessible with no credentials required.

---

## Time validation

Generates a synthetic energy consumption dataset with intentional time-series
errors for testing datasight's quality checks. See
`datasight demo time-validation --help` for details.

```bash
datasight demo time-validation ./tv-project
cd tv-project
datasight quality        # detect the planted errors
datasight run            # explore interactively
```
