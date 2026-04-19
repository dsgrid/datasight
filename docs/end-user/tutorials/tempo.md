# Explore EV charging demand with TEMPO

This tutorial walks you through loading the dsgrid TEMPO dataset — projected
electric vehicle charging demand across the contiguous United States from
2024 to 2050 — and asking your first questions about it. Allow about five
minutes.

## What you'll load

The TEMPO dataset comes from NLR's
[TEMPO project](https://github.com/dsgrid/dsgrid-project-StandardScenarios/blob/main/tempo_project/README.md)
(Transportation Energy & Mobility Pathway Options), part of the
[dsgrid](https://www.nrel.gov/analysis/dsgrid.html) framework. It models
hourly county-level charging load for light-duty passenger EVs.

| Table | Description | Rows |
|-------|-------------|------|
| `charging_profiles` | Hourly demand by census division, scenario, and model year | ~1.4M |
| `annual_state` | Annual demand by state, vehicle type, and scenario | ~16K |
| `annual_county` | Annual demand by county (FIPS), vehicle type, and scenario | ~448K |

Three EV adoption scenarios are modeled:

- **reference** — AEO Reference Case baseline
- **efs_high_ldv** — High electrification of light-duty vehicles
- **ldv_sales_evs_2035** — All new LDV sales are electric by 2035

All energy values are in MWh. Vehicle types are split into BEV and PHEV
variants across four body styles (compact, midsize, SUV, pickup).

## 1. Install datasight

```bash
uv tool install datasight
```

Don't have [uv](https://docs.astral.sh/uv/) yet? See
[Install datasight](../how-to/install.md).

## 2. Download the dataset

```bash
datasight demo dsgrid-tempo ./tempo-project
cd tempo-project
```

This downloads ~19 MB from OEDI's public S3 bucket in about 10 seconds and
creates:

```
tempo-project/
├── .env                     # Connection settings
├── dsgrid_tempo.duckdb      # DuckDB database (~19 MB)
├── schema_description.md    # TEMPO data documentation
└── queries.yaml             # Example EV charging queries
```

## 3. Add an API key

Edit `.env` and add your Anthropic key:

```bash
ANTHROPIC_API_KEY=sk-ant-...
```

Using GitHub Models or Ollama instead? See
[Set up a project](../../project-developer/set-up-project.md#configure) for alternative
configurations.

## 4. Launch the web UI

```bash
datasight run
```

Open <http://localhost:8084>.

## 5. Ask your first questions

Try any of these prompts:

- *Total projected EV charging demand by scenario and year*
- *Which states have the highest projected EV charging demand?*
- *Show the hourly charging profile for a summer day in the Pacific division*
- *Compare BEV vs PHEV charging demand over time*
- *How does the all-EV-by-2035 scenario compare to reference for California?*
- *Average daily charging pattern by season*

## Data source

The data comes from [OEDI](https://data.openei.org/) (Open Energy Data
Initiative) on AWS S3 at `s3://nrel-pds-dsgrid/tempo/tempo-2022/v1.0.0`.
Published by NLR based on the 2023 technical report by Yip, Hoehne, Jadun
et al. The data is publicly accessible with no credentials required.

## What's next

- [Build a dashboard](../how-to/build-a-dashboard.md) — pin results, apply
  cross-card filters, and export as HTML.
- [Ask questions in the web UI](../how-to/ask-in-web-ui.md) — follow-ups,
  clarifying prompts, and the schema sidebar.
- [Configure semantic measures](../how-to/configure-measures.md) — override
  default aggregations for rates, ratios, and calculated measures.
