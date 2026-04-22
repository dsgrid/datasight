"""
Download and prepare a dsgrid TEMPO EV charging demo dataset.

Uses the TEMPO (Transportation Energy & Mobility Pathway Options) project's
county-level light-duty passenger EV charging profiles published on OEDI
(Open Energy Data Initiative). Downloads three aggregated datasets:

- Simple profiles: hourly charging demand by census division (13 MB)
- Annual summary by state: annual charging demand by state and vehicle type (135 KB)
- Annual summary by county: annual charging demand by county and vehicle type (3.2 MB)

Data is publicly accessible on S3 (no credentials needed) and loaded into
a local DuckDB file. All energy values are in MWh.

Reference: Yip, Hoehne, Jadun et al. (2023), NLR Technical Report.
https://github.com/dsgrid/dsgrid-project-StandardScenarios/blob/main/tempo_project/README.md
"""

from __future__ import annotations

from pathlib import Path

from loguru import logger

S3_BASE = "s3://nrel-pds-dsgrid/tempo/tempo-2022/v1.0.0"

DATASETS = {
    "charging_profiles": f"{S3_BASE}/simple_profiles/table.parquet/*.parquet",
    "annual_state": f"{S3_BASE}/annual_summary_state/table.parquet/*.parquet",
    "annual_county": f"{S3_BASE}/annual_summary_county/table.parquet/*.parquet",
}

SCHEMA_DESCRIPTION = """\
# dsgrid TEMPO — EV Charging Demand Projections

Three DuckDB tables derived from the dsgrid TEMPO project's OEDI
publications. All energy values are in **MWh** (measured at the electrical
meter). See the "Additional project context" section at the bottom for the
full upstream README with scenarios, vehicle types, dimensions, bounding-case
assumptions, and timestamp details.

## Tables in this database

### charging_profiles
Hourly EV charging demand aggregated to **census division** level.
One row per scenario × model year × census division × hour.

| Column | Description |
|--------|-------------|
| scenario | Adoption scenario — column values: `reference`, `efs_high_ldv`, `ldv_sales_evs_2035` |
| time_est | Hourly timestamp (EST, 2012 meteorological year) |
| five_year_intervals | Model year (2025, 2030, 2035, 2040, 2045, 2050) |
| census_division | One of: east_north_central, east_south_central, middle_atlantic, mountain, new_england, pacific, south_atlantic, west_north_central, west_south_central |
| value | Charging demand in MWh |

### annual_state
Annual EV charging demand by **state** and **vehicle type**.
One row per scenario × model year × state × subsector.

| Column | Description |
|--------|-------------|
| scenario | Adoption scenario (same values as above) |
| year | Weather year (2012) |
| tempo_project_model_years | Projection year (biennial 2024–2050) |
| state | Two-letter state code |
| subsector | Vehicle type — one of: `bev_compact`, `bev_midsize`, `bev_suv`, `bev_pickup`, `phev_compact`, `phev_midsize`, `phev_suv`, `phev_pickup` |
| value | Annual charging demand in MWh |

### annual_county
Annual EV charging demand by **county** and **vehicle type**.
One row per scenario × model year × county × subsector.

| Column | Description |
|--------|-------------|
| scenario | Adoption scenario |
| year | Weather year (2012) |
| five_year_intervals | Model year (5-year intervals, 2025–2050) |
| county | FIPS county code (e.g. `06037` = Los Angeles County, CA) |
| subsector | Vehicle type (same values as annual_state) |
| value | Annual charging demand in MWh |

## Query defaults

- **Model year**: Unless the user specifies a particular model year, **include all
  model years** in query results. Do not prompt the user to choose a model year —
  the data is small enough that aggregating across all years is fast and gives the
  most useful overview. When all model years are included, always include the
  model year column in the SELECT and GROUP BY so the user can see trends over
  time.
- **Scenario**: If the user does not specify a scenario, **ask which scenario
  they want** before writing the query. You MUST use exactly this markdown
  format (bullet + bold + em-dash) so the UI renders clickable buttons:

  ```
  Which scenario would you like to explore?

  - **Reference** — AEO Reference Case (baseline EV adoption)
  - **EFS High** — High electrification of light-duty vehicles
  - **All EV by 2035** — All new light-duty vehicle sales are electric by 2035
  ```

  Each option line MUST start with `- **` — this is required for the button
  rendering to work. Do NOT omit the bullet or bold markers.

  Map the user's choice to the column value: Reference → 'reference',
  EFS High → 'efs_high_ldv', All EV by 2035 → 'ldv_sales_evs_2035'.

## Tips

- Use charging_profiles for **time-series analysis** (hourly patterns, seasonal trends)
- Use annual_state for **state-level comparisons** across scenarios and vehicle types
- Use annual_county for **geographic drill-down** to individual counties
- Compare scenarios to see how adoption assumptions affect projected demand
- BEV vs PHEV split shows the drivetrain mix impact on grid load
- This is a DuckDB database — use DuckDB SQL syntax

## Additional project context

Upstream project README (dsgrid dimensions, OEDI publication details,
scenario descriptions, bounding-case charging assumptions, weather year,
timestamp conventions):

[include:dsgrid TEMPO project README](https://raw.githubusercontent.com/dsgrid/dsgrid-project-StandardScenarios/main/tempo_project/README.md)
"""

EXAMPLE_QUERIES = """\
- question: Total projected EV charging demand by scenario and year
  sql: |
    SELECT scenario,
           tempo_project_model_years AS model_year,
           ROUND(SUM(value)) AS total_mwh
    FROM annual_state
    GROUP BY scenario, tempo_project_model_years
    ORDER BY scenario, model_year

- question: Which states have the highest projected EV charging demand?
  sql: |
    SELECT state,
           ROUND(SUM(value)) AS total_mwh
    FROM annual_state
    WHERE scenario = 'efs_high_ldv'
      AND tempo_project_model_years = '2050'
    GROUP BY state
    ORDER BY total_mwh DESC
    LIMIT 15

- question: BEV vs PHEV charging demand over time
  sql: |
    SELECT tempo_project_model_years AS model_year,
           CASE WHEN subsector LIKE 'bev_%' THEN 'BEV' ELSE 'PHEV' END AS drivetrain,
           ROUND(SUM(value)) AS total_mwh
    FROM annual_state
    WHERE scenario = 'efs_high_ldv'
    GROUP BY model_year, drivetrain
    ORDER BY model_year, drivetrain

- question: Hourly charging profile for a summer day in the Pacific division
  sql: |
    SELECT time_est,
           ROUND(value, 1) AS demand_mwh
    FROM charging_profiles
    WHERE census_division = 'pacific'
      AND scenario = 'efs_high_ldv'
      AND five_year_intervals = '2040'
      AND time_est BETWEEN '2012-07-15' AND '2012-07-16'
    ORDER BY time_est

- question: Compare charging demand across census divisions for 2050
  sql: |
    SELECT census_division,
           ROUND(SUM(value)) AS total_mwh
    FROM charging_profiles
    WHERE scenario = 'efs_high_ldv'
      AND five_year_intervals = '2050'
    GROUP BY census_division
    ORDER BY total_mwh DESC

- question: Vehicle type breakdown by state for the top 10 states
  sql: |
    WITH top_states AS (
        SELECT state, SUM(value) AS total
        FROM annual_state
        WHERE scenario = 'efs_high_ldv'
          AND tempo_project_model_years = '2050'
        GROUP BY state
        ORDER BY total DESC
        LIMIT 10
    )
    SELECT a.state, a.subsector,
           ROUND(SUM(a.value)) AS total_mwh
    FROM annual_state a
    JOIN top_states t ON a.state = t.state
    WHERE a.scenario = 'efs_high_ldv'
      AND a.tempo_project_model_years = '2050'
    GROUP BY a.state, a.subsector
    ORDER BY a.state, total_mwh DESC

- question: How does the all-EV-by-2035 scenario compare to reference for California?
  sql: |
    SELECT scenario,
           tempo_project_model_years AS model_year,
           ROUND(SUM(value)) AS total_mwh
    FROM annual_state
    WHERE state = 'CA'
    GROUP BY scenario, tempo_project_model_years
    ORDER BY scenario, model_year

- question: Average daily charging pattern by season
  sql: |
    SELECT CASE
             WHEN MONTH(time_est) IN (12, 1, 2) THEN 'Winter'
             WHEN MONTH(time_est) IN (3, 4, 5) THEN 'Spring'
             WHEN MONTH(time_est) IN (6, 7, 8) THEN 'Summer'
             ELSE 'Fall'
           END AS season,
           HOUR(time_est) AS hour_of_day,
           ROUND(AVG(value), 1) AS avg_demand_mwh
    FROM charging_profiles
    WHERE scenario = 'efs_high_ldv'
      AND five_year_intervals = '2040'
    GROUP BY season, hour_of_day
    ORDER BY season, hour_of_day
"""


def download_dsgrid_tempo_dataset(
    dest_dir: Path,
    db_name: str = "dsgrid_tempo.duckdb",
) -> Path:
    """Download dsgrid TEMPO data from OEDI S3 and create a local DuckDB database.

    Parameters
    ----------
    dest_dir:
        Directory to create the project in.
    db_name:
        Name of the DuckDB file to create.

    Returns
    -------
    Path to the created DuckDB file.
    """
    import duckdb

    db_path = dest_dir / db_name
    if db_path.exists():
        db_path.unlink()

    conn = duckdb.connect(str(db_path))
    conn.execute("INSTALL httpfs; LOAD httpfs; SET s3_region='us-west-2';")

    logger.info("Downloading hourly charging profiles (census division level)...")
    conn.execute(f"""
        CREATE TABLE charging_profiles AS
        SELECT * FROM read_parquet('{DATASETS["charging_profiles"]}')
    """)
    row = conn.execute("SELECT COUNT(*) FROM charging_profiles").fetchone()
    logger.info(
        f"  charging_profiles: {row[0]:,} rows" if row else "  charging_profiles: unknown rows"
    )

    logger.info("Downloading annual state-level summary...")
    conn.execute(f"""
        CREATE TABLE annual_state AS
        SELECT * FROM read_parquet('{DATASETS["annual_state"]}')
    """)
    row = conn.execute("SELECT COUNT(*) FROM annual_state").fetchone()
    logger.info(f"  annual_state: {row[0]:,} rows" if row else "  annual_state: unknown rows")

    logger.info("Downloading annual county-level summary...")
    conn.execute(f"""
        CREATE TABLE annual_county AS
        SELECT * FROM read_parquet('{DATASETS["annual_county"]}')
    """)
    row = conn.execute("SELECT COUNT(*) FROM annual_county").fetchone()
    logger.info(f"  annual_county: {row[0]:,} rows" if row else "  annual_county: unknown rows")

    # Create useful views
    logger.info("Creating views...")

    conn.execute("""
        CREATE VIEW v_annual_national_scenario AS
        SELECT
            scenario,
            tempo_project_model_years AS model_year,
            SUM(value) AS total_demand_mwh,
            COUNT(DISTINCT state) AS state_count
        FROM annual_state
        GROUP BY scenario, tempo_project_model_years
    """)

    conn.execute("""
        CREATE VIEW v_annual_state_drivetrain AS
        SELECT
            scenario,
            tempo_project_model_years AS model_year,
            state,
            CASE WHEN subsector LIKE 'bev_%' THEN 'BEV' ELSE 'PHEV' END AS drivetrain,
            SUM(value) AS total_demand_mwh
        FROM annual_state
        GROUP BY scenario, tempo_project_model_years, state, drivetrain
    """)

    conn.execute("""
        CREATE VIEW v_hourly_national AS
        SELECT
            scenario,
            five_year_intervals AS model_year,
            time_est,
            SUM(value) AS total_demand_mwh
        FROM charging_profiles
        GROUP BY scenario, five_year_intervals, time_est
    """)

    conn.close()

    db_size_mb = db_path.stat().st_size / (1024 * 1024)
    logger.info(f"Database created: {db_path} ({db_size_mb:.1f} MB)")

    return db_path


def write_dsgrid_tempo_project_files(dest_dir: Path, db_path: Path) -> None:
    """Write schema_description.md, queries.yaml, and .env for the TEMPO demo."""
    schema_path = dest_dir / "schema_description.md"
    schema_path.write_text(SCHEMA_DESCRIPTION, encoding="utf-8")
    logger.info(f"  Created {schema_path.name}")

    queries_path = dest_dir / "queries.yaml"
    queries_path.write_text(EXAMPLE_QUERIES, encoding="utf-8")
    logger.info(f"  Created {queries_path.name}")

    env_path = dest_dir / ".env"
    db_path_line = f"DB_PATH={db_path}\n"
    if not env_path.exists():
        env_path.write_text(
            "# datasight demo project — dsgrid TEMPO EV charging\n"
            "ANTHROPIC_API_KEY=your-api-key-here\n"
            "DB_MODE=duckdb\n" + db_path_line,
            encoding="utf-8",
        )
        logger.info(f"  Created {env_path.name}")
    else:
        content = env_path.read_text(encoding="utf-8")
        if "DB_PATH" not in content:
            with open(env_path, "a", encoding="utf-8") as f:
                f.write(f"\n# Added by datasight demo\nDB_MODE=duckdb\n{db_path_line}")
            logger.info(f"  Updated {env_path.name} — added DB_PATH")
        else:
            logger.warning(
                f"  {env_path.name} already has DB_PATH set. Make sure it points to: {db_path}"
            )
