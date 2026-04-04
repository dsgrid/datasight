"""
Download and prepare an EIA energy demo dataset from PUDL.

Uses the Public Utility Data Liberation (PUDL) project's nightly data releases
on AWS S3 (publicly accessible, no credentials needed). Downloads three tables:

- EIA-923 monthly generation and fuel consumption (2001-present)
- EIA plant entity data (names, locations, coordinates)
- EIA-860 plant characteristics (balancing authority, NERC region, sector)

Data is filtered to recent years and stored in a local DuckDB file.
"""

from pathlib import Path

from loguru import logger

PUDL_BASE = "https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly"

TABLES = {
    "generation_fuel": f"{PUDL_BASE}/core_eia923__monthly_generation_fuel.parquet",
    "plants": f"{PUDL_BASE}/core_eia__entity_plants.parquet",
    "plant_details": f"{PUDL_BASE}/core_eia860__scd_plants.parquet",
}

# Filter to recent years to keep the demo database small (~50MB vs ~500MB)
DEFAULT_MIN_YEAR = 2020

SCHEMA_DESCRIPTION = """\
# EIA Power Plant Data (PUDL)

This database contains cleaned US power plant data from the
[PUDL project](https://catalyst.coop/pudl/), sourced from EIA Forms 923 and 860.

## Tables

### generation_fuel
Monthly electricity generation and fuel consumption by plant, fuel type, and
prime mover. One row per plant x energy_source x prime_mover x month.

### plants
Static plant information: name, location (city, state, lat/lon), timezone.

### plant_details
Annual plant characteristics from EIA-860: balancing authority, NERC region,
sector, regulatory status. Joined to generation_fuel via plant_id_eia.

## Key Columns

- **plant_id_eia**: Unique EIA plant identifier (joins all three tables)
- **report_date**: First day of each month (DATE) — in generation_fuel and plant_details
- **energy_source_code**: Fuel type code (NG, BIT, SUB, SUN, WND, WAT, NUC, etc.) — in generation_fuel
- **fuel_type_code_agg**: Broader fuel category (NG, COL, SUN, WND, NUC, HYC, etc.) — in generation_fuel
- **prime_mover_code**: Generation technology (ST, GT, CA, CT, PV, WT, HY, etc.) — in generation_fuel
- **net_generation_mwh**: Net electricity generated (MWh) — in generation_fuel
- **fuel_consumed_mmbtu**: Total fuel consumed (MMBtu) — in generation_fuel
- **state**: Two-letter state code — in plants table (NOT in generation_fuel; must JOIN plants to get state)

## Energy Source Codes

- NG = Natural Gas
- BIT/SUB/LIG/RC = Coal (fuel_type_code_agg = COL)
- SUN = Solar, WND = Wind, WAT = Hydro (fuel_type_code_agg = HYC)
- DFO = Distillate Fuel Oil, RFO = Residual Fuel Oil
- GEO = Geothermal
- WDS/BLQ = Wood/Biomass, LFG = Landfill Gas

## Prime Mover Codes

- ST = Steam Turbine, GT = Gas Turbine
- CA/CT/CS = Combined Cycle
- PV = Solar Photovoltaic, WT = Wind Turbine
- HY = Hydraulic Turbine, PS = Pumped Storage
- IC = Internal Combustion, BA = Battery Storage

## Tips

- Join generation_fuel to plants on plant_id_eia for plant names and locations
- Use fuel_type_code_agg for broad fuel categories (fewer codes than energy_source_code)
- This is a DuckDB database — use DuckDB SQL syntax
- For state-level analysis, use the `state` column from the plants table
"""

EXAMPLE_QUERIES = """\
- question: What are the top 10 power plants by total generation?
  sql: |
    SELECT p.plant_name_eia, p.state,
           SUM(g.net_generation_mwh) AS total_mwh
    FROM generation_fuel g
    JOIN plants p USING (plant_id_eia)
    GROUP BY p.plant_name_eia, p.state
    ORDER BY total_mwh DESC
    LIMIT 10

- question: Monthly national generation by fuel type
  sql: |
    SELECT report_date, fuel_type_code_agg,
           SUM(net_generation_mwh) AS total_mwh
    FROM generation_fuel
    GROUP BY report_date, fuel_type_code_agg
    ORDER BY report_date, total_mwh DESC

- question: Which states generate the most solar power?
  sql: |
    SELECT p.state, SUM(g.net_generation_mwh) AS solar_mwh
    FROM generation_fuel g
    JOIN plants p USING (plant_id_eia)
    WHERE g.energy_source_code = 'SUN'
    GROUP BY p.state
    ORDER BY solar_mwh DESC
    LIMIT 15

- question: Compare coal vs natural gas generation over time
  sql: |
    SELECT report_date, fuel_type_code_agg,
           SUM(net_generation_mwh) AS total_mwh
    FROM generation_fuel
    WHERE fuel_type_code_agg IN ('NG', 'COL')
    GROUP BY report_date, fuel_type_code_agg
    ORDER BY report_date

- question: Wind generation growth by year
  sql: |
    SELECT EXTRACT(YEAR FROM report_date) AS year,
           SUM(net_generation_mwh) AS wind_mwh
    FROM generation_fuel
    WHERE energy_source_code = 'WND'
    GROUP BY year
    ORDER BY year

- question: Generation mix for California
  sql: |
    SELECT g.fuel_type_code_agg,
           SUM(g.net_generation_mwh) AS total_mwh
    FROM generation_fuel g
    JOIN plants p USING (plant_id_eia)
    WHERE p.state = 'CA'
    GROUP BY g.fuel_type_code_agg
    ORDER BY total_mwh DESC

- question: Largest natural gas plants by state
  sql: |
    SELECT p.state, p.plant_name_eia,
           SUM(g.net_generation_mwh) AS gas_mwh
    FROM generation_fuel g
    JOIN plants p USING (plant_id_eia)
    WHERE g.energy_source_code = 'NG'
    GROUP BY p.state, p.plant_name_eia
    QUALIFY ROW_NUMBER() OVER (PARTITION BY p.state ORDER BY gas_mwh DESC) = 1
    ORDER BY gas_mwh DESC

- question: Monthly hydroelectric generation and plant count
  sql: |
    SELECT g.report_date,
           SUM(g.net_generation_mwh) AS hydro_mwh,
           COUNT(DISTINCT g.plant_id_eia) AS plant_count
    FROM generation_fuel g
    WHERE g.fuel_type_code_agg = 'HYC'
    GROUP BY g.report_date
    ORDER BY g.report_date

- question: Solar generation over time for the top 5 generating states
  sql: |
    WITH top_states AS (
        SELECT p.state, SUM(g.net_generation_mwh) AS total
        FROM generation_fuel g
        JOIN plants p USING (plant_id_eia)
        WHERE g.energy_source_code = 'SUN'
        GROUP BY p.state
        ORDER BY total DESC
        LIMIT 5
    )
    SELECT p.state, g.report_date,
           SUM(g.net_generation_mwh) AS solar_mwh
    FROM generation_fuel g
    JOIN plants p USING (plant_id_eia)
    WHERE g.energy_source_code = 'SUN'
      AND p.state IN (SELECT state FROM top_states)
    GROUP BY p.state, g.report_date
    ORDER BY g.report_date, p.state
"""


def download_demo_dataset(
    dest_dir: Path,
    db_name: str = "eia_demo.duckdb",
    min_year: int = DEFAULT_MIN_YEAR,
) -> Path:
    """Download PUDL EIA data and create a local DuckDB database.

    Parameters
    ----------
    dest_dir:
        Directory to create the project in.
    db_name:
        Name of the DuckDB file to create.
    min_year:
        Earliest year to include (keeps the database small).

    Returns
    -------
    Path to the created DuckDB file.
    """
    import duckdb

    db_path = dest_dir / db_name
    if db_path.exists():
        db_path.unlink()

    conn = duckdb.connect(str(db_path))
    conn.execute("INSTALL httpfs; LOAD httpfs;")

    logger.info("Downloading EIA-923 generation and fuel data...")
    conn.execute(f"""
        CREATE TABLE generation_fuel AS
        SELECT * FROM read_parquet('{TABLES["generation_fuel"]}')
        WHERE EXTRACT(YEAR FROM report_date) >= {min_year}
    """)
    row = conn.execute("SELECT COUNT(*) FROM generation_fuel").fetchone()
    logger.info(
        f"  generation_fuel: {row[0]:,} rows" if row else "  generation_fuel: unknown rows"
    )

    logger.info("Downloading EIA plant entity data...")
    conn.execute(f"""
        CREATE TABLE plants AS
        SELECT * FROM read_parquet('{TABLES["plants"]}')
    """)
    row = conn.execute("SELECT COUNT(*) FROM plants").fetchone()
    logger.info(f"  plants: {row[0]:,} rows" if row else "  plants: unknown rows")

    logger.info("Downloading EIA-860 plant details...")
    conn.execute(f"""
        CREATE TABLE plant_details AS
        SELECT * FROM read_parquet('{TABLES["plant_details"]}')
        WHERE EXTRACT(YEAR FROM report_date) >= {min_year}
    """)
    row = conn.execute("SELECT COUNT(*) FROM plant_details").fetchone()
    logger.info(f"  plant_details: {row[0]:,} rows" if row else "  plant_details: unknown rows")

    # Create useful views
    logger.info("Creating views...")
    conn.execute("""
        CREATE VIEW v_monthly_national_fuel AS
        SELECT
            report_date,
            fuel_type_code_agg,
            SUM(net_generation_mwh) AS total_net_generation_mwh,
            SUM(fuel_consumed_mmbtu) AS total_fuel_consumed_mmbtu,
            COUNT(DISTINCT plant_id_eia) AS plant_count
        FROM generation_fuel
        GROUP BY report_date, fuel_type_code_agg
    """)

    conn.execute("""
        CREATE VIEW v_monthly_state_fuel AS
        SELECT
            p.state,
            g.report_date,
            g.fuel_type_code_agg,
            SUM(g.net_generation_mwh) AS total_net_generation_mwh,
            SUM(g.fuel_consumed_mmbtu) AS total_fuel_consumed_mmbtu,
            COUNT(DISTINCT g.plant_id_eia) AS plant_count
        FROM generation_fuel g
        JOIN plants p USING (plant_id_eia)
        GROUP BY p.state, g.report_date, g.fuel_type_code_agg
    """)

    conn.execute("""
        CREATE VIEW v_annual_plant_totals AS
        SELECT
            g.plant_id_eia,
            p.plant_name_eia,
            p.state,
            EXTRACT(YEAR FROM g.report_date) AS year,
            SUM(g.net_generation_mwh) AS total_net_generation_mwh,
            SUM(g.fuel_consumed_mmbtu) AS total_fuel_consumed_mmbtu
        FROM generation_fuel g
        JOIN plants p USING (plant_id_eia)
        GROUP BY g.plant_id_eia, p.plant_name_eia, p.state, year
    """)

    conn.close()

    db_size_mb = db_path.stat().st_size / (1024 * 1024)
    logger.info(f"Database created: {db_path} ({db_size_mb:.1f} MB)")

    return db_path


def write_demo_project_files(dest_dir: Path) -> None:
    """Write schema_description.md, queries.yaml, and .env for the demo."""
    schema_path = dest_dir / "schema_description.md"
    schema_path.write_text(SCHEMA_DESCRIPTION)
    logger.info(f"  Created {schema_path.name}")

    queries_path = dest_dir / "queries.yaml"
    queries_path.write_text(EXAMPLE_QUERIES)
    logger.info(f"  Created {queries_path.name}")

    env_path = dest_dir / ".env"
    db_path_line = f"DB_PATH={dest_dir / 'eia_demo.duckdb'}\n"
    if not env_path.exists():
        env_path.write_text(
            "# datasight demo project\n"
            "ANTHROPIC_API_KEY=your-api-key-here\n"
            "DB_MODE=duckdb\n" + db_path_line
        )
        logger.info(f"  Created {env_path.name}")
    else:
        # Existing .env — ensure DB_PATH is set for the demo database
        content = env_path.read_text()
        if "DB_PATH" not in content:
            with open(env_path, "a") as f:
                f.write(f"\n# Added by datasight demo\nDB_MODE=duckdb\n{db_path_line}")
            logger.info(f"  Updated {env_path.name} — added DB_PATH")
        else:
            logger.warning(
                f"  {env_path.name} already has DB_PATH set. "
                f"Make sure it points to: {dest_dir / 'eia_demo.duckdb'}"
            )
