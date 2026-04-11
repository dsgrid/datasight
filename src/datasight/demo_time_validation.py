"""
Generate a synthetic energy consumption dataset with planted time errors.

Creates hourly electricity consumption across US states, sectors, and end uses
for future projection years (2030, 2035, 2040). Intentional gaps, duplicates,
and DST anomalies are injected so users can exercise datasight's time series
quality checks.

The dataset is generated entirely in DuckDB SQL — no network access required.
"""

from __future__ import annotations

from pathlib import Path

from loguru import logger

# ---------------------------------------------------------------------------
# Data dimensions
# ---------------------------------------------------------------------------

US_STATES = [
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
]

SECTORS = {
    "residential": [
        "heating",
        "cooling",
        "water_heating",
        "lighting",
        "appliances",
    ],
    "commercial": [
        "heating",
        "cooling",
        "lighting",
        "ventilation",
        "computing",
    ],
    "industrial": [
        "process_heat",
        "motors",
        "electrolysis",
        "compressed_air",
        "lighting",
    ],
    "transportation": [
        "ev_charging",
        "rail",
        "transit",
        "fleet_charging",
    ],
}

PROJECTION_YEARS = [2038, 2039, 2040]

# ---------------------------------------------------------------------------
# Planted time errors
# ---------------------------------------------------------------------------

# Each entry describes one intentional defect.  The SQL fragments are applied
# after the clean data is generated.
#
# Categories:
#   gap       — DELETE rows to create missing-hour windows
#   duplicate — INSERT extra rows to create repeated timestamps
#
# We plant issues in specific states so the user can discover *where* and
# *when* through datasight quality or the web UI.

PLANTED_ERRORS: list[dict[str, str]] = [
    # 1. Week-long pipeline outage gap — Texas, Dec 2039
    {
        "label": "Week-long pipeline outage (TX, Dec 2039)",
        "kind": "gap",
        "sql": """
            DELETE FROM hourly_consumption
            WHERE state = 'TX'
              AND timestamp_utc >= TIMESTAMP '2039-12-08 00:00:00'
              AND timestamp_utc <  TIMESTAMP '2039-12-15 00:00:00'
        """,
    },
    # 2. DST spring-forward gap — several eastern states, March 2038
    #    In 2038, US spring forward is March 14 at 2:00 AM.
    #    Remove the 07:00 UTC hour (which is 2:00 AM EST) for a few states
    #    to simulate local-time data where the spring-forward hour was dropped.
    {
        "label": "DST spring-forward gap (NY/PA/OH, Mar 2038)",
        "kind": "gap",
        "sql": """
            DELETE FROM hourly_consumption
            WHERE state IN ('NY', 'PA', 'OH')
              AND timestamp_utc = TIMESTAMP '2038-03-14 07:00:00'
        """,
    },
    # 3. DST fall-back duplicate — same eastern states, November 2038
    #    In 2038, US fall back is November 7 at 2:00 AM.
    #    Duplicate the 06:00 UTC hour (1:00 AM EST) — the repeated hour.
    {
        "label": "DST fall-back duplicate (NY/PA/OH, Nov 2038)",
        "kind": "duplicate",
        "sql": """
            INSERT INTO hourly_consumption
            SELECT * FROM hourly_consumption
            WHERE state IN ('NY', 'PA', 'OH')
              AND timestamp_utc = TIMESTAMP '2038-11-07 06:00:00'
        """,
    },
    # 4. Scattered 24-hour gap — California, Aug 2040
    {
        "label": "24-hour data drop (CA, Aug 15 2040)",
        "kind": "gap",
        "sql": """
            DELETE FROM hourly_consumption
            WHERE state = 'CA'
              AND timestamp_utc >= TIMESTAMP '2040-08-15 00:00:00'
              AND timestamp_utc <  TIMESTAMP '2040-08-16 00:00:00'
        """,
    },
    # 5. Triple duplicate — Florida, New Year's Day 2039 midnight
    {
        "label": "Triple duplicate (FL, Jan 1 2039 00:00)",
        "kind": "duplicate",
        "sql": """
            INSERT INTO hourly_consumption
            SELECT * FROM hourly_consumption
            WHERE state = 'FL'
              AND timestamp_utc = TIMESTAMP '2039-01-01 00:00:00'
        """,
    },
    # 6. Two extra copies (triple) — Florida again, same timestamp
    #    (stacks on #5 to make some rows appear 3x)
    {
        "label": "Triple duplicate second copy (FL, Jan 1 2039 00:00)",
        "kind": "duplicate",
        "sql": """
            INSERT INTO hourly_consumption
            SELECT DISTINCT * FROM hourly_consumption
            WHERE state = 'FL'
              AND timestamp_utc = TIMESTAMP '2039-01-01 00:00:00'
        """,
    },
    # 7. Weekend gap — Illinois, every Saturday in June 2040
    #    Simulates a reporting system that goes offline on weekends.
    {
        "label": "Weekend reporting gap (IL, Saturdays in Jun 2040)",
        "kind": "gap",
        "sql": """
            DELETE FROM hourly_consumption
            WHERE state = 'IL'
              AND timestamp_utc >= TIMESTAMP '2040-06-01 00:00:00'
              AND timestamp_utc <  TIMESTAMP '2040-07-01 00:00:00'
              AND DAYOFWEEK(timestamp_utc) = 6
        """,
    },
]


# ---------------------------------------------------------------------------
# Dataset generation
# ---------------------------------------------------------------------------


def generate_time_validation_dataset(
    dest_dir: Path,
    db_name: str = "time_validation_demo.duckdb",
) -> Path:
    """Generate a synthetic hourly consumption dataset with planted errors.

    Returns the path to the created DuckDB file.
    """
    import duckdb

    db_path = dest_dir / db_name
    if db_path.exists():
        db_path.unlink()

    conn = duckdb.connect(str(db_path))

    # Build dimension tables in-memory for the cross join
    states_csv = ", ".join(f"('{s}')" for s in US_STATES)
    sectors_rows = []
    for sector, end_uses in SECTORS.items():
        for eu in end_uses:
            sectors_rows.append(f"('{sector}', '{eu}')")
    sectors_csv = ", ".join(sectors_rows)

    # Generate all year ranges as a UNION of generate_series calls
    year_series_parts = []
    for year in PROJECTION_YEARS:
        year_series_parts.append(
            f"SELECT ts FROM generate_series("
            f"TIMESTAMP '{year}-01-01 00:00:00', "
            f"TIMESTAMP '{year}-12-31 23:00:00', "
            f"INTERVAL '1 HOUR') t(ts)"
        )
    hours_union = " UNION ALL ".join(year_series_parts)

    logger.info("Building dimension tables...")

    conn.execute(f"""
        CREATE TEMP TABLE dim_state AS
        SELECT col0 AS state FROM (VALUES {states_csv})
    """)

    conn.execute(f"""
        CREATE TEMP TABLE dim_sector AS
        SELECT col0 AS sector, col1 AS end_use
        FROM (VALUES {sectors_csv})
    """)

    conn.execute(f"""
        CREATE TEMP TABLE dim_hour AS
        {hours_union}
    """)

    row = conn.execute("SELECT COUNT(*) FROM dim_hour").fetchone()
    total_hours = row[0] if row else 0
    total_end_uses = sum(len(v) for v in SECTORS.values())
    expected_rows = len(US_STATES) * total_end_uses * total_hours
    logger.info(
        f"  {len(US_STATES)} states × {total_end_uses} end uses × "
        f"{total_hours:,} hours = {expected_rows:,} expected rows"
    )

    logger.info("Generating hourly consumption data (cross join + synthetic values)...")

    # Generate consumption values using a deterministic formula based on
    # hour-of-day, month, sector, and state — no randomness so the dataset
    # is reproducible.  The formula produces realistic-looking diurnal and
    # seasonal patterns.
    conn.execute("""
        CREATE TABLE hourly_consumption AS
        SELECT
            h.ts                  AS timestamp_utc,
            s.state,
            e.sector,
            e.end_use,
            -- Deterministic synthetic consumption (MWh)
            -- Base load varies by sector, modulated by hour-of-day and month.
            ROUND(CAST(
                CASE e.sector
                    WHEN 'residential'    THEN 0.8
                    WHEN 'commercial'     THEN 1.2
                    WHEN 'industrial'     THEN 2.5
                    WHEN 'transportation' THEN 0.4
                END
                -- Diurnal pattern: peak mid-afternoon, trough overnight
                * (1.0 + 0.3 * SIN((EXTRACT(HOUR FROM h.ts) - 6) * 3.14159 / 12.0))
                -- Seasonal pattern: higher in summer (cooling) and winter (heating)
                * (1.0 + 0.2 * COS((EXTRACT(MONTH FROM h.ts) - 7) * 3.14159 / 6.0))
                -- State population proxy: hash state code to a scale factor 0.5–2.0
                * (0.5 + 1.5 * (ASCII(s.state[1]) % 10) / 9.0)
                -- Year growth factor: 2% per 5-year step
                * POWER(1.02, (EXTRACT(YEAR FROM h.ts) - 2030) / 5.0)
            AS DOUBLE), 3)       AS consumption_mwh
        FROM dim_hour h
        CROSS JOIN dim_state s
        CROSS JOIN dim_sector e
    """)

    row = conn.execute("SELECT COUNT(*) FROM hourly_consumption").fetchone()
    clean_count = row[0] if row else 0
    logger.info(f"  Clean dataset: {clean_count:,} rows")

    # Apply planted errors
    logger.info("Injecting planted time errors...")
    for error in PLANTED_ERRORS:
        conn.execute(error["sql"])
        logger.info(f"  {error['label']}")

    row = conn.execute("SELECT COUNT(*) FROM hourly_consumption").fetchone()
    final_count = row[0] if row else 0
    delta = final_count - clean_count
    sign = "+" if delta >= 0 else ""
    logger.info(f"  Final dataset: {final_count:,} rows ({sign}{delta:,})")

    # Create useful views
    logger.info("Creating views...")

    conn.execute("""
        CREATE VIEW v_hourly_state_sector AS
        SELECT
            timestamp_utc,
            state,
            sector,
            SUM(consumption_mwh) AS total_consumption_mwh,
            COUNT(DISTINCT end_use) AS end_use_count
        FROM hourly_consumption
        GROUP BY timestamp_utc, state, sector
    """)

    conn.execute("""
        CREATE VIEW v_daily_national AS
        SELECT
            DATE_TRUNC('day', timestamp_utc) AS date,
            sector,
            SUM(consumption_mwh) AS total_consumption_mwh
        FROM hourly_consumption
        GROUP BY date, sector
        ORDER BY date, sector
    """)

    conn.execute("""
        CREATE VIEW v_monthly_state AS
        SELECT
            DATE_TRUNC('month', timestamp_utc) AS month,
            state,
            sector,
            SUM(consumption_mwh) AS total_consumption_mwh,
            COUNT(*) AS hour_count
        FROM hourly_consumption
        GROUP BY month, state, sector
    """)

    conn.close()

    db_size_mb = db_path.stat().st_size / (1024 * 1024)
    logger.info(f"Database created: {db_path} ({db_size_mb:.1f} MB)")

    return db_path


# ---------------------------------------------------------------------------
# Project files
# ---------------------------------------------------------------------------

SCHEMA_DESCRIPTION = """\
# Projected US Electricity Consumption (Synthetic)

This is a **synthetic demo dataset** containing hourly electricity consumption
projections for all 50 US states across four sectors and their end uses.
The projection years are 2038, 2039, and 2040.

**This dataset contains intentional time errors** — gaps and duplicates planted
to exercise datasight's time series quality checks. Run `datasight quality` to
find them.

## Tables

### hourly_consumption
Hourly electricity consumption by state, sector, and end use.
One row per state × sector × end_use × hour.

**Columns:**
- **timestamp_utc** (TIMESTAMP): Hour timestamp in UTC
- **state** (VARCHAR): Two-letter US state code
- **sector** (VARCHAR): One of: residential, commercial, industrial, transportation
- **end_use** (VARCHAR): Sector-specific end use category
- **consumption_mwh** (DOUBLE): Electricity consumed (MWh)

## Sectors and End Uses

- **residential**: heating, cooling, water_heating, lighting, appliances
- **commercial**: heating, cooling, lighting, ventilation, computing
- **industrial**: process_heat, motors, electrolysis, compressed_air, lighting
- **transportation**: ev_charging, rail, transit, fleet_charging

## Views

- **v_hourly_state_sector**: Hourly totals rolled up to state × sector level
- **v_daily_national**: Daily national totals by sector
- **v_monthly_state**: Monthly totals by state and sector, with hour counts

## Tips

- This is a DuckDB database — use DuckDB SQL syntax
- Use `timestamp_utc` for all time-based analysis
- Group by `state` for geographic breakdowns
- Group by `sector` and `end_use` for demand composition
- The `v_monthly_state` view includes `hour_count` — useful for spotting
  months with missing or duplicate data
- For temporal quality analysis, run `datasight quality` which uses the
  `time_series.yaml` configuration
"""

EXAMPLE_QUERIES = """\
- question: Total consumption by sector for 2038
  sql: |
    SELECT sector,
           ROUND(SUM(consumption_mwh), 0) AS total_mwh
    FROM hourly_consumption
    WHERE EXTRACT(YEAR FROM timestamp_utc) = 2038
    GROUP BY sector
    ORDER BY total_mwh DESC

- question: Monthly consumption trend for Texas
  sql: |
    SELECT DATE_TRUNC('month', timestamp_utc) AS month,
           sector,
           ROUND(SUM(consumption_mwh), 0) AS total_mwh
    FROM hourly_consumption
    WHERE state = 'TX'
    GROUP BY month, sector
    ORDER BY month, sector

- question: Which states consume the most electricity?
  sql: |
    SELECT state,
           ROUND(SUM(consumption_mwh), 0) AS total_mwh
    FROM hourly_consumption
    GROUP BY state
    ORDER BY total_mwh DESC
    LIMIT 15

- question: Hourly consumption pattern for residential heating in New York, January 2038
  sql: |
    SELECT EXTRACT(HOUR FROM timestamp_utc) AS hour_of_day,
           ROUND(AVG(consumption_mwh), 3) AS avg_mwh
    FROM hourly_consumption
    WHERE state = 'NY'
      AND sector = 'residential'
      AND end_use = 'heating'
      AND timestamp_utc >= '2038-01-01'
      AND timestamp_utc < '2038-02-01'
    GROUP BY hour_of_day
    ORDER BY hour_of_day

- question: Compare EV charging across the top 5 states
  sql: |
    SELECT state,
           ROUND(SUM(consumption_mwh), 0) AS ev_mwh
    FROM hourly_consumption
    WHERE end_use = 'ev_charging'
    GROUP BY state
    ORDER BY ev_mwh DESC
    LIMIT 5

- question: Hour counts by state and month for 2039 — which have anomalies?
  sql: |
    SELECT state,
           DATE_TRUNC('month', timestamp_utc) AS month,
           COUNT(*) AS hours,
           COUNT(DISTINCT end_use) AS end_uses
    FROM hourly_consumption
    WHERE EXTRACT(YEAR FROM timestamp_utc) = 2039
    GROUP BY state, month
    HAVING COUNT(*) != (
        SELECT COUNT(DISTINCT end_use) FROM hourly_consumption
    ) * EXTRACT(DAY FROM (month + INTERVAL '1 MONTH' - month)) * 24
    ORDER BY state, month
"""

TIME_SERIES_YAML = """\
# datasight time series declarations
# Declares the temporal structure so quality checks can detect gaps and duplicates.

- table: hourly_consumption
  timestamp_column: timestamp_utc
  frequency: PT1H
  group_columns: [state, sector, end_use]
  time_zone: UTC
"""

MEASURES_YAML = """\
- table: hourly_consumption
  column: consumption_mwh
  role: measure
  unit: MWh
  default_aggregation: sum
  description: Hourly electricity consumption
  display_name: Consumption
  additive_across_time: true
  additive_across_category: true
"""


def write_time_validation_project_files(dest_dir: Path, db_path: Path) -> None:
    """Write project configuration files for the time-validation demo."""
    schema_path = dest_dir / "schema_description.md"
    schema_path.write_text(SCHEMA_DESCRIPTION, encoding="utf-8")
    logger.info(f"  Created {schema_path.name}")

    queries_path = dest_dir / "queries.yaml"
    queries_path.write_text(EXAMPLE_QUERIES, encoding="utf-8")
    logger.info(f"  Created {queries_path.name}")

    ts_path = dest_dir / "time_series.yaml"
    ts_path.write_text(TIME_SERIES_YAML, encoding="utf-8")
    logger.info(f"  Created {ts_path.name}")

    measures_path = dest_dir / "measures.yaml"
    measures_path.write_text(MEASURES_YAML, encoding="utf-8")
    logger.info(f"  Created {measures_path.name}")

    env_path = dest_dir / ".env"
    db_path_line = f"DB_PATH={db_path}\n"
    if not env_path.exists():
        env_path.write_text(
            "# datasight time-validation demo project\nDB_MODE=duckdb\n" + db_path_line,
            encoding="utf-8",
        )
        logger.info(f"  Created {env_path.name}")
    else:
        content = env_path.read_text(encoding="utf-8")
        if "DB_PATH" not in content:
            with open(env_path, "a", encoding="utf-8") as f:
                f.write(
                    f"\n# Added by datasight demo time-validation\nDB_MODE=duckdb\n{db_path_line}"
                )
            logger.info(f"  Updated {env_path.name} — added DB_PATH")
        else:
            logger.warning(
                f"  {env_path.name} already has DB_PATH set. Make sure it points to: {db_path}"
            )
