"""
Configuration helpers for datasight.

Loads schema descriptions, example queries, and creates SQL runners
from environment settings.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from loguru import logger

from datasight.exceptions import ConfigurationError
from datasight.runner import (
    CachingSqlRunner,
    DuckDBRunner,
    FlightSqlRunner,
    PostgresRunner,
    SQLiteRunner,
    SqlRunner,
)
from datasight.settings import DatabaseSettings


def normalize_db_mode(db_mode: str) -> str:
    """Normalize db_mode, accepting 'local' as an alias for 'duckdb'."""
    return "duckdb" if db_mode == "local" else db_mode


def set_env_vars(env_path: Path | str, updates: dict[str, str]) -> None:
    """Create-or-update an ``.env`` file, setting the given ``KEY=value`` pairs.

    Existing lines that match a key (commented or uncommented) are replaced
    in place. Keys not already present are appended. When ``env_path`` does
    not exist, a new file is created from the bundled env template so the
    user's LLM/API placeholders are preserved.
    """
    import re

    env_path = Path(env_path)
    if env_path.exists():
        text = env_path.read_text(encoding="utf-8")
    else:
        template_path = Path(__file__).parent / "templates" / "env.template"
        text = template_path.read_text(encoding="utf-8") if template_path.exists() else ""

    lines = text.splitlines()
    remaining = dict(updates)
    pattern = re.compile(r"^\s*#?\s*([A-Z_][A-Z0-9_]*)\s*=")
    out: list[str] = []
    for line in lines:
        match = pattern.match(line)
        key = match.group(1) if match else None
        if key and key in remaining:
            out.append(f"{key}={remaining.pop(key)}")
        else:
            out.append(line)
    if remaining:
        if out and out[-1].strip():
            out.append("")
        for key, value in remaining.items():
            out.append(f"{key}={value}")

    env_path.parent.mkdir(parents=True, exist_ok=True)
    env_path.write_text("\n".join(out) + "\n", encoding="utf-8")


def create_sql_runner(
    db_mode: str,
    db_path: str = "",
    flight_uri: str = "grpc://localhost:31337",
    flight_token: str | None = None,
    flight_username: str | None = None,
    flight_password: str | None = None,
    postgres_host: str = "localhost",
    postgres_port: int = 5432,
    postgres_database: str = "",
    postgres_user: str = "",
    postgres_password: str = "",
    postgres_url: str = "",
    postgres_sslmode: str = "prefer",
) -> SqlRunner:
    """Create the appropriate SqlRunner based on db_mode.

    Parameters
    ----------
    db_mode:
        Database mode: "duckdb", "sqlite", "postgres", or "flightsql".
    db_path:
        Path to database file (for duckdb and sqlite modes).
    flight_*:
        Flight SQL connection parameters.
    postgres_*:
        PostgreSQL connection parameters.

    Returns
    -------
    A SqlRunner instance for the specified database.

    Raises
    ------
    ConfigurationError:
        If the database mode is invalid or required parameters are missing.
    """
    db_mode = normalize_db_mode(db_mode)

    match db_mode:
        case "flightsql":
            logger.info(f"Connecting to Flight SQL server: {flight_uri}")
            return FlightSqlRunner(
                uri=flight_uri,
                token=flight_token,
                username=flight_username,
                password=flight_password,
            )
        case "postgres":
            logger.info("Connecting to PostgreSQL")
            return PostgresRunner(
                host=postgres_host,
                port=postgres_port,
                dbname=postgres_database,
                user=postgres_user,
                password=postgres_password,
                url=postgres_url,
                sslmode=postgres_sslmode,
            )
        case "sqlite":
            if not db_path:
                raise ConfigurationError("DB_PATH is required for SQLite mode")
            logger.info(f"Opening SQLite: {db_path}")
            return SQLiteRunner(database_path=db_path)
        case "duckdb":
            if not db_path:
                raise ConfigurationError("DB_PATH is required for DuckDB mode")
            logger.info(f"Opening local DuckDB: {db_path}")
            return DuckDBRunner(database_path=db_path)
        case _:
            raise ConfigurationError(f"Invalid database mode: {db_mode}")


def create_sql_runner_from_settings(
    settings: DatabaseSettings,
    project_dir: str = "",
    *,
    sql_cache_max_bytes: int = 1 << 30,
) -> SqlRunner:
    """Create a SqlRunner from DatabaseSettings.

    Parameters
    ----------
    settings:
        Database settings.
    project_dir:
        Optional project directory for resolving relative paths.

    Returns
    -------
    A SqlRunner instance.
    """
    db_path = settings.path
    if settings.mode in ("duckdb", "sqlite") and db_path and not os.path.isabs(db_path):
        if project_dir:
            db_path = str(Path(project_dir) / db_path)

    runner = create_sql_runner(
        db_mode=settings.mode,
        db_path=db_path,
        flight_uri=settings.flight_uri,
        flight_token=settings.flight_token,
        flight_username=settings.flight_username,
        flight_password=settings.flight_password,
        postgres_host=settings.postgres_host,
        postgres_port=settings.postgres_port,
        postgres_database=settings.postgres_database,
        postgres_user=settings.postgres_user,
        postgres_password=settings.postgres_password,
        postgres_url=settings.postgres_url,
        postgres_sslmode=settings.postgres_sslmode,
    )
    if sql_cache_max_bytes > 0:
        runner = CachingSqlRunner(runner, max_bytes=sql_cache_max_bytes)
    return runner


def load_schema_description(path: str | None, project_dir: str) -> str | None:
    """Load user-provided schema description markdown.

    Parameters
    ----------
    path:
        Explicit path to schema description file.
    project_dir:
        Project directory to look for default file.

    Returns
    -------
    Schema description content, or None if not found.
    """
    if not path:
        default = os.path.join(project_dir, "schema_description.md")
        if os.path.exists(default):
            path = default
        else:
            return None
    if not os.path.exists(path):
        logger.warning(f"Schema description not found: {path}")
        return None
    with open(path, encoding="utf-8") as f:
        return f.read()


def load_example_queries(path: str | None, project_dir: str) -> list[dict[str, Any]]:
    """Load example question/SQL pairs from YAML file.

    Parameters
    ----------
    path:
        Explicit path to queries file.
    project_dir:
        Project directory to look for default file.

    Returns
    -------
    List of query dicts with 'question', 'sql', and optional 'expected' keys.
    """
    if not path:
        default = os.path.join(project_dir, "queries.yaml")
        if os.path.exists(default):
            path = default
        else:
            return []
    if not os.path.exists(path):
        logger.warning(f"Example queries file not found: {path}")
        return []
    with open(path, encoding="utf-8") as f:
        try:
            data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            logger.warning(f"Failed to parse {path}: {e}")
            return []
    if not isinstance(data, list):
        logger.warning(f"Expected a list in {path}, got {type(data).__name__}")
        return []
    valid: list[dict[str, Any]] = []
    for entry in data:
        if isinstance(entry, dict) and "question" in entry and "sql" in entry:
            item: dict[str, Any] = {"question": entry["question"], "sql": entry["sql"].strip()}
            if "expected" in entry:
                item["expected"] = entry["expected"]
            valid.append(item)
    return valid


def load_measure_overrides(path: str | None, project_dir: str) -> list[dict[str, Any]]:
    """Load project-level measure overrides from YAML.

    Expected format is a list of mappings with at least ``table`` plus either
    ``column`` for physical measures or ``name`` + ``expression``/``sql_expression``
    for calculated measures. Any of the semantic-measure fields may be overridden.
    """
    if not path:
        default = os.path.join(project_dir, "measures.yaml")
        if os.path.exists(default):
            path = default
        else:
            return []
    if not os.path.exists(path):
        logger.warning(f"Measure overrides file not found: {path}")
        return []
    with open(path, encoding="utf-8") as f:
        try:
            data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            logger.warning(f"Failed to parse {path}: {e}")
            return []
    if not isinstance(data, list):
        logger.warning(f"Expected a list in {path}, got {type(data).__name__}")
        return []

    valid: list[dict[str, Any]] = []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        table = str(entry.get("table") or "").strip()
        column = str(entry.get("column") or "").strip()
        name = str(entry.get("name") or "").strip()
        expression = str(entry.get("expression") or entry.get("sql_expression") or "").strip()
        if not table or (not column and not (name and expression)):
            continue
        item: dict[str, Any] = {"table": table}
        if column:
            item["column"] = column
        if name:
            item["name"] = name
        if expression:
            item["expression"] = expression
        for key in (
            "role",
            "unit",
            "default_aggregation",
            "average_strategy",
            "weight_column",
            "reason",
            "description",
            "display_name",
            "format",
        ):
            if key in entry:
                item[key] = entry[key]
        for key in ("allowed_aggregations", "forbidden_aggregations"):
            if key in entry and isinstance(entry[key], list):
                item[key] = [str(v) for v in entry[key]]
        if "preferred_chart_types" in entry:
            if isinstance(entry["preferred_chart_types"], list):
                item["preferred_chart_types"] = [str(v) for v in entry["preferred_chart_types"]]
            elif entry["preferred_chart_types"]:
                item["preferred_chart_types"] = [str(entry["preferred_chart_types"])]
        for key in ("additive_across_category", "additive_across_time"):
            if key in entry:
                item[key] = bool(entry[key])
        valid.append(item)
    return valid


def load_schema_config(path: str | None, project_dir: str) -> dict[str, Any] | None:
    """Load optional ``schema.yaml`` allowlist for tables and columns.

    Expected format::

        tables:
          - name: orders
          - name: customers
            columns: [id, email, created_at]

    Returns the parsed mapping when present, or ``None`` when the file is
    absent or unparseable. Invalid entries are dropped with a warning.
    """
    if not path:
        default = os.path.join(project_dir, "schema.yaml")
        if os.path.exists(default):
            path = default
        else:
            return None
    if not os.path.exists(path):
        logger.warning(f"Schema config not found: {path}")
        return None
    with open(path, encoding="utf-8") as f:
        try:
            data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            logger.warning(f"Failed to parse {path}: {e}")
            return None
    if not isinstance(data, dict):
        logger.warning(f"Expected a mapping in {path}, got {type(data).__name__}")
        return None

    raw_tables = data.get("tables")
    if raw_tables is None:
        return {"tables": []}
    if not isinstance(raw_tables, list):
        logger.warning(f"Expected 'tables' to be a list in {path}")
        return {"tables": []}

    cleaned: list[dict[str, Any]] = []
    for entry in raw_tables:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("name") or "").strip()
        if not name:
            continue
        item: dict[str, Any] = {"name": name}
        for key in ("columns", "excluded_columns"):
            if key not in entry:
                continue
            val = entry[key]
            if val is None:
                item[key] = []
            elif isinstance(val, list):
                item[key] = [str(c).strip() for c in val if str(c).strip()]
            else:
                logger.warning(f"schema.yaml: ignoring non-list {key!r} for table {name}")
        cleaned.append(item)
    return {"tables": cleaned}


_VALID_FREQUENCIES = {"PT1H", "PT15M", "PT30M", "P1D", "P1M"}


def load_time_series_config(path: str | None, project_dir: str) -> list[dict[str, Any]]:
    """Load time series declarations from YAML.

    Expected format is a list of mappings with at least ``table``,
    ``timestamp_column``, and ``frequency`` (ISO 8601 duration).
    Optional: ``group_columns`` (list of column names) and
    ``time_zone`` (IANA time zone string, default ``UTC``).
    """
    if not path:
        default = os.path.join(project_dir, "time_series.yaml")
        if os.path.exists(default):
            path = default
        else:
            return []
    if not os.path.exists(path):
        logger.warning(f"Time series config not found: {path}")
        return []
    with open(path, encoding="utf-8") as f:
        try:
            data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            logger.warning(f"Failed to parse {path}: {e}")
            return []
    if not isinstance(data, list):
        logger.warning(f"Expected a list in {path}, got {type(data).__name__}")
        return []

    valid: list[dict[str, Any]] = []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        table = str(entry.get("table") or "").strip()
        timestamp_column = str(entry.get("timestamp_column") or "").strip()
        frequency = str(entry.get("frequency") or "").strip().upper()
        if not table or not timestamp_column or not frequency:
            continue
        if frequency not in _VALID_FREQUENCIES:
            logger.warning(
                f"Unsupported frequency '{frequency}' for {table}.{timestamp_column} "
                f"(expected one of {', '.join(sorted(_VALID_FREQUENCIES))})"
            )
            continue
        item: dict[str, Any] = {
            "table": table,
            "timestamp_column": timestamp_column,
            "frequency": frequency,
        }
        group_columns = entry.get("group_columns")
        if isinstance(group_columns, list):
            item["group_columns"] = [str(v).strip() for v in group_columns if v]
        item["time_zone"] = str(entry.get("time_zone") or "UTC").strip()
        valid.append(item)
    return valid


def load_joins_config(path: str | None, project_dir: str) -> list[dict[str, Any]]:
    """Load declared join relationships from YAML.

    Expected format is a list of mappings with ``child_table``,
    ``child_column``, ``parent_table``, and optionally ``parent_column``
    (defaults to ``id``).
    """
    if not path:
        default = os.path.join(project_dir, "joins.yaml")
        if os.path.exists(default):
            path = default
        else:
            return []
    if not os.path.exists(path):
        logger.warning(f"Joins config not found: {path}")
        return []
    with open(path, encoding="utf-8") as f:
        try:
            data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            logger.warning(f"Failed to parse {path}: {e}")
            return []
    if not isinstance(data, list):
        logger.warning(f"Expected a list in {path}, got {type(data).__name__}")
        return []

    valid: list[dict[str, Any]] = []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        child_table = str(entry.get("child_table") or "").strip()
        child_column = str(entry.get("child_column") or "").strip()
        parent_table = str(entry.get("parent_table") or "").strip()
        if not child_table or not child_column or not parent_table:
            continue
        valid.append(
            {
                "child_table": child_table,
                "child_column": child_column,
                "parent_table": parent_table,
                "parent_column": str(entry.get("parent_column") or "id").strip(),
            }
        )
    return valid


def format_example_queries(queries: list[dict[str, str]]) -> str:
    """Format example queries as a system prompt section.

    Parameters
    ----------
    queries:
        List of query dicts with 'question' and 'sql' keys.

    Returns
    -------
    Formatted markdown string for the system prompt.
    """
    if not queries:
        return ""
    parts = ["\n### Example Queries\n"]
    parts.append("Use these as reference for writing correct SQL for this database:\n")
    for i, q in enumerate(queries, 1):
        parts.append(f"**Q{i}: {q['question']}**")
        parts.append(f"```sql\n{q['sql']}\n```\n")
    return "\n".join(parts)
