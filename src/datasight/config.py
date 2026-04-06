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
from datasight.runner import DuckDBRunner, FlightSqlRunner, PostgresRunner, SQLiteRunner, SqlRunner
from datasight.settings import DatabaseSettings


def normalize_db_mode(db_mode: str) -> str:
    """Normalize db_mode, accepting 'local' as an alias for 'duckdb'."""
    return "duckdb" if db_mode == "local" else db_mode


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
    settings: DatabaseSettings, project_dir: str = ""
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

    return create_sql_runner(
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
        data = yaml.safe_load(f)
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
