"""
Configuration helpers for datasight.

Loads schema descriptions, example queries, and creates SQL runners
from environment settings.
"""

import os
from typing import Any

import yaml
from loguru import logger

from datasight.runner import DuckDBRunner, FlightSqlRunner, PostgresRunner, SQLiteRunner


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
):
    """Create the appropriate SqlRunner based on db_mode."""
    db_mode = normalize_db_mode(db_mode)
    if db_mode == "flightsql":
        logger.info(f"Connecting to Flight SQL server: {flight_uri}")
        return FlightSqlRunner(
            uri=flight_uri,
            token=flight_token,
            username=flight_username,
            password=flight_password,
        )
    elif db_mode == "postgres":
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
    elif db_mode == "sqlite":
        logger.info(f"Opening SQLite: {db_path}")
        return SQLiteRunner(database_path=db_path)
    else:
        logger.info(f"Opening local DuckDB: {db_path}")
        return DuckDBRunner(database_path=db_path)


def load_schema_description(path: str | None, project_dir: str) -> str | None:
    """Load user-provided schema description markdown."""
    if not path:
        default = os.path.join(project_dir, "schema_description.md")
        if os.path.exists(default):
            path = default
        else:
            return None
    if not os.path.exists(path):
        logger.warning(f"Schema description not found: {path}")
        return None
    with open(path, "r") as f:
        return f.read()


def load_example_queries(path: str | None, project_dir: str) -> list[dict[str, str]]:
    """Load example question/SQL pairs from YAML file."""
    if not path:
        default = os.path.join(project_dir, "queries.yaml")
        if os.path.exists(default):
            path = default
        else:
            return []
    if not os.path.exists(path):
        logger.warning(f"Example queries file not found: {path}")
        return []
    with open(path, "r") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, list):
        logger.warning(f"Expected a list in {path}, got {type(data).__name__}")
        return []
    valid = []
    for entry in data:
        if isinstance(entry, dict) and "question" in entry and "sql" in entry:
            item: dict[str, Any] = {"question": entry["question"], "sql": entry["sql"].strip()}
            if "expected" in entry:
                item["expected"] = entry["expected"]
            valid.append(item)
    return valid


def format_example_queries(queries: list[dict[str, str]]) -> str:
    """Format example queries as a system prompt section."""
    if not queries:
        return ""
    parts = ["\n### Example Queries\n"]
    parts.append("Use these as reference for writing correct SQL for this database:\n")
    for i, q in enumerate(queries, 1):
        parts.append(f"**Q{i}: {q['question']}**")
        parts.append(f"```sql\n{q['sql']}\n```\n")
    return "\n".join(parts)
