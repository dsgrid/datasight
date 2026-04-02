"""
Configuration helpers for datasight.

Loads schema descriptions, example queries, and creates SQL runners
from environment settings.
"""

import os
from typing import Any

import yaml
from loguru import logger

from datasight.runner import DuckDBRunner, FlightSqlRunner


def create_sql_runner(
    db_mode: str,
    db_path: str = "",
    flight_uri: str = "grpc://localhost:31337",
    flight_token: str | None = None,
    flight_username: str | None = None,
    flight_password: str | None = None,
):
    """Create the appropriate SqlRunner based on db_mode."""
    if db_mode == "flightsql":
        logger.info(f"Connecting to Flight SQL server: {flight_uri}")
        return FlightSqlRunner(
            uri=flight_uri,
            token=flight_token,
            username=flight_username,
            password=flight_password,
        )
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
