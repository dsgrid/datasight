"""
SQL runners for datasight.

Provides a common async interface for executing SQL queries against local
DuckDB files, remote Flight SQL servers, PostgreSQL, or SQLite databases.
"""

from __future__ import annotations

import asyncio
import sqlite3
import traceback
from collections.abc import Awaitable, Callable
from typing import Protocol

import duckdb
import pandas as pd
from loguru import logger

from datasight.exceptions import ConnectionError, QueryError

# Type alias for async SQL execution function
RunSql = Callable[[str], Awaitable[pd.DataFrame]]


class SqlRunner(Protocol):
    """Protocol for SQL execution backends."""

    async def run_sql(self, sql: str) -> pd.DataFrame:
        """Execute a SQL query and return results as a DataFrame."""
        ...

    def close(self) -> None:
        """Close the database connection."""
        ...

    async def __aenter__(self) -> "SqlRunner":
        """Enter async context manager."""
        ...

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit async context manager."""
        ...


class DuckDBRunner:
    """Execute SQL against a local DuckDB file."""

    def __init__(self, database_path: str):
        self._database_path = database_path
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._connect()

    def _connect(self) -> None:
        """Establish connection to DuckDB."""
        try:
            self._conn = duckdb.connect(self._database_path, read_only=True)
            logger.info(f"Connected to DuckDB: {self._database_path}")
        except duckdb.Error as e:
            raise ConnectionError(f"Failed to connect to DuckDB: {e}") from e

    def close(self) -> None:
        """Close the database connection."""
        if self._conn:
            try:
                self._conn.close()
            except duckdb.Error:
                pass  # Ignore errors on close
            self._conn = None

    def __enter__(self) -> "DuckDBRunner":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    async def __aenter__(self) -> "DuckDBRunner":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def _execute(self, sql: str) -> pd.DataFrame:
        """Execute SQL synchronously."""
        if self._conn is None:
            raise ConnectionError("DuckDBRunner is closed")
        try:
            return self._conn.execute(sql).fetchdf()
        except duckdb.Error as e:
            logger.debug(f"DuckDB query error: {e}\nSQL: {sql[:500]}")
            raise QueryError(str(e)) from e

    async def run_sql(self, sql: str) -> pd.DataFrame:
        """Execute SQL asynchronously."""
        return await asyncio.to_thread(self._execute, sql)


class SQLiteRunner:
    """Execute SQL against a local SQLite file."""

    def __init__(self, database_path: str):
        self._database_path = database_path
        self._conn: sqlite3.Connection | None = None
        self._connect()

    def _connect(self) -> None:
        """Establish connection to SQLite."""
        try:
            self._conn = sqlite3.connect(self._database_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            logger.info(f"Connected to SQLite: {self._database_path}")
        except sqlite3.Error as e:
            raise ConnectionError(f"Failed to connect to SQLite: {e}") from e

    def close(self) -> None:
        """Close the database connection."""
        if self._conn:
            try:
                self._conn.close()
            except sqlite3.Error:
                pass  # Ignore errors on close
            self._conn = None

    def __enter__(self) -> "SQLiteRunner":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    async def __aenter__(self) -> "SQLiteRunner":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def _execute(self, sql: str) -> pd.DataFrame:
        """Execute SQL synchronously."""
        if self._conn is None:
            raise ConnectionError("SQLiteRunner is closed")
        try:
            cursor = self._conn.execute(sql)
            rows = cursor.fetchall()
            if not rows:
                cols = [desc[0] for desc in cursor.description] if cursor.description else []
                return pd.DataFrame(columns=cols)
            cols = [desc[0] for desc in cursor.description]
            return pd.DataFrame(rows, columns=cols)
        except sqlite3.Error as e:
            logger.debug(f"SQLite query error: {e}\nSQL: {sql[:500]}")
            raise QueryError(str(e)) from e

    async def run_sql(self, sql: str) -> pd.DataFrame:
        """Execute SQL asynchronously."""
        return await asyncio.to_thread(self._execute, sql)


class PostgresRunner:
    """Execute SQL against a PostgreSQL database using psycopg."""

    def __init__(
        self,
        *,
        host: str = "localhost",
        port: int = 5432,
        dbname: str = "",
        user: str = "",
        password: str = "",
        url: str = "",
        sslmode: str = "prefer",
    ):
        self._conn = None
        self._psycopg = None  # Store module reference for exception handling
        self._connection_info = f"{host}:{port}/{dbname}" if not url else "via URL"

        try:
            import psycopg  # ty: ignore[unresolved-import]

            self._psycopg = psycopg
        except ImportError as e:
            raise ImportError(
                "The 'psycopg' package is required for PostgreSQL support. "
                "Install it with: pip install 'datasight[postgres]'"
            ) from e

        try:
            if url:
                self._conn = psycopg.connect(url, autocommit=True)
                logger.info("Connected to PostgreSQL via URL")
            else:
                self._conn = psycopg.connect(
                    host=host,
                    port=port,
                    dbname=dbname,
                    user=user,
                    password=password,
                    sslmode=sslmode,
                    autocommit=True,
                )
                logger.info(f"Connected to PostgreSQL: {host}:{port}/{dbname}")
        except psycopg.Error as e:
            raise ConnectionError(f"Failed to connect to PostgreSQL: {e}") from e

    def close(self) -> None:
        """Close the database connection."""
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass  # Ignore errors on close
            self._conn = None

    def __enter__(self) -> "PostgresRunner":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    async def __aenter__(self) -> "PostgresRunner":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def _execute(self, sql: str) -> pd.DataFrame:
        """Execute SQL synchronously."""
        if self._conn is None:
            raise ConnectionError("PostgresRunner is closed")
        try:
            cursor = self._conn.execute(sql)
            rows = cursor.fetchall()
            if not rows:
                cols = [desc[0] for desc in cursor.description] if cursor.description else []
                return pd.DataFrame(columns=cols)
            cols = [desc[0] for desc in cursor.description]
            return pd.DataFrame(rows, columns=cols)
        except self._psycopg.Error as e:  # ty: ignore[unresolved-attribute]
            logger.debug(f"PostgreSQL query error: {e}\nSQL: {sql[:500]}")
            raise QueryError(str(e)) from e

    async def run_sql(self, sql: str) -> pd.DataFrame:
        """Execute SQL asynchronously."""
        return await asyncio.to_thread(self._execute, sql)


class FlightSqlRunner:
    """Execute SQL against a remote Flight SQL server via ADBC."""

    def __init__(
        self,
        uri: str = "grpc://localhost:31337",
        token: str | None = None,
        username: str | None = None,
        password: str | None = None,
        timeout: float = 30.0,
    ):
        self.uri = uri
        self.token = token
        self.username = username
        self.password = password
        self.timeout = timeout
        self._conn = None
        self._connect()

    def _connect(self) -> None:
        """Establish connection to the Flight SQL server."""
        try:
            import adbc_driver_flightsql.dbapi as flightsql

            db_kwargs = {}
            if self.username:
                db_kwargs["username"] = self.username
            if self.password:
                db_kwargs["password"] = self.password
            if self.token:
                db_kwargs["authorization"] = f"Bearer {self.token}"

            self._conn = flightsql.connect(uri=self.uri, db_kwargs=db_kwargs)
            logger.info(f"Connected to Flight SQL server: {self.uri}")
        except Exception as e:
            logger.error(f"Flight SQL connection error:\n{traceback.format_exc()}")
            raise ConnectionError(f"Failed to connect to Flight SQL server: {e}") from e

    def close(self) -> None:
        """Close the database connection."""
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass  # Ignore errors on close
            self._conn = None

    def __enter__(self) -> "FlightSqlRunner":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    async def __aenter__(self) -> "FlightSqlRunner":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def get_table_names(self) -> list[str]:
        """Use the ADBC GetObjects RPC to list tables."""
        if self._conn is None:
            raise ConnectionError("FlightSqlRunner is closed")
        try:
            objects = self._conn.adbc_get_objects().read_all()
            names = []
            for catalog_batch in objects.column("catalog_db_schemas"):
                if catalog_batch is None:
                    continue
                for schema_entry in catalog_batch:
                    tables = schema_entry["db_schema_tables"]
                    if tables is None:
                        continue
                    for table_entry in tables:
                        names.append(table_entry["table_name"].as_py())
            return names
        except Exception as e:
            logger.debug(f"Flight SQL GetObjects error: {e}")
            raise QueryError(f"Failed to get table names: {e}") from e

    def _execute(self, sql: str) -> pd.DataFrame:
        """Execute SQL synchronously."""
        if self._conn is None:
            raise ConnectionError("FlightSqlRunner is closed")
        sql = sql.strip().rstrip(";")
        logger.info(f"Flight SQL query: {sql[:200]}")
        try:
            cursor = self._conn.cursor()
            cursor.execute(sql)
            table = cursor.fetch_arrow_table()
            df = table.to_pandas()
            logger.info(f"Flight SQL returned {len(df)} rows, {len(df.columns)} columns")
            return df
        except Exception as e:
            logger.debug(f"Flight SQL query error: {e}\nSQL: {sql[:500]}")
            raise QueryError(str(e)) from e

    async def run_sql(self, sql: str) -> pd.DataFrame:
        """Execute SQL asynchronously."""
        return await asyncio.to_thread(self._execute, sql)
