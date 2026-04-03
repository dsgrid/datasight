"""
SQL runners for datasight.

Provides a common async interface for executing SQL queries against local
DuckDB files or remote Flight SQL servers.
"""

from typing import Protocol

import duckdb
import pandas as pd
from loguru import logger


class SqlRunner(Protocol):
    """Protocol for SQL execution backends."""

    async def run_sql(self, sql: str) -> pd.DataFrame: ...


class DuckDBRunner:
    """Execute SQL against a local DuckDB file."""

    def __init__(self, database_path: str):
        self._conn = duckdb.connect(database_path, read_only=True)
        logger.info(f"Connected to DuckDB: {database_path}")

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    async def run_sql(self, sql: str) -> pd.DataFrame:
        assert self._conn is not None, "DuckDBRunner is closed"
        return self._conn.execute(sql).fetchdf()


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

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def _get_conn(self):
        if self._conn is None:
            import adbc_driver_flightsql.dbapi as flightsql

            db_kwargs = {}
            if self.username:
                db_kwargs["username"] = self.username
            if self.password:
                db_kwargs["password"] = self.password
            if self.token:
                db_kwargs["authorization"] = f"Bearer {self.token}"

            self._conn = flightsql.connect(uri=self.uri, db_kwargs=db_kwargs)
            logger.info("Connected to Flight SQL server via ADBC")
        return self._conn

    def get_table_names(self) -> list[str]:
        """Use the ADBC GetObjects RPC to list tables."""
        conn = self._get_conn()
        objects = conn.adbc_get_objects().read_all()
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

    async def run_sql(self, sql: str) -> pd.DataFrame:
        sql = sql.strip().rstrip(";")
        logger.info(f"Flight SQL query: {sql[:200]}")

        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute(sql)
        table = cursor.fetch_arrow_table()
        df = table.to_pandas()
        logger.info(f"Flight SQL returned {len(df)} rows, {len(df.columns)} columns")
        return df
