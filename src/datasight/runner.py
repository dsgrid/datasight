"""
SQL runners for datasight.

Provides a common async interface for executing SQL queries against local
DuckDB files, remote Flight SQL servers, PostgreSQL, SQLite databases, or
Apache Spark via Spark Connect.
"""

from __future__ import annotations

import asyncio
import sqlite3
import uuid
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

import duckdb
import pandas as pd
from loguru import logger

from datasight.exceptions import ConnectionError, QueryError, QueryTimeoutError

# Default timeout for SQL queries (seconds). Can be overridden per-runner.
DEFAULT_QUERY_TIMEOUT: float = 120.0

# Default SQL result cache size (1 GiB). 0 disables caching.
DEFAULT_SQL_CACHE_MAX_BYTES: int = 1 << 30

# Default byte cap for Spark Connect results (100 MiB on the wire).
# Spark sits in front of multi-TB datasets, so the client cannot afford to
# materialize an unbounded result. This protects the FastAPI process from
# OOMing when the agent forgets to aggregate.
DEFAULT_SPARK_MAX_RESULT_BYTES: int = 100 * 1024 * 1024

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

    def __init__(self, database_path: str, query_timeout: float = DEFAULT_QUERY_TIMEOUT):
        self._database_path = database_path
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._query_timeout = query_timeout
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
        """Execute SQL asynchronously with timeout."""
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(self._execute, sql),
                timeout=self._query_timeout,
            )
        except TimeoutError:
            raise QueryTimeoutError(
                f"Query timed out after {self._query_timeout:.0f}s. "
                "Try a simpler query or add filters to reduce the result set."
            )


class EphemeralDuckDBRunner:
    """Execute SQL against an in-memory DuckDB connection.

    Used for ephemeral "explore" sessions where users want to quickly
    analyze CSV/Parquet files without setting up a project.
    """

    def __init__(
        self, conn: duckdb.DuckDBPyConnection, query_timeout: float = DEFAULT_QUERY_TIMEOUT
    ):
        """Initialize with an existing DuckDB connection.

        Parameters
        ----------
        conn:
            An existing DuckDB connection (typically in-memory).
        query_timeout:
            Maximum seconds to wait for a query to complete.
        """
        self._conn = conn
        self._query_timeout = query_timeout

    def close(self) -> None:
        """Close the database connection."""
        if self._conn:
            try:
                self._conn.close()
            except duckdb.Error:
                pass
            self._conn = None

    def __enter__(self) -> "EphemeralDuckDBRunner":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    async def __aenter__(self) -> "EphemeralDuckDBRunner":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def _execute(self, sql: str) -> pd.DataFrame:
        """Execute SQL synchronously."""
        if self._conn is None:
            raise ConnectionError("EphemeralDuckDBRunner is closed")
        try:
            return self._conn.execute(sql).fetchdf()
        except duckdb.Error as e:
            logger.debug(f"DuckDB query error: {e}\nSQL: {sql[:500]}")
            raise QueryError(str(e)) from e

    async def run_sql(self, sql: str) -> pd.DataFrame:
        """Execute SQL asynchronously with timeout."""
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(self._execute, sql),
                timeout=self._query_timeout,
            )
        except TimeoutError:
            raise QueryTimeoutError(
                f"Query timed out after {self._query_timeout:.0f}s. "
                "Try a simpler query or add filters to reduce the result set."
            )


class SQLiteRunner:
    """Execute SQL against a local SQLite file."""

    def __init__(self, database_path: str, query_timeout: float = DEFAULT_QUERY_TIMEOUT):
        self._database_path = database_path
        self._conn: sqlite3.Connection | None = None
        self._query_timeout = query_timeout
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
        """Execute SQL asynchronously with timeout."""
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(self._execute, sql),
                timeout=self._query_timeout,
            )
        except TimeoutError:
            raise QueryTimeoutError(
                f"Query timed out after {self._query_timeout:.0f}s. "
                "Try a simpler query or add filters to reduce the result set."
            )


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
        query_timeout: float = DEFAULT_QUERY_TIMEOUT,
    ):
        import psycopg

        self._conn = None
        self._psycopg = psycopg
        self._query_timeout = query_timeout
        self._connection_info = f"{host}:{port}/{dbname}" if not url else "via URL"
        self.mixed_case_identifiers: dict[str, str] | None = None

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
        if self.mixed_case_identifiers:
            from datasight.identifiers import quote_mixed_case_identifiers

            sql = quote_mixed_case_identifiers(sql, self.mixed_case_identifiers)
        try:
            cursor = self._conn.execute(sql)  # ty: ignore[no-matching-overload]
            rows = cursor.fetchall()
            if not rows:
                cols = [desc[0] for desc in cursor.description] if cursor.description else []
                return pd.DataFrame(columns=cols)
            cols = [desc[0] for desc in cursor.description]
            return pd.DataFrame(rows, columns=cols)
        except self._psycopg.Error as e:
            logger.debug(f"PostgreSQL query error: {e}\nSQL: {sql[:500]}")
            raise QueryError(str(e)) from e

    async def run_sql(self, sql: str) -> pd.DataFrame:
        """Execute SQL asynchronously with timeout."""
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(self._execute, sql),
                timeout=self._query_timeout,
            )
        except TimeoutError:
            raise QueryTimeoutError(
                f"Query timed out after {self._query_timeout:.0f}s. "
                "Try a simpler query or add filters to reduce the result set."
            )


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
            logger.exception("Flight SQL connection error")
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
        """Execute SQL asynchronously with timeout."""
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(self._execute, sql),
                timeout=self.timeout,
            )
        except TimeoutError:
            raise QueryTimeoutError(
                f"Query timed out after {self.timeout:.0f}s. "
                "Try a simpler query or add filters to reduce the result set."
            )


class SparkConnectRunner:
    """Execute SQL against an Apache Spark cluster via Spark Connect.

    Streams Arrow batches from the server and stops once the accumulated
    result exceeds ``max_result_bytes``. The returned DataFrame carries
    ``df.attrs['truncated'] = True`` and ``df.attrs['truncation_reason']``
    when truncation occurred so the UI can surface that to the user.

    Cancellation on timeout uses Spark Connect's tag-based interrupt API
    (``addTag`` / ``interruptTag``), which actually frees cluster resources
    rather than just abandoning the gRPC stream client-side.
    """

    def __init__(
        self,
        remote: str = "sc://localhost:15002",
        *,
        token: str | None = None,
        max_result_bytes: int = DEFAULT_SPARK_MAX_RESULT_BYTES,
        query_timeout: float = DEFAULT_QUERY_TIMEOUT,
        spark: Any = None,
    ):
        self._remote = remote
        self._token = token
        self._max_result_bytes = max(0, int(max_result_bytes))
        self._query_timeout = query_timeout
        self._spark: Any = spark
        if self._spark is None:
            self._connect()

    def _connect(self) -> None:
        try:
            from pyspark.sql import SparkSession  # ty: ignore[unresolved-import]
        except ImportError as e:
            raise ConnectionError(
                "Spark Connect support requires pyspark. "
                "Install with: pip install 'datasight[spark]'"
            ) from e
        try:
            builder = SparkSession.builder.remote(self._remote)
            if self._token:
                builder = builder.config("spark.connect.client.token", self._token)
            self._spark = builder.getOrCreate()
            logger.info(f"Connected to Spark Connect: {self._remote}")
        except Exception as e:
            logger.exception("Spark Connect connection error")
            raise ConnectionError(f"Failed to connect to Spark Connect: {e}") from e

    def close(self) -> None:
        if self._spark is not None:
            try:
                self._spark.stop()
            except Exception:
                pass
            self._spark = None

    def __enter__(self) -> "SparkConnectRunner":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    async def __aenter__(self) -> "SparkConnectRunner":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    @staticmethod
    def _iter_arrow_batches(spark: Any, sdf: Any):
        """Yield ``pyarrow.RecordBatch`` from a Spark DataFrame.

        Prefers the Spark Connect client's streaming iterator so we can stop
        before materializing the full result. Falls back to ``toArrow()`` /
        ``toPandas()`` if the streaming API isn't exposed.
        """
        import pyarrow as pa

        client = getattr(spark, "client", None)
        plan = getattr(sdf, "_plan", None)
        if client is not None and plan is not None and hasattr(client, "to_table_as_iterator"):
            for table, _ in client.to_table_as_iterator(plan, observations={}):
                for batch in table.to_batches():
                    yield batch
            return
        if hasattr(sdf, "toArrow"):
            table = sdf.toArrow()
            for batch in table.to_batches():
                yield batch
            return
        df = sdf.toPandas()
        yield pa.RecordBatch.from_pandas(df)

    def _execute(self, sql: str) -> pd.DataFrame:
        if self._spark is None:
            raise ConnectionError("SparkConnectRunner is closed")
        import pyarrow as pa

        sql = sql.strip().rstrip(";")
        logger.info(f"Spark SQL: {sql[:200]}")
        try:
            sdf = self._spark.sql(sql)
            iterator = self._iter_arrow_batches(self._spark, sdf)
            batches: list[pa.RecordBatch] = []
            schema: pa.Schema | None = None
            total_bytes = 0
            truncated = False
            try:
                for batch in iterator:
                    if schema is None:
                        schema = batch.schema
                    batches.append(batch)
                    total_bytes += batch.nbytes
                    if total_bytes > self._max_result_bytes:
                        truncated = True
                        break
            finally:
                close = getattr(iterator, "close", None)
                if close is not None:
                    try:
                        close()
                    except Exception:
                        pass
        except Exception as e:
            logger.debug(f"Spark SQL error: {e}\nSQL: {sql[:500]}")
            raise QueryError(str(e)) from e

        if not batches:
            return pd.DataFrame()
        table = pa.Table.from_batches(batches, schema=schema)
        df = table.to_pandas()
        if truncated:
            df.attrs["truncated"] = True
            df.attrs["truncation_reason"] = (
                f"Result exceeded {self._max_result_bytes:,} bytes "
                f"({total_bytes:,} bytes streamed). Add aggregation or a tighter "
                "LIMIT to your question to see the full answer."
            )
            logger.warning(
                f"Spark result truncated at {total_bytes:,} bytes ({len(df):,} rows returned)"
            )
        else:
            logger.info(f"Spark returned {len(df)} rows, {len(df.columns)} columns")
        return df

    async def run_sql(self, sql: str) -> pd.DataFrame:
        if self._spark is None:
            raise ConnectionError("SparkConnectRunner is closed")

        # Tag every query so we can interrupt server-side on timeout. Without
        # this, asyncio.wait_for cancels the local task but the gRPC call and
        # underlying Spark job keep running and burning cluster resources.
        tag = f"datasight-{uuid.uuid4().hex}"
        spark = self._spark
        add_tag = getattr(spark, "addTag", None)
        remove_tag = getattr(spark, "removeTag", None)
        interrupt_tag = getattr(spark, "interruptTag", None)

        def _exec() -> pd.DataFrame:
            if add_tag is not None:
                try:
                    add_tag(tag)
                except Exception:
                    pass
            try:
                return self._execute(sql)
            finally:
                if remove_tag is not None:
                    try:
                        remove_tag(tag)
                    except Exception:
                        pass

        try:
            return await asyncio.wait_for(
                asyncio.to_thread(_exec),
                timeout=self._query_timeout,
            )
        except TimeoutError:
            if interrupt_tag is not None:
                try:
                    interrupt_tag(tag)
                except Exception:
                    logger.warning("Failed to interrupt Spark query on timeout")
            raise QueryTimeoutError(
                f"Query timed out after {self._query_timeout:.0f}s. "
                "Try a simpler query or add filters to reduce the result set."
            )


class CachingSqlRunner:
    """Wraps a SqlRunner with a byte-bounded LRU cache of SQL-result DataFrames.

    Keyed on normalized SQL text. Cache is cleared via `clear_cache()` on
    schema reload. Results larger than `max_bytes` are not cached.
    """

    def __init__(self, inner: SqlRunner, max_bytes: int = DEFAULT_SQL_CACHE_MAX_BYTES):
        self._inner = inner
        self._max_bytes = max(0, int(max_bytes))
        self._cache: OrderedDict[str, tuple[pd.DataFrame, int]] = OrderedDict()
        self._total_bytes = 0
        self._lock = asyncio.Lock()
        self.hits = 0
        self.misses = 0

    @staticmethod
    def _normalize(sql: str) -> str:
        collapsed = " ".join(sql.split())
        # Strip trailing semicolons plus any whitespace they were separated by.
        while collapsed.endswith(";") or collapsed.endswith(" "):
            collapsed = collapsed[:-1]
        return collapsed.lower()

    @staticmethod
    def _estimate_bytes(df: pd.DataFrame) -> int:
        try:
            return int(df.memory_usage(deep=True).sum())
        except Exception:
            return 0

    def clear_cache(self) -> None:
        self._cache.clear()
        self._total_bytes = 0

    @property
    def cache_bytes(self) -> int:
        return self._total_bytes

    @property
    def cache_entries(self) -> int:
        return len(self._cache)

    async def run_sql(self, sql: str) -> pd.DataFrame:
        if self._max_bytes == 0:
            return await self._inner.run_sql(sql)

        key = self._normalize(sql)
        async with self._lock:
            entry = self._cache.get(key)
            if entry is not None:
                self._cache.move_to_end(key)
                self.hits += 1
                logger.debug(f"[sql-cache] HIT ({len(entry[0])} rows, {entry[1]} bytes)")
                return entry[0].copy()

        # Miss: execute outside the lock to avoid serializing all queries.
        df = await self._inner.run_sql(sql)
        size = self._estimate_bytes(df)

        async with self._lock:
            self.misses += 1
            if size == 0 or size > self._max_bytes:
                logger.debug(f"[sql-cache] SKIP (size={size}, max={self._max_bytes})")
                return df
            # Overwrite any existing entry for this key.
            existing = self._cache.pop(key, None)
            if existing is not None:
                self._total_bytes -= existing[1]
            self._cache[key] = (df.copy(), size)
            self._total_bytes += size
            # Evict LRU entries until within budget.
            while self._total_bytes > self._max_bytes and self._cache:
                _, (_, evicted_size) = self._cache.popitem(last=False)
                self._total_bytes -= evicted_size
            logger.debug(
                f"[sql-cache] STORED ({size} bytes, total={self._total_bytes}, "
                f"entries={len(self._cache)})"
            )
        return df.copy()

    def close(self) -> None:
        self.clear_cache()
        close = getattr(self._inner, "close", None)
        if close is not None:
            close()

    async def __aenter__(self) -> "CachingSqlRunner":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
