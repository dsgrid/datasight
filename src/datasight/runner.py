"""
SQL runners for datasight.

Provides a common async interface for executing SQL queries against local
DuckDB files, remote Flight SQL servers, PostgreSQL, SQLite databases, or
Apache Spark via Spark Connect.
"""

from __future__ import annotations

import asyncio
import sqlite3
import time
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


def _sql_preview(sql: str, max_len: int = 200) -> str:
    """Collapse whitespace and truncate so a SQL log line fits one row."""
    one_line = " ".join(sql.split())
    if len(one_line) <= max_len:
        return one_line
    return one_line[:max_len] + "…"


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
        logger.info(f"DuckDB query: {_sql_preview(sql)}")
        t0 = time.perf_counter()
        try:
            df = self._conn.execute(sql).fetchdf()
        except duckdb.Error as e:
            logger.debug(f"DuckDB query error: {e}\nSQL: {sql[:500]}")
            raise QueryError(str(e)) from e
        elapsed_ms = (time.perf_counter() - t0) * 1000
        logger.info(
            f"DuckDB returned {len(df)} rows, {len(df.columns)} cols in {elapsed_ms:.0f}ms"
        )
        return df

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
        logger.info(f"DuckDB query: {_sql_preview(sql)}")
        t0 = time.perf_counter()
        try:
            df = self._conn.execute(sql).fetchdf()
        except duckdb.Error as e:
            logger.debug(f"DuckDB query error: {e}\nSQL: {sql[:500]}")
            raise QueryError(str(e)) from e
        elapsed_ms = (time.perf_counter() - t0) * 1000
        logger.info(
            f"DuckDB returned {len(df)} rows, {len(df.columns)} cols in {elapsed_ms:.0f}ms"
        )
        return df

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
            self._log_session_info(self._spark)
        except Exception as e:
            logger.exception("Spark Connect connection error")
            raise ConnectionError(f"Failed to connect to Spark Connect: {e}") from e

    @staticmethod
    def _log_session_info(spark: Any) -> None:
        """Log Spark session details so users can verify they're distributed.

        Surfaces version, master URL, app id, parallelism, and executor
        configuration. When ``spark.master`` starts with ``local`` the Connect
        server is running all work in one JVM — a common "why is only one
        node busy" cause — so we log a loud warning pointing at the fix.

        Each property and config is fetched via a synchronous gRPC call
        to the Connect server, so we log per-key progress (in case one
        hangs) and time-bound the whole probe so a slow cluster cannot
        freeze the CLI. If the probe times out, the connection is still
        usable; only the diagnostics are skipped. Runs on a daemon thread
        so a hung gRPC call doesn't keep the process alive.
        """
        import queue
        import threading

        logger.info("Fetching Spark session info (one gRPC call per property)…")

        def _gather() -> dict[str, Any]:
            info: dict[str, Any] = {}
            try:
                info["version"] = getattr(spark, "version", None)
                logger.info(f"  spark.version = {info['version']}")
            except Exception as e:
                info["version"] = None
                logger.info(f"  spark.version unavailable: {e}")

            keys = [
                "spark.master",
                "spark.app.name",
                "spark.app.id",
                "spark.default.parallelism",
                "spark.sql.shuffle.partitions",
                "spark.executor.instances",
                "spark.executor.cores",
                "spark.executor.memory",
                "spark.dynamicAllocation.enabled",
                "spark.dynamicAllocation.minExecutors",
                "spark.dynamicAllocation.maxExecutors",
            ]
            conf = getattr(spark, "conf", None)
            for key in keys:
                if conf is None:
                    info[key] = None
                    continue
                try:
                    info[key] = conf.get(key)
                except Exception:
                    # conf.get raises on unset keys in some Spark versions;
                    # treat that as "not configured" rather than failing.
                    info[key] = None
                logger.info(f"  {key} = {info[key]}")
            return info

        result_q: queue.Queue = queue.Queue(maxsize=1)

        def _runner() -> None:
            try:
                result_q.put(("ok", _gather()))
            except Exception as e:
                result_q.put(("error", e))

        # Daemon thread: if the gRPC calls hang forever the process can
        # still exit cleanly when the rest of datasight finishes.
        threading.Thread(target=_runner, daemon=True).start()
        try:
            status, payload = result_q.get(timeout=30.0)
        except queue.Empty:
            logger.warning(
                "Spark session-info probe timed out after 30s — the Connect "
                "server is responding slowly. Skipping the diagnostic block; "
                "the connection itself should still be usable."
            )
            return
        if status == "error":
            logger.warning(f"Spark session-info probe failed: {payload}")
            return
        info: dict[str, Any] = payload

        master = str(info.get("spark.master") or "")
        if master.startswith("local"):
            logger.warning(
                f"Spark master is '{master}' — the Connect server is running "
                "all work in one JVM on the driver node. If you expected "
                "distributed execution, set spark.master on the Connect "
                "server (e.g. spark://master:7077, yarn, or k8s://...) "
                "and restart it."
            )

    def close(self) -> None:
        if self._spark is not None:
            try:
                self._spark.stop()
            except Exception:
                # Best-effort cleanup: session may already be dead, and a
                # failure here would mask the real error from the caller.
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

    def get_table_names(self) -> list[str]:
        """List tables via ``spark.catalog.listTables()``.

        Spark's ``SHOW TABLES`` returns ``namespace | tableName | isTemporary``,
        so the generic first-column strategy in ``schema._get_table_names``
        would pick up database names instead of table names. The catalog
        API returns structured ``Table`` objects with a stable ``.name``.
        """
        if self._spark is None:
            raise ConnectionError("SparkConnectRunner is closed")
        try:
            tables = self._spark.catalog.listTables()
        except Exception as e:
            logger.debug(f"Spark catalog.listTables error: {e}")
            raise QueryError(f"Failed to list Spark tables: {e}") from e
        names: list[str] = []
        for t in tables:
            name = getattr(t, "name", None)
            if name:
                names.append(str(name))
        return names

    async def get_row_count(self, table: str) -> int | None:  # noqa: ARG002
        """Skip row counts for Spark.

        A naive ``SELECT COUNT(*)`` on a multi-TB partitioned table kicks
        off a full cluster job at introspection time, which is exactly
        the foot-gun the byte-cap design exists to prevent. Return None
        so the schema prompt simply omits row counts for Spark tables.
        Statistics from ``DESCRIBE TABLE EXTENDED`` could be wired in
        here later if we need approximate counts.
        """
        return None

    @staticmethod
    def _iter_arrow_batches(spark: Any, sdf: Any):
        """Yield ``pyarrow.RecordBatch`` from a Spark DataFrame.

        Uses the Spark Connect client's streaming iterator so we can stop
        before materializing the full result. Falling back to ``toArrow()`` /
        ``toPandas()`` would defeat the byte cap (they fully materialize the
        result client-side), so we require the streaming API and fail fast
        otherwise. The API is public on pyspark 3.5+, which is our minimum.
        """
        client = getattr(spark, "client", None)
        plan = getattr(sdf, "_plan", None)
        if client is None or plan is None or not hasattr(client, "to_table_as_iterator"):
            raise QueryError(
                "Spark Connect streaming API (client.to_table_as_iterator) is "
                "unavailable — pyspark>=3.5 is required. Upgrade pyspark so "
                "the client-side byte cap remains effective."
            )
        for table, _ in client.to_table_as_iterator(plan, observations={}):
            for batch in table.to_batches():
                yield batch

    def _execute(self, sql: str) -> pd.DataFrame:
        if self._spark is None:
            raise ConnectionError("SparkConnectRunner is closed")
        import pyarrow as pa

        sql = sql.strip().rstrip(";")
        logger.info(f"Spark SQL: {sql[:200]}")
        try:
            sdf = self._spark.sql(sql)
            columns = list(getattr(sdf, "columns", []) or [])
            iterator = self._iter_arrow_batches(self._spark, sdf)
            batches: list[pa.RecordBatch] = []
            schema: pa.Schema | None = None
            total_bytes = 0
            truncated = False
            try:
                for batch in iterator:
                    # Check the cap *before* appending so the returned result
                    # is a true upper bound (cap=0 → zero-row truncated result).
                    if total_bytes + batch.nbytes > self._max_result_bytes:
                        truncated = True
                        break
                    if schema is None:
                        schema = batch.schema
                    batches.append(batch)
                    total_bytes += batch.nbytes
            finally:
                close = getattr(iterator, "close", None)
                if close is not None:
                    try:
                        close()
                    except Exception:
                        # Iterator cleanup is best-effort; don't mask any
                        # primary exception raised during iteration.
                        pass
        except QueryError:
            raise
        except Exception as e:
            logger.debug(f"Spark SQL error: {e}\nSQL: {sql[:500]}")
            raise QueryError(str(e)) from e

        if not batches:
            # No data, but preserve the column list so downstream UI and
            # schema logic still sees the shape of the result.
            df = pd.DataFrame(columns=columns)
            if truncated:
                df.attrs["truncated"] = True
                df.attrs["truncation_reason"] = (
                    f"Result exceeded {self._max_result_bytes:,} bytes before any "
                    "batch could be returned. Add aggregation or a tighter LIMIT."
                )
            return df
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
                    # Tag API is best-effort: without it we lose server-side
                    # cancellation on timeout, but the query still runs.
                    pass
            try:
                return self._execute(sql)
            finally:
                if remove_tag is not None:
                    try:
                        remove_tag(tag)
                    except Exception:
                        # Ancillary cleanup; never mask the primary result.
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
