"""
Explore functionality for datasight.

Provides utilities for quickly exploring CSV and Parquet files without
setting up a full project. Creates ephemeral DuckDB sessions with views
pointing to user files, or routes through Spark Connect when the
current project is configured with ``DB_MODE=spark``.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb
from loguru import logger

from datasight.exceptions import ConfigurationError
from datasight.runner import (
    DEFAULT_SPARK_MAX_RESULT_BYTES,
    EphemeralDuckDBRunner,
    SparkConnectRunner,
)

if TYPE_CHECKING:
    from datasight.settings import DatabaseSettings


def detect_file_type(path: str) -> str | None:
    """Detect the type of a data file or directory.

    Parameters
    ----------
    path:
        Path to a file or directory.

    Returns
    -------
    One of: "csv", "parquet", "hive_parquet", "csv_dir", "duckdb", "sqlite",
    or None if not recognized.
    """
    p = Path(path)

    if not p.exists():
        return None

    if p.is_file():
        suffix = p.suffix.lower()
        if suffix == ".csv":
            return "csv"
        if suffix == ".parquet":
            return "parquet"
        if suffix in (".sqlite", ".sqlite3"):
            return "sqlite"
        if suffix == ".duckdb":
            return "duckdb"
        if suffix == ".db":
            with p.open("rb") as f:
                header = f.read(16)
            if header == b"SQLite format 3\x00":
                return "sqlite"
            return "duckdb"
        return None

    if p.is_dir():
        # Check for hive-partitioned parquet (directory with .parquet files inside)
        if any(p.rglob("*.parquet")):
            return "hive_parquet"
        # Check for CSV files in directory
        if any(p.glob("*.csv")):
            return "csv_dir"
        return None

    return None


def scan_directory_for_data_files(
    directory: str | Path,
    *,
    max_files: int = 100,
) -> tuple[list[dict], bool]:
    """Find CSV and Parquet files at the top level of ``directory``.

    Hidden files (dot-prefixed) and unreadable entries are skipped. Only
    single-file entries are returned; nested directories are ignored so the
    regular ExploreCard can still be used for hive-partitioned layouts.

    Returns
    -------
    Tuple of ``(files, truncated)`` where each file dict has ``path``, ``name``,
    ``type`` (``"csv"`` or ``"parquet"``), and ``size_bytes``. ``truncated`` is
    ``True`` when more than ``max_files`` matches were present.
    """
    root = Path(directory)
    if not root.is_dir():
        return [], False

    matches: list[Path] = []
    try:
        entries = sorted(root.iterdir(), key=lambda p: p.name.lower())
    except OSError as e:
        logger.warning(f"Unable to list {root}: {e}")
        return [], False

    for entry in entries:
        if entry.name.startswith("."):
            continue
        try:
            if not entry.is_file():
                continue
        except OSError:
            continue
        suffix = entry.suffix.lower()
        if suffix not in (".csv", ".parquet"):
            continue
        matches.append(entry)

    truncated = len(matches) > max_files
    files: list[dict] = []
    for entry in matches[:max_files]:
        try:
            size = entry.stat().st_size
        except OSError:
            continue
        files.append(
            {
                "path": str(entry.resolve()),
                "name": entry.name,
                "type": "csv" if entry.suffix.lower() == ".csv" else "parquet",
                "size_bytes": size,
            }
        )
    return files, truncated


def sanitize_table_name(name: str) -> str:
    """Convert a filename to a valid SQL table name.

    Parameters
    ----------
    name:
        Original filename (without extension).

    Returns
    -------
    A valid SQL identifier.
    """
    # Replace non-alphanumeric characters with underscores
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    # Ensure it starts with a letter or underscore
    if sanitized and sanitized[0].isdigit():
        sanitized = "_" + sanitized
    # Avoid empty names
    if not sanitized:
        sanitized = "_table"
    return sanitized.lower()


def create_view_sql(table_name: str, file_path: str, file_type: str) -> str:
    """Generate SQL to create a view for a data file.

    Parameters
    ----------
    table_name:
        Name for the view.
    file_path:
        Path to the data file.
    file_type:
        Type of file: "csv", "parquet", "hive_parquet", or "csv_dir".

    Returns
    -------
    SQL CREATE VIEW statement.
    """
    # Escape single quotes in path
    escaped_path = file_path.replace("'", "''")

    # Double-quote the view name to handle reserved words (e.g., "select")
    quoted_name = f'"{table_name}"'

    if file_type == "csv":
        return f"CREATE VIEW {quoted_name} AS SELECT * FROM read_csv_auto('{escaped_path}')"
    elif file_type == "parquet":
        return f"CREATE VIEW {quoted_name} AS SELECT * FROM read_parquet('{escaped_path}')"
    elif file_type == "hive_parquet":
        glob_path = f"{escaped_path}/**/*.parquet"
        return f"CREATE VIEW {quoted_name} AS SELECT * FROM read_parquet('{glob_path}', hive_partitioning=true)"
    elif file_type == "csv_dir":
        glob_path = f"{escaped_path}/*.csv"
        return f"CREATE VIEW {quoted_name} AS SELECT * FROM read_csv_auto('{glob_path}')"
    else:
        raise ValueError(f"Unknown file type: {file_type}")


def _attach_duckdb_file(
    conn: duckdb.DuckDBPyConnection,
    db_path: str,
    existing_names: set[str],
) -> list[dict]:
    """Attach a DuckDB file and create views for its tables.

    Parameters
    ----------
    conn:
        The in-memory DuckDB connection.
    db_path:
        Path to the .duckdb file.
    existing_names:
        Set of already-used table names (modified in place).

    Returns
    -------
    List of table info dicts for each table found.
    """
    tables_info: list[dict] = []
    escaped_path = db_path.replace("'", "''")

    # Generate unique alias for this database
    db_alias = sanitize_table_name(Path(db_path).stem) + "_db"
    counter = 2
    base_alias = db_alias
    while db_alias in existing_names:
        db_alias = f"{base_alias}_{counter}"
        counter += 1
    existing_names.add(db_alias)

    # Attach the database read-only (quote alias to handle reserved words)
    conn.execute(f"ATTACH '{escaped_path}' AS \"{db_alias}\" (READ_ONLY)")

    # Get list of tables in the attached database
    escaped_alias = db_alias.replace("'", "''")
    tables_df = conn.execute(
        f"SELECT table_name FROM information_schema.tables "
        f"WHERE table_catalog = '{escaped_alias}' AND table_schema = 'main'"
    ).fetchdf()

    for row in tables_df.itertuples():
        source_table = row.table_name  # ty: ignore[unresolved-attribute]
        view_name = sanitize_table_name(source_table)

        # Handle duplicate view names
        if view_name in existing_names:
            suffix = 2
            while f"{view_name}_{suffix}" in existing_names:
                suffix += 1
            view_name = f"{view_name}_{suffix}"

        # Create view pointing to the attached table (quote all identifiers)
        conn.execute(f'CREATE VIEW "{view_name}" AS SELECT * FROM "{db_alias}"."{source_table}"')
        existing_names.add(view_name)
        tables_info.append(
            {
                "name": view_name,
                "path": db_path,
                "type": "duckdb",
                "source_table": source_table,
            }
        )
        logger.info(f"Created view '{view_name}' from {db_alias}.{source_table}")

    return tables_info


def create_ephemeral_session(file_paths: list[str]) -> tuple[EphemeralDuckDBRunner, list[dict]]:
    """Create an ephemeral DuckDB session with views for the given files.

    Parameters
    ----------
    file_paths:
        List of paths to CSV/Parquet/DuckDB files or directories.

    Returns
    -------
    Tuple of (runner, tables_info) where tables_info is a list of dicts
    with "name", "path", and "type" keys.

    Raises
    ------
    ConfigurationError:
        If no valid files are found or view creation fails.
    """
    if not file_paths:
        raise ConfigurationError("No file paths provided")

    # Categorize files by type
    resolved_paths = [
        (str(Path(p).resolve()), detect_file_type(str(Path(p).resolve()))) for p in file_paths
    ]
    duckdb_files = [p for p, t in resolved_paths if t == "duckdb"]
    other_files = [(p, t) for p, t in resolved_paths if t is not None and t != "duckdb"]
    invalid_files = [p for p, t in resolved_paths if t is None]

    # Special case: single DuckDB file with no other files - open directly
    if len(duckdb_files) == 1 and not other_files:
        db_path = duckdb_files[0]
        try:
            conn = duckdb.connect(db_path, read_only=True)
            # Get list of tables
            tables_df = conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' AND table_type = 'BASE TABLE'"
            ).fetchdf()
            views_df = conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' AND table_type = 'VIEW'"
            ).fetchdf()

            tables_info = []
            for row in tables_df.itertuples():
                tables_info.append({"name": row.table_name, "path": db_path, "type": "duckdb"})  # ty: ignore[unresolved-attribute]
            for row in views_df.itertuples():
                tables_info.append({"name": row.table_name, "path": db_path, "type": "duckdb"})  # ty: ignore[unresolved-attribute]

            if not tables_info:
                conn.close()
                raise ConfigurationError(f"No tables found in {db_path}")

            logger.info(f"Opened DuckDB directly: {db_path} ({len(tables_info)} tables)")
            return EphemeralDuckDBRunner(conn), tables_info
        except duckdb.Error as e:
            raise ConfigurationError(f"Failed to open DuckDB file {db_path}: {e}") from e

    # General case: create in-memory DB with views
    conn = duckdb.connect(":memory:")
    tables_info: list[dict] = []
    errors: list[str] = list(f"Unrecognized or missing file: {p}" for p in invalid_files)
    existing_names: set[str] = set()

    # Handle DuckDB files - attach and create views
    for db_path in duckdb_files:
        try:
            db_tables = _attach_duckdb_file(conn, db_path, existing_names)
            tables_info.extend(db_tables)
        except Exception as e:
            errors.append(f"Failed to attach DuckDB file {db_path}: {e}")
            logger.warning(f"Failed to attach DuckDB file {db_path}: {e}")

    # Handle other file types
    for path, file_type in other_files:
        p = Path(path)
        base_name = p.stem if p.is_file() else p.name
        table_name = sanitize_table_name(base_name)

        # Handle duplicate names
        if table_name in existing_names:
            counter = 2
            while f"{table_name}_{counter}" in existing_names:
                counter += 1
            table_name = f"{table_name}_{counter}"

        try:
            sql = create_view_sql(table_name, path, file_type)
            conn.execute(sql)
            existing_names.add(table_name)
            tables_info.append({"name": table_name, "path": path, "type": file_type})
            logger.info(f"Created view '{table_name}' from {file_type}: {path}")
        except Exception as e:
            errors.append(f"Failed to create view for {path}: {e}")
            logger.warning(f"Failed to create view for {path}: {e}")

    if not tables_info:
        conn.close()
        msg = "No valid data files found."
        if errors:
            msg += " " + " ".join(errors)
        raise ConfigurationError(msg)

    runner = EphemeralDuckDBRunner(conn)
    return runner, tables_info


_SPARK_SUPPORTED_TYPES = {"parquet", "hive_parquet", "csv", "csv_dir"}


def create_spark_files_session(
    file_paths: list[str],
    *,
    spark_remote: str,
    spark_token: str | None = None,
    spark_max_result_bytes: int = DEFAULT_SPARK_MAX_RESULT_BYTES,
    spark: Any = None,
) -> tuple[SparkConnectRunner, list[dict]]:
    """Register parquet/CSV files as Spark temp views on a Spark Connect session.

    Each file or directory becomes a temp view that the rest of datasight
    (schema introspection, the agent, the web UI) can query by name. The
    paths must be reachable by the Spark workers at the *same* absolute
    path — Spark does not upload local files to the cluster.

    Parameters
    ----------
    file_paths:
        Paths to Parquet/CSV files or directories. SQLite and DuckDB files
        are not supported on this path.
    spark_remote, spark_token, spark_max_result_bytes:
        Forwarded to ``SparkConnectRunner``.
    spark:
        Optional pre-built Spark session (used by tests to inject a fake).

    Raises
    ------
    ConfigurationError
        If a path doesn't exist, isn't absolute-resolvable, or is a file
        type Spark can't read (duckdb/sqlite).
    """
    if not file_paths:
        raise ConfigurationError("No file paths provided")

    resolved: list[tuple[str, str]] = []
    for p in file_paths:
        abs_path = str(Path(p).resolve())
        ftype = detect_file_type(abs_path)
        if ftype is None:
            raise ConfigurationError(f"Unrecognized or missing file: {p}")
        if ftype not in _SPARK_SUPPORTED_TYPES:
            raise ConfigurationError(
                f"Spark backend cannot read {ftype!r} files ({p}). "
                "Use DuckDB mode, or re-export to Parquet."
            )
        resolved.append((abs_path, ftype))

    runner = SparkConnectRunner(
        remote=spark_remote,
        token=spark_token,
        max_result_bytes=spark_max_result_bytes,
        spark=spark,
    )

    tables_info: list[dict] = []
    existing_names: set[str] = set()
    errors: list[str] = []
    for path, file_type in resolved:
        p = Path(path)
        base_name = p.stem if p.is_file() else p.name
        view_name = sanitize_table_name(base_name)
        if view_name in existing_names:
            counter = 2
            while f"{view_name}_{counter}" in existing_names:
                counter += 1
            view_name = f"{view_name}_{counter}"
        try:
            _register_spark_view(runner._spark, view_name, path, file_type)
            existing_names.add(view_name)
            tables_info.append({"name": view_name, "path": path, "type": file_type})
            logger.info(f"Registered Spark temp view '{view_name}' from {file_type}: {path}")
        except Exception as e:
            errors.append(f"Failed to register {path}: {e}")
            logger.warning(f"Failed to register Spark view for {path}: {e}")

    if not tables_info:
        runner.close()
        msg = "No valid data files registered with Spark."
        if errors:
            msg += " " + " ".join(errors)
        raise ConfigurationError(msg)

    return runner, tables_info


def _register_spark_view(spark: Any, view_name: str, path: str, file_type: str) -> None:
    """Read ``path`` with Spark and expose it as a temp view.

    ``hive_parquet`` and ``csv_dir`` point at directories; Spark autodetects
    the partitioning. Plain parquet / csv are single files.
    """
    reader = spark.read
    if file_type in ("parquet", "hive_parquet"):
        df = reader.parquet(path)
    elif file_type in ("csv", "csv_dir"):
        df = reader.option("header", "true").option("inferSchema", "true").csv(path)
    else:
        raise ConfigurationError(f"Unsupported Spark file type: {file_type}")
    df.createOrReplaceTempView(view_name)


def create_files_session_for_settings(
    file_paths: list[str],
    settings: DatabaseSettings | None = None,
) -> tuple[Any, list[dict]]:
    """Pick a file-session backend based on the project's configured DB_MODE.

    - ``spark``: register files as temp views on a SparkConnectRunner so
      the cluster does the reading — cheap to set up on HPC where the
      files already live on a shared filesystem.
    - ``duckdb`` / ``sqlite`` / no settings: local ephemeral DuckDB session
      (the long-standing default).
    - ``postgres`` / ``flightsql``: fall back to DuckDB with a warning —
      those backends can't read arbitrary local files.
    """
    if settings is None or settings.mode in ("duckdb", "sqlite"):
        return create_ephemeral_session(file_paths)
    if settings.mode == "spark":
        logger.info(f"File inspection routed through Spark Connect: {settings.spark_remote}")
        return create_spark_files_session(
            file_paths,
            spark_remote=settings.spark_remote,
            spark_token=settings.spark_token,
            spark_max_result_bytes=settings.spark_max_result_bytes,
        )
    logger.warning(
        f"DB_MODE={settings.mode!r} cannot read local files directly — "
        "file inspection will use a local DuckDB session, independent of "
        "the configured database."
    )
    return create_ephemeral_session(file_paths)


def add_files_to_connection(
    conn: duckdb.DuckDBPyConnection,
    file_paths: list[str],
    existing_table_names: set[str],
) -> list[dict]:
    """Add new file views to an existing DuckDB connection.

    Parameters
    ----------
    conn:
        An open DuckDB connection (must be writable).
    file_paths:
        Paths to CSV/Parquet/DuckDB files or directories to add.
    existing_table_names:
        Set of table names already in use (to avoid collisions).

    Returns
    -------
    List of table info dicts for the newly created views.

    Raises
    ------
    ConfigurationError:
        If no valid files are found.
    """
    tables_info: list[dict] = []
    errors: list[str] = []
    names = set(existing_table_names)

    for path in file_paths:
        path = str(Path(path).resolve())
        file_type = detect_file_type(path)

        if file_type is None:
            errors.append(f"Unrecognized or missing file: {path}")
            continue

        if file_type == "duckdb":
            try:
                db_tables = _attach_duckdb_file(conn, path, names)
                tables_info.extend(db_tables)
            except Exception as e:
                errors.append(f"Failed to attach DuckDB file {path}: {e}")
            continue

        p = Path(path)
        base_name = p.stem if p.is_file() else p.name
        table_name = sanitize_table_name(base_name)

        if table_name in names:
            counter = 2
            while f"{table_name}_{counter}" in names:
                counter += 1
            table_name = f"{table_name}_{counter}"

        try:
            sql = create_view_sql(table_name, path, file_type)
            conn.execute(sql)
            names.add(table_name)
            tables_info.append({"name": table_name, "path": path, "type": file_type})
            logger.info(f"Added view '{table_name}' from {file_type}: {path}")
        except Exception as e:
            errors.append(f"Failed to create view for {path}: {e}")

    if not tables_info:
        msg = "No valid data files found."
        if errors:
            msg += " " + " ".join(errors)
        raise ConfigurationError(msg)

    return tables_info


def build_persistent_duckdb(
    db_path: str | Path,
    tables_info: list[dict],
    *,
    overwrite: bool = False,
) -> Path:
    """Create a DuckDB file with views pointing at the given data sources.

    Parameters
    ----------
    db_path:
        Destination path for the DuckDB file.
    tables_info:
        Table info dicts (from :func:`create_ephemeral_session`) with
        ``name``, ``path``, and ``type`` keys.
    overwrite:
        When ``True``, an existing file at ``db_path`` is removed first.

    Returns
    -------
    The absolute path of the created DuckDB file.
    """
    db_path = Path(db_path).resolve()
    if db_path.exists():
        if not overwrite:
            raise FileExistsError(f"Database already exists: {db_path}")
        db_path.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(db_path))
    attached: dict[str, str] = {}
    used_aliases: set[str] = set()

    try:
        for table in tables_info:
            if table["type"] == "duckdb":
                source_path = table["path"]
                if source_path not in attached:
                    alias = sanitize_table_name(Path(source_path).stem) + "_db"
                    base_alias = alias
                    counter = 2
                    while alias in used_aliases:
                        alias = f"{base_alias}_{counter}"
                        counter += 1
                    used_aliases.add(alias)
                    escaped_path = source_path.replace("'", "''")
                    conn.execute(f"ATTACH '{escaped_path}' AS \"{alias}\" (READ_ONLY)")
                    attached[source_path] = alias
                alias = attached[source_path]
                source_table = table.get("source_table", table["name"])
                conn.execute(
                    f'CREATE VIEW "{table["name"]}" AS SELECT * FROM "{alias}"."{source_table}"'
                )
            else:
                conn.execute(create_view_sql(table["name"], table["path"], table["type"]))
            logger.info(f"Created view '{table['name']}' in {db_path}")
    finally:
        conn.close()

    return db_path


def save_ephemeral_as_project(
    runner: object,  # noqa: ARG001  # kept for API consistency
    tables_info: list[dict],
    project_dir: str,
    project_name: str | None = None,
) -> str:
    """Save an ephemeral session as a proper datasight project.

    Creates a project directory with:
    - A DuckDB database file containing views to the original data files
      (or references an existing DuckDB file if that was the source)
    - A .env file configured for DuckDB
    - A basic schema_description.md

    Parameters
    ----------
    runner:
        The ephemeral DuckDB runner (unused but kept for API consistency).
    tables_info:
        List of table info dicts from create_ephemeral_session.
    project_dir:
        Directory to create the project in.
    project_name:
        Optional name for the project (defaults to directory name).

    Returns
    -------
    Path to the created project directory.

    Raises
    ------
    ConfigurationError:
        If the project directory cannot be created.
    """
    dest = Path(project_dir).resolve()
    dest.mkdir(parents=True, exist_ok=True)

    if project_name is None:
        project_name = dest.name

    # Check if this is a single DuckDB file opened directly
    # (all tables have same path and type is duckdb, no source_table key)
    unique_paths = {t["path"] for t in tables_info}
    all_duckdb = all(t["type"] == "duckdb" for t in tables_info)
    direct_duckdb = (
        all_duckdb and len(unique_paths) == 1 and not any("source_table" in t for t in tables_info)
    )

    if direct_duckdb:
        # Just reference the existing database
        db_path_str = next(iter(unique_paths))
    else:
        # Create the DuckDB database file with views (remove old one if overwriting)
        db_path = dest / "data.duckdb"
        if db_path.exists():
            db_path.unlink()
        db_conn = duckdb.connect(str(db_path))

        # Track attached databases to avoid duplicates
        attached_dbs: dict[str, str] = {}  # path -> alias
        used_aliases: set[str] = set()

        for table in tables_info:
            if table["type"] == "duckdb":
                # For DuckDB sources, attach the database and create a view
                source_path = table["path"]
                if source_path not in attached_dbs:
                    alias = sanitize_table_name(Path(source_path).stem) + "_db"
                    base_alias = alias
                    counter = 2
                    while alias in used_aliases:
                        alias = f"{base_alias}_{counter}"
                        counter += 1
                    used_aliases.add(alias)
                    escaped_path = source_path.replace("'", "''")
                    db_conn.execute(f"ATTACH '{escaped_path}' AS \"{alias}\" (READ_ONLY)")
                    attached_dbs[source_path] = alias

                alias = attached_dbs[source_path]
                source_table = table.get("source_table", table["name"])
                view_name = table["name"]
                db_conn.execute(
                    f'CREATE VIEW "{view_name}" AS SELECT * FROM "{alias}"."{source_table}"'
                )
            else:
                sql = create_view_sql(table["name"], table["path"], table["type"])
                db_conn.execute(sql)
            logger.info(f"Created persistent view '{table['name']}' in {db_path}")

        db_conn.close()
        db_path_str = "data.duckdb"  # Use relative path for newly created file

    # Create .env file
    env_content = f"""# datasight project configuration
# Created from quick explore session

DB_MODE=duckdb
DB_PATH={db_path_str}

# Uncomment and set your API key:
# ANTHROPIC_API_KEY=your-key-here
"""
    (dest / ".env").write_text(env_content, encoding="utf-8")

    # Create schema_description.md
    table_list = "\n".join(
        f"- **{t['name']}**: {t['type']} from `{t['path']}`" for t in tables_info
    )
    schema_content = f"""# {project_name}

This project was created from a quick explore session.

## Data Sources

{table_list}

## Description

Add a description of your dataset here. Include:
- What the data represents
- Key columns and their meanings
- Any important relationships between tables
- Domain-specific terminology
"""
    (dest / "schema_description.md").write_text(schema_content, encoding="utf-8")

    logger.info(f"Saved project to {dest}")
    return str(dest)
