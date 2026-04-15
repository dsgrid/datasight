"""Tests for datasight.explore module."""

import pytest

from datasight.explore import (
    create_ephemeral_session,
    create_view_sql,
    detect_file_type,
    sanitize_table_name,
    save_ephemeral_as_project,
)
from datasight.exceptions import ConfigurationError


class TestDetectFileType:
    """Tests for detect_file_type function."""

    def test_csv_file(self, tmp_path):
        """Detect CSV file."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("a,b,c\n1,2,3\n", encoding="utf-8")
        assert detect_file_type(str(csv_file)) == "csv"

    def test_parquet_file(self, tmp_path):
        """Detect single parquet file."""
        # Create a minimal parquet file using DuckDB
        import duckdb

        parquet_file = tmp_path / "data.parquet"
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE t AS SELECT 1 as a, 2 as b")
        conn.execute(f"COPY t TO '{parquet_file}' (FORMAT PARQUET)")
        conn.close()

        assert detect_file_type(str(parquet_file)) == "parquet"

    def test_hive_partitioned_directory(self, tmp_path):
        """Detect hive-partitioned parquet directory."""
        import duckdb

        hive_dir = tmp_path / "data"
        hive_dir.mkdir()
        part1 = hive_dir / "year=2024"
        part1.mkdir()
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE t AS SELECT 1 as a")
        conn.execute(f"COPY t TO '{part1}/part.parquet' (FORMAT PARQUET)")
        conn.close()

        assert detect_file_type(str(hive_dir)) == "hive_parquet"

    def test_csv_directory(self, tmp_path):
        """Detect directory with CSV files."""
        csv_dir = tmp_path / "csvs"
        csv_dir.mkdir()
        (csv_dir / "file1.csv").write_text("a,b\n1,2\n", encoding="utf-8")
        (csv_dir / "file2.csv").write_text("a,b\n3,4\n", encoding="utf-8")

        assert detect_file_type(str(csv_dir)) == "csv_dir"

    def test_duckdb_file(self, tmp_path):
        """Detect DuckDB database file."""
        import duckdb

        db_file = tmp_path / "data.duckdb"
        conn = duckdb.connect(str(db_file))
        conn.execute("CREATE TABLE t (x INT)")
        conn.close()

        assert detect_file_type(str(db_file)) == "duckdb"

    def test_db_extension(self, tmp_path):
        """Detect .db extension as DuckDB."""
        import duckdb

        db_file = tmp_path / "data.db"
        conn = duckdb.connect(str(db_file))
        conn.execute("CREATE TABLE t (x INT)")
        conn.close()

        assert detect_file_type(str(db_file)) == "duckdb"

    def test_sqlite_file(self, tmp_path):
        """Detect SQLite database file."""
        import sqlite3

        db_file = tmp_path / "data.sqlite"
        conn = sqlite3.connect(str(db_file))
        conn.execute("CREATE TABLE t (x INTEGER)")
        conn.close()

        assert detect_file_type(str(db_file)) == "sqlite"

    def test_sqlite_db_extension(self, tmp_path):
        """Detect SQLite database file with .db extension."""
        import sqlite3

        db_file = tmp_path / "data.db"
        conn = sqlite3.connect(str(db_file))
        conn.execute("CREATE TABLE t (x INTEGER)")
        conn.close()

        assert detect_file_type(str(db_file)) == "sqlite"

    def test_nonexistent_path(self):
        """Return None for nonexistent paths."""
        assert detect_file_type("/nonexistent/path.csv") is None

    def test_unknown_extension(self, tmp_path):
        """Return None for unknown file types."""
        txt_file = tmp_path / "data.txt"
        txt_file.write_text("hello", encoding="utf-8")
        assert detect_file_type(str(txt_file)) is None

    def test_empty_directory(self, tmp_path):
        """Return None for empty directory."""
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        assert detect_file_type(str(empty_dir)) is None


class TestSanitizeTableName:
    """Tests for sanitize_table_name function."""

    def test_basic_name(self):
        """Keep simple names unchanged (except lowercasing)."""
        assert sanitize_table_name("MyTable") == "mytable"

    def test_spaces_and_dashes(self):
        """Convert spaces and dashes to underscores."""
        assert sanitize_table_name("my-table name") == "my_table_name"

    def test_special_characters(self):
        """Remove special characters."""
        assert sanitize_table_name("data@2024!") == "data_2024_"

    def test_leading_digit(self):
        """Prefix with underscore if starts with digit."""
        assert sanitize_table_name("2024_data") == "_2024_data"

    def test_empty_string(self):
        """Return default name for empty string."""
        assert sanitize_table_name("") == "_table"


class TestCreateViewSql:
    """Tests for create_view_sql function."""

    def test_csv_view(self):
        """Generate correct SQL for CSV file."""
        sql = create_view_sql("sales", "/path/to/sales.csv", "csv")
        assert (
            sql == """CREATE VIEW "sales" AS SELECT * FROM read_csv_auto('/path/to/sales.csv')"""
        )

    def test_parquet_view(self):
        """Generate correct SQL for parquet file."""
        sql = create_view_sql("events", "/data/events.parquet", "parquet")
        assert (
            sql == """CREATE VIEW "events" AS SELECT * FROM read_parquet('/data/events.parquet')"""
        )

    def test_hive_parquet_view(self):
        """Generate correct SQL for hive-partitioned parquet."""
        sql = create_view_sql("logs", "/data/logs", "hive_parquet")
        assert sql == (
            """CREATE VIEW "logs" AS SELECT * FROM """
            """read_parquet('/data/logs/**/*.parquet', hive_partitioning=true)"""
        )

    def test_csv_dir_view(self):
        """Generate correct SQL for CSV directory."""
        sql = create_view_sql("reports", "/data/reports", "csv_dir")
        assert (
            sql
            == """CREATE VIEW "reports" AS SELECT * FROM read_csv_auto('/data/reports/*.csv')"""
        )

    def test_path_with_quotes(self):
        """Escape single quotes in path."""
        sql = create_view_sql("data", "/path/with'quote/file.csv", "csv")
        assert "with''quote" in sql

    def test_unknown_type_raises(self):
        """Raise ValueError for unknown file type."""
        with pytest.raises(ValueError, match="Unknown file type"):
            create_view_sql("x", "/path", "unknown")


class TestCreateEphemeralSession:
    """Tests for create_ephemeral_session function."""

    def test_single_csv(self, tmp_path):
        """Create session from single CSV file."""
        csv_file = tmp_path / "sales.csv"
        csv_file.write_text("product,quantity\nwidget,10\ngadget,5\n", encoding="utf-8")

        runner, tables = create_ephemeral_session([str(csv_file)])

        assert len(tables) == 1
        assert tables[0]["name"] == "sales"
        assert tables[0]["type"] == "csv"

        # Verify we can query the data
        import asyncio

        df = asyncio.run(runner.run_sql("SELECT * FROM sales"))
        assert len(df) == 2
        assert list(df.columns) == ["product", "quantity"]

        runner.close()

    def test_multiple_files(self, tmp_path):
        """Create session from multiple files."""
        csv1 = tmp_path / "orders.csv"
        csv1.write_text("id,amount\n1,100\n2,200\n", encoding="utf-8")
        csv2 = tmp_path / "customers.csv"
        csv2.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")

        runner, tables = create_ephemeral_session([str(csv1), str(csv2)])

        assert len(tables) == 2
        names = {t["name"] for t in tables}
        assert names == {"orders", "customers"}

        runner.close()

    def test_duplicate_names(self, tmp_path):
        """Handle duplicate file names from different directories."""
        dir1 = tmp_path / "dir1"
        dir1.mkdir()
        dir2 = tmp_path / "dir2"
        dir2.mkdir()

        (dir1 / "data.csv").write_text("a\n1\n", encoding="utf-8")
        (dir2 / "data.csv").write_text("b\n2\n", encoding="utf-8")

        runner, tables = create_ephemeral_session([str(dir1 / "data.csv"), str(dir2 / "data.csv")])

        assert len(tables) == 2
        names = {t["name"] for t in tables}
        assert "data" in names
        assert "data_2" in names

        runner.close()

    def test_parquet_file(self, tmp_path):
        """Create session from parquet file."""
        import duckdb

        parquet_file = tmp_path / "events.parquet"
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE t AS SELECT 1 as id, 'click' as event_type")
        conn.execute(f"COPY t TO '{parquet_file}' (FORMAT PARQUET)")
        conn.close()

        runner, tables = create_ephemeral_session([str(parquet_file)])

        assert len(tables) == 1
        assert tables[0]["name"] == "events"
        assert tables[0]["type"] == "parquet"

        runner.close()

    def test_single_duckdb_file(self, tmp_path):
        """Open single DuckDB file directly (no views)."""
        import duckdb as ddb

        db_file = tmp_path / "mydata.duckdb"
        conn = ddb.connect(str(db_file))
        conn.execute("CREATE TABLE users (id INT, name VARCHAR)")
        conn.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
        conn.execute("CREATE TABLE orders (id INT, user_id INT)")
        conn.execute("INSERT INTO orders VALUES (1, 1), (2, 2)")
        conn.close()

        runner, tables = create_ephemeral_session([str(db_file)])

        # Should have both tables
        assert len(tables) == 2
        names = {t["name"] for t in tables}
        assert names == {"users", "orders"}

        # All should be type duckdb and reference the same file
        assert all(t["type"] == "duckdb" for t in tables)
        assert all(t["path"] == str(db_file) for t in tables)

        # Verify we can query the actual tables (not views)
        import asyncio

        df = asyncio.run(runner.run_sql("SELECT * FROM users ORDER BY id"))
        assert len(df) == 2
        assert df["name"].tolist() == ["Alice", "Bob"]

        runner.close()

    def test_duckdb_with_csv_creates_views(self, tmp_path):
        """Mixing DuckDB with CSV creates views for all."""
        import duckdb as ddb

        db_file = tmp_path / "data.duckdb"
        conn = ddb.connect(str(db_file))
        conn.execute("CREATE TABLE products (id INT, name VARCHAR)")
        conn.execute("INSERT INTO products VALUES (1, 'Widget')")
        conn.close()

        csv_file = tmp_path / "sales.csv"
        csv_file.write_text("product_id,amount\n1,100\n", encoding="utf-8")

        runner, tables = create_ephemeral_session([str(db_file), str(csv_file)])

        # Should have both tables
        names = {t["name"] for t in tables}
        assert "products" in names
        assert "sales" in names

        # Verify we can query both
        import asyncio

        df = asyncio.run(runner.run_sql("SELECT * FROM products"))
        assert len(df) == 1

        df = asyncio.run(runner.run_sql("SELECT * FROM sales"))
        assert len(df) == 1

        runner.close()

    def test_empty_paths_raises(self):
        """Raise error for empty paths list."""
        with pytest.raises(ConfigurationError, match="No file paths"):
            create_ephemeral_session([])

    def test_all_invalid_paths_raises(self, tmp_path):
        """Raise error when all paths are invalid."""
        with pytest.raises(ConfigurationError, match="No valid data files"):
            create_ephemeral_session(["/nonexistent/file.csv"])

    def test_skips_invalid_files(self, tmp_path):
        """Skip invalid files but continue with valid ones."""
        csv_file = tmp_path / "valid.csv"
        csv_file.write_text("x\n1\n", encoding="utf-8")

        runner, tables = create_ephemeral_session(["/nonexistent/file.csv", str(csv_file)])

        assert len(tables) == 1
        assert tables[0]["name"] == "valid"

        runner.close()


class TestSaveEphemeralAsProject:
    """Tests for save_ephemeral_as_project function."""

    def test_save_project(self, tmp_path):
        """Save ephemeral session as project."""
        # Create ephemeral session
        csv_file = tmp_path / "source" / "data.csv"
        csv_file.parent.mkdir()
        csv_file.write_text("value\n42\n", encoding="utf-8")

        runner, tables = create_ephemeral_session([str(csv_file)])

        # Save as project
        project_dir = tmp_path / "my_project"
        save_ephemeral_as_project(runner, tables, str(project_dir), "Test Project")

        # Verify files created
        assert (project_dir / ".env").exists()
        assert (project_dir / "schema_description.md").exists()
        assert (project_dir / "data.duckdb").exists()

        # Verify .env content
        env_content = (project_dir / ".env").read_text()
        assert "DB_MODE=duckdb" in env_content
        assert "DB_PATH=data.duckdb" in env_content

        # Verify schema description
        schema_content = (project_dir / "schema_description.md").read_text()
        assert "Test Project" in schema_content
        assert "data" in schema_content  # table name

        # Verify the DuckDB file has the view
        import duckdb

        conn = duckdb.connect(str(project_dir / "data.duckdb"), read_only=True)
        result = conn.execute("SELECT * FROM data").fetchall()
        assert result == [(42,)]
        conn.close()

        runner.close()

    def test_save_duckdb_references_original(self, tmp_path):
        """Save DuckDB file session references original database."""
        import duckdb

        # Create source database
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        db_file = source_dir / "original.duckdb"
        conn = duckdb.connect(str(db_file))
        conn.execute("CREATE TABLE items (id INT, name VARCHAR)")
        conn.execute("INSERT INTO items VALUES (1, 'Test')")
        conn.close()

        runner, tables = create_ephemeral_session([str(db_file)])

        # Save as project
        project_dir = tmp_path / "my_project"
        save_ephemeral_as_project(runner, tables, str(project_dir))

        # Verify .env points to original database (not data.duckdb)
        env_content = (project_dir / ".env").read_text()
        assert str(db_file) in env_content
        assert "data.duckdb" not in env_content

        # No new database file should be created
        assert not (project_dir / "data.duckdb").exists()

        runner.close()

    def test_save_with_default_name(self, tmp_path):
        """Use directory name as project name when not specified."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("x\n1\n", encoding="utf-8")

        runner, tables = create_ephemeral_session([str(csv_file)])

        project_dir = tmp_path / "my_analysis"
        save_ephemeral_as_project(runner, tables, str(project_dir))

        schema_content = (project_dir / "schema_description.md").read_text()
        assert "my_analysis" in schema_content

        runner.close()

    def test_creates_parent_directories(self, tmp_path):
        """Create parent directories if they don't exist."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("x\n1\n", encoding="utf-8")

        runner, tables = create_ephemeral_session([str(csv_file)])

        project_dir = tmp_path / "nested" / "deep" / "project"
        save_ephemeral_as_project(runner, tables, str(project_dir))

        assert project_dir.exists()
        assert (project_dir / ".env").exists()

        runner.close()
