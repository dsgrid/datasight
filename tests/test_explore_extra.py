"""Extra tests for datasight.explore covering duplicate handling and error paths."""

import duckdb
import pytest

from datasight.exceptions import ConfigurationError
from datasight.explore import (
    add_files_to_connection,
    create_ephemeral_session,
    save_ephemeral_as_project,
)


def test_multiple_duckdb_files_attached_with_aliases(tmp_path):
    """Two DuckDB files get attached with unique aliases."""
    db1 = tmp_path / "a.duckdb"
    db2 = tmp_path / "a_db.duckdb"  # name that could collide after sanitizing

    conn1 = duckdb.connect(str(db1))
    conn1.execute("CREATE TABLE t1 (x INT)")
    conn1.execute("INSERT INTO t1 VALUES (1)")
    conn1.close()

    conn2 = duckdb.connect(str(db2))
    conn2.execute("CREATE TABLE t2 (y INT)")
    conn2.execute("INSERT INTO t2 VALUES (2)")
    conn2.close()

    # Also add a CSV to prevent the "single duckdb" fast path
    csv_file = tmp_path / "extra.csv"
    csv_file.write_text("a\n1\n", encoding="utf-8")

    runner, tables = create_ephemeral_session([str(db1), str(db2), str(csv_file)])

    names = {t["name"] for t in tables}
    assert "t1" in names
    assert "t2" in names
    assert "extra" in names

    runner.close()


def test_duplicate_view_names_from_different_dbs(tmp_path):
    """Two DuckDB files with same table name get disambiguated."""
    db1 = tmp_path / "one.duckdb"
    db2 = tmp_path / "two.duckdb"

    for p in (db1, db2):
        conn = duckdb.connect(str(p))
        conn.execute("CREATE TABLE common (x INT)")
        conn.close()

    csv_file = tmp_path / "extra.csv"
    csv_file.write_text("a\n1\n", encoding="utf-8")

    runner, tables = create_ephemeral_session([str(db1), str(db2), str(csv_file)])
    names = {t["name"] for t in tables}
    assert "common" in names
    assert "common_2" in names

    runner.close()


def test_single_duckdb_file_with_only_views(tmp_path):
    """A DuckDB file that contains views should be opened directly."""
    db = tmp_path / "views.duckdb"
    conn = duckdb.connect(str(db))
    conn.execute("CREATE TABLE base (x INT)")
    conn.execute("INSERT INTO base VALUES (1)")
    conn.execute("CREATE VIEW my_view AS SELECT * FROM base")
    conn.close()

    runner, tables = create_ephemeral_session([str(db)])
    names = {t["name"] for t in tables}
    assert "base" in names
    assert "my_view" in names
    runner.close()


def test_single_duckdb_file_empty_raises(tmp_path):
    """A DuckDB file with no tables raises ConfigurationError."""
    db = tmp_path / "empty.duckdb"
    conn = duckdb.connect(str(db))
    conn.close()

    with pytest.raises(ConfigurationError):
        create_ephemeral_session([str(db)])


def test_single_duckdb_file_unreadable_raises(tmp_path):
    """A corrupt DuckDB file bubbles up as ConfigurationError."""
    bad = tmp_path / "bad.duckdb"
    bad.write_bytes(b"not a duckdb file at all")

    with pytest.raises(ConfigurationError):
        create_ephemeral_session([str(bad)])


def test_add_files_to_connection_basic(tmp_path):
    """add_files_to_connection adds new CSV views to an existing connection."""
    conn = duckdb.connect(":memory:")

    csv1 = tmp_path / "a.csv"
    csv1.write_text("x\n1\n", encoding="utf-8")

    tables = add_files_to_connection(conn, [str(csv1)], existing_table_names=set())
    assert len(tables) == 1
    assert tables[0]["name"] == "a"

    df = conn.execute("SELECT * FROM a").fetchdf()
    assert len(df) == 1
    conn.close()


def test_add_files_to_connection_with_duckdb(tmp_path):
    """add_files_to_connection supports .duckdb file attachment."""
    conn = duckdb.connect(":memory:")

    db = tmp_path / "src.duckdb"
    c2 = duckdb.connect(str(db))
    c2.execute("CREATE TABLE foo (y INT)")
    c2.execute("INSERT INTO foo VALUES (99)")
    c2.close()

    tables = add_files_to_connection(conn, [str(db)], existing_table_names=set())
    assert any(t["name"] == "foo" for t in tables)
    conn.close()


def test_add_files_to_connection_invalid_raises(tmp_path):
    conn = duckdb.connect(":memory:")
    with pytest.raises(ConfigurationError):
        add_files_to_connection(conn, ["/nonexistent/thing.csv"], existing_table_names=set())
    conn.close()


def test_add_files_to_connection_duplicate_name(tmp_path):
    """Duplicate names get a numeric suffix."""
    conn = duckdb.connect(":memory:")
    csv = tmp_path / "data.csv"
    csv.write_text("x\n1\n", encoding="utf-8")

    # Pretend "data" already exists
    conn.execute("CREATE TABLE data (x INT)")
    tables = add_files_to_connection(conn, [str(csv)], existing_table_names={"data"})
    assert tables[0]["name"] == "data_2"
    conn.close()


def test_save_project_overwrites_existing_duckdb(tmp_path):
    """Saving over an existing data.duckdb should remove the old one."""
    csv = tmp_path / "d.csv"
    csv.write_text("x\n1\n", encoding="utf-8")

    project_dir = tmp_path / "proj"
    project_dir.mkdir()
    # Pre-existing data.duckdb to trigger the unlink path
    (project_dir / "data.duckdb").write_bytes(b"junk")

    runner, tables = create_ephemeral_session([str(csv)])
    save_ephemeral_as_project(runner, tables, str(project_dir))

    # new file should be a valid duckdb
    conn = duckdb.connect(str(project_dir / "data.duckdb"), read_only=True)
    result = conn.execute("SELECT * FROM d").fetchall()
    assert result == [(1,)]
    conn.close()
    runner.close()


def test_save_project_with_mixed_duckdb_and_csv(tmp_path):
    """When tables_info mixes duckdb source_table entries and CSV, save attaches both."""
    src_db = tmp_path / "src.duckdb"
    c = duckdb.connect(str(src_db))
    c.execute("CREATE TABLE items (id INT)")
    c.execute("INSERT INTO items VALUES (42)")
    c.close()

    csv = tmp_path / "orders.csv"
    csv.write_text("order_id\n7\n", encoding="utf-8")

    runner, tables = create_ephemeral_session([str(src_db), str(csv)])
    project_dir = tmp_path / "proj"
    save_ephemeral_as_project(runner, tables, str(project_dir))

    # Project duckdb file and env/schema should be created
    assert (project_dir / "data.duckdb").exists()
    assert (project_dir / ".env").exists()
    # Both names captured in schema description
    schema_text = (project_dir / "schema_description.md").read_text()
    assert "items" in schema_text
    assert "orders" in schema_text
    runner.close()
