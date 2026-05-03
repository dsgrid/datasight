"""Extra tests for datasight.explore covering duplicate handling and error paths."""

import duckdb
import pandas as pd
import pytest

from datasight.exceptions import ConfigurationError
from datasight.explore import (
    add_files_to_connection,
    create_ephemeral_session,
    detect_file_type,
    save_ephemeral_as_project,
    scan_directory_for_data_files,
)


def _write_xlsx(path, sheets: dict[str, pd.DataFrame]) -> None:
    """Write a multi-sheet Excel workbook using the openpyxl engine."""
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, sheet_name=name, index=False)


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
    """add_files_to_connection uses auto import mode for new CSV tables."""
    conn = duckdb.connect(":memory:")

    csv1 = tmp_path / "a.csv"
    csv1.write_text("x\n1\n", encoding="utf-8")

    tables = add_files_to_connection(conn, [str(csv1)], existing_table_names=set())
    assert len(tables) == 1
    assert tables[0]["name"] == "a"
    assert tables[0]["import_mode"] == "table"

    relation = conn.execute(
        "SELECT table_type FROM information_schema.tables WHERE table_schema='main' AND table_name='a'"
    ).fetchone()
    assert relation == ("BASE TABLE",)

    df = conn.execute("SELECT * FROM a").fetchdf()
    assert len(df) == 1
    conn.close()


def test_create_ephemeral_session_csv_view_mode(tmp_path):
    """CSV inputs can stay source-backed views when requested explicitly."""
    csv = tmp_path / "generation.csv"
    csv.write_text("x\n1\n2\n", encoding="utf-8")

    runner, tables = create_ephemeral_session([str(csv)], import_mode="view")
    with runner:
        assert tables[0]["import_mode"] == "view"
        relation = runner._conn.execute(  # ty: ignore[unresolved-attribute]
            "SELECT table_type FROM information_schema.tables "
            "WHERE table_schema='main' AND table_name='generation'"
        ).fetchone()
        assert relation == ("VIEW",)


def test_create_ephemeral_session_parquet_auto_mode_uses_view(tmp_path):
    """Parquet auto mode remains view-backed."""
    parquet = tmp_path / "generation.parquet"
    pd.DataFrame({"x": [1, 2]}).to_parquet(parquet)

    runner, tables = create_ephemeral_session([str(parquet)])
    with runner:
        assert tables[0]["import_mode"] == "view"
        relation = runner._conn.execute(  # ty: ignore[unresolved-attribute]
            "SELECT table_type FROM information_schema.tables "
            "WHERE table_schema='main' AND table_name='generation'"
        ).fetchone()
        assert relation == ("VIEW",)


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


def test_detect_file_type_xlsx(tmp_path):
    xlsx = tmp_path / "data.xlsx"
    _write_xlsx(xlsx, {"Sheet1": pd.DataFrame({"x": [1]})})
    assert detect_file_type(str(xlsx)) == "xlsx"


def test_scan_directory_includes_xlsx(tmp_path):
    csv = tmp_path / "a.csv"
    csv.write_text("x\n1\n", encoding="utf-8")
    xlsx = tmp_path / "b.xlsx"
    _write_xlsx(xlsx, {"Sheet1": pd.DataFrame({"y": [2]})})

    files, _ = scan_directory_for_data_files(tmp_path)
    by_name = {f["name"]: f["type"] for f in files}
    assert by_name == {"a.csv": "csv", "b.xlsx": "xlsx"}


def test_xlsx_single_sheet_uses_file_stem_as_table_name(tmp_path):
    xlsx = tmp_path / "plants.xlsx"
    _write_xlsx(
        xlsx,
        {"Sheet1": pd.DataFrame({"plant_id": [1, 2], "name": ["A", "B"]})},
    )

    runner, tables = create_ephemeral_session([str(xlsx)])
    with runner:
        assert [t["name"] for t in tables] == ["plants"]
        assert tables[0]["type"] == "xlsx"
        assert tables[0]["sheet_name"] == "Sheet1"
        rows = runner._conn.execute(  # ty: ignore[unresolved-attribute]
            "SELECT plant_id, name FROM plants ORDER BY plant_id"
        ).fetchall()
        assert rows == [(1, "A"), (2, "B")]


def test_xlsx_multi_sheet_creates_one_table_per_sheet(tmp_path):
    xlsx = tmp_path / "grid.xlsx"
    _write_xlsx(
        xlsx,
        {
            "generation": pd.DataFrame({"mwh": [10, 20]}),
            "plants": pd.DataFrame({"plant_id": [1, 2, 3]}),
        },
    )

    runner, tables = create_ephemeral_session([str(xlsx)])
    with runner:
        names = {t["name"] for t in tables}
        assert names == {"generation", "plants"}
        assert all(t["type"] == "xlsx" for t in tables)
        assert runner._conn.execute("SELECT COUNT(*) FROM generation").fetchone()[0] == 2  # ty: ignore[unresolved-attribute, not-subscriptable]
        assert runner._conn.execute("SELECT COUNT(*) FROM plants").fetchone()[0] == 3  # ty: ignore[unresolved-attribute, not-subscriptable]


def test_xlsx_sheet_name_collision_with_existing_table(tmp_path):
    """A multi-sheet workbook whose sheet name collides gets a _2 suffix."""
    csv = tmp_path / "generation.csv"
    csv.write_text("x\n1\n", encoding="utf-8")
    xlsx = tmp_path / "wb.xlsx"
    # Two sheets => sheet name (not file stem) is used for table naming
    _write_xlsx(
        xlsx,
        {"generation": pd.DataFrame({"y": [9]}), "plants": pd.DataFrame({"id": [1]})},
    )

    runner, tables = create_ephemeral_session([str(csv), str(xlsx)])
    with runner:
        names = [t["name"] for t in tables]
        assert "generation" in names  # from CSV
        assert "generation_2" in names  # from xlsx sheet, deduped
        assert "plants" in names


def test_xlsx_single_sheet_collides_on_file_stem(tmp_path):
    """A single-sheet workbook uses the file stem; collisions get a _2 suffix."""
    csv = tmp_path / "report.csv"
    csv.write_text("x\n1\n", encoding="utf-8")
    xlsx = tmp_path / "report.xlsx"
    _write_xlsx(xlsx, {"Sheet1": pd.DataFrame({"y": [42]})})

    runner, tables = create_ephemeral_session([str(csv), str(xlsx)])
    with runner:
        names = [t["name"] for t in tables]
        assert "report" in names
        assert "report_2" in names


def test_add_files_to_connection_with_xlsx(tmp_path):
    conn = duckdb.connect(":memory:")
    xlsx = tmp_path / "extra.xlsx"
    _write_xlsx(
        xlsx,
        {"a": pd.DataFrame({"v": [1]}), "b": pd.DataFrame({"v": [2]})},
    )

    tables = add_files_to_connection(conn, [str(xlsx)], existing_table_names=set())
    names = {t["name"] for t in tables}
    assert names == {"a", "b"}
    assert conn.execute("SELECT v FROM a").fetchone() == (1,)
    assert conn.execute("SELECT v FROM b").fetchone() == (2,)
    conn.close()


def test_save_project_rebuilds_xlsx_sheets(tmp_path):
    xlsx = tmp_path / "src.xlsx"
    _write_xlsx(
        xlsx,
        {
            "raw": pd.DataFrame({"id": [1, 2]}),
            "clean": pd.DataFrame({"id": [10]}),
        },
    )

    runner, tables = create_ephemeral_session([str(xlsx)])
    project_dir = tmp_path / "proj"
    save_ephemeral_as_project(runner, tables, str(project_dir))
    runner.close()

    db_file = project_dir / "data.duckdb"
    assert db_file.exists()
    conn = duckdb.connect(str(db_file), read_only=True)
    try:
        assert conn.execute("SELECT COUNT(*) FROM raw").fetchone()[0] == 2  # ty: ignore[not-subscriptable]
        assert conn.execute("SELECT id FROM clean").fetchone() == (10,)
    finally:
        conn.close()
