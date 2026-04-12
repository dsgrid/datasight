"""Tests for datasight.integrity.build_integrity_overview."""

from __future__ import annotations

from dataclasses import asdict

import pytest

from datasight.integrity import build_integrity_overview
from datasight.runner import DuckDBRunner
from datasight.schema import introspect_schema


def _schema_info(tables):
    return [
        {
            "name": t.name,
            "row_count": t.row_count,
            "columns": [asdict(c) for c in t.columns],
        }
        for t in tables
    ]


@pytest.mark.asyncio
async def test_integrity_overview_infers_pks_and_fks(test_duckdb_path):
    runner = DuckDBRunner(test_duckdb_path)
    try:
        tables = await introspect_schema(runner.run_sql, runner=runner)
        schema_info = _schema_info(tables)
        result = await build_integrity_overview(schema_info, runner.run_sql)
    finally:
        runner.close()

    assert result["table_count"] == 2
    pk_tables = {pk["table"] for pk in result["primary_keys"]}
    assert {"products", "orders"} <= pk_tables
    # Every inferred PK is unique on this tiny dataset
    for pk in result["primary_keys"]:
        assert pk["is_unique"] is True
    # No orphans and no join explosions expected for the seed data
    assert result["orphan_foreign_keys"] == []
    assert result["join_explosions"] == []
    joined_notes = " ".join(result["notes"]).lower()
    assert "orphan" in joined_notes or "references resolve" in joined_notes
    assert "unique" in joined_notes


@pytest.mark.asyncio
async def test_integrity_overview_with_declared_joins(test_duckdb_path):
    runner = DuckDBRunner(test_duckdb_path)
    try:
        tables = await introspect_schema(runner.run_sql, runner=runner)
        schema_info = _schema_info(tables)
        declared = [
            {
                "child_table": "orders",
                "child_column": "product_id",
                "parent_table": "products",
                "parent_column": "id",
            }
        ]
        result = await build_integrity_overview(
            schema_info, runner.run_sql, declared_joins=declared
        )
    finally:
        runner.close()

    # With declared_joins, the fk-inference branch is skipped but the join
    # still resolves.
    assert result["orphan_foreign_keys"] == []


@pytest.mark.asyncio
async def test_integrity_overview_detects_orphans_and_explosion(tmp_path):
    import duckdb

    db_path = tmp_path / "explode.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE products (id INTEGER, name VARCHAR)")
    # Duplicate id => not a unique PK => triggers duplicate_keys branch AND
    # row multiplication (join explosion) when orders joins on product_id.
    conn.execute("INSERT INTO products VALUES (1, 'A'), (1, 'A dup'), (1, 'A dup2'), (2, 'B')")
    conn.execute("CREATE TABLE orders (id INTEGER, product_id INTEGER)")
    conn.execute(
        "INSERT INTO orders VALUES (1, 1), (2, 2), (3, 999)"
    )  # 999 is an orphan; product_id=1 multiplies 3x
    conn.close()

    runner = DuckDBRunner(str(db_path))
    try:
        tables = await introspect_schema(runner.run_sql, runner=runner)
        schema_info = _schema_info(tables)
        # Declare the join explicitly since products has a non-unique id
        declared = [
            {
                "child_table": "orders",
                "child_column": "product_id",
                "parent_table": "products",
                "parent_column": "id",
            }
        ]
        result = await build_integrity_overview(
            schema_info, runner.run_sql, declared_joins=declared
        )
    finally:
        runner.close()

    assert any(dk["table"] == "products" for dk in result["duplicate_keys"])
    assert any(o["orphan_count"] > 0 for o in result["orphan_foreign_keys"])
    assert any(e["explosion_factor"] > 1.0 for e in result["join_explosions"])


@pytest.mark.asyncio
async def test_integrity_overview_empty_tables_produce_no_pks(tmp_path):
    import duckdb

    db_path = tmp_path / "empty.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE lonely (id INTEGER, name VARCHAR)")
    # No rows
    conn.close()

    runner = DuckDBRunner(str(db_path))
    try:
        tables = await introspect_schema(runner.run_sql, runner=runner)
        schema_info = _schema_info(tables)
        result = await build_integrity_overview(schema_info, runner.run_sql)
    finally:
        runner.close()

    assert result["primary_keys"] == []
    notes = " ".join(result["notes"]).lower()
    assert "no obvious primary key" in notes
    assert "no foreign-key relationships" in notes


@pytest.mark.asyncio
async def test_integrity_overview_fk_inference_skips_unknown_parent(tmp_path):
    import duckdb

    db_path = tmp_path / "noparent.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE orders (id INTEGER, product_id INTEGER)")
    conn.execute("INSERT INTO orders VALUES (1, 10), (2, 20)")
    conn.close()

    runner = DuckDBRunner(str(db_path))
    try:
        tables = await introspect_schema(runner.run_sql, runner=runner)
        schema_info = _schema_info(tables)
        result = await build_integrity_overview(schema_info, runner.run_sql)
    finally:
        runner.close()

    # No products table exists, so product_id must not be inferred as an FK.
    assert result["orphan_foreign_keys"] == []
    assert result["join_explosions"] == []
