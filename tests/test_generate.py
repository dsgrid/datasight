"""Tests for datasight.generate module."""

import pytest

from datasight.generate import (
    build_generation_context,
    parse_generation_response,
    sample_enum_columns,
)
from datasight.schema import ColumnInfo, TableInfo


class TestParseGenerationResponse:
    """Tests for parse_generation_response."""

    def test_valid_response(self):
        """Parse a well-formed response with both markers."""
        text = (
            "--- schema_description.md ---\n"
            "# My Schema\nSome description\n\n"
            "--- queries.yaml ---\n"
            "- question: How many rows?\n"
            "  sql: SELECT COUNT(*) FROM t\n"
        )
        schema, queries = parse_generation_response(text)

        assert schema is not None
        assert "My Schema" in schema
        assert queries is not None
        assert "question" in queries

    def test_missing_markers(self):
        """Return raw text as schema when markers missing."""
        text = "Just some plain text without markers"
        schema, queries = parse_generation_response(text)

        assert schema == "Just some plain text without markers"
        assert queries is None

    def test_empty_response(self):
        """Handle empty response."""
        schema, queries = parse_generation_response("")

        assert schema is None
        assert queries is None

    def test_only_schema_marker(self):
        """Handle response with only schema marker."""
        text = "--- schema_description.md ---\n# Schema\n"
        schema, queries = parse_generation_response(text)

        # Falls through to raw text path since queries marker missing
        assert schema is not None
        assert queries is None


class TestBuildGenerationContext:
    """Tests for build_generation_context."""

    def test_basic_context(self):
        """Build context without user description."""
        tables = [
            TableInfo(
                name="users",
                columns=[
                    ColumnInfo(name="id", dtype="INTEGER"),
                    ColumnInfo(name="name", dtype="VARCHAR"),
                ],
                row_count=100,
            )
        ]
        system, user_msg = build_generation_context(tables, "duckdb", "")

        assert "expert data analyst" in system
        assert "users" in user_msg
        assert "duckdb" in user_msg

    def test_with_user_description(self):
        """Include user description in context."""
        tables = [TableInfo(name="t", columns=[], row_count=10)]
        system, user_msg = build_generation_context(
            tables, "duckdb", "", user_description="This is healthcare data"
        )

        assert "User-Provided Context" in user_msg
        assert "healthcare data" in user_msg

    def test_with_samples(self):
        """Include sampled values in context."""
        tables = [TableInfo(name="t", columns=[], row_count=10)]
        samples = "**t.status** (3 distinct): active, inactive, pending"
        system, user_msg = build_generation_context(tables, "duckdb", samples)

        assert "Sampled Column Values" in user_msg
        assert "active, inactive, pending" in user_msg


class TestSampleEnumColumns:
    """Tests for sample_enum_columns."""

    @pytest.mark.asyncio
    async def test_samples_string_columns(self):
        """Sample low-cardinality string columns."""
        import duckdb

        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE t (id INT, status VARCHAR, name VARCHAR)")
        conn.execute(
            "INSERT INTO t VALUES (1, 'active', 'Alice'), (2, 'inactive', 'Bob'), (3, 'active', 'Charlie')"
        )

        async def run_sql(sql):
            return conn.execute(sql).fetchdf()

        tables = [
            TableInfo(
                name="t",
                columns=[
                    ColumnInfo(name="id", dtype="INTEGER"),
                    ColumnInfo(name="status", dtype="VARCHAR"),
                    ColumnInfo(name="name", dtype="VARCHAR"),
                ],
                row_count=3,
            )
        ]
        result = await sample_enum_columns(run_sql, tables)

        assert "t.status" in result
        assert "active" in result
        conn.close()

    @pytest.mark.asyncio
    async def test_skips_high_cardinality(self):
        """Skip columns with > 50 distinct values."""
        import duckdb

        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE t (id INT, val VARCHAR)")
        for i in range(100):
            conn.execute(f"INSERT INTO t VALUES ({i}, 'val_{i}')")

        async def run_sql(sql):
            return conn.execute(sql).fetchdf()

        tables = [
            TableInfo(
                name="t",
                columns=[ColumnInfo(name="val", dtype="VARCHAR")],
                row_count=100,
            )
        ]
        result = await sample_enum_columns(run_sql, tables)

        assert result == ""
        conn.close()
