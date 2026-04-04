"""Tests for SQL validation."""

from datasight.sql_validation import validate_sql, build_schema_map


SCHEMA = {
    "products": {"id", "name", "category", "price"},
    "orders": {"id", "product_id", "quantity", "order_date", "customer_state"},
}


def test_valid_simple_select():
    result = validate_sql("SELECT * FROM products", SCHEMA)
    assert result.valid
    assert not result.errors


def test_valid_join():
    sql = """
        SELECT p.name, SUM(o.quantity)
        FROM orders o JOIN products p ON o.product_id = p.id
        GROUP BY p.name
    """
    result = validate_sql(sql, SCHEMA)
    assert result.valid


def test_unknown_table():
    result = validate_sql("SELECT * FROM nonexistent", SCHEMA)
    assert not result.valid
    assert "nonexistent" in result.errors[0].lower()


def test_cte_not_flagged():
    sql = """
        WITH top_products AS (
            SELECT product_id, SUM(quantity) AS total
            FROM orders GROUP BY product_id
        )
        SELECT p.name, t.total
        FROM top_products t JOIN products p ON t.product_id = p.id
    """
    result = validate_sql(sql, SCHEMA)
    assert result.valid


def test_subquery_alias_not_flagged():
    sql = """
        SELECT sub.name FROM (SELECT name FROM products) sub
    """
    result = validate_sql(sql, SCHEMA)
    assert result.valid


def test_postgres_dialect():
    sql = "SELECT * FROM products WHERE price > 10"
    result = validate_sql(sql, SCHEMA, dialect="postgres")
    assert result.valid


def test_sqlite_dialect():
    sql = "SELECT * FROM products WHERE price > 10"
    result = validate_sql(sql, SCHEMA, dialect="sqlite")
    assert result.valid


def test_error_message_property():
    result = validate_sql("SELECT * FROM bad_table", SCHEMA)
    assert "bad_table" in result.error_message.lower()


def test_build_schema_map():
    schema_info = [
        {
            "name": "Products",
            "columns": [
                {"name": "ID", "dtype": "INTEGER"},
                {"name": "Name", "dtype": "VARCHAR"},
            ],
        }
    ]
    smap = build_schema_map(schema_info)
    assert "products" in smap
    assert "id" in smap["products"]
    assert "name" in smap["products"]
