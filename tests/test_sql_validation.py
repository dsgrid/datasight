"""Tests for SQL validation."""

from datasight.sql_validation import (
    build_measure_rule_map,
    build_schema_map,
    validate_sql,
)


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


def test_measure_rule_allows_any_aggregation_in_allowed_list():
    schema = {"generation_fuel": {"report_date", "net_generation_mwh"}}
    measure_rules = build_measure_rule_map(
        [
            {
                "table": "generation_fuel",
                "column": "net_generation_mwh",
                "default_aggregation": "max",
                "allowed_aggregations": ["sum", "avg", "min", "max"],
            }
        ]
    )
    sql = """
        SELECT report_date, SUM(net_generation_mwh) AS total_generation
        FROM generation_fuel
        GROUP BY report_date
    """

    result = validate_sql(sql, schema, measure_rules=measure_rules)

    assert result.valid


def test_measure_rule_rejects_disallowed_aggregation():
    schema = {"generation_fuel": {"net_generation_mwh"}}
    measure_rules = build_measure_rule_map(
        [
            {
                "table": "generation_fuel",
                "column": "net_generation_mwh",
                "default_aggregation": "max",
                "allowed_aggregations": ["max"],
            }
        ]
    )

    result = validate_sql(
        "SELECT SUM(net_generation_mwh) FROM generation_fuel",
        schema,
        measure_rules=measure_rules,
    )

    assert not result.valid
    assert "not allowed" in result.error_message
