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


def test_measure_rule_rejects_non_default_aggregation_without_explicit_request():
    schema = {
        "generation_fuel": {
            "report_date",
            "net_generation_mwh",
            "plant_id_eia",
            "energy_source_code",
        },
        "plants": {"plant_id_eia", "state"},
    }
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
        SELECT g.report_date, SUM(g.net_generation_mwh) AS wind_generation_mwh
        FROM generation_fuel g
        JOIN plants p USING (plant_id_eia)
        WHERE g.energy_source_code = 'WND' AND p.state = 'CO'
        GROUP BY g.report_date
    """

    result = validate_sql(
        sql,
        schema,
        measure_rules=measure_rules,
        user_question="make a plot of net_generation_mwh for fuel type of wind in colorado",
    )

    assert not result.valid
    assert "default `max`" in result.error_message


def test_measure_rule_allows_explicit_total_request_for_non_default_aggregation():
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

    result = validate_sql(
        sql,
        schema,
        measure_rules=measure_rules,
        user_question="plot the total net_generation_mwh over time",
    )

    assert result.valid


def test_measure_rule_rejects_disallowed_aggregation_even_when_requested():
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
        user_question="show the total net_generation_mwh",
    )

    assert not result.valid
    assert "not allowed" in result.error_message
