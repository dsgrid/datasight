"""Shared test fixtures for datasight."""

import sqlite3

import duckdb
import pytest


@pytest.fixture(scope="session")
def test_duckdb_path(tmp_path_factory):
    """Create a small DuckDB with known test data."""
    db_path = tmp_path_factory.mktemp("db") / "test.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            category VARCHAR,
            price DECIMAL(10,2)
        )
    """)
    conn.execute("""
        INSERT INTO products VALUES
        (1, 'Widget A', 'electronics', 29.99),
        (2, 'Widget B', 'electronics', 49.99),
        (3, 'Gadget X', 'tools', 15.50),
        (4, 'Gadget Y', 'tools', 22.00),
        (5, 'Doohickey', 'misc', 5.99)
    """)
    conn.execute("""
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            product_id INTEGER,
            quantity INTEGER,
            order_date DATE,
            customer_state VARCHAR(2)
        )
    """)
    conn.execute("""
        INSERT INTO orders VALUES
        (1, 1, 10, '2024-01-15', 'CA'),
        (2, 2, 5, '2024-01-20', 'NY'),
        (3, 1, 8, '2024-02-10', 'TX'),
        (4, 3, 20, '2024-02-15', 'CA'),
        (5, 4, 3, '2024-03-01', 'NY'),
        (6, 5, 50, '2024-03-10', 'TX'),
        (7, 2, 7, '2024-03-15', 'CA'),
        (8, 1, 12, '2024-04-01', 'FL'),
        (9, 3, 15, '2024-04-10', 'FL'),
        (10, 4, 6, '2024-04-20', 'CA')
    """)
    conn.close()
    return str(db_path)


@pytest.fixture(scope="session")
def test_sqlite_path(tmp_path_factory):
    """Create a small SQLite DB with the same test data."""
    db_path = tmp_path_factory.mktemp("db") / "test.sqlite"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT,
            category TEXT,
            price REAL
        )
    """)
    conn.executemany(
        "INSERT INTO products VALUES (?, ?, ?, ?)",
        [
            (1, "Widget A", "electronics", 29.99),
            (2, "Widget B", "electronics", 49.99),
            (3, "Gadget X", "tools", 15.50),
            (4, "Gadget Y", "tools", 22.00),
            (5, "Doohickey", "misc", 5.99),
        ],
    )
    conn.execute("""
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            product_id INTEGER,
            quantity INTEGER,
            order_date TEXT,
            customer_state TEXT
        )
    """)
    conn.executemany(
        "INSERT INTO orders VALUES (?, ?, ?, ?, ?)",
        [
            (1, 1, 10, "2024-01-15", "CA"),
            (2, 2, 5, "2024-01-20", "NY"),
            (3, 1, 8, "2024-02-10", "TX"),
            (4, 3, 20, "2024-02-15", "CA"),
            (5, 4, 3, "2024-03-01", "NY"),
            (6, 5, 50, "2024-03-10", "TX"),
            (7, 2, 7, "2024-03-15", "CA"),
            (8, 1, 12, "2024-04-01", "FL"),
            (9, 3, 15, "2024-04-10", "FL"),
            (10, 4, 6, "2024-04-20", "CA"),
        ],
    )
    conn.commit()
    conn.close()
    return str(db_path)


SCHEMA_DESCRIPTION = """\
# Test Store Data

## Tables

### products
Product catalog with id, name, category, and price.

### orders
Order records with product_id (FK to products), quantity, order_date, and customer_state.

## Key Columns
- **id**: Primary key in both tables
- **product_id**: Foreign key from orders to products
- **category**: Product category (electronics, tools, misc)
- **customer_state**: Two-letter US state code (CA, NY, TX, FL)
"""

EXAMPLE_QUERIES_YAML = """\
- question: How many orders are there?
  sql: SELECT COUNT(*) AS order_count FROM orders

- question: Total quantity sold by product
  sql: |
    SELECT p.name, SUM(o.quantity) AS total_qty
    FROM orders o
    JOIN products p ON o.product_id = p.id
    GROUP BY p.name
    ORDER BY total_qty DESC
"""


@pytest.fixture()
def project_dir(tmp_path, test_duckdb_path):
    """Create a project directory with .env and config files pointing to test DB."""
    env_content = (
        f"LLM_PROVIDER=ollama\n"
        f"OLLAMA_MODEL=qwen3.5:35b-a3b\n"
        f"DB_MODE=duckdb\n"
        f"DB_PATH={test_duckdb_path}\n"
    )
    (tmp_path / ".env").write_text(env_content)
    (tmp_path / "schema_description.md").write_text(SCHEMA_DESCRIPTION)
    (tmp_path / "queries.yaml").write_text(EXAMPLE_QUERIES_YAML)
    return str(tmp_path)
