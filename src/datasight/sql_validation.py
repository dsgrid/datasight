"""
Validate LLM-generated SQL against the known database schema.

Uses sqlglot to parse SQL and extract table/column references, then checks
them against the discovered schema to catch hallucinated identifiers before
executing queries.
"""

from __future__ import annotations

from dataclasses import dataclass

try:
    import sqlglot
    from sqlglot import exp

    HAS_SQLGLOT = True
except ImportError:
    HAS_SQLGLOT = False


@dataclass
class ValidationResult:
    """Result of validating SQL against the schema."""

    valid: bool
    errors: list[str]

    @property
    def error_message(self) -> str:
        return "; ".join(self.errors)


def validate_sql(
    sql: str,
    schema: dict[str, set[str]],
) -> ValidationResult:
    """Validate that table references in *sql* exist in *schema*.

    Only validates table names — column validation is left to the database
    engine, which produces clear error messages the LLM can self-correct from.
    Table validation catches the most common hallucination (inventing table
    names) with near-zero false positives.

    Parameters
    ----------
    sql:
        The SQL query to validate.
    schema:
        Mapping of lowercase table name -> set of lowercase column names.

    Returns
    -------
    ValidationResult with ``valid=True`` if all references check out, or a
    list of human-readable error strings otherwise.
    """
    if not HAS_SQLGLOT:
        return ValidationResult(valid=True, errors=[])

    try:
        parsed = sqlglot.parse(sql, read="duckdb")
    except sqlglot.errors.ParseError as e:
        return ValidationResult(valid=False, errors=[f"SQL parse error: {e}"])

    if not parsed or parsed[0] is None:
        return ValidationResult(valid=True, errors=[])

    tree = parsed[0]
    errors: list[str] = []

    # Collect CTE names so we don't flag them as unknown tables
    cte_names: set[str] = set()
    for cte in tree.find_all(exp.CTE):
        alias = cte.alias
        if alias:
            cte_names.add(alias.lower())

    # Collect subquery/derived-table aliases
    subquery_aliases: set[str] = set()
    for subq in tree.find_all(exp.Subquery):
        alias = subq.alias
        if alias:
            subquery_aliases.add(alias.lower())

    # Validate table references
    for tbl in tree.find_all(exp.Table):
        table_name = tbl.name
        if not table_name:
            continue
        lower_name = table_name.lower()

        # Skip CTEs and subquery aliases
        if lower_name in cte_names or lower_name in subquery_aliases:
            continue

        if lower_name not in schema:
            errors.append(
                f"Unknown table '{table_name}'. "
                f"Available tables: {', '.join(sorted(schema.keys()))}"
            )

    return (
        ValidationResult(valid=False, errors=errors)
        if errors
        else ValidationResult(valid=True, errors=[])
    )


def build_schema_map(schema_info: list[dict]) -> dict[str, set[str]]:
    """Build a schema lookup map from the AppState.schema_info structure.

    Parameters
    ----------
    schema_info:
        List of table dicts with "name" and "columns" keys, as stored in
        ``AppState.schema_info``.

    Returns
    -------
    Dict mapping lowercase table name to set of lowercase column names.
    """
    result: dict[str, set[str]] = {}
    for table in schema_info:
        tname = table["name"].lower()
        cols = {c["name"].lower() for c in table.get("columns", [])}
        result[tname] = cols
    return result
