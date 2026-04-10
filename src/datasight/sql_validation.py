"""
Validate LLM-generated SQL against the known database schema.

Uses sqlglot to parse SQL and extract table/column references, then checks
them against the discovered schema to catch hallucinated identifiers before
executing queries.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any

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


@dataclass(frozen=True)
class MeasureAggregationRule:
    """Project-defined aggregation semantics for a physical measure column."""

    table: str
    column: str
    default_aggregation: str
    allowed_aggregations: tuple[str, ...]


def build_measure_rule_map(
    overrides: list[dict[str, Any]],
) -> dict[tuple[str, str], MeasureAggregationRule]:
    """Build a lookup of project-defined physical measure aggregation rules."""
    rules: dict[tuple[str, str], MeasureAggregationRule] = {}
    for item in overrides:
        table = str(item.get("table") or "").strip().lower()
        column = str(item.get("column") or "").strip().lower()
        default = str(item.get("default_aggregation") or "").strip().lower()
        if not table or not column or not default:
            continue
        allowed = tuple(
            str(value).strip().lower()
            for value in (item.get("allowed_aggregations") or [])
            if str(value).strip()
        ) or (default,)
        rules[(table, column)] = MeasureAggregationRule(
            table=table,
            column=column,
            default_aggregation=default,
            allowed_aggregations=allowed,
        )
    return rules


_AGGREGATION_KEYWORDS = {
    "sum": ("sum", "total", "totaled", "cumulative", "combined"),
    "avg": ("avg", "average", "mean"),
    "max": ("max", "maximum", "highest", "peak"),
    "min": ("min", "minimum", "lowest"),
}


def _requested_aggregations(question: str) -> set[str]:
    lower = question.lower()
    requested: set[str] = set()
    for aggregation, keywords in _AGGREGATION_KEYWORDS.items():
        if any(re.search(rf"\b{re.escape(keyword)}\b", lower) for keyword in keywords):
            requested.add(aggregation)
    return requested


def _find_measure_rule(
    column: "exp.Column",
    alias_to_table: dict[str, str],
    measure_rules: dict[tuple[str, str], MeasureAggregationRule],
) -> MeasureAggregationRule | None:
    column_name = str(column.name or "").strip().lower()
    if not column_name:
        return None

    table_name = str(column.table or "").strip().lower()
    if table_name:
        resolved_table = alias_to_table.get(table_name, table_name)
        return measure_rules.get((resolved_table, column_name))

    matches = [rule for (table, col), rule in measure_rules.items() if col == column_name]
    if len(matches) == 1:
        return matches[0]
    return None


def _validate_measure_aggregations(
    tree: "exp.Expression",
    measure_rules: dict[tuple[str, str], MeasureAggregationRule],
    user_question: str,
) -> list[str]:
    if not measure_rules:
        return []

    alias_to_table: dict[str, str] = {}
    for tbl in tree.find_all(exp.Table):
        table_name = str(tbl.name or "").strip().lower()
        if not table_name:
            continue
        alias_to_table[table_name] = table_name
        alias = str(tbl.alias or "").strip().lower()
        if alias:
            alias_to_table[alias] = table_name

    requested = _requested_aggregations(user_question)
    errors: list[str] = []
    seen: set[tuple[str, str, str]] = set()
    agg_types: tuple[type[exp.Expression], ...] = (exp.Sum, exp.Avg, exp.Max, exp.Min)

    for node in tree.walk():
        if not isinstance(node, agg_types):
            continue
        columns = list(node.find_all(exp.Column))
        if len(columns) != 1:
            continue
        rule = _find_measure_rule(columns[0], alias_to_table, measure_rules)
        if rule is None:
            continue

        actual = node.key.lower()
        key = (rule.table, rule.column, actual)
        if key in seen:
            continue
        seen.add(key)

        if actual not in rule.allowed_aggregations:
            errors.append(
                f"Aggregation `{actual}` is not allowed for project measure "
                f"`{rule.table}.{rule.column}`. Allowed aggregations: "
                f"{', '.join(rule.allowed_aggregations)}."
            )
            continue

        if actual != rule.default_aggregation and actual not in requested:
            errors.append(
                f"Aggregation `{actual}` for project measure `{rule.table}.{rule.column}` "
                f"conflicts with its default `{rule.default_aggregation}`. "
                f"Use `{rule.default_aggregation}` unless the user explicitly asks for "
                f"{actual}."
            )

    return errors


def validate_sql(
    sql: str,
    schema: dict[str, set[str]],
    dialect: str = "duckdb",
    measure_rules: dict[tuple[str, str], MeasureAggregationRule] | None = None,
    user_question: str = "",
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

    _SQLGLOT_DIALECTS = {
        "duckdb": "duckdb",
        "postgres": "postgres",
        "sqlite": "sqlite",
    }
    sqlglot_dialect = _SQLGLOT_DIALECTS.get(dialect, "duckdb")

    try:
        parsed = sqlglot.parse(sql, read=sqlglot_dialect)
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

    errors.extend(_validate_measure_aggregations(tree, measure_rules or {}, user_question))

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
