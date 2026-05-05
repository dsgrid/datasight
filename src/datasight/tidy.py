"""Detect untidy dataset patterns and suggest tidy-reshape DDL.

Inspects table schemas and flags columns whose names appear to encode a
dimension value (year, month, quarter, hour, day) rather than holding a
single measure across rows. Each suggestion carries a previewable DuckDB
DDL statement so users can inspect or materialize the reshape.

The DDL emitted depends on the target object:

- ``CREATE OR REPLACE TABLE`` uses the canonical DuckDB ``UNPIVOT`` form,
  which is what ``tidy table`` runs and what ``tidy suggest`` displays.
- ``CREATE OR REPLACE VIEW`` falls back to ``UNION ALL`` branches because
  of a regression in the Python ``duckdb`` 1.5.2 binding — UNPIVOT stored
  inside a view fails to re-bind on a fresh connection in that binding,
  raising ``Binder Error: UNPIVOT name count mismatch``. (The standalone
  ``duckdb`` CLI 1.5.1 is unaffected, but datasight depends on the Python
  binding for every internal query path, so views created through
  datasight need the workaround to remain queryable from datasight
  itself.) Materialized tables don't re-bind, so they keep the cleaner
  UNPIVOT form.

Detection is deterministic and purely structural (column names plus
dtypes plus row count), so it runs without issuing extra SQL. The DDL is
DuckDB-specific — which matches the typical workflow for this feature:
untidy CSV / Parquet / Excel sources land in DuckDB before being
reshaped.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import asdict, dataclass
from typing import Any

from datasight.schema import _quote_identifier

MIN_GROUP_SIZE = 3
WIDE_TABLE_COLUMN_THRESHOLD = 30

_MONTH_TOKENS: frozenset[str] = frozenset(
    {
        "jan",
        "feb",
        "mar",
        "apr",
        "may",
        "jun",
        "jul",
        "aug",
        "sep",
        "sept",
        "oct",
        "nov",
        "dec",
        "january",
        "february",
        "march",
        "april",
        "june",
        "july",
        "august",
        "september",
        "october",
        "november",
        "december",
    }
)

_PERIOD_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("year_month", re.compile(r"^(?:19|20)\d{2}[-_]?(?:0[1-9]|1[0-2])$")),
    ("year_quarter", re.compile(r"^(?:19|20)\d{2}[-_]?q[1-4]$")),
    ("year", re.compile(r"^(?:y|yr|year)?(?:19|20)\d{2}$")),
    ("quarter", re.compile(r"^q[1-4]$")),
    ("hour", re.compile(r"^h(?:our|r)?[-_]?(?:0?\d|1\d|2[0-3])$")),
    ("day", re.compile(r"^d(?:ay)?[-_]?(?:0?[1-9]|[12]\d|3[01])$")),
    ("month_num", re.compile(r"^m(?:onth|o)?[-_]?(?:0?[1-9]|1[0-2])$")),
)


def _classify_period_token(token: str) -> str | None:
    """Return a period kind name if ``token`` looks like a period value, else None."""
    t = token.lower()
    if t in _MONTH_TOKENS:
        return "month"
    for kind, pattern in _PERIOD_PATTERNS:
        if pattern.match(t):
            return kind
    parts = re.split(r"[-_]", t)
    if len(parts) == 2:
        a, b = parts
        if a in _MONTH_TOKENS and re.fullmatch(r"(?:19|20)\d{2}", b):
            return "month_year"
        if b in _MONTH_TOKENS and re.fullmatch(r"(?:19|20)\d{2}", a):
            return "year_month_word"
    return None


def _split_prefix_period(name: str) -> tuple[str, str, str] | None:
    """Split ``name`` into (prefix, period_token, period_kind) or return None.

    Tries progressively longer trailing segments so that names like
    ``sales_2020_01`` resolve to ``("sales", "2020_01", "year_month")``.
    """
    parts = name.split("_")
    max_take = min(3, len(parts))
    for take in range(max_take, 0, -1):
        suffix_parts = parts[-take:]
        suffix = "_".join(suffix_parts)
        kind = _classify_period_token(suffix)
        if kind:
            prefix = "_".join(parts[:-take]) if take < len(parts) else ""
            return (prefix, suffix, kind)
        if take >= 2:
            suffix_concat = "".join(suffix_parts)
            kind = _classify_period_token(suffix_concat)
            if kind:
                prefix = "_".join(parts[:-take]) if take < len(parts) else ""
                return (prefix, suffix_concat, kind)
    kind = _classify_period_token(name)
    if kind:
        return ("", name, kind)
    return None


_PERIOD_COLUMN_NAMES: dict[str, str] = {
    "year": "year",
    "year_month": "year_month",
    "year_quarter": "year_quarter",
    "year_month_word": "year_month",
    "month_year": "year_month",
    "quarter": "quarter",
    "month": "month",
    "month_num": "month",
    "hour": "hour",
    "day": "day",
}


@dataclass
class TidySuggestion:
    """A concrete suggestion to reshape an untidy table into long form."""

    pattern: str
    table: str
    affected_columns: list[str]
    id_columns: list[str]
    period_kind: str
    common_prefix: str
    period_column_name: str
    value_column_name: str
    suggested_view: str
    rationale: str
    reshape_sql: str

    def build_sql(self, mode: str = "table") -> str:
        """Return DDL that materializes this suggestion as a table or view."""
        if mode not in ("view", "table"):
            raise ValueError(f"mode must be 'view' or 'table', got {mode!r}")
        pairs = _column_period_pairs(self.affected_columns)
        return _build_reshape_sql(
            self.table,
            self.id_columns,
            pairs,
            self.period_column_name,
            self.value_column_name,
            self.suggested_view,
            mode=mode,
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _column_period_pairs(affected_columns: list[str]) -> list[tuple[str, str]]:
    """Recover (column_name, period_token) pairs by re-running the period split.

    The period token isn't stored on ``TidySuggestion`` because it can be
    derived deterministically from the column name; this helper exposes that
    derivation for callers (CLI ``--create-table``) that need to regenerate
    the SQL with a different ``CREATE`` mode.
    """
    pairs: list[tuple[str, str]] = []
    for col in affected_columns:
        result = _split_prefix_period(col)
        if result is None:
            continue
        _, period_token, _ = result
        pairs.append((col, period_token))
    return pairs


@dataclass
class WideTableNote:
    """A softer note for tables that look transposed but have no reshape inferred."""

    table: str
    column_count: int
    row_count: int | None
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _build_unpivot_sql(
    table: str,
    column_period_pairs: list[tuple[str, str]],
    period_column_name: str,
    value_column_name: str,
    suggested_view: str,
    mode: str,
) -> str:
    """Build a ``CREATE OR REPLACE TABLE|VIEW`` using DuckDB ``UNPIVOT``.

    Use ``UNPIVOT … ON col AS '<period>'`` so each affected column maps to
    a clean period token in the new period column. UNPIVOT carries the
    remaining id columns through automatically.

    Note: this form is only safe for ``mode='table'``. The Python
    ``duckdb`` 1.5.2 binding has a regression where UNPIVOT stored inside
    a view fails to re-bind on a fresh connection (the standalone CLI
    1.5.1 is unaffected). ``_build_reshape_sql`` falls back to UNION ALL
    for view mode so views remain queryable through datasight itself.
    """
    keyword = "TABLE" if mode == "table" else "VIEW"
    on_clause = ",\n    ".join(
        f"{_quote_identifier(col)} AS '{period.replace(chr(39), chr(39) * 2)}'"
        for col, period in column_period_pairs
    )
    return (
        f"CREATE OR REPLACE {keyword} {_quote_identifier(suggested_view)} AS\n"
        f"SELECT * FROM (\n"
        f"  UNPIVOT {_quote_identifier(table)}\n"
        f"  ON {on_clause}\n"
        f"  INTO\n"
        f"    NAME {_quote_identifier(period_column_name)}\n"
        f"    VALUE {_quote_identifier(value_column_name)}\n"
        f");"
    )


def _build_union_all_view_sql(
    table: str,
    id_columns: list[str],
    column_period_pairs: list[tuple[str, str]],
    period_column_name: str,
    value_column_name: str,
    suggested_view: str,
) -> str:
    """Build a ``CREATE OR REPLACE VIEW`` using ``UNION ALL`` branches.

    A workaround for a regression in the Python ``duckdb`` 1.5.2 binding:
    UNPIVOT stored inside a view re-binds incorrectly on a fresh
    connection through that binding, raising ``UNPIVOT name count
    mismatch``. The standalone ``duckdb`` CLI 1.5.1 doesn't reproduce
    the failure, but every datasight query path uses the Python binding,
    so we need a form that survives re-bind. Materialized tables
    (``CREATE OR REPLACE TABLE``) don't re-bind and keep the cleaner
    UNPIVOT form.
    """
    quoted_period = _quote_identifier(period_column_name)
    quoted_value = _quote_identifier(value_column_name)
    quoted_table = _quote_identifier(table)
    id_select = ", ".join(_quote_identifier(c) for c in id_columns)
    id_prefix = f"{id_select}, " if id_select else ""
    branches: list[str] = []
    for index, (col, period) in enumerate(column_period_pairs):
        period_literal = period.replace("'", "''")
        quoted_col = _quote_identifier(col)
        if index == 0:
            branches.append(
                f"SELECT {id_prefix}'{period_literal}' AS {quoted_period}, "
                f"{quoted_col} AS {quoted_value} FROM {quoted_table}"
            )
        else:
            branches.append(
                f"UNION ALL SELECT {id_prefix}'{period_literal}', {quoted_col} FROM {quoted_table}"
            )
    body = "\n".join(branches)
    return (
        f"-- Python `duckdb` 1.5.2 fails to re-bind UNPIVOT inside views on a fresh\n"
        f"-- connection (Binder Error: UNPIVOT name count mismatch), so we use\n"
        f"-- UNION ALL here. `tidy table` keeps the cleaner UNPIVOT form because\n"
        f"-- materialized tables don't re-bind. The standalone duckdb CLI 1.5.1\n"
        f"-- is unaffected.\n"
        f"CREATE OR REPLACE VIEW {_quote_identifier(suggested_view)} AS\n"
        f"{body};"
    )


def _build_reshape_sql(
    table: str,
    id_columns: list[str],
    column_period_pairs: list[tuple[str, str]],
    period_column_name: str,
    value_column_name: str,
    suggested_view: str,
    mode: str = "table",
) -> str:
    """Build the DDL for one tidy reshape.

    For ``mode='table'`` (the default), emits the canonical DuckDB
    ``UNPIVOT`` form. For ``mode='view'``, falls back to ``UNION ALL``
    branches because of a DuckDB binder bug that breaks UNPIVOT inside
    stored views.
    """
    if mode == "table":
        return _build_unpivot_sql(
            table,
            column_period_pairs,
            period_column_name,
            value_column_name,
            suggested_view,
            mode,
        )
    if mode == "view":
        return _build_union_all_view_sql(
            table,
            id_columns,
            column_period_pairs,
            period_column_name,
            value_column_name,
            suggested_view,
        )
    raise ValueError(f"mode must be 'view' or 'table', got {mode!r}")


def _build_period_suggestion(
    table_name: str,
    all_columns: list[str],
    column_period_pairs: list[tuple[str, str]],
    period_kind: str,
    common_prefix: str,
    excluded_from_id: set[str] | None = None,
) -> TidySuggestion:
    affected_columns = [col for col, _ in column_period_pairs]
    excluded = set(affected_columns) if excluded_from_id is None else excluded_from_id
    id_columns = [c for c in all_columns if c not in excluded]
    period_column_name = _PERIOD_COLUMN_NAMES.get(period_kind, "period")
    value_column_name = common_prefix.strip("_") if common_prefix else "value"
    if not value_column_name:
        value_column_name = "value"
    suggested_view = f"{table_name}_long"
    pattern = "repeated_prefix_period" if common_prefix else "date_in_column_names"
    if common_prefix:
        rationale = (
            f"Columns share prefix `{common_prefix}` with {period_kind} suffixes — "
            f"{len(affected_columns)} columns look like a single measure spread across "
            f"{period_kind} values."
        )
    else:
        rationale = (
            f"{len(affected_columns)} columns are named like {period_kind} values — "
            f"the {period_kind} dimension appears to be encoded in column headers."
        )
    reshape_sql = _build_reshape_sql(
        table_name,
        id_columns,
        column_period_pairs,
        period_column_name,
        value_column_name,
        suggested_view,
        mode="table",
    )
    return TidySuggestion(
        pattern=pattern,
        table=table_name,
        affected_columns=affected_columns,
        id_columns=id_columns,
        period_kind=period_kind,
        common_prefix=common_prefix,
        period_column_name=period_column_name,
        value_column_name=value_column_name,
        suggested_view=suggested_view,
        rationale=rationale,
        reshape_sql=reshape_sql,
    )


def _detect_period_groups(table: dict[str, Any]) -> list[TidySuggestion]:
    columns = [c["name"] for c in table.get("columns", [])]
    groups: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(list)
    for column_name in columns:
        result = _split_prefix_period(column_name)
        if result is None:
            continue
        prefix, period_token, kind = result
        groups[(prefix, kind)].append((column_name, period_token))
    qualifying_groups = [
        ((prefix, kind), entries)
        for (prefix, kind), entries in groups.items()
        if len(entries) >= MIN_GROUP_SIZE
    ]
    excluded_from_id: set[str] = {col for _, entries in qualifying_groups for col, _ in entries}
    suggestions: list[TidySuggestion] = []
    for (prefix, kind), entries in qualifying_groups:
        suggestions.append(
            _build_period_suggestion(
                table_name=table["name"],
                all_columns=columns,
                column_period_pairs=entries,
                period_kind=kind,
                common_prefix=prefix,
                excluded_from_id=excluded_from_id,
            )
        )
    suggestions.sort(key=lambda s: (-len(s.affected_columns), s.common_prefix))
    return suggestions


def _detect_wide_low_rows(
    table: dict[str, Any],
    period_suggestions: list[TidySuggestion],
) -> WideTableNote | None:
    columns = table.get("columns", [])
    column_count = len(columns)
    if column_count < WIDE_TABLE_COLUMN_THRESHOLD:
        return None
    if period_suggestions:
        return None
    row_count = table.get("row_count")
    if row_count is not None and row_count > column_count * 2:
        return None
    if row_count is None:
        reason = f"table has {column_count} columns; row count unknown"
    elif row_count <= column_count:
        reason = f"table has {column_count} columns and only {row_count} rows"
    else:
        reason = f"table has {column_count} columns and only {row_count} rows ({row_count // column_count}x)"
    return WideTableNote(
        table=table["name"],
        column_count=column_count,
        row_count=row_count,
        reason=reason,
    )


def analyze_tidy_patterns(schema_info: list[dict[str, Any]]) -> dict[str, Any]:
    """Inspect ``schema_info`` and return tidy-reshape suggestions and notes.

    The result has the same shape as other ``data_profile`` overviews: a
    JSON-serializable dict suitable for printing or feeding to a renderer.
    """
    period_suggestions: list[TidySuggestion] = []
    wide_tables: list[WideTableNote] = []
    notes: list[str] = []

    for table in schema_info:
        if not table.get("name"):
            continue
        table_period = _detect_period_groups(table)
        period_suggestions.extend(table_period)
        wide_note = _detect_wide_low_rows(table, table_period)
        if wide_note is not None:
            wide_tables.append(wide_note)

    if not period_suggestions and not wide_tables:
        notes.append("No untidy column-shape patterns detected.")

    return {
        "table_count": len(schema_info),
        "period_suggestions": [s.to_dict() for s in period_suggestions],
        "wide_tables": [w.to_dict() for w in wide_tables],
        "notes": notes,
    }


__all__ = [
    "MIN_GROUP_SIZE",
    "WIDE_TABLE_COLUMN_THRESHOLD",
    "TidySuggestion",
    "WideTableNote",
    "analyze_tidy_patterns",
]
