"""Detect untidy dataset patterns and generate tidy-reshape DDL.

The deterministic detector inspects table schemas and flags columns whose
names appear to encode a dimension value (year, month, quarter, hour, day)
rather than holding a single measure across rows. Each suggestion carries a
previewable DuckDB DDL statement so users can inspect or materialize the
reshape.

The data model supports multi-pivot proposals as a first-class case: a
suggestion carries a ``dimensions`` list (one or more) and a
``column_mappings`` list whose entries map a source column to one
``dimension_value`` per dimension. Single-pivot is just the
one-dimension case. The deterministic detector emits single-pivot
proposals only; multi-pivot proposals come from the ``tidy review``
LLM-augmented advisor.

The DDL emitted depends on the target object and the dimension count:

- Single-pivot, ``CREATE OR REPLACE TABLE`` — DuckDB ``UNPIVOT``. This
  is the canonical, terse form.
- Single-pivot, ``CREATE OR REPLACE VIEW`` — ``UNION ALL`` branches.
  Workaround for a regression in the Python ``duckdb`` 1.5.2 binding:
  UNPIVOT stored inside a view fails to re-bind on a fresh connection
  (``Binder Error: UNPIVOT name count mismatch``). The standalone
  ``duckdb`` CLI 1.5.1 isn't affected, but every datasight query path
  uses the Python binding, so views need a form that survives re-bind.
- Multi-pivot, table or view — ``UNION ALL`` with N dimension literals
  per branch. UNPIVOT only emits one name column, so it doesn't extend
  cleanly to multiple dimensions.

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


_PERIOD_DIMENSION_NAMES: dict[str, str] = {
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


# Allowed dimension kinds. ``date_period`` covers any time-axis pivot (year,
# month, quarter, hour, day). The remaining kinds are used by LLM-proposed
# reshapes and won't appear in deterministic output.
DIMENSION_KINDS: frozenset[str] = frozenset(
    {"date_period", "category", "geography", "scenario", "other"}
)

# Allowed confidence labels. The deterministic detector always emits "high".
CONFIDENCE_LEVELS: frozenset[str] = frozenset({"high", "medium", "low"})


@dataclass
class Dimension:
    """One axis of a tidy reshape (e.g., year, fuel_type)."""

    name: str
    kind: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ColumnMapping:
    """How one source column maps to dimension values in the long form.

    ``dimension_values`` keys must exactly match the names of every
    ``Dimension`` on the parent ``TidySuggestion``.
    """

    column: str
    dimension_values: dict[str, str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class TidySuggestion:
    """A concrete proposal to reshape an untidy table into long form.

    Multi-pivot is first-class: ``dimensions`` and each
    ``column_mappings[i].dimension_values`` carry one entry per axis.
    Single-pivot is just the one-dimension case.

    ``source`` and ``confidence`` exist so deterministic and LLM-proposed
    suggestions share one rendering path. The deterministic detector
    always emits ``source="deterministic"`` and ``confidence="high"``.
    """

    pattern: str
    table: str
    dimensions: list[Dimension]
    column_mappings: list[ColumnMapping]
    id_columns: list[str]
    value_column: str
    target_object_name: str
    rationale: str
    reshape_sql: str
    confidence: str = "high"
    source: str = "deterministic"

    def build_sql(self, mode: str = "table") -> str:
        """Return DDL that materializes this suggestion as a table or view."""
        if mode not in ("view", "table"):
            raise ValueError(f"mode must be 'view' or 'table', got {mode!r}")
        return _build_reshape_sql(
            self.table,
            self.id_columns,
            self.dimensions,
            self.column_mappings,
            self.value_column,
            self.target_object_name,
            mode=mode,
        )

    @property
    def affected_columns(self) -> list[str]:
        """Source column names involved in the reshape, in mapping order."""
        return [m.column for m in self.column_mappings]

    def to_dict(self) -> dict[str, Any]:
        return {
            "pattern": self.pattern,
            "table": self.table,
            "dimensions": [d.to_dict() for d in self.dimensions],
            "column_mappings": [m.to_dict() for m in self.column_mappings],
            "id_columns": list(self.id_columns),
            "value_column": self.value_column,
            "target_object_name": self.target_object_name,
            "rationale": self.rationale,
            "reshape_sql": self.reshape_sql,
            "confidence": self.confidence,
            "source": self.source,
        }


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
    column_mappings: list[ColumnMapping],
    dimension: Dimension,
    value_column: str,
    target_object_name: str,
    mode: str,
) -> str:
    """Build a single-pivot ``CREATE OR REPLACE TABLE|VIEW`` using ``UNPIVOT``.

    Only safe for ``mode='table'``. The Python ``duckdb`` 1.5.2 binding has a
    regression where UNPIVOT stored inside a view fails to re-bind on a fresh
    connection (the standalone CLI 1.5.1 is unaffected). ``_build_reshape_sql``
    routes view mode to ``_build_union_all_sql`` instead.

    Multi-pivot reshapes never use this builder — UNPIVOT emits a single name
    column, so it doesn't extend to multiple dimensions.
    """
    keyword = "TABLE" if mode == "table" else "VIEW"
    on_clause = ",\n    ".join(
        f"{_quote_identifier(m.column)} AS '{m.dimension_values[dimension.name].replace(chr(39), chr(39) * 2)}'"
        for m in column_mappings
    )
    return (
        f"CREATE OR REPLACE {keyword} {_quote_identifier(target_object_name)} AS\n"
        f"SELECT * FROM (\n"
        f"  UNPIVOT {_quote_identifier(table)}\n"
        f"  ON {on_clause}\n"
        f"  INTO\n"
        f"    NAME {_quote_identifier(dimension.name)}\n"
        f"    VALUE {_quote_identifier(value_column)}\n"
        f");"
    )


def _build_union_all_sql(
    table: str,
    id_columns: list[str],
    dimensions: list[Dimension],
    column_mappings: list[ColumnMapping],
    value_column: str,
    target_object_name: str,
    mode: str,
) -> str:
    """Build a ``CREATE OR REPLACE TABLE|VIEW`` using ``UNION ALL`` branches.

    This is the unified path for:

    - Single-pivot view mode (workaround for the duckdb 1.5.2 view-binding
      bug; see module docstring).
    - Multi-pivot, both table and view mode (UNPIVOT can't emit multiple
      dimension columns).

    Each affected column becomes one branch with N dimension literals plus
    the value, in dimension-declaration order.
    """
    keyword = "TABLE" if mode == "table" else "VIEW"
    quoted_table = _quote_identifier(table)
    quoted_value = _quote_identifier(value_column)
    id_select = ", ".join(_quote_identifier(c) for c in id_columns)
    id_prefix = f"{id_select}, " if id_select else ""

    branches: list[str] = []
    for index, mapping in enumerate(column_mappings):
        dim_literals_first = ", ".join(
            f"'{mapping.dimension_values[d.name].replace(chr(39), chr(39) * 2)}' "
            f"AS {_quote_identifier(d.name)}"
            for d in dimensions
        )
        dim_literals_subsequent = ", ".join(
            f"'{mapping.dimension_values[d.name].replace(chr(39), chr(39) * 2)}'"
            for d in dimensions
        )
        quoted_col = _quote_identifier(mapping.column)
        if index == 0:
            branches.append(
                f"SELECT {id_prefix}{dim_literals_first}, "
                f"{quoted_col} AS {quoted_value} FROM {quoted_table}"
            )
        else:
            branches.append(
                f"UNION ALL SELECT {id_prefix}{dim_literals_subsequent}, "
                f"{quoted_col} FROM {quoted_table}"
            )
    body = "\n".join(branches)

    # The single-pivot view case is the historical reason this builder
    # exists — the comment surfaces that in the generated DDL so a reader
    # tracing the file knows why UNION ALL appears instead of UNPIVOT.
    if mode == "view" and len(dimensions) == 1:
        header = (
            "-- Python `duckdb` 1.5.2 fails to re-bind UNPIVOT inside views on a fresh\n"
            "-- connection (Binder Error: UNPIVOT name count mismatch), so we use\n"
            "-- UNION ALL here. `tidy table` keeps the cleaner UNPIVOT form because\n"
            "-- materialized tables don't re-bind. The standalone duckdb CLI 1.5.1\n"
            "-- is unaffected.\n"
        )
    elif len(dimensions) > 1:
        header = (
            "-- UNPIVOT only emits one name column, so multi-pivot reshapes use\n"
            "-- UNION ALL with one literal per dimension on each branch.\n"
        )
    else:
        header = ""

    return (
        f"{header}CREATE OR REPLACE {keyword} {_quote_identifier(target_object_name)} AS\n{body};"
    )


def _build_reshape_sql(
    table: str,
    id_columns: list[str],
    dimensions: list[Dimension],
    column_mappings: list[ColumnMapping],
    value_column: str,
    target_object_name: str,
    mode: str = "table",
) -> str:
    """Build the DDL for one tidy reshape.

    Dispatch:

    - single-pivot + ``mode='table'`` → UNPIVOT (canonical DuckDB form).
    - single-pivot + ``mode='view'`` → UNION ALL (duckdb 1.5.2 view bug).
    - multi-pivot, any mode → UNION ALL (UNPIVOT can't emit N name columns).
    """
    if mode not in ("view", "table"):
        raise ValueError(f"mode must be 'view' or 'table', got {mode!r}")
    if len(dimensions) == 1 and mode == "table":
        return _build_unpivot_sql(
            table,
            column_mappings,
            dimensions[0],
            value_column,
            target_object_name,
            mode,
        )
    return _build_union_all_sql(
        table,
        id_columns,
        dimensions,
        column_mappings,
        value_column,
        target_object_name,
        mode,
    )


def _build_period_suggestion(
    table_name: str,
    all_columns: list[str],
    column_period_pairs: list[tuple[str, str]],
    period_kind: str,
    common_prefix: str,
    excluded_from_id: set[str] | None = None,
) -> TidySuggestion:
    """Build a single-pivot suggestion from a ``(prefix, period_kind)`` group."""
    affected_column_names = [col for col, _ in column_period_pairs]
    excluded = set(affected_column_names) if excluded_from_id is None else excluded_from_id
    id_columns = [c for c in all_columns if c not in excluded]
    dimension_name = _PERIOD_DIMENSION_NAMES.get(period_kind, "period")
    value_column = common_prefix.strip("_") if common_prefix else "value"
    if not value_column:
        value_column = "value"
    target_object_name = f"{table_name}_long"
    pattern = "repeated_prefix_period" if common_prefix else "date_in_column_names"
    if common_prefix:
        rationale = (
            f"Columns share prefix `{common_prefix}` with {period_kind} suffixes — "
            f"{len(affected_column_names)} columns look like a single measure spread across "
            f"{period_kind} values."
        )
    else:
        rationale = (
            f"{len(affected_column_names)} columns are named like {period_kind} values — "
            f"the {period_kind} dimension appears to be encoded in column headers."
        )

    dimensions = [Dimension(name=dimension_name, kind="date_period")]
    column_mappings = [
        ColumnMapping(column=col, dimension_values={dimension_name: token})
        for col, token in column_period_pairs
    ]
    reshape_sql = _build_reshape_sql(
        table_name,
        id_columns,
        dimensions,
        column_mappings,
        value_column,
        target_object_name,
        mode="table",
    )
    return TidySuggestion(
        pattern=pattern,
        table=table_name,
        dimensions=dimensions,
        column_mappings=column_mappings,
        id_columns=id_columns,
        value_column=value_column,
        target_object_name=target_object_name,
        rationale=rationale,
        reshape_sql=reshape_sql,
    )


def _detect_period_groups(table: dict[str, Any]) -> list[TidySuggestion]:
    """Detect period-pattern groups in one table and emit single-pivot suggestions."""
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
    suggestions.sort(
        key=lambda s: (-len(s.column_mappings), s.value_column),
    )
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
        reason = (
            f"table has {column_count} columns and only {row_count} rows "
            f"({row_count // column_count}x)"
        )
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

    Output keys: ``table_count``, ``suggestions`` (list of suggestion
    dicts), ``wide_tables`` (list of wide-table notes), ``notes`` (list
    of free-text strings). The ``suggestions`` key is generic — it
    carries deterministic period-pattern hits today, and will carry
    LLM-proposed multi-pivot suggestions once ``tidy review`` lands.
    """
    suggestions: list[TidySuggestion] = []
    wide_tables: list[WideTableNote] = []
    notes: list[str] = []

    for table in schema_info:
        if not table.get("name"):
            continue
        table_period = _detect_period_groups(table)
        suggestions.extend(table_period)
        wide_note = _detect_wide_low_rows(table, table_period)
        if wide_note is not None:
            wide_tables.append(wide_note)

    if not suggestions and not wide_tables:
        notes.append("No untidy column-shape patterns detected.")

    return {
        "table_count": len(schema_info),
        "suggestions": [s.to_dict() for s in suggestions],
        "wide_tables": [w.to_dict() for w in wide_tables],
        "notes": notes,
    }


__all__ = [
    "MIN_GROUP_SIZE",
    "WIDE_TABLE_COLUMN_THRESHOLD",
    "DIMENSION_KINDS",
    "CONFIDENCE_LEVELS",
    "Dimension",
    "ColumnMapping",
    "TidySuggestion",
    "WideTableNote",
    "analyze_tidy_patterns",
]
