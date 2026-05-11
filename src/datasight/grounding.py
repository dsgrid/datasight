"""Cheap schema-drift check for grounding files.

The LLM agent loop is steered by three files that live in the project
directory:

- ``queries.yaml`` — few-shot SQL examples
- ``schema_description.md`` — prose schema description
- ``time_series.yaml`` — temporal-structure declarations

Any of these can fall out of sync with the live database after a schema
change (for example a ``datasight tidy review`` that reshapes a wide
table into long form). When that happens the LLM is silently being
trained on wrong column names: the agent either hallucinates plausible
columns, refuses with a citation of the stale grounding, or returns
``SELECT`` results full of zeros.

This module performs a no-LLM, AST-driven check that every column and
table reference in the grounding files resolves against the current
schema. It is intentionally cheap — the goal is to surface drift loudly
at known checkpoints (``datasight verify``, the post-apply step of
``datasight tidy review``) rather than at every agent invocation.

The companion module :mod:`datasight.grounding_repair` consumes a
:class:`DriftReport` from here and uses an LLM to rewrite the affected
files; this module never invokes an LLM.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from difflib import get_close_matches
from pathlib import Path
from typing import Any, Awaitable, Callable

import sqlglot
import yaml
from sqlglot import exp


# Backtick-quoted lowercase identifier in markdown. Matches ``foo``,
# ``foo_bar``, ``foo.bar``. Anything that isn't a snake_case identifier
# inside backticks (prose, multiword content, SQL fragments) is ignored.
_MD_BACKTICK_IDENT = re.compile(r"`([a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)?)`")


# SQL keywords / built-ins that may legitimately appear in backticks
# inside ``schema_description.md`` — used to suppress false positives
# from the markdown scan. Not exhaustive: only the words that show up in
# prose. Anything not here AND not in the current schema gets flagged.
_SQL_KEYWORDS: frozenset[str] = frozenset({
    "all", "and", "as", "asc", "avg", "between", "boolean", "by",
    "case", "cast", "ceil", "coalesce", "corr", "count", "current_date",
    "date", "date_trunc", "datetime", "day", "desc", "distinct",
    "double", "else", "end", "extract", "false", "floor", "from",
    "group", "having", "in", "inner", "integer", "is", "join", "left",
    "limit", "max", "min", "month", "not", "now", "null", "offset",
    "on", "or", "order", "outer", "over", "regr_intercept", "regr_r2",
    "regr_slope", "right", "round", "row_number", "select", "sum",
    "then", "timestamp", "to_date", "true", "union", "varchar",
    "when", "where", "with", "year",
})


@dataclass
class DriftItem:
    """One finding: a claim in a grounding file that doesn't resolve.

    Attributes
    ----------
    file : str
        Source file path (string for serializability).
    line : int | None
        1-based line number when the source format makes it easy to find
        (markdown). ``None`` for YAML SQL bodies where the line of the
        offending token is harder to localize without a full AST walk.
    kind : str
        One of ``"table"``, ``"column"``, ``"ts_table"``, ``"ts_column"``,
        ``"parse_error"``.
    claim : str
        The identifier as it appeared in the source.
    detail : str
        Human-readable explanation suitable for terminal output.
    suggestion : str | None
        Nearest match in the current schema by edit distance, if one is
        within the similarity cutoff.
    """

    file: str
    line: int | None
    kind: str
    claim: str
    detail: str
    suggestion: str | None = None


@dataclass
class DriftReport:
    """Result of a grounding-drift check.

    A ``DriftReport`` with no items means every grounding-file claim
    resolved against the live schema. The grouping helpers exist so
    formatters can render findings per-file without re-iterating.
    """

    items: list[DriftItem] = field(default_factory=list)

    @property
    def is_clean(self) -> bool:
        """True if no drift was detected."""
        return not self.items

    def by_file(self) -> dict[str, list[DriftItem]]:
        """Group items by source file, preserving insertion order."""
        out: dict[str, list[DriftItem]] = {}
        for item in self.items:
            out.setdefault(item.file, []).append(item)
        return out


def check_grounding_drift(
    project_dir: Path,
    schema_truth: dict[str, set[str]],
    *,
    enum_values: set[str] | None = None,
    queries_path: Path | None = None,
    schema_description_path: Path | None = None,
    time_series_path: Path | None = None,
) -> DriftReport:
    """Check grounding files for references the current schema can't resolve.

    Parameters
    ----------
    project_dir : Path
        Project directory; default file paths are resolved relative to it.
    schema_truth : dict[str, set[str]]
        ``{table_name: set(column_names)}`` for the current live schema.
        Build this with :func:`build_schema_truth_sync` or
        :func:`build_schema_truth_async`.
    enum_values : set[str] | None
        Optional allowlist of legal values (distinct VARCHAR values from
        the live schema). When the markdown scan finds a backticked
        identifier that matches one of these, it is treated as an enum
        value rather than a missing-column candidate. Build this with
        :func:`build_enum_values_sync` to suppress false positives from
        prose like "Values: \\`east_north_central\\`, ...".
    queries_path, schema_description_path, time_series_path : Path | None
        Override default file locations. Missing files are silently
        skipped — a project that hasn't added one of the grounding files
        yet should not produce drift items.

    Returns
    -------
    DriftReport
        ``is_clean`` is True when no drift was detected.
    """
    report = DriftReport()
    qpath = queries_path or project_dir / "queries.yaml"
    sdpath = schema_description_path or project_dir / "schema_description.md"
    tspath = time_series_path or project_dir / "time_series.yaml"

    if qpath.exists():
        _check_queries(qpath, schema_truth, report)
    if sdpath.exists():
        _check_schema_description(sdpath, schema_truth, enum_values or set(), report)
    if tspath.exists():
        _check_time_series(tspath, schema_truth, report)

    return report


def _check_queries(  # noqa: C901
    path: Path, schema_truth: dict[str, set[str]], report: DriftReport
) -> None:
    """Walk every ``sql:`` block, flag unresolved table/column references."""
    text = path.read_text(encoding="utf-8")
    try:
        docs = yaml.safe_load(text) or []
    except yaml.YAMLError as exc:
        report.items.append(DriftItem(
            file=str(path), line=None, kind="parse_error",
            claim="", detail=f"yaml parse error: {exc}",
        ))
        return
    if not isinstance(docs, list):
        return

    all_tables = set(schema_truth.keys())
    all_columns: set[str] = set()
    for cols in schema_truth.values():
        all_columns |= cols

    for entry in docs:
        if not isinstance(entry, dict):
            continue
        sql = entry.get("sql", "")
        if not sql:
            continue
        try:
            parsed = sqlglot.parse(sql, read="duckdb")
        except sqlglot.errors.ParseError:
            # Out of scope here — ``sql_validation.py`` covers parse errors.
            continue

        # Collect CTE names so we don't flag them as missing tables.
        cte_names: set[str] = set()
        for stmt in parsed:
            if stmt is None:
                continue
            for cte in stmt.find_all(exp.CTE):
                cte_names.add(cte.alias_or_name)

        # Also collect output aliases (``AS alias``) so we don't flag
        # them when they're referenced later in the same query (e.g. in
        # an ``ORDER BY`` clause on a computed column).
        output_aliases: set[str] = set()
        for stmt in parsed:
            if stmt is None:
                continue
            for alias in stmt.find_all(exp.Alias):
                a = alias.alias_or_name
                if a:
                    output_aliases.add(a)

        for stmt in parsed:
            if stmt is None:
                continue
            for tref in stmt.find_all(exp.Table):
                name = tref.name
                if not name or name in cte_names or name in all_tables:
                    continue
                report.items.append(DriftItem(
                    file=str(path), line=None, kind="table",
                    claim=name,
                    detail=f"table '{name}' not in current schema",
                    suggestion=_nearest(name, all_tables),
                ))
            for cref in stmt.find_all(exp.Column):
                name = cref.name
                if not name or name in all_columns or name in output_aliases:
                    continue
                # Qualified-but-unknown table prefixes are caught by the
                # table check above; ignore the column part to avoid
                # double-reporting.
                if cref.table and cref.table not in all_tables:
                    continue
                report.items.append(DriftItem(
                    file=str(path), line=None, kind="column",
                    claim=name,
                    detail=f"column '{name}' not in any table",
                    suggestion=_nearest(name, all_columns),
                ))


def _check_schema_description(  # noqa: C901
    path: Path,
    schema_truth: dict[str, set[str]],
    enum_values: set[str],
    report: DriftReport,
) -> None:
    """Flag backticked identifiers in markdown that don't resolve."""
    all_tables = set(schema_truth.keys())
    all_columns: set[str] = set()
    for cols in schema_truth.values():
        all_columns |= cols
    known = all_tables | all_columns | _SQL_KEYWORDS | enum_values

    text = path.read_text(encoding="utf-8")
    seen_on_line: set[tuple[int, str]] = set()
    for lineno, line in enumerate(text.splitlines(), start=1):
        for m in _MD_BACKTICK_IDENT.finditer(line):
            ident = m.group(1).lower()

            # ``table.column`` — check the column against that table's
            # column list. Unknown tables are silently ignored here to
            # avoid noise from prose that mentions tables from other DBs.
            parts = ident.split(".")
            if len(parts) == 2:
                table, col = parts
                if table not in all_tables:
                    continue
                if col not in schema_truth.get(table, set()):
                    key = (lineno, ident)
                    if key in seen_on_line:
                        continue
                    seen_on_line.add(key)
                    suggestion = _nearest(col, schema_truth.get(table, set()))
                    report.items.append(DriftItem(
                        file=str(path), line=lineno, kind="column",
                        claim=ident,
                        detail=f"`{ident}` not a column of '{table}'",
                        suggestion=f"{table}.{suggestion}" if suggestion else None,
                    ))
                continue

            if ident in known:
                continue
            # Identifier heuristics: snake_case, not all digits, length >= 3.
            # The minimum length suppresses common English words ("on",
            # "is", "as") that happen to slip through the keyword set.
            if not re.match(r"^[a-z][a-z0-9_]*$", ident) or len(ident) < 3:
                continue
            key = (lineno, ident)
            if key in seen_on_line:
                continue
            seen_on_line.add(key)
            report.items.append(DriftItem(
                file=str(path), line=lineno, kind="column",
                claim=ident,
                detail=f"`{ident}` not in current schema (column or table)",
                suggestion=_nearest(ident, all_columns | all_tables),
            ))


def _check_time_series(
    path: Path, schema_truth: dict[str, set[str]], report: DriftReport
) -> None:
    """Verify each entry's ``table`` / ``timestamp_column`` / ``group_columns``."""
    text = path.read_text(encoding="utf-8")
    try:
        docs = yaml.safe_load(text) or []
    except yaml.YAMLError as exc:
        report.items.append(DriftItem(
            file=str(path), line=None, kind="parse_error",
            claim="", detail=f"yaml parse error: {exc}",
        ))
        return
    if not isinstance(docs, list):
        return

    for entry in docs:
        if not isinstance(entry, dict):
            continue
        table = entry.get("table")
        if not table:
            continue
        if table not in schema_truth:
            report.items.append(DriftItem(
                file=str(path), line=None, kind="ts_table",
                claim=str(table),
                detail=f"time_series table '{table}' not in current schema",
                suggestion=_nearest(str(table), set(schema_truth.keys())),
            ))
            continue
        ts_col = entry.get("timestamp_column")
        if ts_col and ts_col not in schema_truth[table]:
            report.items.append(DriftItem(
                file=str(path), line=None, kind="ts_column",
                claim=str(ts_col),
                detail=f"time_series timestamp_column '{ts_col}' not a column of '{table}'",
                suggestion=_nearest(str(ts_col), schema_truth[table]),
            ))
        for col in entry.get("group_columns") or []:
            if col not in schema_truth[table]:
                report.items.append(DriftItem(
                    file=str(path), line=None, kind="ts_column",
                    claim=str(col),
                    detail=f"time_series group_column '{col}' not a column of '{table}'",
                    suggestion=_nearest(str(col), schema_truth[table]),
                ))


def _nearest(claim: str, candidates: set[str]) -> str | None:
    """Closest match by edit distance, or None when nothing's similar enough."""
    if not candidates:
        return None
    matches = get_close_matches(claim, list(candidates), n=1, cutoff=0.6)
    return matches[0] if matches else None


def build_enum_values_sync(
    conn: Any, schema_truth: dict[str, set[str]], *, max_per_column: int = 200
) -> set[str]:
    """Collect distinct values from low-cardinality VARCHAR columns.

    Used to suppress false positives in the markdown drift scan: a
    prose listing like "Values: \\`east_north_central\\`, ..." would
    otherwise be flagged as references to missing columns. Columns with
    more than ``max_per_column`` distinct values are skipped — those
    aren't enums and aren't worth scanning for.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        Open DuckDB connection.
    schema_truth : dict[str, set[str]]
        Output of :func:`build_schema_truth_sync`. The function reads
        column types from ``information_schema.columns`` and queries
        only those typed VARCHAR/STRING.
    max_per_column : int
        Skip columns that exceed this distinct-value count. Defaults
        to 200, which fits typical enum-shaped columns (regions,
        subsectors, status codes) and skips free-text columns.

    Returns
    -------
    set[str]
        Distinct string values across all qualifying columns. Values
        are lowercased to match the case used by the markdown scan.
    """
    out: set[str] = set()
    rows = conn.execute(
        "SELECT table_name, column_name, data_type "
        "FROM information_schema.columns "
        "WHERE table_schema = current_schema() "
        "AND table_catalog = current_database()"
    ).fetchall()
    for table, col, dtype in rows:
        if table not in schema_truth or col not in schema_truth[table]:
            continue
        if "char" not in str(dtype).lower() and "string" not in str(dtype).lower():
            continue
        try:
            count = conn.execute(
                f"SELECT COUNT(DISTINCT {col}) FROM {table}"
            ).fetchone()
        except Exception:  # noqa: BLE001 — never let one bad column abort the whole scan
            continue
        if count is None or count[0] > max_per_column:
            continue
        try:
            values = conn.execute(
                f"SELECT DISTINCT {col} FROM {table} WHERE {col} IS NOT NULL"
            ).fetchall()
        except Exception:  # noqa: BLE001
            continue
        for (v,) in values:
            if isinstance(v, str):
                out.add(v.lower())
    return out


def build_schema_truth_sync(conn: Any) -> dict[str, set[str]]:
    """Build the ``{table: set(columns)}`` truth set from a sync DuckDB conn.

    Filters to user-visible objects in the current schema/database so a
    ``schema_description.md`` reference can't be silently validated
    against a same-named column in an attached DB.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        Open DuckDB connection.

    Returns
    -------
    dict[str, set[str]]
        ``{table_name: {column_name, ...}}``.
    """
    rows = conn.execute(
        "SELECT table_name, column_name "
        "FROM information_schema.columns "
        "WHERE table_schema = current_schema() "
        "AND table_catalog = current_database()"
    ).fetchall()
    out: dict[str, set[str]] = {}
    for table, col in rows:
        out.setdefault(table, set()).add(col)
    return out


async def build_schema_truth_async(
    run_sql: Callable[[str], Awaitable[Any]],
) -> dict[str, set[str]]:
    """Build the truth set from the async ``run_sql`` callable used by datasight.

    Uses the same ``information_schema.columns`` query as the sync
    variant; works against DuckDB and PostgreSQL alike. For SQLite,
    callers should construct the dict from the existing
    :func:`datasight.schema.introspect_schema` result instead — SQLite
    has no ``information_schema``.
    """
    df = await run_sql(
        "SELECT table_name, column_name "
        "FROM information_schema.columns "
        "WHERE table_schema NOT IN ('information_schema', 'pg_catalog')"
    )
    out: dict[str, set[str]] = {}
    for _, row in df.iterrows():
        out.setdefault(row["table_name"], set()).add(row["column_name"])
    return out


def format_drift_report(
    report: DriftReport, *, max_items_per_file: int = 20
) -> str:
    """Render a DriftReport as a multi-line string for terminal output.

    Truncates per-file listings beyond ``max_items_per_file`` with a
    summary line so a wholesale schema change doesn't dump hundreds of
    items into the terminal.
    """
    if report.is_clean:
        return "grounding clean: no drift detected."
    parts: list[str] = [
        f"grounding drift: {len(report.items)} reference(s) don't resolve",
        "",
    ]
    for file, items in report.by_file().items():
        parts.append(f"  {file}:")
        for item in items[:max_items_per_file]:
            loc = f":{item.line}" if item.line else ""
            sug = f"  (did you mean: {item.suggestion}?)" if item.suggestion else ""
            parts.append(f"    {loc:<5} {item.kind:<10} {item.claim!r}{sug}")
        if len(items) > max_items_per_file:
            parts.append(f"    ... and {len(items) - max_items_per_file} more")
        parts.append("")
    return "\n".join(parts).rstrip()
