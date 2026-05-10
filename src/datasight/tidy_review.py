"""Plan-file loading, validation, and apply pipeline for ``datasight tidy review``.

``tidy review`` is the LLM-augmented sibling of ``tidy {suggest,view,table}``.
This module owns the pieces that are not LLM-specific: the JSON plan format,
its validator, and the transactional apply pipeline that builds the long-form
target, verifies the reshape, and applies the source disposition. The LLM call
and the interactive prompt loop live in ``cli_commands/tidy.py`` and reuse
the helpers here.

The plan format is a small JSON envelope with a list of proposals. Each
proposal mirrors the on-disk shape of a :class:`TidySuggestion` so a plan
can be hand-written, emitted by the deterministic detector, or returned by
the LLM and then round-tripped through ``tidy review --from``. Validation
runs in two passes:

- :func:`load_plan` parses the JSON envelope and structural fields. It does
  not touch the database.
- :func:`validate_against_schema` cross-checks each suggestion against the
  live schema (column existence, target collision). Run this just before
  applying so stale plans are caught early.

The apply pipeline (:func:`apply_proposal`) wraps each proposal in a DuckDB
transaction. After the DDL runs we verify
``count(target) == count(source) × len(column_mappings)`` — a cheap sanity
check that catches the most common authoring mistake (id-column omissions
silently dropping or duplicating rows). Source disposition runs only after
the verify passes; on any failure the transaction rolls back and the
database is left unchanged.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, cast

import yaml
from loguru import logger

from datasight.schema import _quote_identifier
from datasight.tidy import (
    CONFIDENCE_LEVELS,
    DIMENSION_KINDS,
    ColumnMapping,
    Dimension,
    TidySuggestion,
    _build_reshape_sql,
)

# Bumped if the plan format gains a backwards-incompatible field. Today's
# loader only accepts version 1; older or newer plans get a clear error
# instead of being silently coerced.
PLAN_VERSION = 1


SourceDispositionMode = Literal["keep", "rename", "replace", "drop"]


@dataclass
class SourceDisposition:
    """What to do with the source table after a successful reshape.

    Four modes, each producing a different end state:

    - ``keep`` (default) — source stays put. Long form lives at
      ``target_object_name`` alongside it. Useful for comparing wide and
      long forms.
    - ``rename`` — source is renamed to ``new_name``. Long form lives at
      ``target_object_name``. Requires ``new_name``.
    - ``replace`` — source is dropped, then the long form is renamed to
      take over the source's old name. Downstream code that referenced
      the source keeps working without edits. The chosen
      ``target_object_name`` is effectively a temporary intermediate
      name.
    - ``drop`` — source is dropped. The long form keeps its
      ``target_object_name``. Downstream code that referenced the source
      will break; pick this when the new shape is the canonical one
      going forward.
    """

    mode: SourceDispositionMode = "keep"
    new_name: str | None = None

    def __post_init__(self) -> None:
        if self.mode == "rename" and not self.new_name:
            msg = "SourceDisposition(mode='rename') requires new_name"
            raise ValueError(msg)


@dataclass
class ApplyResult:
    """Audit-log entry returned by :func:`apply_proposal`.

    Carries enough information to reconstruct what changed: the source and
    target objects, how many rows moved, what happened to the source, and
    whether this was a dry run. Serialized into the audit log surfaced by
    ``tidy review`` at the end of a run.

    ``final_target_name`` is the name the long-form object goes by *after*
    the disposition step. It differs from ``target_object_name`` only when
    ``source_disposition == "replace"``, in which case the long form takes
    over the source's old name (the source is dropped first).
    """

    table: str
    target_object_name: str
    object_type: str  # "table" | "view"
    affected_columns: list[str]
    row_count_source: int
    row_count_target: int
    source_disposition: str
    source_renamed_to: str | None
    final_target_name: str
    dry_run: bool
    ddl: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "target_object_name": self.target_object_name,
            "final_target_name": self.final_target_name,
            "object_type": self.object_type,
            "affected_columns": list(self.affected_columns),
            "row_count_source": self.row_count_source,
            "row_count_target": self.row_count_target,
            "source_disposition": self.source_disposition,
            "source_renamed_to": self.source_renamed_to,
            "dry_run": self.dry_run,
        }


@dataclass
class Plan:
    """Parsed plan envelope. Carries the version + list of suggestions."""

    version: int = PLAN_VERSION
    proposals: list[TidySuggestion] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Plan I/O
# ---------------------------------------------------------------------------


def load_plan(path: Path) -> Plan:
    """Load and structurally validate a plan file. Raises ``ValueError`` on problems."""
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        msg = f"Plan must be a JSON object, got {type(raw).__name__}"
        raise ValueError(msg)
    version = raw.get("version")
    if version != PLAN_VERSION:
        msg = f"Unsupported plan version {version!r}; this build expects {PLAN_VERSION}"
        raise ValueError(msg)
    proposals_raw = raw.get("proposals", [])
    if not isinstance(proposals_raw, list):
        msg = "Plan 'proposals' must be a list"
        raise ValueError(msg)
    proposals = [_parse_proposal(p, index=i) for i, p in enumerate(proposals_raw)]
    return Plan(version=version, proposals=proposals)


def dump_plan(suggestions: list[TidySuggestion], path: Path) -> None:
    """Write a list of suggestions to a plan file (the inverse of :func:`load_plan`)."""
    payload = {
        "version": PLAN_VERSION,
        "proposals": [_suggestion_to_proposal_dict(s) for s in suggestions],
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _suggestion_to_proposal_dict(s: TidySuggestion) -> dict[str, Any]:
    """Serialize a TidySuggestion into the on-disk proposal shape.

    Skips ``reshape_sql`` — it's deterministically derivable from the other
    fields and including it would just bloat the file and risk getting out of
    sync with edits to the dimensions or mappings.
    """
    return {
        "pattern": s.pattern,
        "table": s.table,
        "dimensions": [d.to_dict() for d in s.dimensions],
        "id_columns": list(s.id_columns),
        "value_column": s.value_column,
        "target_object_name": s.target_object_name,
        "column_mappings": [m.to_dict() for m in s.column_mappings],
        "confidence": s.confidence,
        "source": s.source,
        "rationale": s.rationale,
        "include_nulls": s.include_nulls,
    }


def _parse_proposal(p: Any, *, index: int) -> TidySuggestion:  # noqa: C901
    """Parse one proposal dict into a :class:`TidySuggestion`.

    Pure structural validation only — no database lookups. Cross-checks
    against the live schema (column existence, target collision) happen in
    :func:`validate_against_schema` so a plan can be parsed offline.
    """
    where = f"proposal[{index}]"
    if not isinstance(p, dict):
        msg = f"{where}: must be an object, got {type(p).__name__}"
        raise ValueError(msg)

    table = _required_str(p, "table", where)
    dimensions = _parse_dimensions(p.get("dimensions"), where)
    dim_names = [d.name for d in dimensions]
    column_mappings = _parse_column_mappings(p.get("column_mappings"), dim_names, where)

    id_columns_raw = p.get("id_columns") or []
    if not isinstance(id_columns_raw, list) or any(not isinstance(c, str) for c in id_columns_raw):
        msg = f"{where}: 'id_columns' must be a list of strings"
        raise ValueError(msg)
    if len(set(id_columns_raw)) != len(id_columns_raw):
        msg = f"{where}: duplicate id_columns {id_columns_raw}"
        raise ValueError(msg)
    mapped_columns = {m.column for m in column_mappings}
    overlap = set(id_columns_raw) & mapped_columns
    if overlap:
        msg = f"{where}: id_columns overlap column_mappings: {sorted(overlap)}"
        raise ValueError(msg)

    value_column = p.get("value_column") or "value"
    if not isinstance(value_column, str) or not value_column:
        msg = f"{where}: 'value_column' must be a non-empty string"
        raise ValueError(msg)
    target_object_name = p.get("target_object_name") or f"{table}_long"
    if not isinstance(target_object_name, str) or not target_object_name:
        msg = f"{where}: 'target_object_name' must be a non-empty string"
        raise ValueError(msg)

    confidence = p.get("confidence", "high")
    if confidence not in CONFIDENCE_LEVELS:
        msg = (
            f"{where}: 'confidence' must be one of {sorted(CONFIDENCE_LEVELS)}, got {confidence!r}"
        )
        raise ValueError(msg)
    source = p.get("source", "user")
    if not isinstance(source, str):
        msg = f"{where}: 'source' must be a string"
        raise ValueError(msg)
    rationale = p.get("rationale", "")
    if not isinstance(rationale, str):
        msg = f"{where}: 'rationale' must be a string"
        raise ValueError(msg)
    pattern = p.get("pattern") or "user_proposed"
    if not isinstance(pattern, str):
        msg = f"{where}: 'pattern' must be a string"
        raise ValueError(msg)

    # Default False matches TidySuggestion's default — most NULLs in wide
    # tables are structural placeholders, not real missing observations.
    include_nulls_raw = p.get("include_nulls", False)
    if not isinstance(include_nulls_raw, bool):
        msg = f"{where}: 'include_nulls' must be a boolean"
        raise ValueError(msg)
    include_nulls = include_nulls_raw

    reshape_sql = _build_reshape_sql(
        table=table,
        id_columns=list(id_columns_raw),
        dimensions=dimensions,
        column_mappings=column_mappings,
        value_column=value_column,
        target_object_name=target_object_name,
        mode="table",
        include_nulls=include_nulls,
    )
    return TidySuggestion(
        pattern=pattern,
        table=table,
        dimensions=dimensions,
        column_mappings=column_mappings,
        id_columns=list(id_columns_raw),
        value_column=value_column,
        target_object_name=target_object_name,
        rationale=rationale,
        reshape_sql=reshape_sql,
        confidence=confidence,
        source=source,
        include_nulls=include_nulls,
    )


def _required_str(d: dict[str, Any], key: str, where: str) -> str:
    value = d.get(key)
    if not isinstance(value, str) or not value:
        msg = f"{where}: {key!r} must be a non-empty string"
        raise ValueError(msg)
    return value


def _parse_dimensions(raw: Any, where: str) -> list[Dimension]:
    if not isinstance(raw, list) or not raw:
        msg = f"{where}: 'dimensions' must be a non-empty list"
        raise ValueError(msg)
    dimensions: list[Dimension] = []
    for di, d in enumerate(raw):
        sub = f"{where}.dimensions[{di}]"
        if not isinstance(d, dict):
            msg = f"{sub}: must be an object"
            raise ValueError(msg)
        # ty narrows `dict` past `isinstance` to dict[Never, Never], so casts
        # are needed to access string keys.
        d_dict = cast(dict[str, Any], d)
        name = d_dict.get("name")
        kind = d_dict.get("kind")
        if not isinstance(name, str) or not name:
            msg = f"{sub}: 'name' must be a non-empty string"
            raise ValueError(msg)
        if not isinstance(kind, str) or kind not in DIMENSION_KINDS:
            msg = f"{sub}: 'kind' must be one of {sorted(DIMENSION_KINDS)}, got {kind!r}"
            raise ValueError(msg)
        dtype = d_dict.get("dtype", "VARCHAR")
        if not isinstance(dtype, str) or dtype.upper() not in _ALLOWED_DTYPES:
            msg = f"{sub}: 'dtype' must be one of {sorted(_ALLOWED_DTYPES)}, got {dtype!r}"
            raise ValueError(msg)
        dimensions.append(Dimension(name=name, kind=kind, dtype=dtype.upper()))
    names = [d.name for d in dimensions]
    if len(set(names)) != len(names):
        msg = f"{where}: duplicate dimension names {names}"
        raise ValueError(msg)
    return dimensions


# Whitelist of dimension column dtypes. Restricted because the value lands
# inside a generated CAST clause, so accepting arbitrary strings would let
# a hand-crafted plan inject SQL. Covers the period kinds the deterministic
# detector emits (year/hour/day → INTEGER) plus the common LLM-friendly
# string/date/numeric options.
_ALLOWED_DTYPES: frozenset[str] = frozenset(
    {
        "VARCHAR",
        "INTEGER",
        "BIGINT",
        "SMALLINT",
        "DOUBLE",
        "DATE",
        "TIMESTAMP",
    }
)


def _parse_column_mappings(raw: Any, dim_names: list[str], where: str) -> list[ColumnMapping]:  # noqa: C901
    if not isinstance(raw, list):
        msg = f"{where}: 'column_mappings' must be a list"
        raise ValueError(msg)
    if len(raw) < 2:
        msg = f"{where}: 'column_mappings' must have at least 2 entries to be a meaningful reshape"
        raise ValueError(msg)
    mappings: list[ColumnMapping] = []
    seen_columns: set[str] = set()
    seen_value_tuples: set[tuple[str, ...]] = set()
    for mi, m in enumerate(raw):
        sub = f"{where}.column_mappings[{mi}]"
        if not isinstance(m, dict):
            msg = f"{sub}: must be an object"
            raise ValueError(msg)
        m_dict = cast(dict[str, Any], m)
        col = m_dict.get("column")
        dv = m_dict.get("dimension_values")
        if not isinstance(col, str) or not col:
            msg = f"{sub}: 'column' must be a non-empty string"
            raise ValueError(msg)
        if col in seen_columns:
            msg = f"{sub}: duplicate column {col!r}"
            raise ValueError(msg)
        seen_columns.add(col)
        if not isinstance(dv, dict) or set(dv.keys()) != set(dim_names):
            msg = f"{sub}: 'dimension_values' keys must equal {sorted(dim_names)}, got {sorted(dv) if isinstance(dv, dict) else dv!r}"
            raise ValueError(msg)
        for dn, dval in dv.items():
            if not isinstance(dval, str):
                msg = (
                    f"{sub}.dimension_values[{dn!r}]: must be a string, got {type(dval).__name__}"
                )
                raise ValueError(msg)
        value_tuple = tuple(dv[dn] for dn in dim_names)
        if value_tuple in seen_value_tuples:
            msg = f"{sub}: duplicate dimension-value tuple {value_tuple} — every mapping must be unique"
            raise ValueError(msg)
        seen_value_tuples.add(value_tuple)
        mappings.append(ColumnMapping(column=col, dimension_values=dict(dv)))
    return mappings


# ---------------------------------------------------------------------------
# Schema cross-validation
# ---------------------------------------------------------------------------


def validate_against_schema(
    suggestion: TidySuggestion, schema_info: list[dict[str, Any]]
) -> list[str]:
    """Return a list of human-readable problems; empty list means valid.

    Catches the class of bugs that depend on the live database state:
    columns the plan references that don't actually exist, and target names
    that would collide with an existing object. Cross-dimension uniqueness
    and structural shape are already enforced by :func:`load_plan`.
    """
    problems: list[str] = []
    table = next((t for t in schema_info if t["name"] == suggestion.table), None)
    if table is None:
        problems.append(f"source table {suggestion.table!r} not found in database")
        return problems
    column_names = {c["name"] for c in table.get("columns", [])}
    for m in suggestion.column_mappings:
        if m.column not in column_names:
            problems.append(
                f"column {m.column!r} (mapped to {m.dimension_values}) "
                f"is not in {suggestion.table}"
            )
    for c in suggestion.id_columns:
        if c not in column_names:
            problems.append(f"id_column {c!r} is not in {suggestion.table}")
    if suggestion.target_object_name == suggestion.table:
        problems.append(
            f"target_object_name {suggestion.target_object_name!r} "
            f"would collide with the source table"
        )
    elif any(t["name"] == suggestion.target_object_name for t in schema_info):
        problems.append(
            f"target_object_name {suggestion.target_object_name!r} "
            f"already exists; pick a different name or drop it first"
        )
    return problems


# ---------------------------------------------------------------------------
# Apply pipeline
# ---------------------------------------------------------------------------


def apply_proposal(  # noqa: C901
    conn: Any,
    suggestion: TidySuggestion,
    *,
    mode: str,
    source_disposition: SourceDisposition,
    dry_run: bool,
) -> ApplyResult:
    """Apply one proposal inside a DuckDB transaction.

    Steps:

    1. Build the long-form target with the suggestion's DDL.
    2. Verify ``count(target) == count(source) × len(column_mappings)``.
       This catches id-column omissions and dropped-row bugs *before* the
       source is touched.
    3. Apply source disposition:

       - ``keep`` — no-op.
       - ``rename`` — ``ALTER TABLE/VIEW … RENAME TO`` the source.
       - ``replace`` — drop the source, then rename the long-form target
         to the source's old name. The long form replaces the source so
         downstream consumers (and any ``schema.yaml`` allowlist) keep
         working without manual edits. The user-chosen
         ``target_object_name`` is effectively a transient intermediate
         here.
       - ``drop`` — drop the source, leave the long form at its
         ``target_object_name``. Pick this when the new shape is the
         canonical one going forward and you don't need to preserve the
         source's name.

    Source / target may be either tables or views (e.g. a view backed by a
    CSV). The DDL keyword is chosen per object via ``duckdb_views()``.

    Any failure rolls the transaction back, leaving the database unchanged.
    Dry-run skips the transaction entirely and returns a preview audit
    entry derived from the source's pre-run row count so the caller can
    show a "would create N rows" message without touching state.
    """
    if mode not in ("table", "view"):
        msg = f"mode must be 'view' or 'table', got {mode!r}"
        raise ValueError(msg)
    # A view's body references its source by name. Renaming or dropping
    # the source while the long form is a view leaves a dangling reference
    # — or, for ``replace`` (which renames the long form into the source's
    # slot), an *infinite recursion* on bind. ``keep`` is the only mode
    # that leaves the source untouched, so anything else needs ``--as
    # table`` to materialize the long form before we modify the source.
    if mode == "view" and source_disposition.mode != "keep":
        if source_disposition.mode == "replace":
            consequence = "recursively self-referencing"
        else:
            consequence = "pointing at a missing object"
        verb = {
            "rename": "renaming",
            "replace": "replacing",
            "drop": "dropping",
        }[source_disposition.mode]
        msg = (
            f"source disposition {source_disposition.mode!r} requires --as "
            f"table: a view references its source by name, so {verb} "
            f"the source would leave the view {consequence}."
        )
        raise ValueError(msg)
    ddl = suggestion.build_sql(mode)

    final_target_name = (
        suggestion.table if source_disposition.mode == "replace" else suggestion.target_object_name
    )

    if dry_run:
        source_rows = _query_count(conn, suggestion.table)
        return ApplyResult(
            table=suggestion.table,
            target_object_name=suggestion.target_object_name,
            object_type=mode,
            affected_columns=suggestion.affected_columns,
            row_count_source=source_rows,
            row_count_target=source_rows * len(suggestion.column_mappings),
            source_disposition=source_disposition.mode,
            source_renamed_to=(
                source_disposition.new_name if source_disposition.mode == "rename" else None
            ),
            final_target_name=final_target_name,
            dry_run=True,
            ddl=ddl,
        )

    conn.execute("BEGIN")
    try:
        source_rows = _query_count(conn, suggestion.table)
        conn.execute(ddl)
        target_rows = _query_count(conn, suggestion.target_object_name)
        expected = source_rows * len(suggestion.column_mappings)
        # When the proposal preserves NULLs, the row count must be exact:
        # source_rows × mapped columns. id_column omissions or dropped
        # rows show up here loud and clear. When the proposal drops NULLs
        # we can't predict the count without an extra scan; the only
        # invariant left is that the target sits within [0, expected].
        # Anything outside that range still indicates a builder bug.
        if suggestion.include_nulls:
            if target_rows != expected:
                msg = (
                    f"Reshape verification failed for {suggestion.target_object_name!r}: "
                    f"expected {expected} rows ({source_rows} source rows × "
                    f"{len(suggestion.column_mappings)} mapped columns), got {target_rows}. "
                    f"This usually means id_columns omits a column whose values "
                    f"duplicate or drop rows."
                )
                raise RuntimeError(msg)
        else:
            if target_rows < 0 or target_rows > expected:
                msg = (
                    f"Reshape verification failed for {suggestion.target_object_name!r}: "
                    f"got {target_rows} rows, but with include_nulls=False the result "
                    f"must sit between 0 and {expected} (source × mapped columns). "
                    f"An out-of-range count signals a builder bug."
                )
                raise RuntimeError(msg)
        source_renamed_to: str | None = None
        if source_disposition.mode == "rename":
            assert source_disposition.new_name is not None
            source_kw = _alter_keyword(conn, suggestion.table)
            conn.execute(
                f"ALTER {source_kw} {_quote_identifier(suggestion.table)} "
                f"RENAME TO {_quote_identifier(source_disposition.new_name)}"
            )
            source_renamed_to = source_disposition.new_name
        elif source_disposition.mode == "replace":
            # Drop the source, then rename the long form into its slot so
            # downstream consumers (and any schema.yaml allowlist) keep
            # working without manual edits.
            source_kw = _drop_keyword(conn, suggestion.table)
            conn.execute(f"DROP {source_kw} {_quote_identifier(suggestion.table)}")
            target_kw = _alter_keyword(conn, suggestion.target_object_name)
            conn.execute(
                f"ALTER {target_kw} {_quote_identifier(suggestion.target_object_name)} "
                f"RENAME TO {_quote_identifier(suggestion.table)}"
            )
        elif source_disposition.mode == "drop":
            # Bare drop: the long form keeps its target_object_name. Any
            # downstream code that referenced the source by name will need
            # to update its references.
            source_kw = _drop_keyword(conn, suggestion.table)
            conn.execute(f"DROP {source_kw} {_quote_identifier(suggestion.table)}")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    return ApplyResult(
        table=suggestion.table,
        target_object_name=suggestion.target_object_name,
        object_type=mode,
        affected_columns=suggestion.affected_columns,
        row_count_source=source_rows,
        row_count_target=target_rows,
        source_disposition=source_disposition.mode,
        source_renamed_to=source_renamed_to,
        final_target_name=final_target_name,
        dry_run=False,
        ddl=ddl,
    )


def _query_count(conn: Any, object_name: str) -> int:
    row = conn.execute(f"SELECT COUNT(*) FROM {_quote_identifier(object_name)}").fetchone()
    if row is None:
        return 0
    return int(row[0])


def _object_kind(conn: Any, name: str) -> str:
    """Return ``'view'`` or ``'table'`` for ``name`` in DuckDB.

    Falls back to ``'table'`` when the object cannot be classified — the
    caller already verified the object exists (the apply flow either just
    queried its row count or just created it), so an unknown classification
    almost always means a base table the introspection query missed.
    """
    quoted = name.replace("'", "''")
    # Scope to the current database/schema so an unqualified name resolves
    # the same way it does in the surrounding DDL — otherwise a same-named
    # view in another attached DB or non-default schema could pick the
    # wrong row and flip the keyword.
    if conn.execute(
        f"SELECT 1 FROM duckdb_views() WHERE view_name = '{quoted}' "
        "AND schema_name = current_schema() "
        "AND database_name = current_database()"
    ).fetchone():
        return "view"
    return "table"


def _drop_keyword(conn: Any, name: str) -> str:
    """``'VIEW'`` if ``name`` is a view, otherwise ``'TABLE'``."""
    return "VIEW" if _object_kind(conn, name) == "view" else "TABLE"


def _alter_keyword(conn: Any, name: str) -> str:
    """``'VIEW'`` if ``name`` is a view, otherwise ``'TABLE'`` — for ``ALTER``."""
    return "VIEW" if _object_kind(conn, name) == "view" else "TABLE"


def update_schema_yaml_for_apply(  # noqa: C901
    project_dir: str,
    *,
    source_table: str,
    target_table: str,
    disposition_mode: str,
    rename_to: str | None = None,
    create_if_absent: bool = False,
) -> bool:
    """Sync ``schema.yaml`` with what just changed in the database.

    By default this is a no-op when ``schema.yaml`` is absent — the project
    doesn't maintain an allowlist, so live introspection already exposes
    whatever's there. The CLI relies on this default so a one-off
    ``tidy review`` doesn't side-effect a project that never had an
    allowlist. Pass ``create_if_absent=True`` from contexts where the user
    explicitly opted in to persisting the reshape (e.g. the web Apply
    button) — the helper will materialize a fresh ``schema.yaml`` listing
    both the source and the new long-form object.

    When the file is present (or being created), the goal is to keep it
    aligned with the database so introspected tables don't silently
    disappear from the UI:

    - ``keep`` — append a new entry for ``target_table`` so the long form
      shows up alongside the source.
    - ``rename`` — rename the existing source entry to ``rename_to`` and
      append a new entry for ``target_table``.
    - ``replace`` — leave the source entry's *name* in place (the long
      form took it over), but clear any ``columns`` /
      ``excluded_columns`` filter on it since the long form has a
      different shape and an old filter would hide the new columns.
    - ``drop`` — remove the source entry entirely and append a new entry
      for ``target_table``. Downstream code referencing the source by
      name will break; the allowlist reflects that.

    Returns ``True`` when the file was rewritten (or freshly written).
    Comments and original formatting in the YAML are not preserved — the
    file is round-tripped through ``yaml.safe_dump``.
    """
    path = Path(project_dir) / "schema.yaml"
    if not path.exists():
        if not create_if_absent:
            return False
        # Seed with the source table so the long-form append below has the
        # same starting point as the existing-file path. Without this seed,
        # a freshly created allowlist would silently exclude the source.
        data: dict[str, Any] = {"tables": [{"name": source_table}]}
    else:
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as exc:
            logger.warning(f"schema.yaml: not updated (parse error: {exc})")
            return False
        if not isinstance(data, dict):
            logger.warning(
                f"schema.yaml: not updated (expected mapping, got {type(data).__name__})"
            )
            return False
    raw_tables = data.get("tables")
    if raw_tables is not None and not isinstance(raw_tables, list):
        logger.warning("schema.yaml: 'tables' must be a list, ignoring the file")
        return False
    tables: list[Any] = list(raw_tables) if isinstance(raw_tables, list) else []

    source_entry: dict[str, Any] | None = None
    for entry in tables:
        if isinstance(entry, dict) and entry.get("name") == source_table:
            source_entry = cast(dict[str, Any], entry)
            break

    match disposition_mode:
        case "replace":
            # Long form took over the source's name: same slot, different
            # columns. Clear any old column filter so the new columns
            # surface.
            if source_entry is not None:
                source_entry.pop("columns", None)
                source_entry.pop("excluded_columns", None)
            else:
                tables.append({"name": source_table})
            # Remove stale target_table entries from a prior keep/rename.
            tables = [
                e for e in tables if not (isinstance(e, dict) and e.get("name") == target_table)
            ]
        case "drop":
            # Bare drop: source goes away entirely, long form lives at
            # target_table.
            tables = [
                e for e in tables if not (isinstance(e, dict) and e.get("name") == source_table)
            ]
            if not _has_table_entry(tables, target_table):
                tables.append({"name": target_table})
        case "rename":
            if source_entry is not None and rename_to:
                source_entry["name"] = rename_to
            if not _has_table_entry(tables, target_table):
                tables.append({"name": target_table})
        case _:  # keep
            if not _has_table_entry(tables, target_table):
                tables.append({"name": target_table})

    data["tables"] = tables
    path.write_text(
        yaml.safe_dump(data, sort_keys=False, default_flow_style=False),
        encoding="utf-8",
    )
    return True


def _has_table_entry(tables: list[Any], name: str) -> bool:
    return any(isinstance(e, dict) and e.get("name") == name for e in tables)


_MEASURES_YAML_HEADER = (
    "# datasight measure overrides\n"
    "# Edit these entries to lock in project-specific aggregation behavior.\n"
    "# Fields omitted here will keep using inferred defaults.\n"
    "\n"
)


def _build_value_entry(suggestion: TidySuggestion, table: str) -> dict[str, Any]:
    """Build the seeded measures.yaml entry for the long form's value column.

    Mirrors what ``datasight generate`` writes: full inferred fields with
    None / empty values stripped, so the user can see what's available to
    override without consulting docs. The dtype assumption (DOUBLE) is
    safe because DuckDB UNPIVOT always widens the value column to the
    broadest numeric type of the mapped columns.
    """
    # Late import — data_profile imports tidy_review-adjacent helpers at
    # module load, so importing it at the top here would create a cycle.
    from datasight.data_profile import _infer_measure_semantics

    value_column = suggestion.value_column
    sibling_columns = (
        list(suggestion.id_columns) + [d.name for d in suggestion.dimensions] + [value_column]
    )
    inferred = _infer_measure_semantics(value_column, "DOUBLE", sibling_columns)
    if inferred is None:
        return {"table": table, "column": value_column}

    candidate: dict[str, Any] = {
        "table": table,
        "column": value_column,
        "role": inferred.get("role", "measure"),
        "unit": inferred.get("unit"),
        "default_aggregation": inferred.get("default_aggregation", "avg"),
        "average_strategy": inferred.get("average_strategy", "avg"),
        "weight_column": inferred.get("weight_column"),
        "allowed_aggregations": inferred.get("allowed_aggregations", []),
        "forbidden_aggregations": inferred.get("forbidden_aggregations", []),
        "additive_across_category": bool(inferred.get("additive_across_category")),
        "additive_across_time": bool(inferred.get("additive_across_time")),
        "reason": inferred.get("reason", ""),
    }
    return {key: value for key, value in candidate.items() if value not in (None, [], "")}


def update_measures_yaml_for_apply(  # noqa: C901
    project_dir: str,
    *,
    suggestion: TidySuggestion,
    result: ApplyResult,
    create_if_absent: bool = False,
) -> bool:
    """Sync ``measures.yaml`` with what just changed in the database.

    The wide-form measure columns get pivoted into a single value column on
    the long form, so any pre-existing measure overrides that target those
    source columns become stale (the columns no longer exist on the
    relevant table). This helper cleans those up per disposition mode and
    seeds a fully-inferred entry for the new value column so the user has
    somewhere to attach overrides:

    - ``keep`` — source still has the mapped columns; existing entries
      stay valid. Append a fresh entry for the long form's value column.
    - ``rename`` — the source's table name moved; rewrite ``table`` on
      every entry from ``suggestion.table`` to ``result.source_renamed_to``
      (the columns themselves haven't changed, just the table name).
      Append a fresh entry for the long form's value column.
    - ``replace`` — the source's wide pivot columns are gone (collapsed
      into the value column on the long form, which then takes over the
      source's name). Drop the mapped-column entries on ``source_table``
      (its surviving id-columns keep theirs), and rewrite any entries
      pointing at the intermediate ``target_object_name`` to the final
      name. Append a fresh entry for the value column.
    - ``drop`` — the source table is dropped entirely. Remove every
      entry that references it, then append a fresh entry for the value
      column on the long form (still at ``target_object_name``).

    The seeded entry uses the same inference path as ``datasight
    generate`` so the file remains scannable: role, default_aggregation,
    allowed_aggregations, etc. all show up with sensible defaults the
    user can edit. Existing user overrides on other entries are preserved
    verbatim.

    By default this is a no-op when ``measures.yaml`` is absent — matches
    the CLI semantics for ``schema.yaml``. Pass ``create_if_absent=True``
    from the web Apply path so an explicit user action persists.

    Returns ``True`` when the file was rewritten (or freshly written).
    Comments and original formatting beyond the leading datasight header
    are not preserved — the entries are round-tripped through
    ``yaml.safe_dump``.
    """
    path = Path(project_dir) / "measures.yaml"
    if not path.exists():
        if not create_if_absent:
            return False
        existing: list[Any] = []
    else:
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or []
        except yaml.YAMLError as exc:
            logger.warning(f"measures.yaml: not updated (parse error: {exc})")
            return False
        if not isinstance(data, list):
            logger.warning(
                f"measures.yaml: not updated (expected list, got {type(data).__name__})"
            )
            return False
        existing = list(data)

    mapped_columns = {m.column for m in suggestion.column_mappings}
    source_table = suggestion.table

    match result.source_disposition:
        case "rename":
            new_name = result.source_renamed_to
            updated: list[Any] = []
            for entry in existing:
                if isinstance(entry, dict) and entry.get("table") == source_table and new_name:
                    renamed = dict(entry)
                    renamed["table"] = new_name
                    updated.append(renamed)
                else:
                    updated.append(entry)
        case "replace":
            # Source's wide pivot columns are gone; long form is renamed
            # from target_object_name to take over source_table's name.
            # Pre-existing entries pointing at target_object_name now
            # reference a name that no longer exists — rewrite them.
            intermediate = suggestion.target_object_name
            final_name = result.final_target_name
            updated = []
            for entry in existing:
                if not isinstance(entry, dict):
                    updated.append(entry)
                    continue
                entry_table = entry.get("table")
                if entry_table == source_table and entry.get("column") in mapped_columns:
                    continue
                if entry_table == intermediate and intermediate != final_name:
                    rewritten = dict(entry)
                    rewritten["table"] = final_name
                    updated.append(rewritten)
                else:
                    updated.append(entry)
        case "drop":
            # Source table is gone entirely — every entry referencing it
            # is now stale, not just the mapped pivot columns.
            updated = [
                entry
                for entry in existing
                if not (isinstance(entry, dict) and entry.get("table") == source_table)
            ]
        case _:  # keep
            updated = list(existing)

    value_table = result.final_target_name
    value_column = suggestion.value_column
    has_value_entry = any(
        isinstance(entry, dict)
        and entry.get("table") == value_table
        and entry.get("column") == value_column
        for entry in updated
    )
    if not has_value_entry:
        updated.append(_build_value_entry(suggestion, value_table))

    if updated == existing:
        return False

    body = yaml.safe_dump(updated, sort_keys=False, allow_unicode=False).strip()
    path.write_text(_MEASURES_YAML_HEADER + body + "\n", encoding="utf-8")
    return True


def resolve_source_disposition(
    keep: bool,
    rename_to: str | None,
    replace: bool,
    drop: bool,
) -> SourceDisposition:
    """Translate the four CLI flags into a :class:`SourceDisposition`.

    Enforces mutual exclusion: at most one of the four may be active. The
    default (no flag set) is ``keep`` to match the documented behavior.

    ``replace`` corresponds to ``--replace-source`` (drop source, long form
    takes over the source's name). ``drop`` corresponds to
    ``--drop-source`` (drop source, long form keeps its target name) — a
    breaking change from the prior CLI semantics where ``--drop-source``
    meant what ``--replace-source`` now means.
    """
    active = sum([keep, rename_to is not None, replace, drop])
    if active > 1:
        msg = (
            "--keep-source, --rename-source, --replace-source, and "
            "--drop-source are mutually exclusive"
        )
        raise ValueError(msg)
    if rename_to is not None:
        return SourceDisposition(mode="rename", new_name=rename_to)
    if replace:
        return SourceDisposition(mode="replace")
    if drop:
        return SourceDisposition(mode="drop")
    return SourceDisposition(mode="keep")


__all__ = [
    "PLAN_VERSION",
    "ApplyResult",
    "Plan",
    "SourceDisposition",
    "SourceDispositionMode",
    "apply_proposal",
    "dump_plan",
    "load_plan",
    "resolve_source_disposition",
    "update_measures_yaml_for_apply",
    "update_schema_yaml_for_apply",
    "validate_against_schema",
]
