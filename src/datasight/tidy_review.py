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


SourceDispositionMode = Literal["keep", "rename", "drop"]


@dataclass
class SourceDisposition:
    """What to do with the source table after a successful reshape.

    ``mode='rename'`` requires ``new_name``. The other two modes ignore it.
    ``--keep-source`` is the default; it leaves the source untouched so a
    developer can compare wide and long forms side by side.
    """

    mode: SourceDispositionMode = "keep"
    new_name: str | None = None

    def __post_init__(self) -> None:
        if self.mode == "rename" and not self.new_name:
            raise ValueError("SourceDisposition(mode='rename') requires new_name")


@dataclass
class ApplyResult:
    """Audit-log entry returned by :func:`apply_proposal`.

    Carries enough information to reconstruct what changed: the source and
    target objects, how many rows moved, what happened to the source, and
    whether this was a dry run. Serialized into the audit log surfaced by
    ``tidy review`` at the end of a run.

    ``final_target_name`` is the name the long-form object goes by *after*
    the disposition step. It differs from ``target_object_name`` only when
    ``source_disposition == "drop"``, in which case the long form takes
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
        raise ValueError(f"Plan must be a JSON object, got {type(raw).__name__}")
    version = raw.get("version")
    if version != PLAN_VERSION:
        raise ValueError(
            f"Unsupported plan version {version!r}; this build expects {PLAN_VERSION}"
        )
    proposals_raw = raw.get("proposals", [])
    if not isinstance(proposals_raw, list):
        raise ValueError("Plan 'proposals' must be a list")
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
    }


def _parse_proposal(p: Any, *, index: int) -> TidySuggestion:
    """Parse one proposal dict into a :class:`TidySuggestion`.

    Pure structural validation only — no database lookups. Cross-checks
    against the live schema (column existence, target collision) happen in
    :func:`validate_against_schema` so a plan can be parsed offline.
    """
    where = f"proposal[{index}]"
    if not isinstance(p, dict):
        raise ValueError(f"{where}: must be an object, got {type(p).__name__}")

    table = _required_str(p, "table", where)
    dimensions = _parse_dimensions(p.get("dimensions"), where)
    dim_names = [d.name for d in dimensions]
    column_mappings = _parse_column_mappings(p.get("column_mappings"), dim_names, where)

    id_columns_raw = p.get("id_columns") or []
    if not isinstance(id_columns_raw, list) or any(not isinstance(c, str) for c in id_columns_raw):
        raise ValueError(f"{where}: 'id_columns' must be a list of strings")
    if len(set(id_columns_raw)) != len(id_columns_raw):
        raise ValueError(f"{where}: duplicate id_columns {id_columns_raw}")
    mapped_columns = {m.column for m in column_mappings}
    overlap = set(id_columns_raw) & mapped_columns
    if overlap:
        raise ValueError(f"{where}: id_columns overlap column_mappings: {sorted(overlap)}")

    value_column = p.get("value_column") or "value"
    if not isinstance(value_column, str) or not value_column:
        raise ValueError(f"{where}: 'value_column' must be a non-empty string")
    target_object_name = p.get("target_object_name") or f"{table}_long"
    if not isinstance(target_object_name, str) or not target_object_name:
        raise ValueError(f"{where}: 'target_object_name' must be a non-empty string")

    confidence = p.get("confidence", "high")
    if confidence not in CONFIDENCE_LEVELS:
        raise ValueError(
            f"{where}: 'confidence' must be one of {sorted(CONFIDENCE_LEVELS)}, got {confidence!r}"
        )
    source = p.get("source", "user")
    if not isinstance(source, str):
        raise ValueError(f"{where}: 'source' must be a string")
    rationale = p.get("rationale", "")
    if not isinstance(rationale, str):
        raise ValueError(f"{where}: 'rationale' must be a string")
    pattern = p.get("pattern") or "user_proposed"
    if not isinstance(pattern, str):
        raise ValueError(f"{where}: 'pattern' must be a string")

    reshape_sql = _build_reshape_sql(
        table=table,
        id_columns=list(id_columns_raw),
        dimensions=dimensions,
        column_mappings=column_mappings,
        value_column=value_column,
        target_object_name=target_object_name,
        mode="table",
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
    )


def _required_str(d: dict[str, Any], key: str, where: str) -> str:
    value = d.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{where}: {key!r} must be a non-empty string")
    return value


def _parse_dimensions(raw: Any, where: str) -> list[Dimension]:
    if not isinstance(raw, list) or not raw:
        raise ValueError(f"{where}: 'dimensions' must be a non-empty list")
    dimensions: list[Dimension] = []
    for di, d in enumerate(raw):
        sub = f"{where}.dimensions[{di}]"
        if not isinstance(d, dict):
            raise ValueError(f"{sub}: must be an object")
        # ty narrows `dict` past `isinstance` to dict[Never, Never], so casts
        # are needed to access string keys.
        d_dict = cast(dict[str, Any], d)
        name = d_dict.get("name")
        kind = d_dict.get("kind")
        if not isinstance(name, str) or not name:
            raise ValueError(f"{sub}: 'name' must be a non-empty string")
        if not isinstance(kind, str) or kind not in DIMENSION_KINDS:
            raise ValueError(
                f"{sub}: 'kind' must be one of {sorted(DIMENSION_KINDS)}, got {kind!r}"
            )
        dimensions.append(Dimension(name=name, kind=kind))
    names = [d.name for d in dimensions]
    if len(set(names)) != len(names):
        raise ValueError(f"{where}: duplicate dimension names {names}")
    return dimensions


def _parse_column_mappings(raw: Any, dim_names: list[str], where: str) -> list[ColumnMapping]:
    if not isinstance(raw, list):
        raise ValueError(f"{where}: 'column_mappings' must be a list")
    if len(raw) < 2:
        raise ValueError(
            f"{where}: 'column_mappings' must have at least 2 entries to be a meaningful reshape"
        )
    mappings: list[ColumnMapping] = []
    seen_columns: set[str] = set()
    seen_value_tuples: set[tuple[str, ...]] = set()
    for mi, m in enumerate(raw):
        sub = f"{where}.column_mappings[{mi}]"
        if not isinstance(m, dict):
            raise ValueError(f"{sub}: must be an object")
        m_dict = cast(dict[str, Any], m)
        col = m_dict.get("column")
        dv = m_dict.get("dimension_values")
        if not isinstance(col, str) or not col:
            raise ValueError(f"{sub}: 'column' must be a non-empty string")
        if col in seen_columns:
            raise ValueError(f"{sub}: duplicate column {col!r}")
        seen_columns.add(col)
        if not isinstance(dv, dict) or set(dv.keys()) != set(dim_names):
            raise ValueError(
                f"{sub}: 'dimension_values' keys must equal {sorted(dim_names)}, got {sorted(dv) if isinstance(dv, dict) else dv!r}"
            )
        for dn, dval in dv.items():
            if not isinstance(dval, str):
                raise ValueError(
                    f"{sub}.dimension_values[{dn!r}]: must be a string, got {type(dval).__name__}"
                )
        value_tuple = tuple(dv[dn] for dn in dim_names)
        if value_tuple in seen_value_tuples:
            raise ValueError(
                f"{sub}: duplicate dimension-value tuple {value_tuple} — every mapping must be unique"
            )
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


def apply_proposal(
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
       - ``drop`` — drop the source, then rename the long-form target to
         the source's old name. The long form replaces the source so
         downstream consumers (and any ``schema.yaml`` allowlist) keep
         working without manual edits.

    Source / target may be either tables or views (e.g. a view backed by a
    CSV). The DDL keyword is chosen per object via ``information_schema``.

    Any failure rolls the transaction back, leaving the database unchanged.
    Dry-run skips the transaction entirely and returns a preview audit
    entry derived from the source's pre-run row count so the caller can
    show a "would create N rows" message without touching state.
    """
    if mode not in ("table", "view"):
        raise ValueError(f"mode must be 'view' or 'table', got {mode!r}")
    # A view's body references its source by name. Renaming or dropping the
    # source while the long form is a view leaves a dangling reference (or,
    # for ``drop`` after the long form is renamed into the source's slot, an
    # *infinite recursion* on bind). Force ``--as table`` so the long form
    # is a self-contained materialization before we touch the source.
    if mode == "view" and source_disposition.mode in ("rename", "drop"):
        action = "drop" if source_disposition.mode == "drop" else "rename"
        consequence = (
            "recursively self-referencing"
            if source_disposition.mode == "drop"
            else "pointing at a missing object"
        )
        raise ValueError(
            f"--{action}-source requires --as table: a view references its "
            f"source by name, so {action}ing the source would leave the view "
            f"{consequence}."
        )
    ddl = suggestion.build_sql(mode)

    final_target_name = (
        suggestion.table if source_disposition.mode == "drop" else suggestion.target_object_name
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
        if target_rows != expected:
            raise RuntimeError(
                f"Reshape verification failed for {suggestion.target_object_name!r}: "
                f"expected {expected} rows ({source_rows} source rows × "
                f"{len(suggestion.column_mappings)} mapped columns), got {target_rows}. "
                f"This usually means id_columns omits a column whose values "
                f"duplicate or drop rows."
            )
        source_renamed_to: str | None = None
        if source_disposition.mode == "rename":
            assert source_disposition.new_name is not None
            source_kw = _alter_keyword(conn, suggestion.table)
            conn.execute(
                f"ALTER {source_kw} {_quote_identifier(suggestion.table)} "
                f"RENAME TO {_quote_identifier(source_disposition.new_name)}"
            )
            source_renamed_to = source_disposition.new_name
        elif source_disposition.mode == "drop":
            source_kw = _drop_keyword(conn, suggestion.table)
            conn.execute(f"DROP {source_kw} {_quote_identifier(suggestion.table)}")
            # The long form takes over the source's name so downstream
            # consumers (and any schema.yaml allowlist) keep working.
            target_kw = _alter_keyword(conn, suggestion.target_object_name)
            conn.execute(
                f"ALTER {target_kw} {_quote_identifier(suggestion.target_object_name)} "
                f"RENAME TO {_quote_identifier(suggestion.table)}"
            )
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
    if conn.execute(f"SELECT 1 FROM duckdb_views() WHERE view_name = '{quoted}'").fetchone():
        return "view"
    return "table"


def _drop_keyword(conn: Any, name: str) -> str:
    """``'VIEW'`` if ``name`` is a view, otherwise ``'TABLE'``."""
    return "VIEW" if _object_kind(conn, name) == "view" else "TABLE"


def _alter_keyword(conn: Any, name: str) -> str:
    """``'VIEW'`` if ``name`` is a view, otherwise ``'TABLE'`` — for ``ALTER``."""
    return "VIEW" if _object_kind(conn, name) == "view" else "TABLE"


def update_schema_yaml_for_apply(
    project_dir: str,
    *,
    source_table: str,
    target_table: str,
    disposition_mode: str,
    rename_to: str | None = None,
) -> bool:
    """Sync ``schema.yaml`` with what just changed in the database.

    No-op when ``schema.yaml`` is absent — the project doesn't maintain an
    allowlist, so live introspection already exposes whatever's there. When
    it is present, the goal is to keep the file aligned with the database
    so introspected tables don't silently disappear from the UI:

    - ``keep`` — append a new entry for ``target_table`` so the long form
      shows up alongside the source.
    - ``rename`` — rename the existing source entry to ``rename_to`` and
      append a new entry for ``target_table``.
    - ``drop`` — leave the source entry's *name* in place (the long form
      took it over), but clear any ``columns`` / ``excluded_columns``
      filter on it, since the long form has a different shape and an old
      filter would hide the new columns.

    Returns ``True`` when the file was rewritten. Comments and original
    formatting in the YAML are not preserved — the file is round-tripped
    through ``yaml.safe_dump``.
    """
    path = Path(project_dir) / "schema.yaml"
    if not path.exists():
        return False
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        logger.warning(f"schema.yaml: not updated (parse error: {exc})")
        return False
    if not isinstance(data, dict):
        logger.warning(f"schema.yaml: not updated (expected mapping, got {type(data).__name__})")
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

    if disposition_mode == "drop":
        if source_entry is not None:
            # Long form replaces source: same name, different columns. Drop
            # any old column filter so the new columns surface.
            source_entry.pop("columns", None)
            source_entry.pop("excluded_columns", None)
        else:
            tables.append({"name": source_table})
        # Remove stale target_table entries from a prior keep/rename.
        tables = [e for e in tables if not (isinstance(e, dict) and e.get("name") == target_table)]
    elif disposition_mode == "rename":
        if source_entry is not None and rename_to:
            source_entry["name"] = rename_to
        if not _has_table_entry(tables, target_table):
            tables.append({"name": target_table})
    else:  # keep
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


def resolve_source_disposition(keep: bool, rename_to: str | None, drop: bool) -> SourceDisposition:
    """Translate the three CLI flags into a :class:`SourceDisposition`.

    Enforces mutual exclusion: at most one of the three may be active.
    The default (no flag set) is ``keep`` to match the documented behavior.
    """
    active = sum([keep, rename_to is not None, drop])
    if active > 1:
        raise ValueError(
            "--keep-source, --rename-source, and --drop-source are mutually exclusive"
        )
    if rename_to is not None:
        return SourceDisposition(mode="rename", new_name=rename_to)
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
    "update_schema_yaml_for_apply",
    "validate_against_schema",
]
