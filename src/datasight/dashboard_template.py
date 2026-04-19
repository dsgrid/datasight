"""Dashboard templates.

A dashboard template captures the structure of a dashboard (cards, filters,
layout) in a form that can be re-applied to other tables or parquet files.

Templates are stored as JSON files inside a project's
``.datasight/templates/`` directory.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from datasight.schema import _quote_identifier

try:
    import sqlglot
    from sqlglot import exp

    HAS_SQLGLOT = True
except ImportError:
    HAS_SQLGLOT = False


TEMPLATE_VERSION = 2

_NAME_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]*$")
_VAR_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_PLACEHOLDER_RE = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")


class TemplateError(Exception):
    """Raised for template load/save/validation failures."""


def project_template_dir(project_dir: Path | str) -> Path:
    """Return the templates directory for a given project."""
    return Path(project_dir).expanduser().resolve() / ".datasight" / "templates"


def _validate_name(name: str) -> None:
    if not name or not _NAME_RE.match(name):
        raise TemplateError(f"Invalid template name: {name!r}. Use letters, digits, '_', '-', '.'")


def template_path(name: str, project_dir: Path | str) -> Path:
    """Return the filesystem path for a named template inside a project."""
    _validate_name(name)
    return project_template_dir(project_dir) / f"{name}.json"


def collect_required_tables(items: list[dict[str, Any]]) -> list[str]:
    """Collect every table referenced across a dashboard's card SQL.

    Ordered by first appearance across cards. Returns an empty list when
    sqlglot is unavailable or no SQL could be parsed.
    """
    if not HAS_SQLGLOT:
        return []
    seen: dict[str, None] = {}
    for item in items:
        sql = item.get("sql")
        if not sql:
            continue
        try:
            parsed = sqlglot.parse_one(sql, read="duckdb")
        except Exception:
            continue
        for table in parsed.find_all(exp.Table):
            name = table.name
            if name and name not in seen:
                seen[name] = None
    return list(seen.keys())


def _normalize_template(data: dict[str, Any]) -> dict[str, Any]:
    """Migrate older template shapes into the current schema.

    v1 templates used a single ``source_table`` field. We promote that
    into ``required_tables`` at load time so the rest of the code can
    assume the new shape.
    """
    if not data.get("required_tables"):
        legacy = data.get("source_table")
        if legacy:
            data = dict(data)
            data["required_tables"] = [legacy]
    if "variables" not in data:
        data = dict(data)
        data["variables"] = []
    return data


def _validate_variable(var: dict[str, Any]) -> dict[str, Any]:
    name = var.get("name")
    if not isinstance(name, str) or not _VAR_NAME_RE.match(name):
        raise TemplateError(f"Invalid variable name: {name!r}.")
    default = var.get("default")
    if default is not None and not isinstance(default, str):
        raise TemplateError(f"Variable {name!r} default must be a string.")
    from_filename = var.get("from_filename")
    if from_filename is not None:
        if not isinstance(from_filename, str):
            raise TemplateError(f"Variable {name!r} from_filename must be a string.")
        try:
            re.compile(from_filename)
        except re.error as err:
            raise TemplateError(
                f"Variable {name!r} from_filename regex is invalid: {err}"
            ) from err
    return {
        "name": name,
        "default": default if default is not None else "",
        **({"from_filename": from_filename} if from_filename else {}),
    }


def render_sql(sql: str, values: Mapping[str, str]) -> str:
    """Substitute ``{{name}}`` placeholders in SQL with provided values.

    Unknown placeholders raise :class:`TemplateError` so apply-time callers
    see a clear error rather than a silently-broken query.
    """

    def _sub(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in values:
            raise TemplateError(f"SQL references unknown variable {{{{{name}}}}}.")
        return str(values[name])

    return _PLACEHOLDER_RE.sub(_sub, sql)


def resolve_variables(
    template: dict[str, Any],
    *,
    filename: str | Path | None = None,
    overrides: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Resolve values for every declared template variable.

    Precedence (highest first): ``overrides`` > ``from_filename`` match >
    ``default``. Variables with a ``from_filename`` regex raise
    :class:`TemplateError` when the filename is missing or the regex does
    not match — per design, apply-time callers must pass a matching input
    rather than silently falling back to the default.
    """
    overrides = dict(overrides or {})
    resolved: dict[str, str] = {}
    file_str = Path(filename).name if filename is not None else None
    for raw in template.get("variables") or []:
        var = _validate_variable(raw)
        name = var["name"]
        if name in overrides:
            resolved[name] = str(overrides[name])
            continue
        regex = var.get("from_filename")
        if regex:
            if file_str is None:
                raise TemplateError(
                    f"Variable {name!r} extracts from filename but no input filename is available."
                )
            match = re.search(regex, file_str)
            if not match:
                raise TemplateError(
                    f"Filename {file_str!r} does not match regex {regex!r} for variable {name!r}."
                )
            resolved[name] = match.group(match.lastindex or 0)
            continue
        resolved[name] = var["default"]
    for key, val in overrides.items():
        resolved.setdefault(key, str(val))
    return resolved


def _rewrite_sql_with_variables(
    items: list[dict[str, Any]],
    variables: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return ``items`` with literal variable values replaced by ``{{name}}``.

    For each variable that has a non-empty ``default``, every occurrence of
    that literal in every card's SQL is rewritten to ``{{name}}``. Useful
    at save time so a user who says ``--var year=2020`` doesn't have to
    hand-edit the generated JSON.
    """
    rewrites = [(v["name"], v["default"]) for v in variables if v.get("default")]
    if not rewrites:
        return items
    # Longest literal first — avoids shadowing when one default is a
    # substring of another.
    rewrites.sort(key=lambda p: len(p[1]), reverse=True)
    out: list[dict[str, Any]] = []
    for item in items:
        sql = item.get("sql")
        if not isinstance(sql, str) or not sql:
            out.append(item)
            continue
        new_sql = sql
        for var_name, literal in rewrites:
            new_sql = new_sql.replace(literal, f"{{{{{var_name}}}}}")
        if new_sql == sql:
            out.append(item)
        else:
            out.append({**item, "sql": new_sql})
    return out


def build_template(
    name: str,
    dashboard: dict[str, Any],
    *,
    required_tables: list[str] | None = None,
    description: str | None = None,
    required_columns: list[dict[str, str]] | None = None,
    variables: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Construct a template dict from a dashboard snapshot.

    ``dashboard`` should have the shape returned by
    :meth:`datasight.web.app.DashboardStore.get_all` — an ``items`` list,
    an integer ``columns`` layout, and a ``filters`` list.

    ``required_tables`` lists every table the template's card SQL needs.
    When omitted, it is inferred by parsing each card's SQL with sqlglot.
    """
    _validate_name(name)
    items = list(dashboard.get("items") or [])
    columns = int(dashboard.get("columns") or 0)
    filters = list(dashboard.get("filters") or [])

    if not items:
        raise TemplateError("Cannot save an empty dashboard as a template.")

    if required_tables is None:
        required_tables = collect_required_tables(items)
    required_tables = [t for t in required_tables if t]
    if not required_tables:
        raise TemplateError(
            "Could not infer required_tables from dashboard SQL. "
            "Pass --table explicitly (repeatable)."
        )

    normalized_vars = [_validate_variable(v) for v in (variables or [])]
    seen_var_names: set[str] = set()
    for var in normalized_vars:
        if var["name"] in seen_var_names:
            raise TemplateError(f"Duplicate variable name: {var['name']!r}.")
        seen_var_names.add(var["name"])
    if normalized_vars:
        items = _rewrite_sql_with_variables(items, normalized_vars)

    return {
        "name": name,
        "version": TEMPLATE_VERSION,
        "description": description or "",
        "required_tables": required_tables,
        "required_columns": list(required_columns or []),
        "variables": normalized_vars,
        "items": items,
        "columns": columns,
        "filters": filters,
    }


def save_template(
    template: dict[str, Any],
    project_dir: Path | str,
    *,
    overwrite: bool = False,
) -> Path:
    """Persist a template dict into the project's templates directory."""
    name = template.get("name")
    if not isinstance(name, str):
        raise TemplateError("Template is missing a 'name' field.")
    _validate_name(name)

    path = template_path(name, project_dir)
    if path.exists() and not overwrite:
        raise TemplateError(
            f"Template {name!r} already exists at {path}. "
            "Pass overwrite=True (or --overwrite) to replace it."
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(template, indent=2), encoding="utf-8")
    return path


def load_template(name: str, project_dir: Path | str) -> dict[str, Any]:
    """Load a template by name from a project."""
    path = template_path(name, project_dir)
    if not path.exists():
        raise TemplateError(f"Template {name!r} not found at {path}.")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as err:
        raise TemplateError(f"Template {name!r} is not valid JSON: {err}") from err
    if not isinstance(data, dict):
        raise TemplateError(f"Template {name!r} must be a JSON object.")
    return _normalize_template(data)


def list_templates(project_dir: Path | str) -> list[dict[str, Any]]:
    """List templates saved inside a project."""
    directory = project_template_dir(project_dir)
    if not directory.exists():
        return []
    out: list[dict[str, Any]] = []
    for path in sorted(directory.glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        if not isinstance(data, dict):
            continue
        data = _normalize_template(data)
        out.append(
            {
                "name": data.get("name") or path.stem,
                "description": data.get("description") or "",
                "required_tables": list(data.get("required_tables") or []),
                "cards": len(data.get("items") or []),
                "path": str(path),
            }
        )
    return out


def delete_template(name: str, project_dir: Path | str) -> bool:
    """Delete a template by name from a project. Returns True if removed."""
    path = template_path(name, project_dir)
    if not path.exists():
        return False
    path.unlink()
    return True


# ---------------------------------------------------------------------------
# Template application (ephemeral DuckDB + HTML export)
# ---------------------------------------------------------------------------


@dataclass
class CardRenderResult:
    """Outcome of running a single dashboard card against a target dataset."""

    idx: int
    title: str
    ok: bool
    error: str | None = None


@dataclass
class ApplyResult:
    """Outcome of applying a template to one set of inputs."""

    label: str
    output: Path | None
    ok: bool
    cards: list[CardRenderResult] = field(default_factory=list)
    error: str | None = None


def _list_attached_tables(conn) -> set[str]:
    rows = conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "UNION SELECT view_name FROM duckdb_views() WHERE internal = FALSE"
    ).fetchall()
    return {str(r[0]) for r in rows}


async def apply_template(
    template: dict[str, Any],
    output_path: Path,
    *,
    sources: Mapping[str, str | Path] | None = None,
    base_db: str | Path | None = None,
    title: str | None = None,
    variables: Mapping[str, str] | None = None,
) -> ApplyResult:
    """Apply a template to a set of inputs, writing a dashboard HTML file.

    Creates an in-memory DuckDB connection, attaches ``base_db`` (if
    provided) so its tables are queryable, and registers each parquet in
    ``sources`` as a view under the given table name. Runs every card's
    SQL via the standard tool pipeline and writes the rendered dashboard
    to ``output_path``.

    Parameters
    ----------
    template:
        The template dict as produced by :func:`build_template` or
        :func:`load_template`.
    output_path:
        File path to write the HTML dashboard to.
    sources:
        Mapping of table name → parquet file path. Every table name must
        appear in the template's ``required_tables`` unless also present
        in ``base_db``.
    base_db:
        Optional path to a DuckDB file. All of its tables/views are made
        available by ATTACH, letting the template reference fixed tables
        (e.g. ``plants``) that don't change per invocation.
    title:
        Optional page title. Defaults to the template name + parquet stem.
    """
    import duckdb

    from datasight.agent import execute_tool
    from datasight.export import export_dashboard_html
    from datasight.runner import EphemeralDuckDBRunner

    resolved_sources: dict[str, Path] = {k: Path(v).resolve() for k, v in (sources or {}).items()}
    required = list(template.get("required_tables") or [])
    label_parts = [p.name for p in resolved_sources.values()] or [
        template.get("name", "dashboard")
    ]
    label = ", ".join(label_parts)

    for name, path in resolved_sources.items():
        if not path.exists():
            return ApplyResult(
                label=label,
                output=None,
                ok=False,
                error=f"Parquet file not found for {name!r}: {path}",
            )

    if not required:
        return ApplyResult(
            label=label,
            output=None,
            ok=False,
            error="Template has no required_tables.",
        )

    conn = duckdb.connect(":memory:")
    try:
        if base_db is not None:
            base_path = Path(base_db).resolve()
            if not base_path.exists():
                return ApplyResult(
                    label=label,
                    output=None,
                    ok=False,
                    error=f"Base database not found: {base_path}",
                )
            escaped_db = str(base_path).replace("'", "''")
            conn.execute(f"ATTACH '{escaped_db}' AS base (READ_ONLY)")
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_catalog = 'base' AND table_schema = 'main'"
            ).fetchall():
                source_table = str(row[0])
                if source_table in resolved_sources:
                    continue
                conn.execute(
                    f"CREATE VIEW {_quote_identifier(source_table)} "
                    f"AS SELECT * FROM base.main.{_quote_identifier(source_table)}"
                )

        for name, path in resolved_sources.items():
            escaped = str(path).replace("'", "''")
            try:
                conn.execute(
                    f"CREATE OR REPLACE VIEW {_quote_identifier(name)} "
                    f"AS SELECT * FROM read_parquet('{escaped}')"
                )
            except duckdb.Error as err:
                conn.close()
                return ApplyResult(
                    label=label,
                    output=None,
                    ok=False,
                    error=f"Failed to register parquet {path} as {name!r}: {err}",
                )

        attached = _list_attached_tables(conn)
        missing = [t for t in required if t not in attached]
        if missing:
            conn.close()
            hint = (
                "Pass --table NAME=PATH for rotating inputs, or --base-db PATH for fixed tables."
            )
            return ApplyResult(
                label=label,
                output=None,
                ok=False,
                error=f"Required tables not provided: {', '.join(missing)}. {hint}",
            )

        runner = EphemeralDuckDBRunner(conn)
        items = list(template.get("items") or [])
        columns = int(template.get("columns") or 2)
        filters = list(template.get("filters") or [])
        page_title = title or f"{template.get('name', 'dashboard')} — {label}"

        card_results: list[CardRenderResult] = []
        rendered_items: list[dict[str, Any]] = []

        variable_values: dict[str, str] = dict(variables or {})

        for idx, item in enumerate(items):
            item_type = item.get("type") or "table"
            title_str = str(item.get("title") or "")
            sql = item.get("sql")

            if item_type in {"note", "section"}:
                rendered_items.append(dict(item))
                card_results.append(CardRenderResult(idx=idx, title=title_str, ok=True))
                continue

            if not sql:
                card_results.append(
                    CardRenderResult(idx=idx, title=title_str, ok=False, error="Card has no SQL")
                )
                continue

            try:
                sql = render_sql(sql, variable_values)
            except TemplateError as err:
                card_results.append(
                    CardRenderResult(idx=idx, title=title_str, ok=False, error=str(err))
                )
                continue

            tool = "visualize_data" if item_type == "chart" else "run_sql"
            tool_input: dict[str, Any] = {"sql": sql, "title": title_str}
            if tool == "visualize_data":
                tool_input["plotly_spec"] = item.get("plotly_spec") or {}

            result = await execute_tool(tool, tool_input, run_sql=runner.run_sql, dialect="duckdb")
            err = result.meta.get("error") if result.meta else None
            card_results.append(
                CardRenderResult(
                    idx=idx,
                    title=title_str,
                    ok=not bool(err),
                    error=err,
                )
            )
            rendered_items.append(
                {
                    "id": item.get("id", idx + 1),
                    "type": item_type,
                    "title": title_str,
                    "html": result.result_html or "",
                    "source_meta": item.get("source_meta") or {},
                }
            )

        html = export_dashboard_html(
            rendered_items,
            title=page_title,
            columns=columns,
            filters=filters,
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(html, encoding="utf-8")

        any_failed = any(not c.ok for c in card_results)
        return ApplyResult(
            label=label,
            output=output_path,
            ok=not any_failed,
            cards=card_results,
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass
