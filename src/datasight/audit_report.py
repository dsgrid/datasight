"""Composite audit report builder and renderers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from datasight.data_profile import build_dataset_overview, build_quality_overview
from datasight.distribution import build_distribution_overview
from datasight.integrity import build_integrity_overview
from datasight.runner import RunSql
from datasight.templating import render_template
from datasight.validation import build_validation_report


async def build_audit_report(
    schema_info: list[dict[str, Any]],
    run_sql: RunSql,
    overrides: list[dict[str, Any]] | None = None,
    validation_rules: list[dict[str, Any]] | None = None,
    declared_joins: list[dict[str, Any]] | None = None,
    project_name: str | None = None,
) -> dict[str, Any]:
    """Run all audit checks and assemble a composite report."""
    dataset_overview = await build_dataset_overview(schema_info, run_sql)
    quality = await build_quality_overview(schema_info, run_sql)
    integrity = await build_integrity_overview(schema_info, run_sql, declared_joins)
    distribution = await build_distribution_overview(schema_info, run_sql, overrides)

    validation = None
    if validation_rules:
        validation = await build_validation_report(schema_info, run_sql, validation_rules)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "project_name": project_name,
        "dataset_overview": dataset_overview,
        "quality": quality,
        "integrity": integrity,
        "distribution": distribution,
        "validation": validation,
    }


# ---------------------------------------------------------------------------
# Markdown renderer
# ---------------------------------------------------------------------------


def _md_table(headers: list[str], rows: list[list[str]]) -> str:
    """Build a simple markdown table."""
    if not rows:
        return ""
    lines = ["| " + " | ".join(headers) + " |"]
    lines.append("| " + " | ".join("---" for _ in headers) + " |")
    for row in rows:
        lines.append("| " + " | ".join(str(c) for c in row) + " |")
    return "\n".join(lines)


def _fmt(value: Any, default: str = "-") -> str:
    if value is None:
        return default
    if isinstance(value, float):
        return f"{value:.4g}"
    return str(value)


def render_audit_report_markdown(data: dict[str, Any]) -> str:
    """Render the composite audit report as Markdown."""
    project_name = data.get("project_name")
    title = (
        f"# datasight Audit Report — {project_name}"
        if project_name
        else "# datasight Audit Report"
    )
    lines = [
        title,
        "",
        f"Generated: {data.get('generated_at', '')}",
    ]

    # --- Dataset overview ---
    overview = data.get("dataset_overview") or {}
    lines.extend(
        [
            "",
            "## Dataset Overview",
            "",
            f"- Tables: {overview.get('table_count', 0)}",
            f"- Total rows: {overview.get('total_rows', 0)}",
            f"- Total columns: {overview.get('total_columns', 0)}",
        ]
    )

    # --- Quality ---
    quality = data.get("quality") or {}
    lines.extend(["", "## Data Quality"])
    if quality.get("null_columns"):
        lines.extend(["", "### Null-heavy Columns", ""])
        lines.append(
            _md_table(
                ["Column", "Nulls", "Null %"],
                [
                    [
                        f"`{i['table']}.{i['column']}`",
                        str(i["null_count"]),
                        str(i.get("null_rate", 0)),
                    ]
                    for i in quality["null_columns"]
                ],
            )
        )
    if quality.get("numeric_flags"):
        lines.extend(["", "### Numeric Range Flags", ""])
        lines.append(
            _md_table(
                ["Column", "Issue"],
                [[f"`{i['table']}.{i['column']}`", i["issue"]] for i in quality["numeric_flags"]],
            )
        )
    for note in quality.get("notes", []):
        lines.append(f"- {note}")

    # --- Integrity ---
    integrity = data.get("integrity") or {}
    lines.extend(["", "## Referential Integrity"])
    if integrity.get("primary_keys"):
        lines.extend(["", "### Primary Keys", ""])
        lines.append(
            _md_table(
                ["Table", "Column", "Unique"],
                [
                    [i["table"], i["column"], "yes" if i["is_unique"] else "NO"]
                    for i in integrity["primary_keys"]
                ],
            )
        )
    if integrity.get("duplicate_keys"):
        lines.extend(["", "### Duplicate Keys", ""])
        lines.append(
            _md_table(
                ["Table", "Column", "Duplicates"],
                [
                    [i["table"], i["column"], str(i["duplicate_count"])]
                    for i in integrity["duplicate_keys"]
                ],
            )
        )
    if integrity.get("orphan_foreign_keys"):
        lines.extend(["", "### Orphan Foreign Keys", ""])
        lines.append(
            _md_table(
                ["Child", "Parent", "Orphans"],
                [
                    [
                        f"`{i['child_table']}.{i['child_column']}`",
                        f"`{i['parent_table']}.{i['parent_column']}`",
                        str(i["orphan_count"]),
                    ]
                    for i in integrity["orphan_foreign_keys"]
                ],
            )
        )
    if integrity.get("join_explosions"):
        lines.extend(["", "### Join Explosion Risks", ""])
        lines.append(
            _md_table(
                ["Tables", "Column", "Factor"],
                [
                    [
                        f"{i['table_a']} x {i['table_b']}",
                        i["join_column"],
                        f"{i['explosion_factor']}x",
                    ]
                    for i in integrity["join_explosions"]
                ],
            )
        )
    for note in integrity.get("notes", []):
        lines.append(f"- {note}")

    # --- Distribution ---
    distribution = data.get("distribution") or {}
    lines.extend(["", "## Distribution Profiling"])
    if distribution.get("distributions"):
        lines.extend(["", "### Distributions", ""])
        lines.append(
            _md_table(
                ["Column", "p5", "p50", "p95", "Zero %", "Neg %", "Outliers"],
                [
                    [
                        f"`{d['table']}.{d['column']}`",
                        _fmt(d.get("p5")),
                        _fmt(d.get("p50")),
                        _fmt(d.get("p95")),
                        _fmt(d.get("zero_rate")),
                        _fmt(d.get("negative_rate")),
                        str(d.get("outlier_count", 0)),
                    ]
                    for d in distribution["distributions"]
                ],
            )
        )
    if distribution.get("energy_flags"):
        lines.extend(["", "### Energy Flags", ""])
        for f in distribution["energy_flags"]:
            lines.append(f"- `{f['table']}.{f['column']}`: {f['detail']}")
    if distribution.get("spikes"):
        lines.extend(["", "### Temporal Spikes", ""])
        for s in distribution["spikes"]:
            lines.append(f"- {s['detail']}")
    for note in distribution.get("notes", []):
        lines.append(f"- {note}")

    # --- Validation ---
    validation = data.get("validation")
    if validation:
        summary = validation.get("summary", {})
        lines.extend(
            [
                "",
                "## Validation Rules",
                "",
                f"- Rules run: {validation.get('rule_count', 0)}",
                f"- Pass: {summary.get('pass', 0)}, Fail: {summary.get('fail', 0)}, Warn: {summary.get('warn', 0)}",
            ]
        )
        if validation.get("results"):
            lines.extend(["", "### Results", ""])
            lines.append(
                _md_table(
                    ["Table", "Rule", "Column", "Status", "Detail"],
                    [
                        [
                            r["table"],
                            r["rule"],
                            r.get("column") or "-",
                            r["status"].upper(),
                            r["detail"],
                        ]
                        for r in validation["results"]
                    ],
                )
            )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# HTML renderer
# ---------------------------------------------------------------------------


def render_audit_report_html(data: dict[str, Any]) -> str:
    """Render the composite audit report as a self-contained HTML page."""
    # Flatten the nested data into template-friendly structures
    overview = data.get("dataset_overview") or {}
    quality = data.get("quality") or {}
    integrity = data.get("integrity") or {}
    distribution = data.get("distribution") or {}
    validation = data.get("validation")

    project_name = data.get("project_name")
    template_data = {
        "generated_at": data.get("generated_at", ""),
        "project_name": project_name,
        "has_project_name": bool(project_name),
        "table_count": overview.get("table_count", 0),
        "total_rows": overview.get("total_rows", 0),
        "total_columns": overview.get("total_columns", 0),
        "null_columns": quality.get("null_columns", []),
        "has_null_columns": bool(quality.get("null_columns")),
        "numeric_flags": quality.get("numeric_flags", []),
        "has_numeric_flags": bool(quality.get("numeric_flags")),
        "quality_notes": quality.get("notes", []),
        "primary_keys": integrity.get("primary_keys", []),
        "has_primary_keys": bool(integrity.get("primary_keys")),
        "duplicate_keys": integrity.get("duplicate_keys", []),
        "has_duplicate_keys": bool(integrity.get("duplicate_keys")),
        "orphan_foreign_keys": integrity.get("orphan_foreign_keys", []),
        "has_orphan_foreign_keys": bool(integrity.get("orphan_foreign_keys")),
        "join_explosions": integrity.get("join_explosions", []),
        "has_join_explosions": bool(integrity.get("join_explosions")),
        "integrity_notes": integrity.get("notes", []),
        "distributions": [
            {
                **d,
                "p5_fmt": _fmt(d.get("p5")),
                "p50_fmt": _fmt(d.get("p50")),
                "p95_fmt": _fmt(d.get("p95")),
                "zero_rate_fmt": _fmt(d.get("zero_rate")),
                "negative_rate_fmt": _fmt(d.get("negative_rate")),
            }
            for d in distribution.get("distributions", [])
        ],
        "has_distributions": bool(distribution.get("distributions")),
        "energy_flags": distribution.get("energy_flags", []),
        "has_energy_flags": bool(distribution.get("energy_flags")),
        "spikes": distribution.get("spikes", []),
        "has_spikes": bool(distribution.get("spikes")),
        "distribution_notes": distribution.get("notes", []),
        "has_validation": validation is not None,
    }

    if validation:
        summary = validation.get("summary", {})
        results = []
        for r in validation.get("results", []):
            results.append(
                {
                    **r,
                    "is_pass": r["status"] == "pass",
                    "is_fail": r["status"] == "fail",
                    "is_warn": r["status"] == "warn",
                    "status_upper": r["status"].upper(),
                    "column_display": r.get("column") or "-",
                }
            )
        template_data.update(
            {
                "validation_rule_count": validation.get("rule_count", 0),
                "validation_pass": summary.get("pass", 0),
                "validation_fail": summary.get("fail", 0),
                "validation_warn": summary.get("warn", 0),
                "validation_results": results,
                "has_validation_results": bool(results),
            }
        )

    return render_template("audit_report", template_data)
