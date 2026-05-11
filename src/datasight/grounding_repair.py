"""LLM-driven repair of grounding files after a schema-changing transform.

This module is invoked from ``datasight tidy review`` (or the web UI
equivalent) after :func:`datasight.tidy_review.apply_proposal` reshapes
a table. The drift detector in :mod:`datasight.grounding` has already
told us *that* the grounding files are stale; here we ask the LLM to
rewrite them.

Design
------
The repair flow is deliberately conservative:

1. The caller snapshots the schema **before** the tidy transform, so
   the LLM can see both old and new structure. Without the before
   snapshot the prompt degenerates to "rewrite from scratch" and loses
   any human customizations.
2. The LLM is asked to return a single JSON object keyed by filename.
   No unified-diff parsing; the full proposed file contents are
   returned and we compute diffs locally.
3. Every SQL example in the proposed ``queries.yaml`` is executed
   against the live database before the result is shown to the user.
   Failures trigger up to ``max_retries`` LLM retries with the error
   context attached. If a query still fails after retries, the
   :class:`RepairFile` records the validation errors and the orchestrator
   decides whether to fall through to manual edit mode.
4. Nothing is written to disk inside this module. The orchestrator in
   the CLI flow shows the diff, prompts for confirmation, and only then
   calls :func:`write_repair_atomic`.

The atomic-write helper writes each accepted file via a sibling
``.new`` tempfile + ``os.replace``, so an interrupted repair can't
half-corrupt the grounding.
"""

from __future__ import annotations

import difflib
import json
import os
import re
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

import yaml
from loguru import logger

from datasight.grounding import DriftReport
from datasight.llm import LLMClient, TextBlock

# Pre-tidy schema snapshot: written by tidy apply, consumed by grounding
# repair. Lives under ``.datasight/`` (same dir as conversations,
# query_log.jsonl, etc.) so it survives server restarts and is
# accessible to both the CLI and the web endpoint.
SNAPSHOT_RELATIVE_PATH = Path(".datasight") / "grounding_snapshot.json"
SNAPSHOT_SCHEMA_VERSION = 1


def snapshot_path(project_dir: Path | str) -> Path:
    """Return the absolute path to the project's grounding snapshot file."""
    return Path(project_dir) / SNAPSHOT_RELATIVE_PATH


def write_snapshot(project_dir: Path | str, schema: dict[str, set[str]]) -> Path:
    """Persist the pre-tidy schema snapshot atomically.

    Overwrites any prior snapshot — there's only one "most recent
    apply" per project. Sets are serialized as sorted lists for stable
    file content (so a snapshot diff in git review is meaningful).

    Parameters
    ----------
    project_dir : Path | str
        Project root containing ``.datasight/``.
    schema : dict[str, set[str]]
        Tables → column-name set, as captured immediately before the
        tidy apply mutates the database.

    Returns
    -------
    Path
        The path that was written.
    """
    payload = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "applied_at": datetime.now(timezone.utc).isoformat(),
        "schema": {table: sorted(cols) for table, cols in schema.items()},
    }
    target = snapshot_path(project_dir)
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=target.parent, delete=False,
        prefix=".grounding-snapshot-",
    ) as tmp:
        json.dump(payload, tmp, indent=2, sort_keys=True)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_path = Path(tmp.name)
    try:
        os.replace(tmp_path, target)
    except OSError:
        tmp_path.unlink(missing_ok=True)
        raise
    return target


def read_snapshot(project_dir: Path | str) -> dict[str, set[str]] | None:
    """Load the most recent pre-tidy schema snapshot, or None if absent.

    Returns None for missing files, malformed JSON, or unrecognized
    schema versions — callers treat absence as "no prior apply on
    record" and surface that to the user. We deliberately don't raise
    on corruption so a bad snapshot can't permanently brick the
    repair flow; the user can re-apply or pass an explicit fallback.
    """
    path = snapshot_path(project_dir)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning(f"grounding snapshot at {path} is unreadable: {exc}")
        return None
    if not isinstance(payload, dict) or payload.get("schema_version") != SNAPSHOT_SCHEMA_VERSION:
        logger.warning(
            f"grounding snapshot at {path} has unexpected schema_version "
            f"{payload.get('schema_version') if isinstance(payload, dict) else 'n/a'!r}; ignoring"
        )
        return None
    raw_schema = payload.get("schema")
    if not isinstance(raw_schema, dict):
        return None
    out: dict[str, set[str]] = {}
    for table, cols in raw_schema.items():
        if not isinstance(table, str) or not isinstance(cols, list):
            continue
        out[table] = {c for c in cols if isinstance(c, str)}
    return out


# Files the repair flow may touch. Other grounding-adjacent files
# (schema.yaml, measures.yaml) are owned by tidy_review's own update
# helpers and stay out of scope here.
REPAIR_FILE_NAMES: tuple[str, ...] = (
    "queries.yaml",
    "schema_description.md",
    "time_series.yaml",
)


# Match an opening ```json or ``` fence, then anything (non-greedy) up
# to the closing ```. Used to peel a JSON object out of an LLM response
# that wraps it in a markdown code block.
_FENCED_JSON = re.compile(r"```(?:json)?\s*\n(.*?)\n```", re.DOTALL)


@dataclass
class RepairFile:
    """One file's before/after content and validation outcome."""

    name: str
    path: Path
    old_text: str
    new_text: str
    validation_errors: list[str] = field(default_factory=list)

    @property
    def changed(self) -> bool:
        """True if the LLM proposed any change to this file."""
        return self.new_text != self.old_text

    @property
    def ok(self) -> bool:
        """True if the file is unchanged or its changes validated cleanly."""
        return not self.validation_errors

    def unified_diff(self) -> str:
        """Unified diff between ``old_text`` and ``new_text``."""
        return "".join(
            difflib.unified_diff(
                self.old_text.splitlines(keepends=True),
                self.new_text.splitlines(keepends=True),
                fromfile=f"a/{self.name}",
                tofile=f"b/{self.name}",
            )
        )


@dataclass
class RepairResult:
    """Outcome of one repair attempt, before any files are written."""

    files: list[RepairFile]
    llm_retries: int = 0

    @property
    def overall_ok(self) -> bool:
        """True if every changed file's validation came back clean."""
        return all(f.ok for f in self.files)

    @property
    def any_changes(self) -> bool:
        """True if the LLM proposed changes to at least one file."""
        return any(f.changed for f in self.files)


async def repair_grounding(
    project_dir: Path,
    old_schema: dict[str, set[str]],
    new_schema: dict[str, set[str]],
    drift: DriftReport,
    *,
    llm_client: LLMClient,
    model: str,
    run_sql: Callable[[str], Awaitable[Any]],
    max_tokens: int = 16384,
    max_retries: int = 2,
) -> RepairResult:
    """Ask the LLM to rewrite grounding files so they match ``new_schema``.

    Does not write to disk. The caller is responsible for showing the
    diff, prompting for confirmation, and calling
    :func:`write_repair_atomic` to apply.

    Parameters
    ----------
    project_dir : Path
        Directory containing the grounding files.
    old_schema, new_schema : dict[str, set[str]]
        Schemas before and after the transform that triggered repair.
    drift : DriftReport
        Output of :func:`datasight.grounding.check_grounding_drift`
        against the new schema.
    llm_client : LLMClient
        Same client the agent loop uses; this repair is a one-shot
        text completion (no tool use).
    model : str
        Model name passed through to ``llm_client.create_message``.
    run_sql : async callable
        Async SQL runner used to validate each proposed query.
    max_tokens : int
        Output budget for the LLM call. Defaults to 16 384, which is
        enough to rewrite a schema_description.md plus queries.yaml
        for most projects.
    max_retries : int
        How many times to re-prompt the LLM with validation-error
        context if proposed queries don't execute. Defaults to 2 (so
        up to 3 LLM calls in total).
    """
    files = _load_repair_files(project_dir)
    prompt = _build_repair_prompt(old_schema, new_schema, drift, files)
    system = _SYSTEM_PROMPT

    proposed: dict[str, str] | None = None
    last_error: str | None = None
    retries = 0
    for attempt in range(max_retries + 1):
        user_prompt = prompt if last_error is None else (
            f"{prompt}\n\nYour previous response had validation errors. "
            f"Fix them and return the full corrected JSON object:\n\n{last_error}"
        )
        response = await llm_client.create_message(
            model=model,
            system=system,
            messages=[{"role": "user", "content": user_prompt}],
            tools=[],
            max_tokens=max_tokens,
        )
        text = "".join(b.text for b in response.content if isinstance(b, TextBlock))
        try:
            proposed = _parse_repair_json(text)
        except ValueError as exc:
            last_error = f"Could not parse JSON from your response: {exc}"
            retries = attempt + 1
            logger.warning(f"repair attempt {attempt + 1}: {last_error}")
            continue

        # Apply proposed contents to RepairFile objects and validate.
        for f in files:
            if f.name in proposed:
                f.new_text = proposed[f.name]
            f.validation_errors = []
        await _validate_repair(files, run_sql=run_sql)

        if all(f.ok for f in files):
            return RepairResult(files=files, llm_retries=attempt)

        # Build a summarized error report for the next retry.
        error_lines: list[str] = []
        for f in files:
            for err in f.validation_errors:
                error_lines.append(f"- {f.name}: {err}")
        last_error = "\n".join(error_lines)
        retries = attempt + 1
        logger.warning(
            f"repair attempt {attempt + 1}: {len(error_lines)} validation error(s)"
        )

    # Out of retries — return the last attempt with its errors so the
    # caller can fall back to manual edit mode.
    return RepairResult(files=files, llm_retries=retries)


def write_repair_atomic(result: RepairResult, project_dir: Path) -> list[Path]:
    """Write each changed, validated file via tempfile + ``os.replace``.

    Only files where ``RepairFile.changed`` and ``RepairFile.ok`` are
    True are written. The function fsyncs each temp file before the
    rename so an interrupted run can't leave partial content visible.

    Parameters
    ----------
    result : RepairResult
        Output of :func:`repair_grounding`.
    project_dir : Path
        Project directory. Files are written under their stored
        ``RepairFile.path`` which is rooted here.

    Returns
    -------
    list[Path]
        Paths that were written. Empty list when nothing changed.
    """
    written: list[Path] = []
    for f in result.files:
        if not f.changed or not f.ok:
            continue
        parent = f.path.parent
        parent.mkdir(parents=True, exist_ok=True)
        # NamedTemporaryFile with delete=False so we can keep the path
        # after closing; os.replace then atomically swaps it in.
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=parent, delete=False, prefix=".grounding-",
        ) as tmp:
            tmp.write(f.new_text)
            tmp.flush()
            os.fsync(tmp.fileno())
            tmp_path = Path(tmp.name)
        try:
            os.replace(tmp_path, f.path)
        except OSError:
            tmp_path.unlink(missing_ok=True)
            raise
        written.append(f.path)
    return written


def format_repair_summary(result: RepairResult) -> str:
    """One-paragraph summary suitable for terminal output before the diff."""
    changed = [f for f in result.files if f.changed]
    if not changed:
        return "Repair: no files changed."
    parts = [f"Repair: LLM proposed changes to {len(changed)} file(s)"]
    if result.llm_retries:
        parts.append(f"after {result.llm_retries} retry/retries")
    parts.append(":")
    for f in changed:
        status = "ok" if f.ok else f"FAILED ({len(f.validation_errors)} error(s))"
        parts.append(f"  {f.name} [{status}]")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


_SYSTEM_PROMPT = (
    "You are rewriting grounding files for a SQL-aware LLM agent so that every "
    "column and table reference resolves against the current database schema. "
    "Preserve human prose, comments, structure, and any example that still "
    "applies. Do not invent new sections or example questions. Every SQL "
    "snippet in the rewritten queries.yaml must execute successfully against "
    "the NEW schema. Reply with a single JSON object whose keys are the "
    "filenames you are rewriting and whose values are the full new file "
    "contents as strings. Do not include any prose outside the JSON."
)


def _build_repair_prompt(
    old_schema: dict[str, set[str]],
    new_schema: dict[str, set[str]],
    drift: DriftReport,
    files: list[RepairFile],
) -> str:
    """Compose the user-side prompt sent to the repair LLM."""
    sections: list[str] = []
    sections.append("OLD SCHEMA (before the transform):")
    sections.append(_schema_text(old_schema))
    sections.append("")
    sections.append("NEW SCHEMA (current state):")
    sections.append(_schema_text(new_schema))
    sections.append("")
    sections.append("DRIFT DETECTED:")
    if drift.is_clean:
        sections.append("(no drift; only schema-shape changes)")
    else:
        for item in drift.items:
            loc = f":{item.line}" if item.line else ""
            sug = f" (suggested: {item.suggestion})" if item.suggestion else ""
            sections.append(f"  - {item.file}{loc}  {item.kind}  {item.claim!r}{sug}")
    sections.append("")
    sections.append("FILES TO REWRITE:")
    sections.append("")
    for f in files:
        sections.append(f"--- {f.name} ---")
        sections.append(f.old_text)
        sections.append("")
    sections.append(
        "Return a single JSON object. Keys: filenames. Values: full new "
        "contents. Files that need no changes may be omitted."
    )
    return "\n".join(sections)


def _schema_text(schema: dict[str, set[str]]) -> str:
    """Compact human-readable rendering of a {table: columns} dict."""
    if not schema:
        return "(empty)"
    lines: list[str] = []
    for table in sorted(schema.keys()):
        cols = sorted(schema[table])
        lines.append(f"  {table}({', '.join(cols)})")
    return "\n".join(lines)


def _load_repair_files(project_dir: Path) -> list[RepairFile]:
    """Read each in-scope grounding file. Missing files are skipped."""
    out: list[RepairFile] = []
    for name in REPAIR_FILE_NAMES:
        path = project_dir / name
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        out.append(RepairFile(name=name, path=path, old_text=text, new_text=text))
    return out


def _parse_repair_json(text: str) -> dict[str, str]:
    """Extract a JSON object from the LLM's text response.

    Accepts the JSON either bare or wrapped in a ```json fenced block.
    Raises ``ValueError`` with a short reason on parse failure.
    """
    candidate = text.strip()
    fence_match = _FENCED_JSON.search(candidate)
    if fence_match:
        candidate = fence_match.group(1).strip()
    # The bare-JSON case: find the first ``{`` and parse forward.
    if not candidate.startswith("{"):
        start = candidate.find("{")
        if start == -1:
            msg = "no JSON object found in LLM response"
            raise ValueError(msg)
        candidate = candidate[start:]
    try:
        parsed = json.loads(candidate)
    except json.JSONDecodeError as exc:
        msg = f"invalid JSON: {exc}"
        raise ValueError(msg) from exc
    if not isinstance(parsed, dict):
        msg = f"expected JSON object, got {type(parsed).__name__}"
        raise ValueError(msg)
    out: dict[str, str] = {}
    for key, value in parsed.items():
        if not isinstance(value, str):
            msg = f"value for {key!r} is not a string"
            raise ValueError(msg)
        if key in REPAIR_FILE_NAMES:
            out[key] = value
        # Unknown keys are silently ignored — the LLM may include
        # commentary fields that we don't want to write to disk.
    return out


async def _validate_repair(
    files: list[RepairFile],
    *,
    run_sql: Callable[[str], Awaitable[Any]],
) -> None:
    """Run each SQL example from the proposed queries.yaml against the DB.

    Records any execution errors on the corresponding ``RepairFile``.
    Also catches yaml parse failures for queries.yaml and time_series.yaml.
    """
    for f in files:
        if not f.changed:
            continue
        if f.name == "queries.yaml":
            try:
                docs = yaml.safe_load(f.new_text) or []
            except yaml.YAMLError as exc:
                f.validation_errors.append(f"yaml parse error: {exc}")
                continue
            if not isinstance(docs, list):
                f.validation_errors.append("expected a top-level YAML list")
                continue
            for i, entry in enumerate(docs, start=1):
                if not isinstance(entry, dict):
                    continue
                sql = entry.get("sql")
                if not sql:
                    continue
                try:
                    await run_sql(sql)
                except Exception as exc:  # noqa: BLE001
                    question = entry.get("question", "(no question)")
                    f.validation_errors.append(
                        f"query {i} ({question!r}) failed: {exc}"
                    )
        elif f.name == "time_series.yaml":
            try:
                yaml.safe_load(f.new_text)
            except yaml.YAMLError as exc:
                f.validation_errors.append(f"yaml parse error: {exc}")
        # schema_description.md has no executable content; trust the
        # markdown parse to be valid by inspection.
