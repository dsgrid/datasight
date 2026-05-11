"""Tests for the LLM-driven grounding-repair flow."""

from __future__ import annotations

import asyncio
import json
import textwrap
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pytest

from datasight.grounding import DriftItem, DriftReport
from datasight.grounding_repair import (
    REPAIR_FILE_NAMES,
    RepairFile,
    RepairResult,
    _parse_repair_json,
    format_repair_summary,
    repair_grounding,
    write_repair_atomic,
)
from datasight.llm import CallStats, LLMResponse, TextBlock, Usage


class _FakeLLMClient:
    """Returns canned text responses in sequence.

    Each ``create_message`` call pops the next response. ``aclose`` is a
    no-op so the contract matches :class:`datasight.llm.LLMClient`.
    """

    def __init__(self, responses: list[str]):
        self._responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    async def create_message(self, *, model, system, messages, tools, max_tokens):
        self.calls.append({"messages": messages, "tools": tools})
        if not self._responses:
            msg = "fake client ran out of canned responses"
            raise RuntimeError(msg)
        text = self._responses.pop(0)
        return LLMResponse(
            content=[TextBlock(text=text)],
            stop_reason="end_turn",
            usage=Usage(input_tokens=10, output_tokens=20),
            call_stats=CallStats(),
        )

    async def aclose(self) -> None:
        return None


def _make_run_sql(db_path: str):
    """Async wrapper matching datasight's ``run_sql`` signature."""

    async def run_sql(sql: str) -> pd.DataFrame:
        conn = duckdb.connect(db_path, read_only=True)
        try:
            return conn.execute(sql).fetchdf()
        finally:
            conn.close()

    return run_sql


def _long_format_db(tmp_path: Path) -> str:
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(
        "CREATE TABLE load_data "
        "(geography VARCHAR, fuel_type VARCHAR, end_use VARCHAR, energy_mwh DOUBLE)"
    )
    conn.execute(
        "INSERT INTO load_data VALUES "
        "('pacific', 'elec', 'heating', 10.0), "
        "('pacific', 'ng', 'heating', 20.0)"
    )
    conn.close()
    return str(db_path)


def test_parse_repair_json_bare_object():
    result = _parse_repair_json('{"queries.yaml": "- q: hi"}')
    assert result == {"queries.yaml": "- q: hi"}


def test_parse_repair_json_fenced_block():
    text = '```json\n{"queries.yaml": "a"}\n```'
    assert _parse_repair_json(text) == {"queries.yaml": "a"}


def test_parse_repair_json_with_prose_prefix():
    text = 'Here you go:\n{"queries.yaml": "a"}'
    assert _parse_repair_json(text) == {"queries.yaml": "a"}


def test_parse_repair_json_rejects_non_object():
    # An object-shaped string would parse, but a top-level number with a
    # stray brace must still be rejected as not-an-object.
    with pytest.raises(ValueError):
        _parse_repair_json("just text 42")


def test_parse_repair_json_rejects_malformed():
    with pytest.raises(ValueError, match="no JSON object"):
        _parse_repair_json("nothing useful here")


def test_parse_repair_json_drops_unknown_keys():
    text = json.dumps({
        "queries.yaml": "x",
        "comment": "ignore me",
        "schema_description.md": "y",
    })
    result = _parse_repair_json(text)
    assert set(result.keys()) == {"queries.yaml", "schema_description.md"}


def test_parse_repair_json_rejects_non_string_value():
    with pytest.raises(ValueError, match="not a string"):
        _parse_repair_json('{"queries.yaml": 42}')


def test_repair_file_unified_diff_reflects_changes(tmp_path):
    f = RepairFile(
        name="queries.yaml",
        path=tmp_path / "queries.yaml",
        old_text="- old\n",
        new_text="- new\n",
    )
    diff = f.unified_diff()
    assert "a/queries.yaml" in diff
    assert "-- old" in diff or "-old" in diff
    assert "+- new" in diff or "+new" in diff


def test_repair_file_unchanged_when_text_equal(tmp_path):
    f = RepairFile(
        name="queries.yaml",
        path=tmp_path / "queries.yaml",
        old_text="x",
        new_text="x",
    )
    assert not f.changed


def test_write_repair_atomic_writes_only_validated_changes(tmp_path):
    (tmp_path / "queries.yaml").write_text("- old\n")
    (tmp_path / "schema_description.md").write_text("# old\n")
    f1 = RepairFile(
        name="queries.yaml",
        path=tmp_path / "queries.yaml",
        old_text="- old\n",
        new_text="- new\n",
    )
    f2 = RepairFile(
        name="schema_description.md",
        path=tmp_path / "schema_description.md",
        old_text="# old\n",
        new_text="# new\n",
        validation_errors=["bad sql"],
    )
    result = RepairResult(files=[f1, f2])
    written = write_repair_atomic(result, tmp_path)
    assert written == [tmp_path / "queries.yaml"]
    assert (tmp_path / "queries.yaml").read_text() == "- new\n"
    # File with validation errors is left untouched.
    assert (tmp_path / "schema_description.md").read_text() == "# old\n"


def test_write_repair_atomic_no_changes_writes_nothing(tmp_path):
    f = RepairFile(
        name="queries.yaml",
        path=tmp_path / "queries.yaml",
        old_text="same",
        new_text="same",
    )
    written = write_repair_atomic(RepairResult(files=[f]), tmp_path)
    assert written == []
    assert not (tmp_path / "queries.yaml").exists()


def test_format_repair_summary_lists_changed_files():
    f = RepairFile(
        name="queries.yaml",
        path=Path("queries.yaml"),
        old_text="a",
        new_text="b",
    )
    result = RepairResult(files=[f])
    text = format_repair_summary(result)
    assert "queries.yaml" in text
    assert "[ok]" in text


def test_format_repair_summary_with_no_changes():
    text = format_repair_summary(RepairResult(files=[]))
    assert "no files changed" in text.lower()


def test_repair_grounding_happy_path(tmp_path):
    """LLM proposal validates cleanly on first try; result is well-formed."""
    db_path = _long_format_db(tmp_path)
    (tmp_path / "queries.yaml").write_text(textwrap.dedent("""
        - question: "Old top regions"
          sql: SELECT * FROM load_data WHERE elec_heating > 0;
    """).strip())

    new_queries = textwrap.dedent("""
        - question: "Top regions"
          sql: SELECT geography, SUM(energy_mwh) AS total FROM load_data WHERE fuel_type = 'elec' AND end_use = 'heating' GROUP BY geography;
    """).strip()
    llm_response = json.dumps({"queries.yaml": new_queries})
    client = _FakeLLMClient([llm_response])
    run_sql = _make_run_sql(db_path)

    drift = DriftReport(items=[DriftItem(
        file=str(tmp_path / "queries.yaml"), line=None, kind="column",
        claim="elec_heating", detail="missing",
    )])
    old_schema = {"load_data": {"elec_heating", "geography"}}
    new_schema = {"load_data": {"geography", "fuel_type", "end_use", "energy_mwh"}}

    result = asyncio.run(repair_grounding(
        tmp_path, old_schema, new_schema, drift,
        llm_client=client, model="test", run_sql=run_sql,
    ))
    assert result.overall_ok
    assert result.any_changes
    assert result.llm_retries == 0
    q_file = next(f for f in result.files if f.name == "queries.yaml")
    assert q_file.changed
    assert not q_file.validation_errors


def test_repair_grounding_retries_on_invalid_sql(tmp_path):
    """First proposal fails to execute; second one validates."""
    db_path = _long_format_db(tmp_path)
    (tmp_path / "queries.yaml").write_text(textwrap.dedent("""
        - question: "Stale"
          sql: SELECT foo FROM load_data;
    """).strip())

    broken = json.dumps({"queries.yaml": "- question: q\n  sql: SELECT still_broken FROM load_data;"})
    good = json.dumps({"queries.yaml": "- question: q\n  sql: SELECT geography FROM load_data;"})
    client = _FakeLLMClient([broken, good])
    run_sql = _make_run_sql(db_path)

    drift = DriftReport(items=[DriftItem(
        file=str(tmp_path / "queries.yaml"), line=None, kind="column",
        claim="foo", detail="missing",
    )])
    old_schema = {"load_data": {"foo"}}
    new_schema = {"load_data": {"geography", "fuel_type", "end_use", "energy_mwh"}}

    result = asyncio.run(repair_grounding(
        tmp_path, old_schema, new_schema, drift,
        llm_client=client, model="test", run_sql=run_sql,
        max_retries=2,
    ))
    assert result.overall_ok
    assert result.llm_retries == 1
    # The client should have been called twice and the second user prompt
    # should include the validation error context.
    assert len(client.calls) == 2
    second_user_prompt = client.calls[1]["messages"][0]["content"]
    assert "still_broken" in second_user_prompt or "validation error" in second_user_prompt.lower()


def test_repair_grounding_gives_up_after_max_retries(tmp_path):
    """When every proposal is broken, the result surfaces validation errors."""
    db_path = _long_format_db(tmp_path)
    (tmp_path / "queries.yaml").write_text(textwrap.dedent("""
        - question: "Stale"
          sql: SELECT foo FROM load_data;
    """).strip())

    bad = json.dumps({"queries.yaml": "- question: q\n  sql: SELECT nope_a FROM load_data;"})
    worse = json.dumps({"queries.yaml": "- question: q\n  sql: SELECT nope_b FROM load_data;"})
    worst = json.dumps({"queries.yaml": "- question: q\n  sql: SELECT nope_c FROM load_data;"})
    client = _FakeLLMClient([bad, worse, worst])
    run_sql = _make_run_sql(db_path)

    drift = DriftReport(items=[])
    old_schema: dict[str, set[str]] = {}
    new_schema = {"load_data": {"geography"}}

    result = asyncio.run(repair_grounding(
        tmp_path, old_schema, new_schema, drift,
        llm_client=client, model="test", run_sql=run_sql,
        max_retries=2,
    ))
    assert not result.overall_ok
    q_file = next(f for f in result.files if f.name == "queries.yaml")
    assert q_file.validation_errors


def test_repair_files_are_loaded_from_disk(tmp_path):
    """Only existing files in the scope set are presented to the LLM."""
    (tmp_path / "queries.yaml").write_text("- q: y")
    (tmp_path / "schema_description.md").write_text("hello")
    # time_series.yaml deliberately missing.

    db_path = _long_format_db(tmp_path)
    run_sql = _make_run_sql(db_path)
    new_queries_value = "- question: q\n  sql: SELECT geography FROM load_data;"
    response = json.dumps({"queries.yaml": new_queries_value})
    client = _FakeLLMClient([response])

    drift = DriftReport(items=[])
    new_schema = {"load_data": {"geography"}}

    result = asyncio.run(repair_grounding(
        tmp_path, {}, new_schema, drift,
        llm_client=client, model="test", run_sql=run_sql,
    ))
    file_names = {f.name for f in result.files}
    assert file_names == {"queries.yaml", "schema_description.md"}
    assert "time_series.yaml" not in file_names


def test_repair_file_names_constant_matches_user_scope():
    """The hardcoded scope in REPAIR_FILE_NAMES matches what was agreed."""
    assert REPAIR_FILE_NAMES == (
        "queries.yaml",
        "schema_description.md",
        "time_series.yaml",
    )
