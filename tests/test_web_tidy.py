"""Tests for the per-table Tidy review endpoints.

Covers POST /api/tidy/{propose,preview,apply}. The apply path opens a
writable DuckDB connection, runs DDL, and re-introspects schema, so the
fixture builds an isolated per-test DuckDB with a wide table the
deterministic detector recognizes (``sales_2020`` … ``sales_2023``).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import duckdb
import pytest
from fastapi.testclient import TestClient

import datasight.web.app as web_app
from datasight.runner import DuckDBRunner
from datasight.tidy import _detect_period_groups
from datasight.tidy_llm import ProposeResult

from tests._env_helpers import scrub_datasight_env


@pytest.fixture(autouse=True)
def _scrub_datasight_env():
    scrub_datasight_env()
    web_app._state.clear_project()


@pytest.fixture()
def wide_db(tmp_path: Path) -> str:
    """Build a per-test DuckDB with a recognizably-untidy wide table."""
    db_path = tmp_path / "wide.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE sales (
            region VARCHAR,
            sales_2020 INTEGER,
            sales_2021 INTEGER,
            sales_2022 INTEGER,
            sales_2023 INTEGER
        )
        """
    )
    conn.executemany(
        "INSERT INTO sales VALUES (?, ?, ?, ?, ?)",
        [
            ("north", 100, 110, 120, 130),
            ("south", 200, 210, 220, 230),
        ],
    )
    conn.close()
    return str(db_path)


@pytest.fixture()
def loaded_state(wide_db: str):
    """Point the global app state at the wide DB and restore on teardown."""
    state = web_app._state
    runner = DuckDBRunner(wide_db)
    state.sql_runner = cast(Any, runner)
    state.project_loaded = True
    state.project_dir = str(Path(wide_db).parent)
    state.sql_dialect = "duckdb"
    state.schema_info = [
        {
            "name": "sales",
            "row_count": 2,
            "columns": [
                {"name": "region", "dtype": "VARCHAR", "nullable": True},
                {"name": "sales_2020", "dtype": "INTEGER", "nullable": True},
                {"name": "sales_2021", "dtype": "INTEGER", "nullable": True},
                {"name": "sales_2022", "dtype": "INTEGER", "nullable": True},
                {"name": "sales_2023", "dtype": "INTEGER", "nullable": True},
            ],
        }
    ]
    state.schema_map = {"sales": {str(c["name"]) for c in state.schema_info[0]["columns"]}}
    yield state
    state.clear_project()


def _parse_sse_events(text: str) -> list[tuple[str, dict[str, Any]]]:
    """Parse an SSE response body into ``(event, data)`` pairs."""
    events: list[tuple[str, dict[str, Any]]] = []
    event = ""
    data_lines: list[str] = []
    for line in text.splitlines():
        if line.startswith("event: "):
            event = line[len("event: ") :]
        elif line.startswith("data: "):
            data_lines.append(line[len("data: ") :])
        elif line == "":
            if event:
                payload = "\n".join(data_lines) or "{}"
                events.append((event, json.loads(payload)))
            event = ""
            data_lines = []
    return events


def _suggestion_to_proposal_dict(s: Any) -> dict[str, Any]:
    """Strip the wire-only fields (preview_sql, reshape_sql_*) from a payload."""
    d = s.to_dict()
    d.pop("reshape_sql", None)
    return d


def test_detect_returns_deterministic_proposals(loaded_state):
    """GET /api/tidy/detect runs the regex detector synchronously, no LLM."""
    with TestClient(web_app.app) as client:
        response = client.get("/api/tidy/detect", params={"table": "sales"})

    assert response.status_code == 200
    body = response.json()
    assert body["error"] is None
    assert len(body["proposals"]) >= 1
    first = body["proposals"][0]
    assert first["table"] == "sales"
    assert first["target_object_name"] == "sales_long"
    mapped = {m["column"] for m in first["column_mappings"]}
    assert mapped == {"sales_2020", "sales_2021", "sales_2022", "sales_2023"}
    # Wire-only convenience fields the drawer relies on.
    assert first["preview_sql"]
    assert first["reshape_sql_view"]
    assert first["reshape_sql_table"]


def test_detect_rejects_non_duckdb(loaded_state):
    loaded_state.sql_dialect = "sqlite"
    with TestClient(web_app.app) as client:
        response = client.get("/api/tidy/detect", params={"table": "sales"})
    body = response.json()
    assert body["proposals"] == []
    assert body["error"] == "Tidy review requires DuckDB"


def test_detect_rejects_unknown_table(loaded_state):
    with TestClient(web_app.app) as client:
        response = client.get("/api/tidy/detect", params={"table": "ghost"})
    body = response.json()
    assert body["proposals"] == []
    assert "not found" in body["error"]


def test_propose_streams_llm_proposals_only(loaded_state, monkeypatch):
    """POST /api/tidy/propose no longer emits the deterministic event —
    that's the detect endpoint's job. It runs the LLM advisor and emits
    ``llm_started`` → ``llm_proposals`` → ``done`` (or ``llm_error``)."""

    async def fake_propose(*args, **kwargs):  # noqa: ARG001
        return ProposeResult(suggestions=[], raw_proposals=[], parse_warnings=[])

    monkeypatch.setattr(web_app, "propose_reshapes", fake_propose)

    # Set the LLM stub *inside* the TestClient context: FastAPI startup
    # runs ``init_llm_client(state)`` which clears llm_client to None
    # when the env has no API key (the case in CI). Setting after the
    # context enters means our stub survives that re-init.
    with TestClient(web_app.app) as client:
        loaded_state.llm_client = cast(Any, object())
        loaded_state.model = "stub"
        response = client.post("/api/tidy/propose", json={"table": "sales"})

    assert response.status_code == 200
    events = _parse_sse_events(response.text)
    event_names = [e for e, _ in events]
    assert "deterministic" not in event_names, (
        "deterministic event should be gone — fetch via /api/tidy/detect"
    )
    assert "llm_started" in event_names
    assert "llm_proposals" in event_names
    assert "done" in event_names


def test_propose_rejects_non_duckdb(loaded_state):
    loaded_state.sql_dialect = "sqlite"
    with TestClient(web_app.app) as client:
        response = client.post("/api/tidy/propose", json={"table": "sales"})
    assert response.status_code == 200
    events = _parse_sse_events(response.text)
    assert events == [("error", {"error": "Tidy review requires DuckDB"})]


def test_propose_rejects_unknown_table(loaded_state):
    with TestClient(web_app.app) as client:
        response = client.post("/api/tidy/propose", json={"table": "unknown"})
    events = _parse_sse_events(response.text)
    assert events[0][0] == "error"
    assert "not found" in events[0][1]["error"]


def test_propose_emits_llm_error_event_on_provider_failure(loaded_state, monkeypatch):
    """If the LLM call raises, the stream emits llm_error then done."""

    async def boom(*args, **kwargs):  # noqa: ARG001
        raise RuntimeError("provider-down")

    monkeypatch.setattr(web_app, "propose_reshapes", boom)

    # See note in test_propose_streams_llm_proposals_only — set the LLM
    # stub inside the TestClient context so it survives FastAPI startup's
    # init_llm_client re-initialization.
    with TestClient(web_app.app) as client:
        loaded_state.llm_client = cast(Any, object())
        loaded_state.model = "stub"
        response = client.post("/api/tidy/propose", json={"table": "sales"})

    events = _parse_sse_events(response.text)
    names = [e for e, _ in events]
    assert "llm_error" in names
    assert "done" in names
    assert "provider-down" in next(d for e, d in events if e == "llm_error")["error"]


def test_preview_returns_long_form_sample(loaded_state):
    suggestion = _detect_period_groups(loaded_state.schema_info[0])[0]
    proposal = _suggestion_to_proposal_dict(suggestion)

    with TestClient(web_app.app) as client:
        response = client.post("/api/tidy/preview", json={"proposal": proposal})

    assert response.status_code == 200
    body = response.json()
    assert body["error"] is None
    # 2 source rows × 4 mapped columns = 8 long-form rows; well under LIMIT 50.
    assert body["row_count"] == 8
    assert body["html"]
    # The long form should expose the `year` dimension column the
    # detector inferred from the YYYY suffixes.
    assert "year" in body["html"]


def test_preview_rejects_invalid_proposal(loaded_state):
    with TestClient(web_app.app) as client:
        response = client.post("/api/tidy/preview", json={"proposal": {"table": "sales"}})
    body = response.json()
    assert body["error"]
    assert body["row_count"] == 0


def test_apply_creates_long_form_view(loaded_state):
    suggestion = _detect_period_groups(loaded_state.schema_info[0])[0]
    proposal = _suggestion_to_proposal_dict(suggestion)

    with TestClient(web_app.app) as client:
        response = client.post(
            "/api/tidy/apply",
            json={
                "proposal": proposal,
                "mode": "view",
                "disposition": {"mode": "keep"},
            },
        )

    body = response.json()
    assert body["success"], body
    assert body["result"]["object_type"] == "view"
    assert body["result"]["row_count_target"] == 8
    # Schema state should now include the long form alongside the source.
    table_names = {t["name"] for t in body["schema_info"]}
    assert {"sales", "sales_long"} <= table_names


def test_apply_appends_long_form_to_schema_yaml(loaded_state):
    """When schema.yaml exists, apply should register the long-form table."""
    project_dir = Path(loaded_state.project_dir)
    yaml_path = project_dir / "schema.yaml"
    yaml_path.write_text("tables:\n  - name: sales\n", encoding="utf-8")

    suggestion = _detect_period_groups(loaded_state.schema_info[0])[0]
    proposal = _suggestion_to_proposal_dict(suggestion)

    with TestClient(web_app.app) as client:
        response = client.post(
            "/api/tidy/apply",
            json={
                "proposal": proposal,
                "mode": "view",
                "disposition": {"mode": "keep"},
            },
        )

    assert response.json()["success"]
    rewritten = yaml_path.read_text(encoding="utf-8")
    assert "sales_long" in rewritten, rewritten


def test_apply_creates_schema_yaml_when_absent(loaded_state):
    """Applying should persist the long-form table even on projects that
    didn't keep an allowlist before — the explicit Apply action means the
    user wants the reshape registered."""
    project_dir = Path(loaded_state.project_dir)
    yaml_path = project_dir / "schema.yaml"
    assert not yaml_path.exists()

    suggestion = _detect_period_groups(loaded_state.schema_info[0])[0]
    proposal = _suggestion_to_proposal_dict(suggestion)

    with TestClient(web_app.app) as client:
        response = client.post(
            "/api/tidy/apply",
            json={
                "proposal": proposal,
                "mode": "view",
                "disposition": {"mode": "keep"},
            },
        )

    assert response.json()["success"]
    assert yaml_path.exists(), "schema.yaml should have been created"
    rewritten = yaml_path.read_text(encoding="utf-8")
    assert "sales_long" in rewritten, rewritten


def test_apply_respects_include_nulls_toggle(tmp_path: Path):
    """End-to-end: a proposal with ``include_nulls=False`` produces a denser
    long form than the same proposal with the default ``True``. Pins the
    web endpoint round-trip of the toggle."""
    db_path = tmp_path / "sparse.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(
        "CREATE TABLE sales ("
        "region VARCHAR, sales_2020 INTEGER, sales_2021 INTEGER, sales_2022 INTEGER)"
    )
    conn.execute("INSERT INTO sales VALUES ('north', 100, 110, 120), ('south', 200, NULL, 220)")
    conn.close()

    state = web_app._state
    runner = DuckDBRunner(str(db_path))
    state.sql_runner = cast(Any, runner)
    state.project_loaded = True
    state.project_dir = str(tmp_path)
    state.sql_dialect = "duckdb"
    state.schema_info = [
        {
            "name": "sales",
            "row_count": 2,
            "columns": [
                {"name": "region", "dtype": "VARCHAR", "nullable": True},
                {"name": "sales_2020", "dtype": "INTEGER", "nullable": True},
                {"name": "sales_2021", "dtype": "INTEGER", "nullable": True},
                {"name": "sales_2022", "dtype": "INTEGER", "nullable": True},
            ],
        }
    ]
    state.schema_map = {"sales": {str(c["name"]) for c in state.schema_info[0]["columns"]}}

    try:
        suggestion = _detect_period_groups(state.schema_info[0])[0]
        proposal = _suggestion_to_proposal_dict(suggestion)
        proposal["include_nulls"] = False  # explicit drop

        with TestClient(web_app.app) as client:
            response = client.post(
                "/api/tidy/apply",
                json={
                    "proposal": proposal,
                    "mode": "table",
                    "disposition": {"mode": "keep"},
                },
            )

        body = response.json()
        assert body["success"], body
        # 2 rows × 3 cols = 6 expected; minus the 1 NULL = 5.
        assert body["result"]["row_count_target"] == 5
    finally:
        state.clear_project()


def test_apply_rejects_view_with_rename_disposition(loaded_state):
    """Mirrors the CLI safety rule: view mode + rename leaves a dangling view."""
    suggestion = _detect_period_groups(loaded_state.schema_info[0])[0]
    proposal = _suggestion_to_proposal_dict(suggestion)

    with TestClient(web_app.app) as client:
        response = client.post(
            "/api/tidy/apply",
            json={
                "proposal": proposal,
                "mode": "view",
                "disposition": {"mode": "rename", "new_name": "sales_raw"},
            },
        )

    body = response.json()
    assert body["success"] is False
    assert "table" in body["error"]


def test_apply_rejects_when_target_collides(loaded_state):
    """Schema cross-check should catch a target name that already exists."""
    suggestion = _detect_period_groups(loaded_state.schema_info[0])[0]
    proposal = _suggestion_to_proposal_dict(suggestion)
    proposal["target_object_name"] = "sales"  # collides with the source

    with TestClient(web_app.app) as client:
        response = client.post(
            "/api/tidy/apply",
            json={
                "proposal": proposal,
                "mode": "view",
                "disposition": {"mode": "keep"},
            },
        )

    body = response.json()
    assert body["success"] is False
    assert "schema cross-check" in body["error"]


def test_apply_rejects_invalid_mode(loaded_state):
    suggestion = _detect_period_groups(loaded_state.schema_info[0])[0]
    proposal = _suggestion_to_proposal_dict(suggestion)
    with TestClient(web_app.app) as client:
        response = client.post(
            "/api/tidy/apply",
            json={"proposal": proposal, "mode": "TABEL", "disposition": {"mode": "keep"}},
        )
    body = response.json()
    assert body["success"] is False
    assert "mode must be" in body["error"]


def test_apply_creates_table_with_replace_disposition(loaded_state):
    """``replace`` disposition swaps the long form into the source's slot:
    the long form takes the source's old name, the user-chosen target
    name is just a transient intermediate."""
    suggestion = _detect_period_groups(loaded_state.schema_info[0])[0]
    proposal = _suggestion_to_proposal_dict(suggestion)

    with TestClient(web_app.app) as client:
        response = client.post(
            "/api/tidy/apply",
            json={
                "proposal": proposal,
                "mode": "table",
                "disposition": {"mode": "replace"},
            },
        )

    body = response.json()
    assert body["success"], body
    # After replace, the long form takes the source's old name.
    assert body["result"]["final_target_name"] == "sales"
    table_names = {t["name"] for t in body["schema_info"]}
    assert "sales" in table_names
    assert "sales_long" not in table_names
    # The new shape is long: it should have a `year` column.
    sales_cols = next(t["columns"] for t in body["schema_info"] if t["name"] == "sales")
    assert any(c["name"] == "year" for c in sales_cols)


def test_apply_creates_table_with_bare_drop_disposition(loaded_state):
    """``drop`` disposition (post-rename) is the bare drop: source goes
    away but the long form keeps its target name. Downstream code that
    referenced the source by name will break — that's the user's call."""
    suggestion = _detect_period_groups(loaded_state.schema_info[0])[0]
    proposal = _suggestion_to_proposal_dict(suggestion)

    with TestClient(web_app.app) as client:
        response = client.post(
            "/api/tidy/apply",
            json={
                "proposal": proposal,
                "mode": "table",
                "disposition": {"mode": "drop"},
            },
        )

    body = response.json()
    assert body["success"], body
    assert body["result"]["final_target_name"] == "sales_long"
    table_names = {t["name"] for t in body["schema_info"]}
    assert "sales" not in table_names
    assert "sales_long" in table_names
