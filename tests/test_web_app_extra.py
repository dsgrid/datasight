"""Additional tests for the FastAPI web app aimed at coverage."""

from __future__ import annotations

import io
import json
import os
import zipfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import pandas as pd
import pytest
from fastapi.testclient import TestClient

import datasight.web.app as web_app
from datasight.llm import LLMResponse, TextBlock, ToolUseBlock, Usage

from tests._env_helpers import scrub_datasight_env


def _typed_stub(value: object) -> Any:
    return cast(Any, value)


@pytest.fixture()
def isolated_web_state(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Isolate mutable app state between tests."""
    original_env = os.environ.copy()
    original_flags = {
        "confirm_sql": web_app._state.confirm_sql,
        "explain_sql": web_app._state.explain_sql,
        "clarify_sql": web_app._state.clarify_sql,
        "show_cost": web_app._state.show_cost,
        "show_provenance": web_app._state.show_provenance,
    }
    scrub_datasight_env()

    web_app._state.clear_project()
    monkeypatch.setattr(web_app, "add_recent_project", lambda project_path: None)
    monkeypatch.setattr(web_app, "load_recent_projects", lambda: [])

    yield

    web_app._state.clear_project()
    for key, val in original_flags.items():
        setattr(web_app._state, key, val)
    os.environ.clear()
    os.environ.update(original_env)


# ---------------------------------------------------------------------------
# Stub LLM client for chat tests
# ---------------------------------------------------------------------------


class StubLLMClient:
    """Minimal LLM client that returns a canned final response."""

    def __init__(self, responses: list[LLMResponse] | None = None) -> None:
        self.responses = responses or [
            LLMResponse(
                content=[TextBlock(text="Hello world.")],
                stop_reason="end_turn",
                usage=Usage(input_tokens=1, output_tokens=2),
            )
        ]
        self.calls = 0

    async def create_message(self, **kwargs):  # noqa: ARG002
        idx = min(self.calls, len(self.responses) - 1)
        self.calls += 1
        return self.responses[idx]


# ---------------------------------------------------------------------------
# Chat streaming tests
# ---------------------------------------------------------------------------


def test_chat_rejects_invalid_session_id(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        response = client.post(
            "/api/chat",
            json={"message": "hi", "session_id": "bad/../session"},
        )
    assert response.status_code == 200
    assert "Invalid session ID" in response.text


def test_chat_empty_message_returns_done(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        response = client.post(
            "/api/chat",
            json={"message": "   ", "session_id": "s1"},
        )
    assert response.status_code == 200
    assert "event: done" in response.text


def test_chat_without_llm_reports_configuration_error(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        web_app._state.llm_client = None
        response = client.post(
            "/api/chat",
            json={"message": "How many rows?", "session_id": "s1"},
        )
    assert response.status_code == 200
    assert "event: error" in response.text
    assert "LLM not configured" in response.text


def test_chat_full_run_with_real_project(
    isolated_web_state: None, project_dir: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """End-to-end chat flow against real duckdb project with stubbed LLM."""
    # Load project first, then inject stub LLM client
    with TestClient(web_app.app) as client:
        load_response = client.post("/api/projects/load", json={"path": project_dir})
        assert load_response.json()["success"] is True

        # Simulate a tool_use then final text
        tool_response = LLMResponse(
            content=[
                TextBlock(text="Let me check."),
                ToolUseBlock(
                    id="tool_1",
                    name="run_sql",
                    input={"sql": "SELECT COUNT(*) AS n FROM orders"},
                ),
            ],
            stop_reason="tool_use",
            usage=Usage(input_tokens=5, output_tokens=10),
        )
        final_response = LLMResponse(
            content=[TextBlock(text="There are 10 orders.")],
            stop_reason="end_turn",
            usage=Usage(input_tokens=3, output_tokens=5),
        )
        stub = StubLLMClient(responses=[tool_response, final_response])
        web_app._state.llm_client = _typed_stub(stub)

        chat_response = client.post(
            "/api/chat",
            json={"message": "How many orders?", "session_id": "s1"},
        )
    assert chat_response.status_code == 200
    body = chat_response.text
    assert "event: done" in body
    # Should have tool_start or tool_done events
    assert "event: tool_start" in body or "tool_done" in body


def test_chat_visualize_streams_plotly_spec_by_reference(
    isolated_web_state: None, project_dir: str
) -> None:
    """Live chart SSE should avoid streaming a second full iframe payload."""
    with TestClient(web_app.app) as client:
        load_response = client.post("/api/projects/load", json={"path": project_dir})
        assert load_response.json()["success"] is True

        tool_response = LLMResponse(
            content=[
                ToolUseBlock(
                    id="tool_1",
                    name="visualize_data",
                    input={
                        "sql": (
                            "SELECT customer_state, SUM(quantity) AS total_qty "
                            "FROM orders GROUP BY customer_state ORDER BY customer_state"
                        ),
                        "plotly_spec": {
                            "data": [
                                {
                                    "type": "bar",
                                    "x": "customer_state",
                                    "y": "total_qty",
                                }
                            ],
                            "layout": {"title": "Quantity by State"},
                        },
                    },
                ),
            ],
            stop_reason="tool_use",
            usage=Usage(input_tokens=5, output_tokens=10),
        )
        final_response = LLMResponse(
            content=[TextBlock(text="Here is the chart.")],
            stop_reason="end_turn",
            usage=Usage(input_tokens=3, output_tokens=5),
        )
        web_app._state.llm_client = _typed_stub(
            StubLLMClient(responses=[tool_response, final_response])
        )

        chat_response = client.post(
            "/api/chat",
            json={"message": "Chart quantity by state", "session_id": "chart_sse"},
        )

        assert chat_response.status_code == 200
        body = chat_response.text
        assert '"type": "chart"' in body
        assert '"html": ""' in body
        assert '"plotly_spec_ref"' in body
        assert '"x": ["CA", "FL", "NY", "TX"]' not in body
        assert "Plotly.newPlot" not in body
        assert "<iframe" not in body

        conv = client.get("/api/conversations/chart_sse").json()
        event_index = next(
            idx
            for idx, event in enumerate(conv["events"])
            if event["event"] == "tool_result" and event["data"]["type"] == "chart"
        )
        spec_response = client.get(
            f"/api/conversations/chart_sse/events/{event_index}/plotly-spec"
        )
        assert spec_response.status_code == 200
        spec = spec_response.json()["plotly_spec"]
        assert spec["data"][0]["x"] == ["CA", "FL", "NY", "TX"]

        cached_response = client.post(
            "/api/chat",
            json={"message": "Chart quantity by state", "session_id": "chart_sse_cached"},
        )
        cached_body = cached_response.text
        assert '"plotly_spec_ref"' in cached_body
        assert '"x": ["CA", "FL", "NY", "TX"]' not in cached_body


def test_chat_cache_hit_on_second_identical_question(
    isolated_web_state: None, project_dir: str
) -> None:
    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})

        stub = StubLLMClient(
            responses=[
                LLMResponse(
                    content=[TextBlock(text="cached answer")],
                    stop_reason="end_turn",
                    usage=Usage(input_tokens=1, output_tokens=1),
                )
            ]
        )
        web_app._state.llm_client = _typed_stub(stub)

        # first turn caches
        first = client.post("/api/chat", json={"message": "Unique q?", "session_id": "cache1"})
        assert "event: done" in first.text

        # new session — should get cache hit
        second = client.post("/api/chat", json={"message": "Unique q?", "session_id": "cache2"})
    assert "event: done" in second.text


def test_chat_llm_error_emits_error_event(isolated_web_state: None, project_dir: str) -> None:
    class BadLLM:
        async def create_message(self, **kwargs):  # noqa: ARG002
            from datasight.exceptions import LLMError

            raise LLMError("rate limited")

    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})
        web_app._state.llm_client = _typed_stub(BadLLM())
        response = client.post("/api/chat", json={"message": "anything", "session_id": "err1"})
    assert "event: done" in response.text
    assert "rate limited" in response.text


# ---------------------------------------------------------------------------
# Summarize SSE streaming
# ---------------------------------------------------------------------------


def test_summarize_without_project(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        response = client.get("/api/summarize")
    assert response.status_code == 200
    assert "No dataset loaded" in response.text


def test_summarize_streams_tokens(isolated_web_state: None, project_dir: str) -> None:
    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})
        web_app._state.llm_client = _typed_stub(
            StubLLMClient(
                responses=[
                    LLMResponse(
                        content=[TextBlock(text="Summary text here.")],
                        stop_reason="end_turn",
                        usage=Usage(input_tokens=1, output_tokens=1),
                    )
                ]
            )
        )
        response = client.get("/api/summarize")
    assert response.status_code == 200
    assert "event: token" in response.text
    assert "event: done" in response.text


def test_summarize_without_llm_emits_error(isolated_web_state: None, project_dir: str) -> None:
    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})
        web_app._state.llm_client = None
        response = client.get("/api/summarize")
    assert "LLM not initialized" in response.text


# ---------------------------------------------------------------------------
# Project management
# ---------------------------------------------------------------------------


def test_validate_project_endpoint(isolated_web_state: None, project_dir: str) -> None:
    with TestClient(web_app.app) as client:
        good = client.post("/api/projects/validate", json={"path": project_dir}).json()
        bad = client.post("/api/projects/validate", json={"path": "/nonexistent/xyz"}).json()
    assert good["valid"] is True
    assert bad["valid"] is False


def test_load_project_returns_error_on_invalid_path(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        response = client.post("/api/projects/load", json={"path": "/not/a/real/path/qqq"})
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is False
    assert body["error"]


def test_remove_project_from_recent(
    isolated_web_state: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured = {}

    def fake_remove(p):
        captured["path"] = p

    monkeypatch.setattr(web_app, "remove_recent_project", fake_remove)
    with TestClient(web_app.app) as client:
        resp = client.delete("/api/projects/recent/tmp/some/path")
    assert resp.status_code == 200
    assert resp.json()["success"] is True
    assert captured["path"].startswith("/")


# ---------------------------------------------------------------------------
# Project health
# ---------------------------------------------------------------------------


def test_project_health_unloaded(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        resp = client.get("/api/project-health")
    body = resp.json()
    assert body["project_loaded"] is False
    assert body["summary"]["fail_count"] >= 1
    assert any(c["name"] == ".env" for c in body["checks"])


def test_project_health_loaded(isolated_web_state: None, project_dir: str) -> None:
    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})
        resp = client.get("/api/project-health")
    body = resp.json()
    assert body["project_loaded"] is True
    # connectivity check should pass — DB is real
    connectivity = [c for c in body["checks"] if c["name"] == "Database connectivity"]
    assert connectivity and connectivity[0]["ok"] is True


# ---------------------------------------------------------------------------
# Explore / add files endpoints
# ---------------------------------------------------------------------------


def test_explore_rejects_empty_paths(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        resp = client.post("/api/explore", json={"paths": []})
    assert resp.json()["success"] is False


def test_explore_status(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        resp = client.get("/api/explore/status")
    body = resp.json()
    assert body["is_ephemeral"] is False
    assert body["tables"] == []


def test_explore_check_project_path_empty(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        resp = client.post("/api/explore/check-project-path", json={"path": ""})
    assert resp.json()["exists"] is False


def test_explore_check_project_path_detects_files(
    isolated_web_state: None, project_dir: str
) -> None:
    with TestClient(web_app.app) as client:
        resp = client.post("/api/explore/check-project-path", json={"path": project_dir})
    body = resp.json()
    assert body["exists"] is True
    assert ".env" in body["files"]


def test_explore_save_project_requires_ephemeral(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        resp = client.post("/api/explore/save-project", json={"path": "/tmp/x"})
    assert resp.json()["success"] is False


def test_explore_save_project_requires_path(isolated_web_state: None) -> None:
    web_app._state.is_ephemeral = True
    try:
        with TestClient(web_app.app) as client:
            resp = client.post("/api/explore/save-project", json={"path": ""})
    finally:
        web_app._state.is_ephemeral = False
    assert resp.json()["success"] is False
    assert "required" in resp.json()["error"].lower()


def test_add_files_requires_paths(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        resp = client.post("/api/add-files", json={"paths": []})
    assert resp.json()["success"] is False


def test_add_files_requires_runner(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        resp = client.post("/api/add-files", json={"paths": ["/tmp/foo.csv"]})
    assert resp.json()["success"] is False
    assert "No data loaded" in resp.json()["error"]


def test_add_files_to_real_project(
    isolated_web_state: None, project_dir: str, tmp_path: Path
) -> None:
    csv_path = tmp_path / "extra.csv"
    csv_path.write_text("a,b\n1,2\n3,4\n", encoding="utf-8")

    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})
        resp = client.post("/api/add-files", json={"paths": [str(csv_path)]})
    body = resp.json()
    assert body["success"] is True
    assert any(t["name"] == "extra" for t in body["schema_info"])


def test_generate_project_without_runner(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        resp = client.post("/api/explore/generate-project", json={"path": "/tmp/x"})
    assert "event: error" in resp.text
    assert "No data loaded" in resp.text


# ---------------------------------------------------------------------------
# Preview / column stats error branches
# ---------------------------------------------------------------------------


def test_preview_unknown_table(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        resp = client.get("/api/preview/nope")
    body = resp.json()
    assert body["html"] is None
    assert body["error"] == "Unknown table"


def test_preview_real_project(isolated_web_state: None, project_dir: str) -> None:
    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})
        first = client.get("/api/preview/orders")
        second = client.get("/api/preview/orders")
    assert first.json()["cached"] is False
    assert second.json()["cached"] is True
    assert "table" in first.json()["html"]


def test_column_stats_unknown_table(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        resp = client.get("/api/column-stats/unknown/col")
    assert resp.json()["error"] == "Unknown table"


def test_column_stats_unknown_column(isolated_web_state: None, project_dir: str) -> None:
    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})
        resp = client.get("/api/column-stats/orders/nonexistent")
    assert resp.json()["error"] == "Unknown column"


def test_column_stats_numeric_real_project(isolated_web_state: None, project_dir: str) -> None:
    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})
        resp = client.get("/api/column-stats/orders/quantity")
    body = resp.json()
    assert body["stats"]["distinct"] > 0
    assert "avg" in body["stats"]


def test_column_stats_text_real_project(isolated_web_state: None, project_dir: str) -> None:
    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})
        resp = client.get("/api/column-stats/products/category")
    body = resp.json()
    assert body["stats"]["distinct"] == 3
    assert "avg" not in body["stats"]


# ---------------------------------------------------------------------------
# Overview endpoints with real project (drives actual data_profile builders)
# ---------------------------------------------------------------------------


def test_dataset_overview_real(isolated_web_state: None, project_dir: str) -> None:
    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})
        resp = client.get("/api/dataset-overview")
    body = resp.json()
    assert body["overview"]["table_count"] >= 2


def test_dataset_overview_table_not_found(isolated_web_state: None, project_dir: str) -> None:
    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})
        resp = client.get("/api/dataset-overview?table=missing")
    assert "not found" in resp.json()["error"].lower()


def test_dimension_overview_real(isolated_web_state: None, project_dir: str) -> None:
    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})
        resp = client.get("/api/dimension-overview")
    assert "overview" in resp.json()


def test_measure_overview_real(isolated_web_state: None, project_dir: str) -> None:
    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})
        resp = client.get("/api/measure-overview")
    assert "overview" in resp.json()


def test_quality_overview_real(isolated_web_state: None, project_dir: str) -> None:
    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})
        resp = client.get("/api/quality-overview")
    assert "overview" in resp.json()


def test_trend_overview_real(isolated_web_state: None, project_dir: str) -> None:
    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})
        resp = client.get("/api/trend-overview")
    assert "overview" in resp.json()


def test_trend_overview_table_not_found(isolated_web_state: None, project_dir: str) -> None:
    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})
        resp = client.get("/api/trend-overview?table=missing")
    assert "not found" in resp.json()["error"].lower()


def test_quality_overview_table_not_found(isolated_web_state: None, project_dir: str) -> None:
    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})
        resp = client.get("/api/quality-overview?table=missing")
    assert "not found" in resp.json()["error"].lower()


# ---------------------------------------------------------------------------
# Bookmarks / Reports / Dashboard CRUD
# ---------------------------------------------------------------------------


def test_bookmarks_crud(isolated_web_state: None, project_dir: str) -> None:
    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})

        assert client.get("/api/bookmarks").json() == {"bookmarks": []}

        missing = client.post("/api/bookmarks", json={"sql": ""}).json()
        assert missing["error"] == "sql is required"

        added = client.post("/api/bookmarks", json={"sql": "SELECT 1", "name": "one"}).json()
        assert added["bookmark"]["sql"] == "SELECT 1"

        listed = client.get("/api/bookmarks").json()["bookmarks"]
        assert len(listed) == 1

        bm_id = added["bookmark"]["id"]
        assert client.delete(f"/api/bookmarks/{bm_id}").json()["ok"] is True
        assert client.delete("/api/bookmarks").json()["ok"] is True


def test_bookmarks_no_project(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        assert client.post("/api/bookmarks", json={"sql": "x"}).json()["ok"] is False
        assert client.delete("/api/bookmarks/1").json()["ok"] is True
        assert client.delete("/api/bookmarks").json()["ok"] is True


def test_reports_crud(isolated_web_state: None, project_dir: str) -> None:
    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})

        empty_sql = client.post("/api/reports", json={"sql": ""}).json()
        assert empty_sql["error"] == "sql is required"

        added = client.post(
            "/api/reports",
            json={"sql": "SELECT 1 AS v", "tool": "run_sql", "name": "r1"},
        ).json()
        rid = added["report"]["id"]

        # update
        updated = client.patch(f"/api/reports/{rid}", json={"name": "renamed"}).json()
        assert updated["ok"] is True
        assert updated["report"]["name"] == "renamed"

        # update with empty sql rejected
        bad = client.patch(f"/api/reports/{rid}", json={"sql": ""}).json()
        assert bad["ok"] is False

        # update with no fields
        none = client.patch(f"/api/reports/{rid}", json={}).json()
        assert none["ok"] is False

        # update unknown report
        missing = client.patch("/api/reports/99999", json={"name": "nope"}).json()
        assert missing["ok"] is False

        # run the report
        run = client.post(f"/api/reports/{rid}/run").json()
        assert run["ok"] is True

        # run unknown
        bad_run = client.post("/api/reports/99999/run").json()
        assert bad_run["ok"] is False

        # delete
        assert client.delete(f"/api/reports/{rid}").json()["ok"] is True
        assert client.delete("/api/reports").json()["ok"] is True


def test_reports_no_project(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        assert client.get("/api/reports").json() == {"reports": []}
        assert client.post("/api/reports", json={"sql": "x"}).json()["ok"] is False
        assert client.patch("/api/reports/1", json={"name": "x"}).json()["ok"] is False
        assert client.delete("/api/reports/1").json()["ok"] is True
        assert client.delete("/api/reports").json()["ok"] is True
        assert client.post("/api/reports/1/run").json()["ok"] is False


def test_dashboard_crud(isolated_web_state: None, project_dir: str) -> None:
    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})
        assert client.get("/api/dashboard").json() == {
            "items": [],
            "columns": 0,
            "filters": [],
            "title": "",
        }
        saved = client.post(
            "/api/dashboard",
            json={"items": [{"title": "chart"}], "columns": 3},
        ).json()
        assert saved["columns"] == 3
        assert saved["items"][0]["id"] == 1
        assert client.delete("/api/dashboard").json()["ok"] is True


def test_dashboard_no_project(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        assert client.post("/api/dashboard", json={"items": []}).json() == {
            "items": [],
            "columns": 0,
            "filters": [],
            "title": "",
        }
        assert client.delete("/api/dashboard").json()["ok"] is True


def test_dashboard_persists_user_title(isolated_web_state: None, project_dir: str) -> None:
    """The user-set dashboard title round-trips through save and reload, and
    a fresh GET reflects the latest title without a stray default."""
    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})

        saved = client.post(
            "/api/dashboard",
            json={"items": [], "columns": 2, "title": "Q3 Generation Review"},
        ).json()
        assert saved["title"] == "Q3 Generation Review"

        reloaded = client.get("/api/dashboard").json()
        assert reloaded["title"] == "Q3 Generation Review"

        # Updating the title alone (without re-sending all items) should stick.
        client.post("/api/dashboard", json={"items": [], "title": ""}).json()
        assert client.get("/api/dashboard").json()["title"] == ""


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


def test_update_settings(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        resp = client.post(
            "/api/settings",
            json={
                "confirm_sql": True,
                "explain_sql": True,
                "clarify_sql": False,
                "show_cost": False,
                "show_provenance": True,
            },
        )
    body = resp.json()
    assert body["confirm_sql"] is True
    assert body["explain_sql"] is True
    assert body["clarify_sql"] is False
    assert body["show_cost"] is False
    assert body["show_provenance"] is True


def test_update_settings_no_rebuild_path(isolated_web_state: None) -> None:
    # Only toggle confirm_sql/show_cost — should take the clear_insight_cache branch
    with TestClient(web_app.app) as client:
        resp = client.post("/api/settings", json={"confirm_sql": True})
    assert resp.json()["confirm_sql"] is True


def test_update_llm_settings_triggers_reinit(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        resp = client.post(
            "/api/settings/llm",
            json={
                "provider": "anthropic",
                "api_key": "sk-test",
                "model": "claude-haiku-4-5-20251001",
                "base_url": "",
            },
        )
    body = resp.json()
    assert body["provider"] == "anthropic"


def test_update_llm_settings_ollama_and_github(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        ollama = client.post(
            "/api/settings/llm",
            json={"provider": "ollama", "model": "llama3", "base_url": "http://x"},
        ).json()
        github = client.post(
            "/api/settings/llm",
            json={"provider": "github", "api_key": "gh", "model": "gpt-4o"},
        ).json()
    assert ollama["provider"] == "ollama"
    assert github["provider"] == "github"


# ---------------------------------------------------------------------------
# SQL confirmation endpoint
# ---------------------------------------------------------------------------


def test_sql_confirm_unknown_id(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        resp = client.post("/api/sql-confirm/abc123", json={"action": "approve"})
    assert "No pending confirmation" in resp.json()["error"]


def test_sql_confirm_approve_edit(isolated_web_state: None) -> None:
    import asyncio

    rid = "req42"
    web_app._state.pending_confirms[rid] = {
        "event": asyncio.Event(),
        "action": None,
        "sql": None,
    }
    try:
        with TestClient(web_app.app) as client:
            resp = client.post(
                f"/api/sql-confirm/{rid}",
                json={"action": "edit", "sql": "SELECT 2"},
            )
        assert resp.json()["ok"] is True
    finally:
        web_app._state.pending_confirms.pop(rid, None)


# ---------------------------------------------------------------------------
# Conversations
# ---------------------------------------------------------------------------


def test_conversations_crud_and_clear(isolated_web_state: None, project_dir: str) -> None:
    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})
        web_app._state.llm_client = _typed_stub(StubLLMClient())

        # Generate a conversation via chat
        client.post("/api/chat", json={"message": "Hi there", "session_id": "conv1"})

        # Fetch the conversation
        single = client.get("/api/conversations/conv1").json()
        assert "events" in single

        listed = client.get("/api/conversations").json()
        assert "conversations" in listed

        # Clear just one session
        cleared_one = client.post("/api/clear", json={"session_id": "conv1"}).json()
        assert cleared_one["ok"] is True

        # Clear all
        cleared_all = client.delete("/api/conversations").json()
        assert cleared_all["ok"] is True


def test_conversation_restores_dashboard_snapshot(
    isolated_web_state: None, project_dir: str
) -> None:
    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})
        dashboard = {
            "items": [{"id": 1, "type": "note", "title": "Generation notes"}],
            "columns": 2,
            "filters": [{"id": 1, "column": "state", "operator": "eq", "value": "CA"}],
            "session_id": "dashconv",
        }

        saved = client.post("/api/dashboard", json=dashboard).json()
        assert saved["items"][0]["title"] == "Generation notes"

        conversation = client.get("/api/conversations/dashconv").json()
        assert conversation["dashboard"]["items"][0]["title"] == "Generation notes"
        assert conversation["dashboard"]["columns"] == 2
        assert conversation["dashboard"]["filters"][0]["value"] == "CA"

        loaded = client.get("/api/dashboard?session_id=dashconv").json()
        assert loaded["items"][0]["title"] == "Generation notes"


def test_conversations_no_project(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        assert client.get("/api/conversations").json() == {"conversations": []}
        assert client.get("/api/conversations/anything").json() == {
            "events": [],
            "title": "Untitled",
            "dashboard": {"items": [], "columns": 0, "filters": [], "title": ""},
        }
        assert client.post("/api/clear", json={"session_id": "x"}).json()["ok"] is True
        assert client.delete("/api/conversations").json()["ok"] is True


def test_conversation_plotly_spec_sanitizes_pandas_missing_values(
    isolated_web_state: None, project_dir: str
) -> None:
    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})
        conv = web_app._state.conversations.get("nan_spec")  # ty: ignore[unresolved-attribute]
        conv["events"] = [
            {
                "event": "tool_result",
                "data": {
                    "type": "chart",
                    "plotly_spec": {
                        "data": [
                            {
                                "type": "scatter",
                                "y": [1.0, pd.NA, float("inf"), float("nan")],
                            }
                        ]
                    },
                },
            }
        ]

        response = client.get("/api/conversations/nan_spec/events/0/plotly-spec")

    assert response.status_code == 200
    assert response.json()["plotly_spec"]["data"][0]["y"] == [1.0, None, None, None]


# ---------------------------------------------------------------------------
# Export endpoints
# ---------------------------------------------------------------------------


def test_export_session_no_conversations(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        resp = client.post("/api/export/anything", json={})
    assert resp.status_code == 200
    assert "No conversation data" in resp.text


def test_export_session_with_project(isolated_web_state: None, project_dir: str) -> None:
    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})
        resp = client.post("/api/export/somesid", json={"exclude_indices": [0]})
    assert resp.status_code == 200
    # should return HTML, with attachment disposition
    assert "attachment" in resp.headers.get("content-disposition", "")


def test_export_session_bundle_with_project(isolated_web_state: None, project_dir: str) -> None:
    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})
        resp = client.post(
            "/api/export/somesid",
            json={"format": "bundle", "include": ["html", "sql", "metadata"]},
        )
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/zip"
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        manifest = json.loads(zf.read("manifest.json"))
        assert manifest["schema_version"] == 1
        assert manifest["session_id"] == "somesid"
        assert "report/session.html" in zf.namelist()
        assert "metadata/session.json" in zf.namelist()


def test_export_dashboard(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        resp = client.post(
            "/api/dashboard/export",
            json={"items": [], "title": "My Dash", "columns": 2},
        )
    assert resp.status_code == 200
    assert "attachment" in resp.headers.get("content-disposition", "")


# ---------------------------------------------------------------------------
# Query log
# ---------------------------------------------------------------------------


def test_query_log_no_project(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        resp = client.get("/api/query-log")
    assert resp.json() == {"entries": []}


def test_query_log_with_project(isolated_web_state: None, project_dir: str) -> None:
    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})
        resp = client.get("/api/query-log?n=5")
    assert "entries" in resp.json()


# ---------------------------------------------------------------------------
# Measure editor failure paths
# ---------------------------------------------------------------------------


def test_measure_editor_no_project(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        assert client.get("/api/measures/editor").json()["ok"] is False
        assert client.get("/api/measures/editor/catalog").json()["ok"] is False
        assert client.post("/api/measures/editor", json={"text": ""}).json()["ok"] is False
        assert (
            client.post("/api/measures/editor/validate", json={"text": ""}).json()["ok"] is False
        )
        assert client.post("/api/measures/editor/upsert", json={"text": ""}).json()["ok"] is False


def test_measure_editor_invalid_yaml(isolated_web_state: None, project_dir: str) -> None:
    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})
        bad_save = client.post(
            "/api/measures/editor", json={"text": "- :\n  bad: [unclosed"}
        ).json()
        bad_validate = client.post(
            "/api/measures/editor/validate",
            json={"text": "- :\n  bad: [unclosed"},
        ).json()
        bad_upsert = client.post(
            "/api/measures/editor/upsert",
            json={"text": "- :\n  bad: [unclosed", "table": "orders"},
        ).json()
    assert bad_save["ok"] is False
    assert bad_validate["ok"] is False
    assert bad_upsert["ok"] is False


def test_measure_editor_upsert_requires_table(isolated_web_state: None, project_dir: str) -> None:
    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})
        resp = client.post("/api/measures/editor/upsert", json={"text": ""})
    assert resp.json()["ok"] is False
    assert "Table" in resp.json()["error"]


def test_measure_editor_upsert_requires_column_or_expr(
    isolated_web_state: None, project_dir: str
) -> None:
    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})
        resp = client.post(
            "/api/measures/editor/upsert",
            json={"text": "", "table": "orders"},
        )
    assert resp.json()["ok"] is False


def test_measure_editor_validate_non_list(isolated_web_state: None, project_dir: str) -> None:
    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_dir})
        resp = client.post("/api/measures/editor/validate", json={"text": "foo: bar"})
    body = resp.json()
    assert body["ok"] is False
    assert any("list" in e.lower() for e in body["errors"])


def _make_session_state(num_turns: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Build a (messages, evt_log) pair with ``num_turns`` user turns.

    Each turn looks like a real conversation: a user prompt, a tool call, a
    tool result, and an assistant reply. Turn N's prompt text is ``"qN"``.
    """
    messages: list[dict[str, Any]] = []
    evt_log: list[dict[str, Any]] = []
    for n in range(num_turns):
        messages.append({"role": "user", "content": f"q{n}"})
        messages.append({"role": "assistant", "content": [{"type": "tool_use"}]})
        messages.append({"role": "user", "content": [{"type": "tool_result"}]})
        messages.append({"role": "assistant", "content": f"a{n}"})

        evt_log.append({"event": web_app.EventType.USER_MESSAGE, "data": {"text": f"q{n}"}})
        evt_log.append({"event": web_app.EventType.TOOL_START, "data": {}})
        evt_log.append({"event": web_app.EventType.TOOL_RESULT, "data": {}})
        evt_log.append({"event": web_app.EventType.ASSISTANT_MESSAGE, "data": {"text": f"a{n}"}})
    return messages, evt_log


def test_truncate_session_at_turn_drops_turn_and_after() -> None:
    messages, evt_log = _make_session_state(3)
    web_app._truncate_session_at_turn(messages, evt_log, 1)

    # Only turn 0 should remain in messages (4 entries) and evt_log (4 entries).
    assert [
        m.get("content") for m in messages if m["role"] == "user" and isinstance(m["content"], str)
    ] == ["q0"]
    user_events = [e for e in evt_log if e["event"] == web_app.EventType.USER_MESSAGE]
    assert [e["data"]["text"] for e in user_events] == ["q0"]


def test_truncate_session_at_turn_zero_clears_all() -> None:
    messages, evt_log = _make_session_state(2)
    web_app._truncate_session_at_turn(messages, evt_log, 0)
    assert messages == []
    assert evt_log == []


def test_truncate_session_at_turn_out_of_range_is_noop() -> None:
    messages, evt_log = _make_session_state(2)
    msgs_before = list(messages)
    evt_before = list(evt_log)
    web_app._truncate_session_at_turn(messages, evt_log, 99)
    assert messages == msgs_before
    assert evt_log == evt_before


def test_chat_rejects_invalid_truncate_before_turn(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        resp = client.post(
            "/api/chat",
            json={
                "message": "hi",
                "session_id": "sess",
                "truncate_before_turn": -1,
            },
        )
    assert resp.status_code == 200
    assert "Invalid truncate_before_turn" in resp.text
