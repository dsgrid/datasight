"""Smoke tests for the rendered web UI shell and boot APIs."""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import datasight.web.app as web_app

from tests._env_helpers import scrub_datasight_env


@pytest.fixture()
def isolated_web_state(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Isolate mutable app state and environment between UI smoke tests."""

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
    web_app._state.confirm_sql = original_flags["confirm_sql"]
    web_app._state.explain_sql = original_flags["explain_sql"]
    web_app._state.clarify_sql = original_flags["clarify_sql"]
    web_app._state.show_cost = original_flags["show_cost"]
    web_app._state.show_provenance = original_flags["show_provenance"]

    os.environ.clear()
    os.environ.update(original_env)


def test_index_renders_svelte_app(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        response = client.get("/")

    assert response.status_code == 200
    html = response.text

    # Vite-built Svelte app mounts into #app and loads compiled assets
    assert '<div id="app">' in html
    assert "/static/assets/" in html


def test_ui_boot_contract_when_unloaded(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        project_response = client.get("/api/project")
        schema_response = client.get("/api/schema")
        queries_response = client.get("/api/queries")
        recipes_response = client.get("/api/recipes")
        bookmarks_response = client.get("/api/bookmarks")
        reports_response = client.get("/api/reports")
        conversations_response = client.get("/api/conversations")
        dashboard_response = client.get("/api/dashboard")
        settings_response = client.get("/api/settings")
        llm_settings_response = client.get("/api/settings/llm")

    assert project_response.json() == {
        "loaded": False,
        "path": None,
        "name": None,
        "is_ephemeral": False,
    }
    assert schema_response.json() == {"tables": []}
    assert queries_response.json() == {"queries": []}
    assert recipes_response.json() == {"recipes": [], "error": "No dataset loaded"}
    assert bookmarks_response.json() == {"bookmarks": []}
    assert reports_response.json() == {"reports": []}
    assert conversations_response.json() == {"conversations": []}
    assert dashboard_response.json() == {"items": [], "columns": 0, "filters": []}

    settings = settings_response.json()
    assert settings == {
        "confirm_sql": False,
        "explain_sql": False,
        "clarify_sql": True,
        "show_cost": True,
        "show_provenance": False,
    }

    llm_settings = llm_settings_response.json()
    assert llm_settings["provider"] in {"anthropic", "ollama", "github"}
    assert "connected" in llm_settings
    assert "model" in llm_settings


def test_ui_boot_contract_when_project_loaded(isolated_web_state: None, project_dir: str) -> None:
    project_path = str(Path(project_dir).resolve())

    with TestClient(web_app.app) as client:
        load_response = client.post("/api/projects/load", json={"path": project_path})
        assert load_response.status_code == 200
        assert load_response.json()["success"] is True

        project_response = client.get("/api/project")
        schema_response = client.get("/api/schema")
        queries_response = client.get("/api/queries")
        recipes_response = client.get("/api/recipes")
        measures_catalog_response = client.get("/api/measures/editor/catalog")
        measures_editor_response = client.get("/api/measures/editor")
        project_health_response = client.get("/api/project-health")
        bookmarks_response = client.get("/api/bookmarks")
        reports_response = client.get("/api/reports")
        conversations_response = client.get("/api/conversations")
        dashboard_response = client.get("/api/dashboard")

    project_payload = project_response.json()
    assert project_payload == {
        "loaded": True,
        "path": project_path,
        "name": Path(project_path).name,
        "is_ephemeral": False,
        "has_time_series": False,
    }

    schema = schema_response.json()["tables"]
    assert {table["name"] for table in schema} >= {"products", "orders"}

    queries = queries_response.json()["queries"]
    assert len(queries) == 2
    assert queries[0]["question"] == "How many orders are there?"

    recipes = recipes_response.json()
    assert isinstance(recipes["recipes"], list)
    assert len(recipes["recipes"]) >= 1
    assert "title" in recipes["recipes"][0]
    assert "prompt" in recipes["recipes"][0]

    measures_catalog = measures_catalog_response.json()
    assert measures_catalog["ok"] is True
    assert isinstance(measures_catalog["measures"], list)

    measures_editor = measures_editor_response.json()
    assert measures_editor["ok"] is True
    assert measures_editor["path"].endswith("measures.yaml")
    assert isinstance(measures_editor["text"], str)

    project_health = project_health_response.json()
    assert "summary" in project_health
    assert "checks" in project_health
    assert isinstance(project_health["checks"], list)

    assert bookmarks_response.json() == {"bookmarks": []}
    assert reports_response.json() == {"reports": []}
    assert conversations_response.json() == {"conversations": []}
    assert dashboard_response.json() == {"items": [], "columns": 0, "filters": []}


def test_dashboard_run_card_applies_result_filter(
    isolated_web_state: None, project_dir: str
) -> None:
    project_path = str(Path(project_dir).resolve())

    with TestClient(web_app.app) as client:
        load_response = client.post("/api/projects/load", json={"path": project_path})
        assert load_response.status_code == 200

        response = client.post(
            "/api/dashboard/run-card",
            json={
                "sql": "SELECT customer_state, SUM(quantity) AS total_qty FROM orders GROUP BY customer_state",
                "tool": "run_sql",
                "filters": [{"column": "customer_state", "operator": "eq", "value": "CA"}],
            },
        )

    payload = response.json()
    assert payload["ok"] is True
    assert "CA" in payload["html"]
    assert "NY" not in payload["html"]
    assert 'WHERE "customer_state" = ' in payload["sql"]


def test_dashboard_run_card_rejects_unsafe_filter_column(
    isolated_web_state: None, project_dir: str
) -> None:
    project_path = str(Path(project_dir).resolve())

    with TestClient(web_app.app) as client:
        load_response = client.post("/api/projects/load", json={"path": project_path})
        assert load_response.status_code == 200

        response = client.post(
            "/api/dashboard/run-card",
            json={
                "sql": "SELECT customer_state, SUM(quantity) AS total_qty FROM orders GROUP BY customer_state",
                "tool": "run_sql",
                "filters": [{"column": "customer_state; DROP TABLE orders", "value": "CA"}],
            },
        )

    payload = response.json()
    assert payload["ok"] is False
    assert "Invalid dashboard filter column" in payload["error"]


def test_dashboard_filter_values_returns_distinct_card_values(
    isolated_web_state: None, project_dir: str
) -> None:
    project_path = str(Path(project_dir).resolve())

    with TestClient(web_app.app) as client:
        load_response = client.post("/api/projects/load", json={"path": project_path})
        assert load_response.status_code == 200

        response = client.post(
            "/api/dashboard/filter-values",
            json={
                "column": "customer_state",
                "allowed_columns": ["customer_state", "total_qty"],
                "items": [
                    {
                        "type": "table",
                        "sql": (
                            "SELECT customer_state, SUM(quantity) AS total_qty "
                            "FROM orders GROUP BY customer_state"
                        ),
                    }
                ],
                "limit": 100,
            },
        )

    payload = response.json()
    assert payload["ok"] is True
    assert "CA" in payload["values"]
    assert "NY" in payload["values"]
    assert len(payload["values"]) <= 100


def test_github_provider_uses_shorter_chat_history(isolated_web_state: None) -> None:
    state = web_app.AppState()
    state.llm_provider = "github"

    messages = [
        {"role": "user", "content": f"question {idx}"}
        if idx % 2 == 0
        else {"role": "assistant", "content": "answer"}
        for idx in range(12)
    ]

    assert state.trim_messages_for_provider(messages) == messages[4:]


def test_timeseries_overview_when_unloaded(isolated_web_state: None) -> None:
    """Timeseries overview should return an error when no project is loaded."""
    with TestClient(web_app.app) as client:
        response = client.get("/api/timeseries-overview")

    assert response.status_code == 200
    assert response.json() == {"error": "No dataset loaded"}


def test_timeseries_overview_no_config(isolated_web_state: None, project_dir: str) -> None:
    """Timeseries overview should report no config when project has no time_series.yaml."""
    project_path = str(Path(project_dir).resolve())

    with TestClient(web_app.app) as client:
        load_response = client.post("/api/projects/load", json={"path": project_path})
        assert load_response.status_code == 200

        ts_response = client.get("/api/timeseries-overview")

    payload = ts_response.json()
    assert "overview" in payload
    assert payload["overview"]["configs"] == []
    assert payload["overview"]["summaries"] == []
    assert "No time_series.yaml" in payload["overview"]["notes"][0]


def test_timeseries_overview_with_config(isolated_web_state: None, project_dir: str) -> None:
    """Timeseries overview should return summaries when time_series.yaml exists."""
    project_path = str(Path(project_dir).resolve())
    ts_yaml = "- table: orders\n  timestamp_column: order_date\n  frequency: P1D\n"
    Path(project_path, "time_series.yaml").write_text(ts_yaml, encoding="utf-8")

    with TestClient(web_app.app) as client:
        load_response = client.post("/api/projects/load", json={"path": project_path})
        assert load_response.status_code == 200

        project_response = client.get("/api/project")
        ts_response = client.get("/api/timeseries-overview")

    # Project should report has_time_series=True
    assert project_response.json()["has_time_series"] is True

    payload = ts_response.json()
    assert "overview" in payload
    assert len(payload["overview"]["configs"]) == 1
    assert len(payload["overview"]["time_series_summaries"]) >= 1
    assert payload["overview"]["time_series_summaries"][0]["table"] == "orders"


def test_timeseries_overview_table_filter(isolated_web_state: None, project_dir: str) -> None:
    """Timeseries overview should filter by table when ?table= is provided."""
    project_path = str(Path(project_dir).resolve())
    ts_yaml = (
        "- table: orders\n  timestamp_column: order_date\n  frequency: P1D\n"
        "- table: products\n  timestamp_column: id\n  frequency: P1D\n"
    )
    Path(project_path, "time_series.yaml").write_text(ts_yaml, encoding="utf-8")

    with TestClient(web_app.app) as client:
        client.post("/api/projects/load", json={"path": project_path})

        all_response = client.get("/api/timeseries-overview")
        filtered_response = client.get("/api/timeseries-overview?table=orders")

    all_configs = all_response.json()["overview"]["configs"]
    filtered_configs = filtered_response.json()["overview"]["configs"]
    assert len(all_configs) == 2
    assert len(filtered_configs) == 1
    assert filtered_configs[0]["table"] == "orders"
