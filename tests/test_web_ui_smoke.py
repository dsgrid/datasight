"""Smoke tests for the rendered web UI shell and boot APIs."""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import datasight.web.app as web_app


@pytest.fixture()
def isolated_web_state(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Isolate mutable app state and environment between UI smoke tests."""

    original_env = os.environ.copy()
    original_flags = {
        "confirm_sql": web_app._state.confirm_sql,
        "explain_sql": web_app._state.explain_sql,
        "clarify_sql": web_app._state.clarify_sql,
        "show_cost": web_app._state.show_cost,
    }

    web_app._state.clear_project()
    monkeypatch.setattr(web_app, "add_recent_project", lambda project_path: None)
    monkeypatch.setattr(web_app, "load_recent_projects", lambda: [])

    yield

    web_app._state.clear_project()
    web_app._state.confirm_sql = original_flags["confirm_sql"]
    web_app._state.explain_sql = original_flags["explain_sql"]
    web_app._state.clarify_sql = original_flags["clarify_sql"]
    web_app._state.show_cost = original_flags["show_cost"]

    os.environ.clear()
    os.environ.update(original_env)


def test_index_renders_split_ui_shell(isolated_web_state: None) -> None:
    with TestClient(web_app.app) as client:
        response = client.get("/")

    assert response.status_code == 200
    html = response.text

    assert '<div class="landing-page" id="landing-page">' in html
    assert '<div class="main-layout" id="main-layout"' in html
    assert 'id="command-palette"' in html
    assert 'id="settings-panel"' in html
    assert "/static/app_state.js?v=1" in html
    assert "/static/app_events.js?v=1" in html
    assert "/static/app_bootstrap.js?v=1" in html


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
    assert dashboard_response.json() == {"items": [], "columns": 0}

    settings = settings_response.json()
    assert settings == {
        "confirm_sql": False,
        "explain_sql": False,
        "clarify_sql": True,
        "show_cost": True,
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
    assert dashboard_response.json() == {"items": [], "columns": 0}
