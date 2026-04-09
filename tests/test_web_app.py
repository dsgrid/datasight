"""Tests for the FastAPI web app."""

from typing import Any, cast

import pandas as pd
from fastapi.testclient import TestClient

import datasight.web.app as web_app


def _typed_stub(value: object) -> Any:
    return cast(Any, value)


def test_chat_reports_configuration_errors(monkeypatch):
    """Configuration failures should still reach the UI as SSE events."""

    async def boom(*args, **kwargs):  # noqa: ARG001
        if False:
            yield ""
        raise RuntimeError("boom")

    monkeypatch.setattr(web_app, "generate_chat_response", boom)

    with TestClient(web_app.app) as client:
        response = client.post(
            "/api/chat",
            json={"message": "How many rows?", "session_id": "test-session"},
        )

    assert response.status_code == 200
    assert "event: error" in response.text
    assert "boom" in response.text
    assert "event: done" in response.text


def test_dataset_overview_returns_profile(monkeypatch):
    """Dataset overview should return deterministic profile data when loaded."""

    class StubRunner:
        async def run_sql(self, sql):  # noqa: ARG002
            return pd.DataFrame()

    original_project_loaded = web_app._state.project_loaded
    original_schema_info = web_app._state.schema_info
    original_sql_runner = web_app._state.sql_runner
    original_insight_cache = dict(web_app._state._insight_cache)

    monkeypatch.setattr(
        web_app,
        "build_dataset_overview",
        lambda schema_info, run_sql: _fake_overview(schema_info, run_sql),  # noqa: ARG005
    )

    web_app._state.project_loaded = True
    web_app._state.schema_info = [{"name": "products", "row_count": 5, "columns": []}]
    web_app._state.sql_runner = _typed_stub(StubRunner())

    try:
        with TestClient(web_app.app) as client:
            response = client.get("/api/dataset-overview")
    finally:
        web_app._state.project_loaded = original_project_loaded
        web_app._state.schema_info = original_schema_info
        web_app._state.sql_runner = original_sql_runner
        web_app._state._insight_cache = original_insight_cache

    assert response.status_code == 200
    body = response.json()
    assert body["overview"]["table_count"] == 1
    assert body["overview"]["largest_tables"][0]["name"] == "products"
    assert body["cached"] is False


def test_dataset_overview_supports_table_scope(monkeypatch):
    class StubRunner:
        async def run_sql(self, sql):  # noqa: ARG002
            return pd.DataFrame()

    captured = {}
    original_project_loaded = web_app._state.project_loaded
    original_schema_info = web_app._state.schema_info
    original_sql_runner = web_app._state.sql_runner
    original_insight_cache = dict(web_app._state._insight_cache)

    async def fake_overview(schema_info, run_sql):  # noqa: ARG001
        captured["tables"] = [table["name"] for table in schema_info]
        return await _fake_overview(schema_info, run_sql)

    monkeypatch.setattr(web_app, "build_dataset_overview", fake_overview)
    web_app._state.project_loaded = True
    web_app._state.schema_info = [
        {"name": "orders", "row_count": 5, "columns": []},
        {"name": "products", "row_count": 3, "columns": []},
    ]
    web_app._state.sql_runner = _typed_stub(StubRunner())
    web_app._state.clear_insight_cache()

    try:
        with TestClient(web_app.app) as client:
            response = client.get("/api/dataset-overview?table=orders")
    finally:
        web_app._state.project_loaded = original_project_loaded
        web_app._state.schema_info = original_schema_info
        web_app._state.sql_runner = original_sql_runner
        web_app._state._insight_cache = original_insight_cache

    assert response.status_code == 200
    assert response.json()["overview"]["table_count"] == 1
    assert captured["tables"] == ["orders"]


def test_dataset_overview_requires_loaded_data():
    """Dataset overview should fail cleanly when no dataset is loaded."""
    original_project_loaded = web_app._state.project_loaded
    original_schema_info = web_app._state.schema_info
    original_sql_runner = web_app._state.sql_runner

    web_app._state.project_loaded = False
    web_app._state.schema_info = []
    web_app._state.sql_runner = None

    try:
        with TestClient(web_app.app) as client:
            response = client.get("/api/dataset-overview")
    finally:
        web_app._state.project_loaded = original_project_loaded
        web_app._state.schema_info = original_schema_info
        web_app._state.sql_runner = original_sql_runner

    assert response.status_code == 200
    assert response.json()["error"] == "No dataset loaded"


def test_dimension_overview_returns_profile(monkeypatch):
    """Dimension overview should return deterministic grouping data when loaded."""

    class StubRunner:
        async def run_sql(self, sql):  # noqa: ARG002
            return pd.DataFrame()

    original_project_loaded = web_app._state.project_loaded
    original_schema_info = web_app._state.schema_info
    original_sql_runner = web_app._state.sql_runner
    original_insight_cache = dict(web_app._state._insight_cache)

    monkeypatch.setattr(
        web_app,
        "build_dimension_overview",
        lambda schema_info, run_sql: _fake_dimension_overview(schema_info, run_sql),  # noqa: ARG005
    )

    web_app._state.project_loaded = True
    web_app._state.schema_info = [{"name": "products", "row_count": 5, "columns": []}]
    web_app._state.sql_runner = _typed_stub(StubRunner())

    try:
        with TestClient(web_app.app) as client:
            response = client.get("/api/dimension-overview")
    finally:
        web_app._state.project_loaded = original_project_loaded
        web_app._state.schema_info = original_schema_info
        web_app._state.sql_runner = original_sql_runner
        web_app._state._insight_cache = original_insight_cache

    assert response.status_code == 200
    body = response.json()
    assert body["overview"]["dimension_columns"][0]["column"] == "category"


def test_dimension_overview_supports_table_scope(monkeypatch):
    class StubRunner:
        async def run_sql(self, sql):  # noqa: ARG002
            return pd.DataFrame()

    captured = {}
    original_project_loaded = web_app._state.project_loaded
    original_schema_info = web_app._state.schema_info
    original_sql_runner = web_app._state.sql_runner
    original_insight_cache = dict(web_app._state._insight_cache)

    async def fake_overview(schema_info, run_sql):  # noqa: ARG001
        captured["tables"] = [table["name"] for table in schema_info]
        return await _fake_dimension_overview(schema_info, run_sql)

    monkeypatch.setattr(web_app, "build_dimension_overview", fake_overview)
    web_app._state.project_loaded = True
    web_app._state.schema_info = [
        {"name": "orders", "row_count": 5, "columns": []},
        {"name": "products", "row_count": 3, "columns": []},
    ]
    web_app._state.sql_runner = _typed_stub(StubRunner())
    web_app._state.clear_insight_cache()

    try:
        with TestClient(web_app.app) as client:
            response = client.get("/api/dimension-overview?table=orders")
    finally:
        web_app._state.project_loaded = original_project_loaded
        web_app._state.schema_info = original_schema_info
        web_app._state.sql_runner = original_sql_runner
        web_app._state._insight_cache = original_insight_cache

    assert response.status_code == 200
    assert captured["tables"] == ["orders"]


def test_dimension_overview_requires_loaded_data():
    """Dimension overview should fail cleanly when no dataset is loaded."""
    original_project_loaded = web_app._state.project_loaded
    original_schema_info = web_app._state.schema_info
    original_sql_runner = web_app._state.sql_runner

    web_app._state.project_loaded = False
    web_app._state.schema_info = []
    web_app._state.sql_runner = None

    try:
        with TestClient(web_app.app) as client:
            response = client.get("/api/dimension-overview")
    finally:
        web_app._state.project_loaded = original_project_loaded
        web_app._state.schema_info = original_schema_info
        web_app._state.sql_runner = original_sql_runner

    assert response.status_code == 200
    assert response.json()["error"] == "No dataset loaded"


def test_quality_overview_returns_profile(monkeypatch):
    """Quality overview should return deterministic quality data when loaded."""

    class StubRunner:
        async def run_sql(self, sql):  # noqa: ARG002
            return pd.DataFrame()

    original_project_loaded = web_app._state.project_loaded
    original_schema_info = web_app._state.schema_info
    original_sql_runner = web_app._state.sql_runner
    original_insight_cache = dict(web_app._state._insight_cache)

    monkeypatch.setattr(
        web_app,
        "build_quality_overview",
        lambda schema_info, run_sql: _fake_quality_overview(schema_info, run_sql),  # noqa: ARG005
    )

    web_app._state.project_loaded = True
    web_app._state.schema_info = [{"name": "orders", "row_count": 10, "columns": []}]
    web_app._state.sql_runner = _typed_stub(StubRunner())

    try:
        with TestClient(web_app.app) as client:
            response = client.get("/api/quality-overview")
    finally:
        web_app._state.project_loaded = original_project_loaded
        web_app._state.schema_info = original_schema_info
        web_app._state.sql_runner = original_sql_runner
        web_app._state._insight_cache = original_insight_cache

    assert response.status_code == 200
    body = response.json()
    assert body["overview"]["null_columns"][0]["column"] == "customer_state"


def test_quality_overview_supports_table_scope(monkeypatch):
    class StubRunner:
        async def run_sql(self, sql):  # noqa: ARG002
            return pd.DataFrame()

    captured = {}
    original_project_loaded = web_app._state.project_loaded
    original_schema_info = web_app._state.schema_info
    original_sql_runner = web_app._state.sql_runner
    original_insight_cache = dict(web_app._state._insight_cache)

    async def fake_overview(schema_info, run_sql):  # noqa: ARG001
        captured["tables"] = [table["name"] for table in schema_info]
        return await _fake_quality_overview(schema_info, run_sql)

    monkeypatch.setattr(web_app, "build_quality_overview", fake_overview)
    web_app._state.project_loaded = True
    web_app._state.schema_info = [
        {"name": "orders", "row_count": 5, "columns": []},
        {"name": "products", "row_count": 3, "columns": []},
    ]
    web_app._state.sql_runner = _typed_stub(StubRunner())
    web_app._state.clear_insight_cache()

    try:
        with TestClient(web_app.app) as client:
            response = client.get("/api/quality-overview?table=orders")
    finally:
        web_app._state.project_loaded = original_project_loaded
        web_app._state.schema_info = original_schema_info
        web_app._state.sql_runner = original_sql_runner
        web_app._state._insight_cache = original_insight_cache

    assert response.status_code == 200
    assert captured["tables"] == ["orders"]


def test_quality_overview_requires_loaded_data():
    """Quality overview should fail cleanly when no dataset is loaded."""
    original_project_loaded = web_app._state.project_loaded
    original_schema_info = web_app._state.schema_info
    original_sql_runner = web_app._state.sql_runner

    web_app._state.project_loaded = False
    web_app._state.schema_info = []
    web_app._state.sql_runner = None

    try:
        with TestClient(web_app.app) as client:
            response = client.get("/api/quality-overview")
    finally:
        web_app._state.project_loaded = original_project_loaded
        web_app._state.schema_info = original_schema_info
        web_app._state.sql_runner = original_sql_runner

    assert response.status_code == 200
    assert response.json()["error"] == "No dataset loaded"


def test_trend_overview_returns_profile(monkeypatch):
    """Trend overview should return deterministic chart ideas when loaded."""

    class StubRunner:
        async def run_sql(self, sql):  # noqa: ARG002
            return pd.DataFrame()

    original_project_loaded = web_app._state.project_loaded
    original_schema_info = web_app._state.schema_info
    original_sql_runner = web_app._state.sql_runner
    original_insight_cache = dict(web_app._state._insight_cache)

    monkeypatch.setattr(
        web_app,
        "build_trend_overview",
        lambda schema_info, run_sql: _fake_trend_overview(schema_info, run_sql),  # noqa: ARG005
    )

    web_app._state.project_loaded = True
    web_app._state.schema_info = [{"name": "orders", "row_count": 10, "columns": []}]
    web_app._state.sql_runner = _typed_stub(StubRunner())

    try:
        with TestClient(web_app.app) as client:
            response = client.get("/api/trend-overview")
    finally:
        web_app._state.project_loaded = original_project_loaded
        web_app._state.schema_info = original_schema_info
        web_app._state.sql_runner = original_sql_runner
        web_app._state._insight_cache = original_insight_cache

    assert response.status_code == 200
    body = response.json()
    assert body["overview"]["trend_candidates"][0]["date_column"] == "order_date"


def test_trend_overview_supports_table_scope(monkeypatch):
    class StubRunner:
        async def run_sql(self, sql):  # noqa: ARG002
            return pd.DataFrame()

    captured = {}
    original_project_loaded = web_app._state.project_loaded
    original_schema_info = web_app._state.schema_info
    original_sql_runner = web_app._state.sql_runner
    original_insight_cache = dict(web_app._state._insight_cache)

    async def fake_overview(schema_info, run_sql):  # noqa: ARG001
        captured["tables"] = [table["name"] for table in schema_info]
        return await _fake_trend_overview(schema_info, run_sql)

    monkeypatch.setattr(web_app, "build_trend_overview", fake_overview)
    web_app._state.project_loaded = True
    web_app._state.schema_info = [
        {"name": "orders", "row_count": 5, "columns": []},
        {"name": "products", "row_count": 3, "columns": []},
    ]
    web_app._state.sql_runner = _typed_stub(StubRunner())
    web_app._state.clear_insight_cache()

    try:
        with TestClient(web_app.app) as client:
            response = client.get("/api/trend-overview?table=orders")
    finally:
        web_app._state.project_loaded = original_project_loaded
        web_app._state.schema_info = original_schema_info
        web_app._state.sql_runner = original_sql_runner
        web_app._state._insight_cache = original_insight_cache

    assert response.status_code == 200
    assert captured["tables"] == ["orders"]


def test_trend_overview_requires_loaded_data():
    """Trend overview should fail cleanly when no dataset is loaded."""
    original_project_loaded = web_app._state.project_loaded
    original_schema_info = web_app._state.schema_info
    original_sql_runner = web_app._state.sql_runner

    web_app._state.project_loaded = False
    web_app._state.schema_info = []
    web_app._state.sql_runner = None

    try:
        with TestClient(web_app.app) as client:
            response = client.get("/api/trend-overview")
    finally:
        web_app._state.project_loaded = original_project_loaded
        web_app._state.schema_info = original_schema_info
        web_app._state.sql_runner = original_sql_runner

    assert response.status_code == 200
    assert response.json()["error"] == "No dataset loaded"


def test_table_scoped_overviews_fail_for_missing_table():
    original_project_loaded = web_app._state.project_loaded
    original_schema_info = web_app._state.schema_info
    original_sql_runner = web_app._state.sql_runner

    class StubRunner:
        async def run_sql(self, sql):  # noqa: ARG002
            return pd.DataFrame()

    web_app._state.project_loaded = True
    web_app._state.schema_info = [{"name": "orders", "row_count": 5, "columns": []}]
    web_app._state.sql_runner = _typed_stub(StubRunner())

    try:
        with TestClient(web_app.app) as client:
            response = client.get("/api/dataset-overview?table=missing_table")
    finally:
        web_app._state.project_loaded = original_project_loaded
        web_app._state.schema_info = original_schema_info
        web_app._state.sql_runner = original_sql_runner

    assert response.status_code == 200
    assert response.json()["error"] == "Table not found: missing_table"


def test_recipes_returns_prompt_recipes(monkeypatch):
    """Recipe endpoint should return reusable prompts when data is loaded."""

    class StubRunner:
        async def run_sql(self, sql):  # noqa: ARG002
            return pd.DataFrame()

    original_project_loaded = web_app._state.project_loaded
    original_schema_info = web_app._state.schema_info
    original_sql_runner = web_app._state.sql_runner
    original_insight_cache = dict(web_app._state._insight_cache)

    async def fake_recipes(schema_info, run_sql):  # noqa: ARG001
        return [
            {
                "title": "Break down by customer_state",
                "category": "Dimensions",
                "reason": "Suggested because customer_state looks like a strong grouping column.",
                "prompt": "Analyze customer_state as a breakdown.",
            }
        ]

    monkeypatch.setattr(web_app, "build_prompt_recipes", fake_recipes)

    web_app._state.project_loaded = True
    web_app._state.schema_info = [{"name": "orders", "row_count": 10, "columns": []}]
    web_app._state.sql_runner = _typed_stub(StubRunner())
    web_app._state.clear_insight_cache()

    try:
        with TestClient(web_app.app) as client:
            response = client.get("/api/recipes")
    finally:
        web_app._state.project_loaded = original_project_loaded
        web_app._state.schema_info = original_schema_info
        web_app._state.sql_runner = original_sql_runner
        web_app._state._insight_cache = original_insight_cache

    assert response.status_code == 200
    body = response.json()
    assert body["recipes"][0]["category"] == "Dimensions"
    assert body["recipes"][0]["reason"].startswith("Suggested because")
    assert body["cached"] is False


def test_recipes_uses_cache(monkeypatch):
    """Repeated recipe requests should reuse cached prompt recipes."""

    class StubRunner:
        async def run_sql(self, sql):  # noqa: ARG002
            return pd.DataFrame()

    original_project_loaded = web_app._state.project_loaded
    original_schema_info = web_app._state.schema_info
    original_sql_runner = web_app._state.sql_runner
    original_insight_cache = dict(web_app._state._insight_cache)
    call_count = {"value": 0}

    async def fake_recipes(schema_info, run_sql):  # noqa: ARG001
        call_count["value"] += 1
        return [
            {
                "title": "Profile the biggest tables",
                "category": "Orientation",
                "reason": "Starts with the highest-row-count tables.",
                "prompt": "Profile the biggest tables.",
            }
        ]

    monkeypatch.setattr(web_app, "build_prompt_recipes", fake_recipes)

    web_app._state.project_loaded = True
    web_app._state.schema_info = [{"name": "orders", "row_count": 10, "columns": []}]
    web_app._state.sql_runner = _typed_stub(StubRunner())
    web_app._state.clear_insight_cache()

    try:
        with TestClient(web_app.app) as client:
            first = client.get("/api/recipes")
            second = client.get("/api/recipes")
    finally:
        web_app._state.project_loaded = original_project_loaded
        web_app._state.schema_info = original_schema_info
        web_app._state.sql_runner = original_sql_runner
        web_app._state._insight_cache = original_insight_cache

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["cached"] is False
    assert second.json()["cached"] is True
    assert call_count["value"] == 1


def test_project_health_reports_loaded_project(monkeypatch):
    """Project health should report checks for the active project."""

    original_insight_cache = dict(web_app._state._insight_cache)

    async def fake_health(state):  # noqa: ARG001
        return {
            "project_loaded": True,
            "project_dir": "/tmp/example-project",
            "checks": [
                {
                    "name": ".env",
                    "ok": True,
                    "detail": "/tmp/example-project/.env",
                    "category": "project",
                    "remediation": "",
                },
                {
                    "name": "Database connectivity",
                    "ok": True,
                    "detail": "SELECT 1",
                    "category": "connectivity",
                    "remediation": "",
                },
            ],
            "summary": {
                "ok_count": 2,
                "fail_count": 0,
                "config_failures": 0,
                "connectivity_failures": 0,
                "project_failures": 0,
            },
        }

    monkeypatch.setattr(web_app, "_build_project_health", fake_health)

    with TestClient(web_app.app) as client:
        response = client.get("/api/project-health")

    web_app._state._insight_cache = original_insight_cache
    assert response.status_code == 200
    body = response.json()
    assert body["project_loaded"] is True
    assert body["checks"][1]["name"] == "Database connectivity"
    assert body["summary"]["fail_count"] == 0
    assert body["cached"] is False


def test_project_health_reports_unloaded_state(monkeypatch):
    """Project health should still respond when no project is loaded."""

    original_insight_cache = dict(web_app._state._insight_cache)

    async def fake_health(state):  # noqa: ARG001
        return {
            "project_loaded": False,
            "project_dir": None,
            "checks": [
                {
                    "name": ".env",
                    "ok": False,
                    "detail": "No project loaded",
                    "category": "project",
                    "remediation": "Create or load a project directory with a .env file.",
                },
            ],
            "summary": {
                "ok_count": 0,
                "fail_count": 1,
                "config_failures": 0,
                "connectivity_failures": 0,
                "project_failures": 1,
            },
        }

    monkeypatch.setattr(web_app, "_build_project_health", fake_health)

    with TestClient(web_app.app) as client:
        response = client.get("/api/project-health")

    web_app._state._insight_cache = original_insight_cache
    assert response.status_code == 200
    assert response.json()["checks"][0]["ok"] is False
    assert response.json()["checks"][0]["remediation"]


def test_dataset_overview_uses_cache(monkeypatch):
    """Repeated dataset overview requests should reuse cached results."""

    class StubRunner:
        async def run_sql(self, sql):  # noqa: ARG002
            return pd.DataFrame()

    original_project_loaded = web_app._state.project_loaded
    original_schema_info = web_app._state.schema_info
    original_sql_runner = web_app._state.sql_runner
    original_insight_cache = dict(web_app._state._insight_cache)
    call_count = {"value": 0}

    async def fake_overview(schema_info, run_sql):  # noqa: ARG001
        call_count["value"] += 1
        return await _fake_overview(schema_info, run_sql)

    monkeypatch.setattr(web_app, "build_dataset_overview", fake_overview)

    web_app._state.project_loaded = True
    web_app._state.schema_info = [{"name": "products", "row_count": 5, "columns": []}]
    web_app._state.sql_runner = _typed_stub(StubRunner())
    web_app._state.clear_insight_cache()

    try:
        with TestClient(web_app.app) as client:
            first = client.get("/api/dataset-overview")
            second = client.get("/api/dataset-overview")
    finally:
        web_app._state.project_loaded = original_project_loaded
        web_app._state.schema_info = original_schema_info
        web_app._state.sql_runner = original_sql_runner
        web_app._state._insight_cache = original_insight_cache

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["cached"] is False
    assert second.json()["cached"] is True
    assert call_count["value"] == 1


def test_project_health_uses_cache(monkeypatch):
    """Repeated health requests should reuse cached checks until invalidated."""

    original_insight_cache = dict(web_app._state._insight_cache)
    call_count = {"value": 0}

    async def fake_health(state):  # noqa: ARG001
        call_count["value"] += 1
        return {
            "project_loaded": True,
            "project_dir": "/tmp/example-project",
            "checks": [
                {
                    "name": ".env",
                    "ok": True,
                    "detail": "/tmp/example-project/.env",
                    "category": "project",
                    "remediation": "",
                }
            ],
            "summary": {
                "ok_count": 1,
                "fail_count": 0,
                "config_failures": 0,
                "connectivity_failures": 0,
                "project_failures": 0,
            },
        }

    monkeypatch.setattr(web_app, "_build_project_health", fake_health)
    web_app._state.clear_insight_cache()

    try:
        with TestClient(web_app.app) as client:
            first = client.get("/api/project-health")
            second = client.get("/api/project-health")
    finally:
        web_app._state._insight_cache = original_insight_cache

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["cached"] is False
    assert second.json()["cached"] is True
    assert call_count["value"] == 1


def test_preview_table_uses_cache():
    """Repeated table preview requests should reuse cached HTML."""

    class StubRunner:
        def __init__(self):
            self.calls = 0

        async def run_sql(self, sql):  # noqa: ARG002
            self.calls += 1
            return pd.DataFrame({"id": [1], "name": ["widget"]})

    original_schema_info = web_app._state.schema_info
    original_sql_runner = web_app._state.sql_runner
    original_insight_cache = dict(web_app._state._insight_cache)
    runner = StubRunner()

    web_app._state.schema_info = [
        {
            "name": "products",
            "row_count": 1,
            "columns": [{"name": "id", "dtype": "INTEGER", "nullable": False}],
        }
    ]
    web_app._state.sql_runner = _typed_stub(runner)
    web_app._state.clear_insight_cache()

    try:
        with TestClient(web_app.app) as client:
            first = client.get("/api/preview/products")
            second = client.get("/api/preview/products")
    finally:
        web_app._state.schema_info = original_schema_info
        web_app._state.sql_runner = original_sql_runner
        web_app._state._insight_cache = original_insight_cache

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["cached"] is False
    assert second.json()["cached"] is True
    assert "<table" in first.json()["html"]
    assert runner.calls == 1


def test_column_stats_uses_cache():
    """Repeated column stats requests should reuse cached results."""

    class StubRunner:
        def __init__(self):
            self.calls = 0

        async def run_sql(self, sql):  # noqa: ARG002
            self.calls += 1
            return pd.DataFrame(
                [
                    {
                        "distinct_count": 3,
                        "null_count": 1,
                        "min_val": 1,
                        "max_val": 10,
                        "avg_val": 4.5,
                    }
                ]
            )

    original_schema_info = web_app._state.schema_info
    original_sql_runner = web_app._state.sql_runner
    original_sql_dialect = web_app._state.sql_dialect
    original_insight_cache = dict(web_app._state._insight_cache)
    runner = StubRunner()

    web_app._state.schema_info = [
        {
            "name": "orders",
            "row_count": 3,
            "columns": [{"name": "quantity", "dtype": "INTEGER", "nullable": True}],
        }
    ]
    web_app._state.sql_runner = _typed_stub(runner)
    web_app._state.sql_dialect = "duckdb"
    web_app._state.clear_insight_cache()

    try:
        with TestClient(web_app.app) as client:
            first = client.get("/api/column-stats/orders/quantity")
            second = client.get("/api/column-stats/orders/quantity")
    finally:
        web_app._state.schema_info = original_schema_info
        web_app._state.sql_runner = original_sql_runner
        web_app._state.sql_dialect = original_sql_dialect
        web_app._state._insight_cache = original_insight_cache

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["cached"] is False
    assert second.json()["cached"] is True
    assert first.json()["stats"]["distinct"] == 3
    assert first.json()["stats"]["avg"] == 4.5
    assert runner.calls == 1


def test_run_report_returns_plotly_spec(monkeypatch):
    """Saved report runs should preserve plotly_spec for frontend reuse."""

    class StubReports:
        def get(self, report_id):  # noqa: ARG002
            return {
                "id": 1,
                "sql": "select 1",
                "tool": "visualize_data",
                "name": "Saved Chart",
                "plotly_spec": {"data": [{"type": "bar"}], "layout": {"title": "Saved Chart"}},
            }

    class StubRunner:
        async def run_sql(self, sql):  # noqa: ARG002
            return pd.DataFrame({"value": [1]})

    async def fake_execute_tool(*args, **kwargs):  # noqa: ARG001
        return type(
            "Result",
            (),
            {
                "result_html": "<html><script>chart</script></html>",
                "meta": {"sql": "select 1", "tool": "visualize_data", "row_count": 1},
            },
        )()

    original_reports = web_app._state.reports
    original_runner = web_app._state.sql_runner

    monkeypatch.setattr(web_app, "execute_tool", fake_execute_tool)
    web_app._state.reports = _typed_stub(StubReports())
    web_app._state.sql_runner = _typed_stub(StubRunner())

    try:
        with TestClient(web_app.app) as client:
            response = client.post("/api/reports/1/run")
    finally:
        web_app._state.reports = original_reports
        web_app._state.sql_runner = original_runner

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["plotly_spec"]["data"][0]["type"] == "bar"


async def _fake_overview(schema_info, run_sql):  # noqa: ARG001
    return {
        "table_count": len(schema_info),
        "total_rows": 5,
        "total_columns": 3,
        "largest_tables": [{"name": "products", "row_count": 5, "column_count": 3}],
        "date_columns": [],
        "measure_columns": [],
        "dimension_columns": [],
        "quality_flags": [],
    }


async def _fake_dimension_overview(schema_info, run_sql):  # noqa: ARG001
    return {
        "table_count": len(schema_info),
        "dimension_columns": [
            {
                "table": "products",
                "column": "category",
                "distinct_count": 3,
                "null_rate": 0.0,
                "sample_values": ["electronics", "tools", "misc"],
            }
        ],
        "date_columns": [],
        "measure_columns": [{"table": "orders", "column": "quantity", "dtype": "INTEGER"}],
        "suggested_breakdowns": [
            {
                "table": "products",
                "column": "category",
                "reason": "3 distinct values, samples: electronics, tools, misc",
            }
        ],
        "join_hints": ["orders.product_id likely joins to products.id"],
    }


async def _fake_quality_overview(schema_info, run_sql):  # noqa: ARG001
    return {
        "table_count": len(schema_info),
        "null_columns": [
            {
                "table": "orders",
                "column": "customer_state",
                "null_count": 2,
                "null_rate": 20.0,
            }
        ],
        "numeric_flags": [
            {
                "table": "orders",
                "column": "quantity",
                "issue": "average sits on boundary (1)",
            }
        ],
        "date_columns": [
            {
                "table": "orders",
                "column": "order_date",
                "min": "2024-01-01",
                "max": "2024-04-01",
            }
        ],
        "notes": ["No obvious duplicate checks included in this quick pass."],
    }


async def _fake_trend_overview(schema_info, run_sql):  # noqa: ARG001
    return {
        "table_count": len(schema_info),
        "trend_candidates": [
            {
                "table": "orders",
                "date_column": "order_date",
                "measure_column": "quantity",
                "measure_dtype": "INTEGER",
                "date_range": "2024-01-01 → 2024-04-01",
            }
        ],
        "breakout_dimensions": [
            {
                "table": "orders",
                "column": "customer_state",
                "distinct_count": 4,
                "null_rate": 0.0,
            }
        ],
        "chart_recommendations": [
            {
                "title": "quantity over order_date",
                "table": "orders",
                "chart_type": "line",
                "reason": "date coverage 2024-01-01 → 2024-04-01",
            }
        ],
        "notes": ["Start with a single-series line chart before adding category splits."],
    }
