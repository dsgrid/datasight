"""Tests for the FastAPI web app."""

from pathlib import Path
from typing import Any, cast

import pandas as pd
import pytest
from fastapi.testclient import TestClient

import datasight.web.app as web_app

from tests._env_helpers import scrub_datasight_env


@pytest.fixture(autouse=True)
def _scrub_datasight_env():
    """Prevent leaked env vars from other test files auto-loading a project."""
    scrub_datasight_env()
    web_app._state.clear_project()


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


def test_measure_overview_returns_profile(monkeypatch):
    """Measure overview should return deterministic aggregation guidance when loaded."""

    class StubRunner:
        async def run_sql(self, sql):  # noqa: ARG002
            return pd.DataFrame()

    original_project_loaded = web_app._state.project_loaded
    original_schema_info = web_app._state.schema_info
    original_sql_runner = web_app._state.sql_runner
    original_insight_cache = dict(web_app._state._insight_cache)

    monkeypatch.setattr(
        web_app,
        "build_measure_overview",
        lambda schema_info, run_sql, overrides=None: _fake_measure_overview(schema_info, run_sql),  # noqa: ARG005
    )

    web_app._state.project_loaded = True
    web_app._state.schema_info = [{"name": "generation_hourly", "row_count": 24, "columns": []}]
    web_app._state.sql_runner = _typed_stub(StubRunner())

    try:
        with TestClient(web_app.app) as client:
            response = client.get("/api/measure-overview")
    finally:
        web_app._state.project_loaded = original_project_loaded
        web_app._state.schema_info = original_schema_info
        web_app._state.sql_runner = original_sql_runner
        web_app._state._insight_cache = original_insight_cache

    assert response.status_code == 200
    body = response.json()
    assert body["overview"]["measures"][0]["column"] == "net_generation_mwh"
    assert body["overview"]["measures"][0]["default_aggregation"] == "sum"


def test_measure_overview_uses_project_overrides(monkeypatch):
    class StubRunner:
        async def run_sql(self, sql):  # noqa: ARG002
            return pd.DataFrame()

    original_project_loaded = web_app._state.project_loaded
    original_project_dir = web_app._state.project_dir
    original_schema_info = web_app._state.schema_info
    original_sql_runner = web_app._state.sql_runner
    original_insight_cache = dict(web_app._state._insight_cache)
    captured = {}

    async def fake_measure_overview(schema_info, run_sql, overrides=None):  # noqa: ARG001
        captured["overrides"] = overrides
        return await _fake_measure_overview(schema_info, run_sql)

    monkeypatch.setattr(web_app, "build_measure_overview", fake_measure_overview)
    monkeypatch.setattr(
        web_app,
        "load_measure_overrides",
        lambda path, project_dir: [  # noqa: ARG005
            {
                "table": "generation_hourly",
                "column": "net_generation_mwh",
                "default_aggregation": "avg",
            }
        ],
    )

    web_app._state.project_loaded = True
    web_app._state.project_dir = "/tmp/example-project"
    web_app._state.schema_info = [{"name": "generation_hourly", "row_count": 24, "columns": []}]
    web_app._state.sql_runner = _typed_stub(StubRunner())
    web_app._state.clear_insight_cache()

    try:
        with TestClient(web_app.app) as client:
            response = client.get("/api/measure-overview")
    finally:
        web_app._state.project_loaded = original_project_loaded
        web_app._state.project_dir = original_project_dir
        web_app._state.schema_info = original_schema_info
        web_app._state.sql_runner = original_sql_runner
        web_app._state._insight_cache = original_insight_cache

    assert response.status_code == 200
    assert captured["overrides"][0]["default_aggregation"] == "avg"


def test_measure_overview_supports_table_scope(monkeypatch):
    class StubRunner:
        async def run_sql(self, sql):  # noqa: ARG002
            return pd.DataFrame()

    captured = {}
    original_project_loaded = web_app._state.project_loaded
    original_schema_info = web_app._state.schema_info
    original_sql_runner = web_app._state.sql_runner
    original_insight_cache = dict(web_app._state._insight_cache)

    async def fake_overview(schema_info, run_sql, overrides=None):  # noqa: ARG001
        captured["tables"] = [table["name"] for table in schema_info]
        return await _fake_measure_overview(schema_info, run_sql)

    monkeypatch.setattr(web_app, "build_measure_overview", fake_overview)
    web_app._state.project_loaded = True
    web_app._state.schema_info = [
        {"name": "generation_hourly", "row_count": 24, "columns": []},
        {"name": "plants", "row_count": 10, "columns": []},
    ]
    web_app._state.sql_runner = _typed_stub(StubRunner())
    web_app._state.clear_insight_cache()

    try:
        with TestClient(web_app.app) as client:
            response = client.get("/api/measure-overview?table=generation_hourly")
    finally:
        web_app._state.project_loaded = original_project_loaded
        web_app._state.schema_info = original_schema_info
        web_app._state.sql_runner = original_sql_runner
        web_app._state._insight_cache = original_insight_cache

    assert response.status_code == 200
    assert captured["tables"] == ["generation_hourly"]


def test_measure_overview_requires_loaded_data():
    """Measure overview should fail cleanly when no dataset is loaded."""
    original_project_loaded = web_app._state.project_loaded
    original_schema_info = web_app._state.schema_info
    original_sql_runner = web_app._state.sql_runner

    web_app._state.project_loaded = False
    web_app._state.schema_info = []
    web_app._state.sql_runner = None

    try:
        with TestClient(web_app.app) as client:
            response = client.get("/api/measure-overview")
    finally:
        web_app._state.project_loaded = original_project_loaded
        web_app._state.schema_info = original_schema_info
        web_app._state.sql_runner = original_sql_runner

    assert response.status_code == 200
    assert response.json()["error"] == "No dataset loaded"


def test_measure_editor_returns_generated_scaffold_when_missing(monkeypatch, tmp_path):
    class StubRunner:
        async def run_sql(self, sql):  # noqa: ARG002
            return pd.DataFrame()

    original_project_loaded = web_app._state.project_loaded
    original_project_dir = web_app._state.project_dir
    original_schema_info = web_app._state.schema_info
    original_sql_runner = web_app._state.sql_runner

    async def fake_measure_overview(schema_info, run_sql, overrides=None):  # noqa: ARG001
        return await _fake_measure_overview(schema_info, run_sql)

    monkeypatch.setattr(web_app, "build_measure_overview", fake_measure_overview)

    web_app._state.project_loaded = True
    web_app._state.project_dir = str(tmp_path)
    web_app._state.schema_info = [{"name": "generation_hourly", "row_count": 24, "columns": []}]
    web_app._state.sql_runner = _typed_stub(StubRunner())

    try:
        with TestClient(web_app.app) as client:
            response = client.get("/api/measures/editor")
    finally:
        web_app._state.project_loaded = original_project_loaded
        web_app._state.project_dir = original_project_dir
        web_app._state.schema_info = original_schema_info
        web_app._state.sql_runner = original_sql_runner

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["generated"] is True
    assert body["path"] == str(tmp_path / "measures.yaml")
    assert "# datasight measure overrides" in body["text"]
    assert "net_generation_mwh" in body["text"]


def test_measure_editor_save_writes_file_and_reloads_project(monkeypatch, tmp_path):
    original_project_loaded = web_app._state.project_loaded
    original_project_dir = web_app._state.project_dir
    original_schema_info = web_app._state.schema_info

    captured: dict[str, Any] = {}

    async def fake_load_project(project_dir, state):  # noqa: ARG001
        captured["project_dir"] = project_dir
        return {"path": project_dir, "name": Path(project_dir).name, "tables": 1, "queries": 0}

    monkeypatch.setattr(web_app, "load_project", fake_load_project)

    web_app._state.project_loaded = True
    web_app._state.project_dir = str(tmp_path)
    web_app._state.schema_info = [
        {
            "name": "generation_hourly",
            "row_count": 24,
            "columns": [
                {"name": "net_generation_mwh", "dtype": "DOUBLE", "nullable": False},
            ],
        }
    ]

    try:
        with TestClient(web_app.app) as client:
            response = client.post(
                "/api/measures/editor",
                json={
                    "text": (
                        "- table: generation_hourly\n"
                        "  column: net_generation_mwh\n"
                        "  default_aggregation: sum\n"
                    )
                },
            )
    finally:
        web_app._state.project_loaded = original_project_loaded
        web_app._state.project_dir = original_project_dir
        web_app._state.schema_info = original_schema_info

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert captured["project_dir"] == str(tmp_path)
    assert (tmp_path / "measures.yaml").read_text(encoding="utf-8").endswith("\n")


def test_measure_editor_catalog_returns_measures(monkeypatch):
    class StubRunner:
        async def run_sql(self, sql):  # noqa: ARG002
            return pd.DataFrame()

    original_project_loaded = web_app._state.project_loaded
    original_project_dir = web_app._state.project_dir
    original_schema_info = web_app._state.schema_info
    original_sql_runner = web_app._state.sql_runner

    async def fake_measure_overview(schema_info, run_sql, overrides=None):  # noqa: ARG001
        return await _fake_measure_overview(schema_info, run_sql)

    monkeypatch.setattr(web_app, "build_measure_overview", fake_measure_overview)

    web_app._state.project_loaded = True
    web_app._state.project_dir = "/tmp/example-project"
    web_app._state.schema_info = [{"name": "generation_hourly", "row_count": 24, "columns": []}]
    web_app._state.sql_runner = _typed_stub(StubRunner())

    try:
        with TestClient(web_app.app) as client:
            response = client.get("/api/measures/editor/catalog")
    finally:
        web_app._state.project_loaded = original_project_loaded
        web_app._state.project_dir = original_project_dir
        web_app._state.schema_info = original_schema_info
        web_app._state.sql_runner = original_sql_runner

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["measures"][0]["column"] == "net_generation_mwh"


def test_measure_editor_validate_reports_unknown_weight_column():
    original_project_loaded = web_app._state.project_loaded
    original_project_dir = web_app._state.project_dir
    original_schema_info = web_app._state.schema_info

    web_app._state.project_loaded = True
    web_app._state.project_dir = "/tmp/example-project"
    web_app._state.schema_info = [
        {
            "name": "generation_hourly",
            "row_count": 24,
            "columns": [
                {"name": "net_generation_mwh", "dtype": "DOUBLE", "nullable": False},
                {"name": "co2_rate_lb_per_mwh", "dtype": "DOUBLE", "nullable": True},
            ],
        }
    ]

    try:
        with TestClient(web_app.app) as client:
            response = client.post(
                "/api/measures/editor/validate",
                json={
                    "text": (
                        "- table: generation_hourly\n"
                        "  column: co2_rate_lb_per_mwh\n"
                        "  default_aggregation: avg\n"
                        "  average_strategy: weighted_avg\n"
                        "  weight_column: missing_weight\n"
                    )
                },
            )
    finally:
        web_app._state.project_loaded = original_project_loaded
        web_app._state.project_dir = original_project_dir
        web_app._state.schema_info = original_schema_info

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is False
    assert "weight_column `missing_weight` not found" in body["errors"][0]


def test_measure_editor_validate_accepts_calculated_measure():
    original_project_loaded = web_app._state.project_loaded
    original_project_dir = web_app._state.project_dir
    original_schema_info = web_app._state.schema_info

    web_app._state.project_loaded = True
    web_app._state.project_dir = "/tmp/example-project"
    web_app._state.schema_info = [
        {
            "name": "generation_hourly",
            "row_count": 24,
            "columns": [
                {"name": "load_mw", "dtype": "DOUBLE", "nullable": False},
                {"name": "renewable_generation_mw", "dtype": "DOUBLE", "nullable": False},
            ],
        }
    ]

    try:
        with TestClient(web_app.app) as client:
            response = client.post(
                "/api/measures/editor/validate",
                json={
                    "text": (
                        "- table: generation_hourly\n"
                        "  name: net_load_mw\n"
                        "  expression: load_mw - renewable_generation_mw\n"
                        "  default_aggregation: avg\n"
                    )
                },
            )
    finally:
        web_app._state.project_loaded = original_project_loaded
        web_app._state.project_dir = original_project_dir
        web_app._state.schema_info = original_schema_info

    assert response.status_code == 200
    assert response.json()["ok"] is True


def test_measure_editor_upsert_adds_override(monkeypatch):
    original_project_loaded = web_app._state.project_loaded
    original_project_dir = web_app._state.project_dir
    original_schema_info = web_app._state.schema_info

    web_app._state.project_loaded = True
    web_app._state.project_dir = "/tmp/example-project"
    web_app._state.schema_info = [
        {
            "name": "generation_hourly",
            "row_count": 24,
            "columns": [
                {"name": "net_generation_mwh", "dtype": "DOUBLE", "nullable": False},
                {"name": "co2_rate_lb_per_mwh", "dtype": "DOUBLE", "nullable": True},
            ],
        }
    ]

    try:
        with TestClient(web_app.app) as client:
            response = client.post(
                "/api/measures/editor/upsert",
                json={
                    "text": "",
                    "table": "generation_hourly",
                    "column": "co2_rate_lb_per_mwh",
                    "default_aggregation": "avg",
                    "average_strategy": "weighted_avg",
                    "weight_column": "net_generation_mwh",
                },
            )
    finally:
        web_app._state.project_loaded = original_project_loaded
        web_app._state.project_dir = original_project_dir
        web_app._state.schema_info = original_schema_info

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert "co2_rate_lb_per_mwh" in body["text"]
    assert "weight_column: net_generation_mwh" in body["text"]


def test_measure_editor_upsert_adds_calculated_measure():
    original_project_loaded = web_app._state.project_loaded
    original_project_dir = web_app._state.project_dir
    original_schema_info = web_app._state.schema_info

    web_app._state.project_loaded = True
    web_app._state.project_dir = "/tmp/example-project"
    web_app._state.schema_info = [
        {
            "name": "generation_hourly",
            "row_count": 24,
            "columns": [
                {"name": "load_mw", "dtype": "DOUBLE", "nullable": False},
                {"name": "renewable_generation_mw", "dtype": "DOUBLE", "nullable": False},
            ],
        }
    ]

    try:
        with TestClient(web_app.app) as client:
            response = client.post(
                "/api/measures/editor/upsert",
                json={
                    "text": "",
                    "table": "generation_hourly",
                    "name": "net_load_mw",
                    "expression": "load_mw - renewable_generation_mw",
                    "default_aggregation": "avg",
                    "average_strategy": "avg",
                },
            )
    finally:
        web_app._state.project_loaded = original_project_loaded
        web_app._state.project_dir = original_project_dir
        web_app._state.schema_info = original_schema_info

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert "name: net_load_mw" in body["text"]
    assert "expression: load_mw - renewable_generation_mw" in body["text"]


def test_measure_editor_upsert_persists_display_and_chart_metadata():
    original_project_loaded = web_app._state.project_loaded
    original_project_dir = web_app._state.project_dir
    original_schema_info = web_app._state.schema_info

    web_app._state.project_loaded = True
    web_app._state.project_dir = "/tmp/example-project"
    web_app._state.schema_info = [
        {
            "name": "generation_hourly",
            "row_count": 24,
            "columns": [
                {"name": "net_generation_mwh", "dtype": "DOUBLE", "nullable": False},
            ],
        }
    ]

    try:
        with TestClient(web_app.app) as client:
            response = client.post(
                "/api/measures/editor/upsert",
                json={
                    "text": "",
                    "table": "generation_hourly",
                    "column": "net_generation_mwh",
                    "default_aggregation": "sum",
                    "display_name": "Net generation",
                    "format": "mwh",
                    "preferred_chart_types": ["line", "area"],
                },
            )
    finally:
        web_app._state.project_loaded = original_project_loaded
        web_app._state.project_dir = original_project_dir
        web_app._state.schema_info = original_schema_info

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert "display_name: Net generation" in body["text"]
    assert "format: mwh" in body["text"]
    assert "- line" in body["text"]


def test_save_explore_project_seeds_measure_overrides(monkeypatch, tmp_path):
    original_is_ephemeral = web_app._state.is_ephemeral
    original_sql_runner = web_app._state.sql_runner
    original_schema_info = web_app._state.schema_info

    class StubRunner:
        async def run_sql(self, sql):  # noqa: ARG002
            return pd.DataFrame()

    captured: dict[str, Any] = {}

    monkeypatch.setattr(web_app, "save_ephemeral_as_project", lambda **kwargs: str(tmp_path))  # noqa: ARG005

    async def fake_seed(project_dir, schema_info, sql_runner):  # noqa: ARG001
        captured["project_dir"] = project_dir
        captured["schema_info"] = schema_info
        return "measures.yaml"

    async def fake_load_project(project_dir, state):  # noqa: ARG001
        return {"path": project_dir, "name": Path(project_dir).name, "tables": 1, "queries": 0}

    monkeypatch.setattr(web_app, "_write_measure_overrides_scaffold", fake_seed)
    monkeypatch.setattr(web_app, "load_project", fake_load_project)

    web_app._state.is_ephemeral = True
    web_app._state.sql_runner = _typed_stub(StubRunner())
    web_app._state.schema_info = [{"name": "generation_hourly", "row_count": 24, "columns": []}]

    try:
        with TestClient(web_app.app) as client:
            response = client.post(
                "/api/explore/save-project",
                json={"path": str(tmp_path), "name": "Example project"},
            )
    finally:
        web_app._state.is_ephemeral = original_is_ephemeral
        web_app._state.sql_runner = original_sql_runner
        web_app._state.schema_info = original_schema_info

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert captured["project_dir"] == str(tmp_path)
    assert captured["schema_info"][0]["name"] == "generation_hourly"


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
        lambda schema_info, run_sql, overrides=None: _fake_trend_overview(schema_info, run_sql),  # noqa: ARG005
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

    async def fake_overview(schema_info, run_sql, overrides=None):  # noqa: ARG001
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

    async def fake_recipes(schema_info, run_sql, overrides=None):  # noqa: ARG001
        return [
            {
                "title": "SUM net_generation_mwh",
                "category": "Measures",
                "reason": "Suggested because generation_hourly.net_generation_mwh looks like an energy measure with default sum aggregation.",
                "prompt": "Analyze net_generation_mwh with SUM.",
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
    assert body["recipes"][0]["category"] == "Measures"
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

    async def fake_recipes(schema_info, run_sql, overrides=None):  # noqa: ARG001
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


def test_sql_execute_returns_error_for_empty_sql():
    """Empty SQL should short-circuit before the runner is touched."""
    original_project_loaded = web_app._state.project_loaded
    original_sql_runner = web_app._state.sql_runner

    web_app._state.project_loaded = True
    web_app._state.sql_runner = _typed_stub(object())

    try:
        with TestClient(web_app.app) as client:
            response = client.post("/api/sql-execute", json={"sql": "   "})
    finally:
        web_app._state.project_loaded = original_project_loaded
        web_app._state.sql_runner = original_sql_runner

    assert response.status_code == 200
    body = response.json()
    assert body["error"] == "SQL is empty"
    assert body["html"] is None
    assert body["row_count"] == 0


def test_sql_execute_runs_query_and_returns_html():
    """Successful execution should return row_count, elapsed, and rendered HTML."""

    class StubRunner:
        async def run_sql(self, sql):  # noqa: ARG002
            return pd.DataFrame({"plant_id": [1, 2], "mwh": [10.0, 20.0]})

    original_project_loaded = web_app._state.project_loaded
    original_sql_runner = web_app._state.sql_runner
    original_query_logger = web_app._state.query_logger

    web_app._state.project_loaded = True
    web_app._state.sql_runner = _typed_stub(StubRunner())
    web_app._state.query_logger = None

    try:
        with TestClient(web_app.app) as client:
            response = client.post(
                "/api/sql-execute",
                json={"sql": "SELECT plant_id, mwh FROM plants", "session_id": "t"},
            )
    finally:
        web_app._state.project_loaded = original_project_loaded
        web_app._state.sql_runner = original_sql_runner
        web_app._state.query_logger = original_query_logger

    assert response.status_code == 200
    body = response.json()
    assert body["error"] is None
    assert body["row_count"] == 2
    assert body["html"] is not None
    assert "plant_id" in body["html"]


def test_sql_validate_returns_parse_error():
    """Malformed SQL should come back with valid=False and a parse error."""
    original_schema_info = web_app._state.schema_info
    original_sql_dialect = web_app._state.sql_dialect

    web_app._state.schema_info = [
        {"name": "plants", "row_count": 0, "columns": [{"name": "plant_id", "dtype": "INT"}]}
    ]
    web_app._state.sql_dialect = "duckdb"

    try:
        with TestClient(web_app.app) as client:
            response = client.post(
                "/api/sql-validate",
                json={"sql": "SELECT FROM WHERE"},
            )
    finally:
        web_app._state.schema_info = original_schema_info
        web_app._state.sql_dialect = original_sql_dialect

    assert response.status_code == 200
    body = response.json()
    assert body["valid"] is False
    assert body["errors"]
    assert "parse error" in body["errors"][0].lower()


def test_sql_validate_accepts_empty_sql():
    """Empty SQL should report as valid without invoking sqlglot."""
    with TestClient(web_app.app) as client:
        response = client.post("/api/sql-validate", json={"sql": ""})

    assert response.status_code == 200
    assert response.json() == {"valid": True, "errors": []}


def test_sql_format_pretty_prints_sql():
    """sqlglot should pretty-print valid SQL without an error."""
    original_sql_dialect = web_app._state.sql_dialect
    web_app._state.sql_dialect = "duckdb"

    try:
        with TestClient(web_app.app) as client:
            response = client.post(
                "/api/sql-format",
                json={"sql": "select a,b from t where a=1"},
            )
    finally:
        web_app._state.sql_dialect = original_sql_dialect

    assert response.status_code == 200
    body = response.json()
    assert body["error"] is None
    assert body["formatted"].strip() != ""
    assert body["formatted"].upper().count("SELECT") >= 1


def test_sql_format_reports_parse_errors():
    """Unparseable SQL should echo input back with a non-null error."""
    original_sql_dialect = web_app._state.sql_dialect
    web_app._state.sql_dialect = "duckdb"

    try:
        with TestClient(web_app.app) as client:
            response = client.post(
                "/api/sql-format",
                json={"sql": "SELECT FROM WHERE"},
            )
    finally:
        web_app._state.sql_dialect = original_sql_dialect

    assert response.status_code == 200
    body = response.json()
    assert body["error"] is not None


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


async def _fake_measure_overview(schema_info, run_sql):  # noqa: ARG001
    return {
        "table_count": len(schema_info),
        "measures": [
            {
                "table": "generation_hourly",
                "column": "net_generation_mwh",
                "role": "energy",
                "unit": "mwh",
                "default_aggregation": "sum",
                "average_strategy": "avg",
                "weight_column": None,
                "recommended_rollup_sql": "SUM(net_generation_mwh) AS total_net_generation_mwh",
                "allowed_aggregations": ["sum", "avg", "min", "max"],
                "forbidden_aggregations": [],
                "additive_across_category": True,
                "additive_across_time": True,
                "confidence": 0.98,
                "reason": "Energy-volume metric; summing across periods is usually meaningful.",
            },
            {
                "table": "load_hourly",
                "column": "demand_mw",
                "role": "power",
                "unit": "mw",
                "default_aggregation": "avg",
                "average_strategy": "avg",
                "weight_column": None,
                "recommended_rollup_sql": "AVG(demand_mw) AS avg_demand_mw",
                "allowed_aggregations": ["avg", "max", "min"],
                "forbidden_aggregations": ["sum"],
                "additive_across_category": True,
                "additive_across_time": False,
                "confidence": 0.95,
                "reason": "Power metric; average or peak over time rather than summing.",
            },
        ],
        "notes": [
            "Energy-volume fields usually roll up with SUM.",
            "Power and demand fields usually need AVG or MAX, not SUM.",
        ],
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
                "measure_role": "count",
                "aggregation": "sum",
                "date_range": "2024-01-01 → 2024-04-01",
                "recommended_query_shape": "SUM(quantity) BY order_date",
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
                "title": "SUM quantity over order_date",
                "table": "orders",
                "chart_type": "line",
                "aggregation": "sum",
                "reason": "count metric with default SUM aggregation; date coverage 2024-01-01 → 2024-04-01",
            }
        ],
        "notes": ["Start with a single-series line chart before adding category splits."],
    }
