"""Tests for the report feature — ReportStore and CLI commands."""

import json

from click.testing import CliRunner

from datasight.cli import cli
from datasight.web.app import ReportStore


# ---------------------------------------------------------------------------
# ReportStore unit tests
# ---------------------------------------------------------------------------


class TestReportStore:
    """Unit tests for ReportStore persistence layer."""

    def test_add_and_list(self, tmp_path):
        store = ReportStore(tmp_path / "reports.json")
        report = store.add("SELECT 1", "run_sql", "test report")
        assert report["id"] == 1
        assert report["sql"] == "SELECT 1"
        assert report["name"] == "test report"
        assert report["tool"] == "run_sql"
        assert len(store.list_all()) == 1

    def test_add_with_plotly_spec(self, tmp_path):
        store = ReportStore(tmp_path / "reports.json")
        spec = {"data": [{"type": "bar", "x": [1], "y": [2]}]}
        report = store.add("SELECT x, y FROM t", "visualize_data", "chart", spec)
        assert report["plotly_spec"] == spec

    def test_add_without_plotly_spec(self, tmp_path):
        store = ReportStore(tmp_path / "reports.json")
        report = store.add("SELECT 1")
        assert "plotly_spec" not in report

    def test_get_existing(self, tmp_path):
        store = ReportStore(tmp_path / "reports.json")
        added = store.add("SELECT 1")
        fetched = store.get(added["id"])
        assert fetched is not None
        assert fetched["sql"] == "SELECT 1"

    def test_get_returns_copy(self, tmp_path):
        store = ReportStore(tmp_path / "reports.json")
        added = store.add("SELECT 1")
        fetched = store.get(added["id"])
        assert fetched is not None
        fetched["sql"] = "MODIFIED"
        refetched = store.get(added["id"])
        assert refetched is not None
        assert refetched["sql"] == "SELECT 1"

    def test_get_nonexistent(self, tmp_path):
        store = ReportStore(tmp_path / "reports.json")
        assert store.get(999) is None

    def test_update_sql(self, tmp_path):
        store = ReportStore(tmp_path / "reports.json")
        added = store.add("SELECT 1", name="orig")
        updated = store.update(added["id"], {"sql": "SELECT 2"})
        assert updated is not None
        assert updated["sql"] == "SELECT 2"
        assert updated["name"] == "orig"

    def test_update_name(self, tmp_path):
        store = ReportStore(tmp_path / "reports.json")
        added = store.add("SELECT 1", name="orig")
        updated = store.update(added["id"], {"name": "renamed"})
        assert updated is not None
        assert updated["name"] == "renamed"
        assert updated["sql"] == "SELECT 1"

    def test_update_plotly_spec(self, tmp_path):
        store = ReportStore(tmp_path / "reports.json")
        added = store.add("SELECT 1", "visualize_data", "chart", {"data": []})
        new_spec = {"data": [{"type": "scatter"}]}
        updated = store.update(added["id"], {"plotly_spec": new_spec})
        assert updated is not None
        assert updated["plotly_spec"] == new_spec

    def test_update_multiple_fields(self, tmp_path):
        store = ReportStore(tmp_path / "reports.json")
        added = store.add("SELECT 1", name="orig")
        updated = store.update(added["id"], {"sql": "SELECT 2", "name": "new"})
        assert updated is not None
        assert updated["sql"] == "SELECT 2"
        assert updated["name"] == "new"

    def test_update_nonexistent(self, tmp_path):
        store = ReportStore(tmp_path / "reports.json")
        assert store.update(999, {"sql": "SELECT 1"}) is None

    def test_update_persists(self, tmp_path):
        path = tmp_path / "reports.json"
        store = ReportStore(path)
        added = store.add("SELECT 1", name="orig")
        store.update(added["id"], {"sql": "SELECT 2"})
        # Reload from disk
        store2 = ReportStore(path)
        result = store2.get(added["id"])
        assert result is not None
        assert result["sql"] == "SELECT 2"

    def test_delete(self, tmp_path):
        store = ReportStore(tmp_path / "reports.json")
        r1 = store.add("SELECT 1")
        r2 = store.add("SELECT 2")
        store.delete(r1["id"])
        assert store.get(r1["id"]) is None
        assert store.get(r2["id"]) is not None
        assert len(store.list_all()) == 1

    def test_clear(self, tmp_path):
        store = ReportStore(tmp_path / "reports.json")
        store.add("SELECT 1")
        store.add("SELECT 2")
        store.clear()
        assert len(store.list_all()) == 0

    def test_auto_increment_ids(self, tmp_path):
        store = ReportStore(tmp_path / "reports.json")
        r1 = store.add("SELECT 1")
        r2 = store.add("SELECT 2")
        assert r2["id"] == r1["id"] + 1

    def test_persistence_across_instances(self, tmp_path):
        path = tmp_path / "reports.json"
        store1 = ReportStore(path)
        store1.add("SELECT 1", name="first")
        store1.add("SELECT 2", name="second")

        store2 = ReportStore(path)
        reports = store2.list_all()
        assert len(reports) == 2
        assert reports[0]["name"] == "first"
        assert reports[1]["name"] == "second"

    def test_id_continues_after_reload(self, tmp_path):
        path = tmp_path / "reports.json"
        store1 = ReportStore(path)
        store1.add("SELECT 1")
        store1.add("SELECT 2")

        store2 = ReportStore(path)
        r3 = store2.add("SELECT 3")
        assert r3["id"] == 3

    def test_corrupted_file_handled(self, tmp_path):
        path = tmp_path / "reports.json"
        path.write_text("not valid json", encoding="utf-8")
        store = ReportStore(path)
        assert len(store.list_all()) == 0
        store.add("SELECT 1")
        assert len(store.list_all()) == 1

    def test_creates_parent_dirs(self, tmp_path):
        path = tmp_path / "nested" / "dir" / "reports.json"
        store = ReportStore(path)
        store.add("SELECT 1")
        assert path.exists()


# ---------------------------------------------------------------------------
# CLI command tests
# ---------------------------------------------------------------------------


def _seed_reports(project_dir):
    """Seed a reports.json with test data and return the path."""
    from pathlib import Path

    ds_dir = Path(project_dir) / ".datasight"
    ds_dir.mkdir(exist_ok=True)
    reports = [
        {
            "id": 1,
            "sql": "SELECT COUNT(*) FROM products",
            "tool": "run_sql",
            "name": "product count",
        },
        {
            "id": 2,
            "sql": "SELECT category, SUM(price) FROM products GROUP BY category",
            "tool": "visualize_data",
            "name": "price by category",
        },
        {
            "id": 3,
            "sql": "SELECT * FROM orders LIMIT 10",
            "tool": "run_sql",
            "name": "recent orders",
        },
    ]
    (ds_dir / "reports.json").write_text(json.dumps(reports), encoding="utf-8")
    return ds_dir / "reports.json"


class TestReportListCLI:
    """Tests for ``datasight report list``."""

    def test_list_empty(self, tmp_path):
        runner = CliRunner()
        result = runner.invoke(cli, ["report", "list", "--project-dir", str(tmp_path)])
        assert result.exit_code == 0
        assert "No saved reports" in result.output

    def test_list_with_reports(self, tmp_path):
        _seed_reports(tmp_path)
        runner = CliRunner()
        result = runner.invoke(cli, ["report", "list", "--project-dir", str(tmp_path)])
        assert result.exit_code == 0
        assert "product count" in result.output
        assert "price by category" in result.output
        assert "recent orders" in result.output
        assert "3 report(s)" in result.output

    def test_list_shows_tool_type(self, tmp_path):
        _seed_reports(tmp_path)
        runner = CliRunner()
        result = runner.invoke(cli, ["report", "list", "--project-dir", str(tmp_path)])
        assert result.exit_code == 0
        assert "run_sql" in result.output
        assert "visualize_data" in result.output

    def test_list_truncates_long_sql(self, tmp_path):
        from pathlib import Path

        ds_dir = Path(str(tmp_path)) / ".datasight"
        ds_dir.mkdir()
        long_sql = "SELECT " + ", ".join(f"col{i}" for i in range(50)) + " FROM big_table"
        reports = [{"id": 1, "sql": long_sql, "tool": "run_sql", "name": "long query"}]
        (ds_dir / "reports.json").write_text(json.dumps(reports), encoding="utf-8")

        runner = CliRunner()
        result = runner.invoke(cli, ["report", "list", "--project-dir", str(tmp_path)])
        assert result.exit_code == 0
        assert "..." in result.output


class TestReportDeleteCLI:
    """Tests for ``datasight report delete``."""

    def test_delete_existing(self, tmp_path):
        reports_path = _seed_reports(tmp_path)
        runner = CliRunner()
        result = runner.invoke(cli, ["report", "delete", "1", "--project-dir", str(tmp_path)])
        assert result.exit_code == 0
        assert "deleted" in result.output

        remaining = json.loads(reports_path.read_text(encoding="utf-8"))
        assert len(remaining) == 2
        assert all(r["id"] != 1 for r in remaining)

    def test_delete_nonexistent(self, tmp_path):
        _seed_reports(tmp_path)
        runner = CliRunner()
        result = runner.invoke(cli, ["report", "delete", "999", "--project-dir", str(tmp_path)])
        assert result.exit_code != 0
        assert "not found" in result.output

    def test_delete_requires_id(self, tmp_path):
        runner = CliRunner()
        result = runner.invoke(cli, ["report", "delete", "--project-dir", str(tmp_path)])
        assert result.exit_code != 0


class TestReportRunCLI:
    """Tests for ``datasight report run``."""

    def test_run_nonexistent_report(self, tmp_path):
        _seed_reports(tmp_path)
        # Create a minimal .env so _resolve_settings works
        (tmp_path / ".env").write_text(
            "LLM_PROVIDER=ollama\nDB_MODE=duckdb\nDB_PATH=test.duckdb\n",
            encoding="utf-8",
        )
        runner = CliRunner()
        result = runner.invoke(cli, ["report", "run", "999", "--project-dir", str(tmp_path)])
        assert result.exit_code != 0
        assert "not found" in result.output

    def test_run_executes_sql(self, project_dir, test_duckdb_path):
        _seed_reports(project_dir)
        runner = CliRunner(env={"DB_PATH": test_duckdb_path})
        result = runner.invoke(cli, ["report", "run", "1", "--project-dir", project_dir])
        assert result.exit_code == 0, f"stderr: {result.output}"
        # Report 1 is SELECT COUNT(*) FROM products, should output "5"
        assert "5" in result.output

    def test_run_csv_format(self, project_dir, test_duckdb_path):
        _seed_reports(project_dir)
        runner = CliRunner(env={"DB_PATH": test_duckdb_path})
        result = runner.invoke(
            cli, ["report", "run", "1", "--project-dir", project_dir, "--format", "csv"]
        )
        assert result.exit_code == 0, f"stderr: {result.output}"
        lines = [line for line in result.output.strip().split("\n") if line.strip()]
        # Header + at least one data row
        assert len(lines) >= 2

    def test_run_json_format(self, project_dir, test_duckdb_path):
        _seed_reports(project_dir)
        runner = CliRunner(env={"DB_PATH": test_duckdb_path})
        result = runner.invoke(
            cli, ["report", "run", "1", "--project-dir", project_dir, "--format", "json"]
        )
        assert result.exit_code == 0, f"stderr: {result.output}"
        # Find JSON array in output
        json_start = result.output.find("[")
        assert json_start >= 0
        data = json.loads(result.output[json_start:])
        assert isinstance(data, list)
        assert len(data) >= 1

    def test_run_csv_writes_output_file(self, project_dir, test_duckdb_path, tmp_path):
        _seed_reports(project_dir)
        output_path = tmp_path / "report.csv"
        runner = CliRunner(env={"DB_PATH": test_duckdb_path})
        result = runner.invoke(
            cli,
            [
                "report",
                "run",
                "1",
                "--project-dir",
                project_dir,
                "--format",
                "csv",
                "--output",
                str(output_path),
            ],
        )
        assert result.exit_code == 0, f"stderr: {result.output}"
        assert output_path.exists()
        text = output_path.read_text(encoding="utf-8")
        assert "count_star()" in text
        assert "5" in text
        assert f"Data saved to {output_path}" in result.output

    def test_run_table_writes_output_file(self, project_dir, test_duckdb_path, tmp_path):
        _seed_reports(project_dir)
        output_path = tmp_path / "report.txt"
        runner = CliRunner(env={"DB_PATH": test_duckdb_path})
        result = runner.invoke(
            cli,
            [
                "report",
                "run",
                "1",
                "--project-dir",
                project_dir,
                "--output",
                str(output_path),
            ],
        )
        assert result.exit_code == 0, f"stderr: {result.output}"
        assert output_path.exists()
        text = output_path.read_text(encoding="utf-8")
        assert "count_star()" in text
        assert "5" in text
        assert f"Data saved to {output_path}" in result.output
