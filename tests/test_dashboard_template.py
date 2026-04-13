"""Tests for dashboard templates module and `datasight templates` CLI."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from datasight import dashboard_template as dt
from datasight.cli import cli


@pytest.fixture
def project(tmp_path):
    """A bare project dir (no .env, no dashboard)."""
    proj = tmp_path / "proj"
    proj.mkdir()
    return proj


def _write_dashboard(project_dir: Path, dashboard: dict) -> None:
    dsdir = project_dir / ".datasight"
    dsdir.mkdir(parents=True, exist_ok=True)
    (dsdir / "dashboard.json").write_text(json.dumps(dashboard), encoding="utf-8")


def _write_env(project_dir: Path, db_path: Path) -> None:
    rel = db_path.name if db_path.parent == project_dir else str(db_path)
    (project_dir / ".env").write_text(f"DB_MODE=duckdb\nDB_PATH={rel}\n", encoding="utf-8")


@pytest.fixture
def sample_dashboard():
    return {
        "items": [
            {
                "id": 1,
                "type": "chart",
                "title": "Generation by fuel",
                "sql": (
                    "SELECT g.energy_source_code, "
                    "SUM(g.net_generation_mwh) AS total_mwh "
                    "FROM generation_fuel g "
                    "JOIN plants p ON g.plant_id = p.plant_id "
                    "GROUP BY 1"
                ),
                "plotly_spec": {"data": [], "layout": {}},
            },
            {
                "id": 2,
                "type": "table",
                "title": "Top rows",
                "sql": "SELECT * FROM generation_fuel LIMIT 10",
            },
        ],
        "columns": 2,
        "filters": [
            {
                "id": "f1",
                "column": "energy_source_code",
                "operator": "eq",
                "value": "WND",
                "enabled": True,
                "scope": {"kind": "all"},
            }
        ],
    }


def test_project_template_dir_inside_project(tmp_path):
    assert dt.project_template_dir(tmp_path) == (tmp_path / ".datasight" / "templates").resolve()


def test_collect_required_tables_finds_all_referenced(sample_dashboard):
    tables = dt.collect_required_tables(sample_dashboard["items"])
    assert set(tables) == {"generation_fuel", "plants"}


def test_collect_required_tables_empty_for_items_without_sql():
    assert dt.collect_required_tables([{"id": 1, "type": "note"}]) == []


def test_build_template_roundtrip(project, sample_dashboard):
    tpl = dt.build_template(
        "generation-by-fuel",
        sample_dashboard,
        description="Fuel mix dashboard",
    )
    assert tpl["name"] == "generation-by-fuel"
    assert set(tpl["required_tables"]) == {"generation_fuel", "plants"}
    assert tpl["version"] == dt.TEMPLATE_VERSION
    assert len(tpl["items"]) == 2
    assert tpl["filters"][0]["column"] == "energy_source_code"

    path = dt.save_template(tpl, project)
    assert path == dt.project_template_dir(project) / "generation-by-fuel.json"
    assert path.exists()

    loaded = dt.load_template("generation-by-fuel", project)
    assert loaded == tpl


def test_build_template_rejects_empty_dashboard():
    with pytest.raises(dt.TemplateError, match="empty"):
        dt.build_template("x", {"items": [], "columns": 0, "filters": []})


def test_build_template_requires_inferable_tables():
    dashboard = {"items": [{"id": 1, "type": "note"}], "columns": 0, "filters": []}
    with pytest.raises(dt.TemplateError, match="required_tables"):
        dt.build_template("x", dashboard)


def test_build_template_accepts_explicit_required_tables():
    dashboard = {"items": [{"id": 1, "type": "note"}], "columns": 0, "filters": []}
    tpl = dt.build_template("x", dashboard, required_tables=["plants"])
    assert tpl["required_tables"] == ["plants"]


def test_load_template_migrates_v1_source_table(project):
    legacy = {
        "name": "legacy",
        "version": 1,
        "description": "",
        "source_table": "generation_fuel",
        "items": [
            {
                "id": 1,
                "type": "table",
                "title": "t",
                "sql": "SELECT * FROM generation_fuel",
            }
        ],
        "columns": 2,
        "filters": [],
    }
    template_dir = dt.project_template_dir(project)
    template_dir.mkdir(parents=True, exist_ok=True)
    (template_dir / "legacy.json").write_text(json.dumps(legacy), encoding="utf-8")
    loaded = dt.load_template("legacy", project)
    assert loaded["required_tables"] == ["generation_fuel"]


def test_save_template_refuses_overwrite_without_flag(project, sample_dashboard):
    tpl = dt.build_template("gen", sample_dashboard)
    dt.save_template(tpl, project)
    with pytest.raises(dt.TemplateError, match="already exists"):
        dt.save_template(tpl, project)
    dt.save_template(tpl, project, overwrite=True)


def test_list_templates_returns_entries(project, sample_dashboard):
    dt.save_template(dt.build_template("a", sample_dashboard, description="first"), project)
    dt.save_template(dt.build_template("b", sample_dashboard, description="second"), project)
    entries = dt.list_templates(project)
    names = [e["name"] for e in entries]
    assert names == ["a", "b"]
    assert entries[0]["cards"] == 2
    assert entries[0]["description"] == "first"
    assert set(entries[0]["required_tables"]) == {"generation_fuel", "plants"}


def test_list_templates_skips_invalid_json(project, sample_dashboard):
    dt.save_template(dt.build_template("good", sample_dashboard), project)
    (dt.project_template_dir(project) / "broken.json").write_text("not json", encoding="utf-8")
    entries = dt.list_templates(project)
    assert [e["name"] for e in entries] == ["good"]


def test_list_templates_empty_when_dir_missing(project):
    assert dt.list_templates(project) == []


def test_delete_template(project, sample_dashboard):
    dt.save_template(dt.build_template("gone", sample_dashboard), project)
    assert dt.delete_template("gone", project) is True
    assert dt.delete_template("gone", project) is False


def test_invalid_name_rejected(project):
    with pytest.raises(dt.TemplateError):
        dt.template_path("../escape", project)
    with pytest.raises(dt.TemplateError):
        dt.template_path("has space", project)


def test_load_template_missing(project):
    with pytest.raises(dt.TemplateError, match="not found"):
        dt.load_template("nope", project)


def test_load_template_invalid_json(project):
    template_dir = dt.project_template_dir(project)
    template_dir.mkdir(parents=True, exist_ok=True)
    (template_dir / "bad.json").write_text("not json", encoding="utf-8")
    with pytest.raises(dt.TemplateError, match="valid JSON"):
        dt.load_template("bad", project)


# ---------------------------------------------------------------------------
# CLI tests
# ---------------------------------------------------------------------------


def test_cli_save_and_list(project, sample_dashboard):
    _write_dashboard(project, sample_dashboard)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "templates",
            "save",
            "fuel-mix",
            "--project-dir",
            str(project),
            "--description",
            "Mix by fuel",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Saved template 'fuel-mix'" in result.output
    assert "required_tables" in result.output
    assert "generation_fuel" in result.output
    assert "plants" in result.output

    result = runner.invoke(cli, ["templates", "list", "--project-dir", str(project)])
    assert result.exit_code == 0, result.output
    assert "fuel-mix" in result.output
    assert "Mix by fuel" in result.output


def test_cli_save_fails_without_dashboard(project):
    runner = CliRunner()
    result = runner.invoke(cli, ["templates", "save", "x", "--project-dir", str(project)])
    assert result.exit_code != 0
    assert "No dashboard" in result.output


def test_cli_save_rejects_existing(project, sample_dashboard):
    _write_dashboard(project, sample_dashboard)
    runner = CliRunner()
    args = ["templates", "save", "dup", "--project-dir", str(project)]
    assert runner.invoke(cli, args).exit_code == 0
    result = runner.invoke(cli, args)
    assert result.exit_code != 0
    assert "already exists" in result.output

    result = runner.invoke(cli, [*args, "--overwrite"])
    assert result.exit_code == 0, result.output


def test_cli_save_accepts_explicit_tables(project):
    _write_dashboard(
        project,
        {
            "items": [{"id": 1, "type": "note", "title": "hi"}],
            "columns": 1,
            "filters": [],
        },
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "templates",
            "save",
            "notes",
            "--project-dir",
            str(project),
            "--table",
            "generation_fuel",
            "--table",
            "plants",
        ],
    )
    assert result.exit_code == 0, result.output
    loaded = dt.load_template("notes", project)
    assert loaded["required_tables"] == ["generation_fuel", "plants"]


def test_cli_show_and_delete(project, sample_dashboard):
    _write_dashboard(project, sample_dashboard)

    runner = CliRunner()
    runner.invoke(cli, ["templates", "save", "showme", "--project-dir", str(project)])

    result = runner.invoke(cli, ["templates", "show", "showme", "--project-dir", str(project)])
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert data["name"] == "showme"
    assert set(data["required_tables"]) == {"generation_fuel", "plants"}

    result = runner.invoke(cli, ["templates", "delete", "showme", "--project-dir", str(project)])
    assert result.exit_code == 0
    assert "Deleted" in result.output

    result = runner.invoke(cli, ["templates", "delete", "showme", "--project-dir", str(project)])
    assert result.exit_code != 0
    assert "not found" in result.output


def test_cli_list_empty(project):
    runner = CliRunner()
    result = runner.invoke(cli, ["templates", "list", "--project-dir", str(project)])
    assert result.exit_code == 0
    assert "No templates" in result.output


# ---------------------------------------------------------------------------
# Apply tests (ephemeral DuckDB + HTML export)
# ---------------------------------------------------------------------------


@pytest.fixture
def generation_parquet(tmp_path):
    pd = pytest.importorskip("pandas")
    path = tmp_path / "data1.parquet"
    pd.DataFrame(
        {
            "energy_source_code": ["WND", "WND", "SUN", "SUN", "NG"],
            "net_generation_mwh": [100.0, 150.0, 80.0, 90.0, 200.0],
            "plant_id": [1, 2, 3, 4, 5],
        }
    ).to_parquet(path)
    return path


@pytest.fixture
def project_with_plants_db(project):
    """A project whose .env points at a DuckDB file containing a `plants` table."""
    import duckdb

    db_path = project / "database.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            "CREATE TABLE plants AS SELECT * FROM (VALUES "
            "(1, 'P1'), (2, 'P2'), (3, 'P3'), (4, 'P4'), (5, 'P5')"
            ") AS t(plant_id, plant_name)"
        )
    _write_env(project, db_path)
    return project


def _single_table_template(name="fuel-mix"):
    return {
        "name": name,
        "version": dt.TEMPLATE_VERSION,
        "description": "",
        "required_tables": ["generation_fuel"],
        "required_columns": [],
        "items": [
            {
                "id": 1,
                "type": "table",
                "title": "Top rows",
                "sql": "SELECT * FROM generation_fuel ORDER BY net_generation_mwh DESC LIMIT 3",
            },
            {
                "id": 2,
                "type": "table",
                "title": "Totals by fuel",
                "sql": (
                    "SELECT energy_source_code, SUM(net_generation_mwh) AS total "
                    "FROM generation_fuel GROUP BY 1 ORDER BY 1"
                ),
            },
        ],
        "columns": 2,
        "filters": [],
    }


def _joined_template(name="joined"):
    return {
        "name": name,
        "version": dt.TEMPLATE_VERSION,
        "description": "",
        "required_tables": ["generation_fuel", "plants"],
        "required_columns": [],
        "items": [
            {
                "id": 1,
                "type": "table",
                "title": "Gen by plant",
                "sql": (
                    "SELECT p.plant_name, SUM(g.net_generation_mwh) AS total "
                    "FROM generation_fuel g JOIN plants p ON g.plant_id = p.plant_id "
                    "GROUP BY 1 ORDER BY 1"
                ),
            },
        ],
        "columns": 1,
        "filters": [],
    }


async def test_apply_template_single_file(tmp_path, generation_parquet):
    out = tmp_path / "out" / "data1.html"
    result = await dt.apply_template(
        _single_table_template(),
        out,
        sources={"generation_fuel": generation_parquet},
    )
    assert result.ok, result.error or [c.error for c in result.cards]
    assert result.output == out
    assert out.exists()
    html = out.read_text(encoding="utf-8")
    assert "Top rows" in html
    assert "Totals by fuel" in html
    assert "WND" in html


async def test_apply_template_missing_parquet(tmp_path):
    out = tmp_path / "out.html"
    result = await dt.apply_template(
        _single_table_template(),
        out,
        sources={"generation_fuel": tmp_path / "missing.parquet"},
    )
    assert result.ok is False
    assert "not found" in (result.error or "")


async def test_apply_template_sql_error_records_failure(tmp_path, generation_parquet):
    tpl = _single_table_template()
    tpl["items"].append(
        {
            "id": 3,
            "type": "table",
            "title": "Bad",
            "sql": "SELECT no_such_column FROM generation_fuel",
        }
    )
    out = tmp_path / "out.html"
    result = await dt.apply_template(tpl, out, sources={"generation_fuel": generation_parquet})
    assert result.ok is False
    assert any(not c.ok for c in result.cards)
    assert out.exists()


async def test_apply_template_missing_required_table(tmp_path, generation_parquet):
    out = tmp_path / "out.html"
    result = await dt.apply_template(
        _joined_template(),
        out,
        sources={"generation_fuel": generation_parquet},
    )
    assert result.ok is False
    assert "plants" in (result.error or "")


def test_cli_apply_single_shot(project, generation_parquet):
    dt.save_template(_single_table_template("fuel-mix"), project)
    out = project / "out.html"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "templates",
            "apply",
            "fuel-mix",
            "--project-dir",
            str(project),
            "--table",
            f"generation_fuel={generation_parquet}",
            "--output",
            str(out),
        ],
    )
    assert result.exit_code == 0, result.output
    assert out.exists()


def test_cli_apply_batch_via_glob(project, generation_parquet):
    dt.save_template(_single_table_template("fuel-mix"), project)
    export_dir = project / "out"

    pd = pytest.importorskip("pandas")
    second = generation_parquet.parent / "data2.parquet"
    pd.read_parquet(generation_parquet).to_parquet(second)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "templates",
            "apply",
            "fuel-mix",
            "--project-dir",
            str(project),
            "--table",
            f"generation_fuel={generation_parquet.parent}/*.parquet",
            "--export-dir",
            str(export_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert (export_dir / "data1.html").exists()
    assert (export_dir / "data2.html").exists()
    assert "2 succeeded" in result.output


def test_cli_apply_no_matches(project, generation_parquet):
    dt.save_template(_single_table_template("fuel-mix"), project)
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "templates",
            "apply",
            "fuel-mix",
            "--project-dir",
            str(project),
            "--table",
            f"generation_fuel={project}/nope*.parquet",
            "--export-dir",
            str(project / "out"),
        ],
    )
    assert result.exit_code != 0
    assert "No files match" in result.output


def test_cli_apply_export_dir_with_literal_path(project, generation_parquet):
    dt.save_template(_single_table_template("fuel-mix"), project)
    export_dir = project / "out"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "templates",
            "apply",
            "fuel-mix",
            "--project-dir",
            str(project),
            "--table",
            f"generation_fuel={generation_parquet}",
            "--export-dir",
            str(export_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert (export_dir / f"{generation_parquet.stem}.html").exists()


def test_cli_apply_rejects_both_output_and_export_dir(project, generation_parquet):
    dt.save_template(_single_table_template("fuel-mix"), project)
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "templates",
            "apply",
            "fuel-mix",
            "--project-dir",
            str(project),
            "--table",
            f"generation_fuel={generation_parquet}",
            "--output",
            str(project / "out.html"),
            "--export-dir",
            str(project / "out"),
        ],
    )
    assert result.exit_code != 0
    assert "not both" in result.output


def test_cli_apply_requires_output_for_single(project, generation_parquet):
    dt.save_template(_single_table_template("fuel-mix"), project)
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "templates",
            "apply",
            "fuel-mix",
            "--project-dir",
            str(project),
            "--table",
            f"generation_fuel={generation_parquet}",
        ],
    )
    assert result.exit_code != 0
    assert "--output" in result.output


def test_cli_apply_uses_project_db_for_unmapped_tables(project_with_plants_db, generation_parquet):
    """`plants` comes from the project DB; only `generation_fuel` rotates."""
    dt.save_template(_joined_template("joined"), project_with_plants_db)
    out = project_with_plants_db / "out.html"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "templates",
            "apply",
            "joined",
            "--project-dir",
            str(project_with_plants_db),
            "--table",
            f"generation_fuel={generation_parquet}",
            "--output",
            str(out),
        ],
    )
    assert result.exit_code == 0, result.output
    assert out.exists()
    assert "P1" in out.read_text(encoding="utf-8")


def test_cli_apply_reports_missing_required_tables(project, generation_parquet):
    """Without a project DB, unmapped required tables are an error."""
    dt.save_template(_joined_template("joined"), project)
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "templates",
            "apply",
            "joined",
            "--project-dir",
            str(project),
            "--table",
            f"generation_fuel={generation_parquet}",
            "--output",
            str(project / "out.html"),
        ],
    )
    assert result.exit_code != 0
    assert "plants" in result.output


# ---------------------------------------------------------------------------
# Templated variables
# ---------------------------------------------------------------------------


def test_render_sql_substitutes_placeholder():
    assert dt.render_sql("SELECT {{year}}", {"year": "2021"}) == "SELECT 2021"


def test_render_sql_unknown_placeholder_raises():
    with pytest.raises(dt.TemplateError, match="unknown variable"):
        dt.render_sql("SELECT {{year}}", {})


def test_build_template_rewrites_literals_to_placeholders(sample_dashboard):
    sample_dashboard["items"] = [
        {
            "id": 1,
            "type": "table",
            "title": "Year",
            "sql": "SELECT * FROM generation_fuel WHERE year = 2020",
        }
    ]
    tpl = dt.build_template(
        "t",
        sample_dashboard,
        required_tables=["generation_fuel"],
        variables=[{"name": "year", "default": "2020"}],
    )
    assert tpl["items"][0]["sql"].endswith("year = {{year}}")
    assert tpl["variables"][0] == {"name": "year", "default": "2020"}


def test_build_template_accepts_from_filename(sample_dashboard):
    tpl = dt.build_template(
        "t",
        sample_dashboard,
        required_tables=["generation_fuel"],
        variables=[{"name": "year", "default": "2020", "from_filename": r"(\d{4})"}],
    )
    assert tpl["variables"][0]["from_filename"] == r"(\d{4})"


def test_build_template_rejects_invalid_variable_name(sample_dashboard):
    with pytest.raises(dt.TemplateError, match="Invalid variable name"):
        dt.build_template(
            "t",
            sample_dashboard,
            required_tables=["generation_fuel"],
            variables=[{"name": "1bad", "default": "x"}],
        )


def test_build_template_rejects_invalid_regex(sample_dashboard):
    with pytest.raises(dt.TemplateError, match="from_filename regex"):
        dt.build_template(
            "t",
            sample_dashboard,
            required_tables=["generation_fuel"],
            variables=[{"name": "y", "default": "1", "from_filename": "([a-z"}],
        )


def test_resolve_variables_prefers_override():
    tpl = {"variables": [{"name": "year", "default": "2020", "from_filename": r"(\d{4})"}]}
    assert dt.resolve_variables(tpl, filename="gen_2021.parquet", overrides={"year": "1999"}) == {
        "year": "1999"
    }


def test_resolve_variables_uses_filename_regex():
    tpl = {"variables": [{"name": "year", "default": "2020", "from_filename": r"(\d{4})"}]}
    assert dt.resolve_variables(tpl, filename="gen_2021.parquet") == {"year": "2021"}


def test_resolve_variables_fails_on_regex_mismatch():
    tpl = {"variables": [{"name": "year", "default": "2020", "from_filename": r"(\d{4})"}]}
    with pytest.raises(dt.TemplateError, match="does not match regex"):
        dt.resolve_variables(tpl, filename="nodigits.parquet")


def test_resolve_variables_falls_back_to_default_without_regex():
    tpl = {"variables": [{"name": "region", "default": "CA"}]}
    assert dt.resolve_variables(tpl, filename="any.parquet") == {"region": "CA"}


async def test_apply_template_substitutes_variables(tmp_path, generation_parquet):
    out = tmp_path / "out.html"
    tpl = {
        "name": "t",
        "version": dt.TEMPLATE_VERSION,
        "required_tables": ["generation_fuel"],
        "variables": [{"name": "code", "default": "WND"}],
        "items": [
            {
                "id": 1,
                "type": "table",
                "title": "Rows",
                "sql": "SELECT * FROM generation_fuel WHERE energy_source_code = '{{code}}'",
            }
        ],
        "columns": 1,
        "filters": [],
    }
    result = await dt.apply_template(
        tpl,
        out,
        sources={"generation_fuel": generation_parquet},
        variables={"code": "SUN"},
    )
    assert result.ok, result.cards[0].error
    html = out.read_text(encoding="utf-8")
    assert "SUN" in html
    assert "WND" not in html


def test_cli_save_records_variables_and_rewrites_sql(project):
    _write_dashboard(
        project,
        {
            "items": [
                {
                    "id": 1,
                    "type": "table",
                    "title": "Y",
                    "sql": "SELECT * FROM generation_fuel WHERE year = 2020",
                }
            ],
            "columns": 1,
            "filters": [],
        },
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "templates",
            "save",
            "t",
            "--project-dir",
            str(project),
            "--var",
            "year=2020",
            "--var-from-filename",
            r"year=(\d{4})",
        ],
    )
    assert result.exit_code == 0, result.output
    loaded = dt.load_template("t", project)
    assert "{{year}}" in loaded["items"][0]["sql"]
    assert loaded["variables"][0]["name"] == "year"
    assert loaded["variables"][0]["default"] == "2020"
    assert loaded["variables"][0]["from_filename"] == r"(\d{4})"


def test_cli_apply_batch_extracts_variable_from_filename(project, tmp_path):
    pd = pytest.importorskip("pandas")
    for year, val in [(2020, 100.0), (2021, 111.0)]:
        pd.DataFrame(
            {"energy_source_code": ["WND"], "net_generation_mwh": [val], "year": [year]}
        ).to_parquet(tmp_path / f"gen_{year}.parquet")

    tpl = {
        "name": "v",
        "version": dt.TEMPLATE_VERSION,
        "required_tables": ["generation_fuel"],
        "variables": [{"name": "year", "default": "2020", "from_filename": r"(\d{4})"}],
        "items": [
            {
                "id": 1,
                "type": "table",
                "title": "Rows",
                "sql": "SELECT * FROM generation_fuel WHERE year = {{year}}",
            }
        ],
        "columns": 1,
        "filters": [],
    }
    dt.save_template(tpl, project)

    out_dir = tmp_path / "out"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "templates",
            "apply",
            "v",
            "--project-dir",
            str(project),
            "--table",
            f"generation_fuel={tmp_path}/gen_*.parquet",
            "--export-dir",
            str(out_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "111" in (out_dir / "gen_2021.html").read_text(encoding="utf-8")
    assert "100" in (out_dir / "gen_2020.html").read_text(encoding="utf-8")


def test_cli_apply_batch_fails_on_filename_regex_mismatch(project, tmp_path):
    pd = pytest.importorskip("pandas")
    p = tmp_path / "nodigits.parquet"
    pd.DataFrame({"energy_source_code": ["WND"], "net_generation_mwh": [1.0]}).to_parquet(p)
    p2 = tmp_path / "other.parquet"
    pd.DataFrame({"energy_source_code": ["WND"], "net_generation_mwh": [1.0]}).to_parquet(p2)

    tpl = {
        "name": "v",
        "version": dt.TEMPLATE_VERSION,
        "required_tables": ["generation_fuel"],
        "variables": [{"name": "year", "default": "2020", "from_filename": r"(\d{4})"}],
        "items": [
            {
                "id": 1,
                "type": "table",
                "title": "Rows",
                "sql": "SELECT * FROM generation_fuel WHERE '{{year}}' = '{{year}}'",
            }
        ],
        "columns": 1,
        "filters": [],
    }
    dt.save_template(tpl, project)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "templates",
            "apply",
            "v",
            "--project-dir",
            str(project),
            "--table",
            f"generation_fuel={tmp_path}/*.parquet",
            "--export-dir",
            str(tmp_path / "out"),
        ],
    )
    assert result.exit_code != 0
    assert "does not match regex" in result.output


def test_cli_apply_var_override_beats_filename(project, tmp_path):
    pd = pytest.importorskip("pandas")
    p = tmp_path / "gen_2020.parquet"
    pd.DataFrame(
        {"energy_source_code": ["WND"], "net_generation_mwh": [100.0], "year": [2099]}
    ).to_parquet(p)

    tpl = {
        "name": "v",
        "version": dt.TEMPLATE_VERSION,
        "required_tables": ["generation_fuel"],
        "variables": [{"name": "year", "default": "2020", "from_filename": r"(\d{4})"}],
        "items": [
            {
                "id": 1,
                "type": "table",
                "title": "Rows",
                "sql": "SELECT * FROM generation_fuel WHERE year = {{year}}",
            }
        ],
        "columns": 1,
        "filters": [],
    }
    dt.save_template(tpl, project)

    out = tmp_path / "out" / "gen_2020.html"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "templates",
            "apply",
            "v",
            "--project-dir",
            str(project),
            "--table",
            f"generation_fuel={p}",
            "--output",
            str(out),
            "--var",
            "year=2099",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "2099" in out.read_text(encoding="utf-8")
