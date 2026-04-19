"""Tests for the `datasight config` subcommands and global env loading."""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from datasight.cli import cli
from datasight.settings import _PROJECT_ENV_VARS, Settings, global_env_path, load_global_env


@pytest.fixture
def isolated_xdg(tmp_path, monkeypatch):
    """Point XDG_CONFIG_HOME at a temp dir so global config writes are isolated."""
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    return tmp_path / "xdg"


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    """Scrub project env vars so each test starts from a clean slate."""
    for key in _PROJECT_ENV_VARS:
        monkeypatch.delenv(key, raising=False)


# ---------------------------------------------------------------------------
# global_env_path
# ---------------------------------------------------------------------------


def test_global_env_path_uses_xdg_config_home(monkeypatch, tmp_path):
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    assert global_env_path() == tmp_path / "xdg" / "datasight" / ".env"


def test_global_env_path_falls_back_to_home_config(monkeypatch, tmp_path):
    monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    assert global_env_path() == tmp_path / ".config" / "datasight" / ".env"


def test_load_global_env_returns_false_when_missing(isolated_xdg):
    assert load_global_env() is False


def test_load_global_env_loads_values(isolated_xdg):
    env = isolated_xdg / "datasight" / ".env"
    env.parent.mkdir(parents=True)
    env.write_text("ANTHROPIC_API_KEY=from-global\n", encoding="utf-8")

    assert load_global_env() is True
    import os

    assert os.environ.get("ANTHROPIC_API_KEY") == "from-global"


# ---------------------------------------------------------------------------
# datasight config init
# ---------------------------------------------------------------------------


def test_config_init_creates_global_file(isolated_xdg):
    runner = CliRunner()
    result = runner.invoke(cli, ["config", "init"])
    assert result.exit_code == 0, result.output

    dest = isolated_xdg / "datasight" / ".env"
    assert dest.exists()
    text = dest.read_text(encoding="utf-8")
    assert "ANTHROPIC_API_KEY" in text
    assert "OPENAI_API_KEY" in text
    assert "GITHUB_TOKEN" in text


def test_config_init_does_not_overwrite_by_default(isolated_xdg):
    dest = isolated_xdg / "datasight" / ".env"
    dest.parent.mkdir(parents=True)
    dest.write_text("ANTHROPIC_API_KEY=existing\n", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(cli, ["config", "init"])
    assert result.exit_code == 0
    assert "already exists" in result.output
    assert dest.read_text(encoding="utf-8") == "ANTHROPIC_API_KEY=existing\n"


def test_config_init_overwrite_flag_replaces(isolated_xdg):
    dest = isolated_xdg / "datasight" / ".env"
    dest.parent.mkdir(parents=True)
    dest.write_text("ANTHROPIC_API_KEY=existing\n", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(cli, ["config", "init", "--overwrite"])
    assert result.exit_code == 0
    text = dest.read_text(encoding="utf-8")
    assert "existing" not in text
    assert "ANTHROPIC_API_KEY" in text  # template placeholder restored


# ---------------------------------------------------------------------------
# datasight config show
# ---------------------------------------------------------------------------


def test_config_show_reports_missing_global(isolated_xdg, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(cli, ["config", "show"])
    assert result.exit_code == 0, result.output
    assert "Global:" in result.output
    assert "(missing)" in result.output
    assert "no datasight project detected" in result.output


def test_config_show_reads_global_api_key(isolated_xdg, tmp_path, monkeypatch):
    env = isolated_xdg / "datasight" / ".env"
    env.parent.mkdir(parents=True)
    env.write_text("ANTHROPIC_API_KEY=sk-abcdefghijkl\n", encoding="utf-8")

    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(cli, ["config", "show"])
    assert result.exit_code == 0, result.output
    assert "(exists)" in result.output
    # Last 4 chars should be visible; full key should not.
    assert "…ijkl" in result.output
    assert "sk-abcdefghijkl" not in result.output


# ---------------------------------------------------------------------------
# Loader precedence: shell > project > global
# ---------------------------------------------------------------------------


def test_project_env_overrides_global(isolated_xdg, tmp_path):
    """A project .env value beats the user-global .env value."""
    global_env = isolated_xdg / "datasight" / ".env"
    global_env.parent.mkdir(parents=True)
    global_env.write_text("ANTHROPIC_API_KEY=global-key\n", encoding="utf-8")

    project = tmp_path / "proj"
    project.mkdir()
    (project / ".env").write_text("ANTHROPIC_API_KEY=project-key\n", encoding="utf-8")

    # Mirror _resolve_settings: project first, then global with override=False.
    from dotenv import load_dotenv

    load_dotenv(project / ".env", override=False)
    load_global_env(override=False)
    settings = Settings.from_env()

    assert settings.llm.anthropic_api_key == "project-key"


def test_global_env_fills_when_project_missing(isolated_xdg, tmp_path):
    """Global value is used when project .env doesn't set the key."""
    global_env = isolated_xdg / "datasight" / ".env"
    global_env.parent.mkdir(parents=True)
    global_env.write_text("ANTHROPIC_API_KEY=global-key\n", encoding="utf-8")

    project = tmp_path / "proj"
    project.mkdir()
    (project / ".env").write_text(
        "LLM_PROVIDER=anthropic\nDB_MODE=duckdb\nDB_PATH=./db.duckdb\n",
        encoding="utf-8",
    )

    from dotenv import load_dotenv

    load_dotenv(project / ".env", override=False)
    load_global_env(override=False)
    settings = Settings.from_env()

    assert settings.llm.anthropic_api_key == "global-key"


def test_shell_env_beats_both(isolated_xdg, tmp_path, monkeypatch):
    """Shell-exported var beats project .env beats global .env."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "shell-key")

    global_env = isolated_xdg / "datasight" / ".env"
    global_env.parent.mkdir(parents=True)
    global_env.write_text("ANTHROPIC_API_KEY=global-key\n", encoding="utf-8")

    project = tmp_path / "proj"
    project.mkdir()
    (project / ".env").write_text("ANTHROPIC_API_KEY=project-key\n", encoding="utf-8")

    from dotenv import load_dotenv

    load_dotenv(project / ".env", override=False)
    load_global_env(override=False)
    settings = Settings.from_env()

    assert settings.llm.anthropic_api_key == "shell-key"


# ---------------------------------------------------------------------------
# Project env.template no longer ships an uncommented placeholder API key
# ---------------------------------------------------------------------------


def test_project_template_does_not_uncomment_api_key():
    template = Path(__file__).resolve().parents[1] / "src/datasight/templates/env.template"
    text = template.read_text(encoding="utf-8")
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("#") or not stripped:
            continue
        # DB_MODE and DB_PATH are the only uncommented lines in the project
        # template — secrets must stay commented so users are nudged toward
        # ~/.config/datasight/.env.
        assert "API_KEY" not in stripped, f"unexpected uncommented secret line: {line!r}"
        assert "TOKEN" not in stripped, f"unexpected uncommented secret line: {line!r}"
