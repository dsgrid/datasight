"""Tests for settings module."""

import os

import pytest

from datasight.exceptions import ConfigurationError
from datasight.settings import (
    Settings,
    _safe_int,
    capture_original_env,
    restore_original_env,
    _PROJECT_ENV_VARS,
)


class TestSafeInt:
    """Tests for _safe_int helper."""

    def test_valid_int(self):
        assert _safe_int("42", 0) == 42

    def test_empty_string_returns_default(self):
        assert _safe_int("", 99) == 99

    def test_invalid_string_returns_default(self):
        assert _safe_int("not-a-number", 123) == 123

    def test_whitespace_returns_default(self):
        assert _safe_int("  ", 5) == 5


class TestEnvIsolation:
    """Tests for capture_original_env and restore_original_env."""

    def test_capture_and_restore_preserves_shell_values(self):
        """Shell values should be restored after project switch."""
        # Simulate shell environment
        os.environ["ANTHROPIC_API_KEY"] = "shell_key"
        os.environ["DB_MODE"] = "duckdb"

        # Capture baseline
        capture_original_env()

        # Simulate loading project A's .env
        os.environ["ANTHROPIC_API_KEY"] = "project_a_key"
        os.environ["DB_MODE"] = "postgres"
        os.environ["POSTGRES_HOST"] = "localhost"

        # Restore should bring back shell values
        restore_original_env()

        assert os.environ.get("ANTHROPIC_API_KEY") == "shell_key"
        assert os.environ.get("DB_MODE") == "duckdb"
        assert os.environ.get("POSTGRES_HOST") is None  # Wasn't in shell

    def test_restore_removes_vars_not_in_baseline(self):
        """Vars added by project should be removed."""
        # Clear any existing values
        for var in ["NEW_PROJECT_VAR", "DB_PATH"]:
            os.environ.pop(var, None)

        capture_original_env()

        # Project adds new vars
        os.environ["DB_PATH"] = "/project/db.duckdb"

        restore_original_env()

        assert os.environ.get("DB_PATH") is None

    def test_project_secrets_dont_leak(self):
        """Secrets from project A shouldn't leak to project B."""
        # Start with no API key
        os.environ.pop("ANTHROPIC_API_KEY", None)
        capture_original_env()

        # Project A sets its key
        os.environ["ANTHROPIC_API_KEY"] = "secret_from_project_a"

        # Switch to project B (restore first)
        restore_original_env()

        # Project A's key should be gone
        assert os.environ.get("ANTHROPIC_API_KEY") is None

    @pytest.fixture(autouse=True)
    def cleanup_env(self):
        """Clean up env vars after each test."""
        yield
        for var in _PROJECT_ENV_VARS:
            os.environ.pop(var, None)


class TestSettingsFromEnv:
    """Tests for Settings.from_env()."""

    def test_invalid_db_mode_raises_error(self, monkeypatch):
        """Invalid DB_MODE should raise ConfigurationError."""
        monkeypatch.setenv("DB_MODE", "postgress")  # typo

        with pytest.raises(ConfigurationError) as exc_info:
            Settings.from_env()

        assert "Invalid DB_MODE" in str(exc_info.value)
        assert "postgress" in str(exc_info.value)
        assert "duckdb, sqlite, postgres, flightsql" in str(exc_info.value)

    def test_local_alias_for_duckdb(self, monkeypatch):
        """'local' should be accepted as alias for 'duckdb'."""
        monkeypatch.setenv("DB_MODE", "local")
        monkeypatch.setenv("DB_PATH", "/tmp/test.db")

        settings = Settings.from_env()

        assert settings.database.mode == "duckdb"

    def test_safe_int_for_ports(self, monkeypatch):
        """Invalid port values should use defaults instead of crashing."""
        monkeypatch.setenv("PORT", "not-a-port")
        monkeypatch.setenv("POSTGRES_PORT", "also-invalid")

        settings = Settings.from_env()

        assert settings.app.port == 8084  # default
        assert settings.database.postgres_port == 5432  # default

    def test_defaults_applied(self):
        """Default values should be applied when env vars not set."""
        # Use a clean environment
        settings = Settings.from_env()

        assert settings.llm.provider == "anthropic"
        assert settings.database.mode == "duckdb"
        assert settings.app.port == 8084

    @pytest.fixture(autouse=True)
    def clean_env(self, monkeypatch):
        """Ensure clean env for each test."""
        for var in _PROJECT_ENV_VARS:
            monkeypatch.delenv(var, raising=False)
