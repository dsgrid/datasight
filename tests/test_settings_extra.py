"""Extra tests for datasight.settings covering provider/mode branches."""

from __future__ import annotations

import pytest

from datasight.exceptions import ConfigurationError
from datasight.settings import (
    DatabaseSettings,
    LLMSettings,
    Settings,
    _PROJECT_ENV_VARS,
)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for var in _PROJECT_ENV_VARS:
        monkeypatch.delenv(var, raising=False)


# ---------------------------------------------------------------------------
# LLMSettings property branches
# ---------------------------------------------------------------------------


class TestLLMSettingsProperties:
    def test_ollama_provider_properties(self):
        s = LLMSettings(
            provider="ollama",
            ollama_model="llama3",
            ollama_base_url="http://localhost:11434/v1",
        )
        assert s.model == "llama3"
        assert s.api_key == "ollama"
        assert s.base_url == "http://localhost:11434/v1"

    def test_github_provider_properties(self):
        s = LLMSettings(
            provider="github",
            github_token="tok",
            github_models_model="gpt-4o-mini",
            github_models_base_url="https://github.example/models",
        )
        assert s.model == "gpt-4o-mini"
        assert s.api_key == "tok"
        assert s.base_url == "https://github.example/models"

    def test_anthropic_provider_properties(self):
        s = LLMSettings(
            provider="anthropic",
            anthropic_api_key="key",
            anthropic_model="claude",
            anthropic_base_url=None,
        )
        assert s.model == "claude"
        assert s.api_key == "key"
        assert s.base_url is None


# ---------------------------------------------------------------------------
# Settings.from_env — DB_MODE branches
# ---------------------------------------------------------------------------


class TestDBModeBranches:
    def test_sqlite_mode(self, monkeypatch):
        monkeypatch.setenv("DB_MODE", "sqlite")
        monkeypatch.setenv("DB_PATH", "/tmp/foo.sqlite")
        s = Settings.from_env()
        assert s.database.mode == "sqlite"
        assert s.database.sql_dialect == "sqlite"

    def test_postgres_mode(self, monkeypatch):
        monkeypatch.setenv("DB_MODE", "postgres")
        s = Settings.from_env()
        assert s.database.mode == "postgres"
        assert s.database.sql_dialect == "postgres"

    def test_flightsql_mode(self, monkeypatch):
        monkeypatch.setenv("DB_MODE", "flightsql")
        s = Settings.from_env()
        assert s.database.mode == "flightsql"
        assert s.database.sql_dialect == "duckdb"

    def test_invalid_mode_raises(self, monkeypatch):
        monkeypatch.setenv("DB_MODE", "cassandra")
        with pytest.raises(ConfigurationError):
            Settings.from_env()


# ---------------------------------------------------------------------------
# Settings.from_env — LLM provider branches
# ---------------------------------------------------------------------------


class TestLLMProviderBranches:
    def test_ollama_provider(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "ollama")
        monkeypatch.setenv("OLLAMA_MODEL", "qwen")
        monkeypatch.setenv("DB_PATH", "/tmp/x.duckdb")
        s = Settings.from_env()
        assert s.llm.provider == "ollama"
        assert s.llm.model == "qwen"

    def test_github_provider(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "github")
        monkeypatch.setenv("GITHUB_TOKEN", "gt")
        monkeypatch.setenv("DB_PATH", "/tmp/x.duckdb")
        s = Settings.from_env()
        assert s.llm.provider == "github"
        assert s.llm.api_key == "gt"

    def test_unknown_provider_raises(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "mystery-llm")
        with pytest.raises(ConfigurationError, match="Invalid LLM_PROVIDER"):
            Settings.from_env()


# ---------------------------------------------------------------------------
# Settings.validate()
# ---------------------------------------------------------------------------


class TestValidate:
    def test_validate_anthropic_missing_key(self):
        s = Settings(
            llm=LLMSettings(provider="anthropic", anthropic_api_key=""),
            database=DatabaseSettings(mode="duckdb", path="/tmp/x.duckdb"),
        )
        errors = s.validate()
        assert any("ANTHROPIC_API_KEY" in e for e in errors)

    def test_validate_github_missing_token(self):
        s = Settings(
            llm=LLMSettings(provider="github", github_token=""),
            database=DatabaseSettings(mode="duckdb", path="/tmp/x.duckdb"),
        )
        errors = s.validate()
        assert any("GITHUB_TOKEN" in e for e in errors)

    def test_validate_duckdb_missing_path(self):
        s = Settings(
            llm=LLMSettings(provider="anthropic", anthropic_api_key="ok"),
            database=DatabaseSettings(mode="duckdb", path=""),
        )
        errors = s.validate()
        assert any("DB_PATH" in e for e in errors)

    def test_validate_sqlite_missing_path(self):
        s = Settings(
            llm=LLMSettings(provider="anthropic", anthropic_api_key="ok"),
            database=DatabaseSettings(mode="sqlite", path=""),
        )
        errors = s.validate()
        assert any("DB_PATH" in e for e in errors)

    def test_validate_ok_for_ollama_postgres(self):
        s = Settings(
            llm=LLMSettings(provider="ollama"),
            database=DatabaseSettings(mode="postgres", path=""),
        )
        assert s.validate() == []
