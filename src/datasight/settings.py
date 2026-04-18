"""
Centralized configuration for datasight.

Provides a single source of truth for all configuration values,
with support for environment variables and .env files.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from dotenv import load_dotenv

from datasight.exceptions import ConfigurationError


def _safe_int(value: str, default: int) -> int:
    """Parse an integer from a string, returning default if empty or invalid."""
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _safe_optional_float(value: str, default: float | None) -> float | None:
    """Parse a float from a string, returning default if empty or invalid.

    An explicit ``"none"`` / ``"off"`` / ``"disabled"`` value (case-insensitive)
    returns ``None`` to disable the associated feature.
    """
    if not value:
        return default
    if value.strip().lower() in {"none", "off", "disabled"}:
        return None
    try:
        return float(value)
    except ValueError:
        return default


def _safe_float(value: str, default: float) -> float:
    """Parse a float from a string, returning default if empty or invalid."""
    if not value:
        return default
    try:
        return float(value)
    except ValueError:
        return default


# All env vars that can be set in a project's .env file.
# These are restored to their original shell values before loading each project.
_PROJECT_ENV_VARS = [
    # Database settings
    "DB_MODE",
    "DB_PATH",
    "FLIGHT_SQL_URI",
    "FLIGHT_SQL_TOKEN",
    "FLIGHT_SQL_USERNAME",
    "FLIGHT_SQL_PASSWORD",
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_DATABASE",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_URL",
    "POSTGRES_SSLMODE",
    # LLM settings
    "LLM_PROVIDER",
    "LLM_TIMEOUT",
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_MODEL",
    "ANTHROPIC_BASE_URL",
    "OLLAMA_BASE_URL",
    "OLLAMA_MODEL",
    "GITHUB_TOKEN",
    "GITHUB_MODELS_MODEL",
    "GITHUB_MODELS_BASE_URL",
    "OPENAI_API_KEY",
    "OPENAI_MODEL",
    "OPENAI_BASE_URL",
    # App settings
    "PORT",
    "LOG_QUERIES",
    "QUERY_LOG_ENABLED",
    "CONFIRM_SQL",
    "EXPLAIN_SQL",
    "CLARIFY_SQL",
    "SHOW_PROVENANCE",
    "QUERY_LOG_PATH",
    "SQL_CACHE_MAX_BYTES",
    "MAX_COST_USD_PER_TURN",
    "MAX_OUTPUT_TOKENS",
    # Project-specific file paths
    "SCHEMA_DESCRIPTION_PATH",
    "EXAMPLE_QUERIES_PATH",
]

# Captured after root .env is loaded - the baseline environment that projects
# should restore to when switching. Populated by capture_original_env().
_original_env: dict[str, str] = {}


def capture_original_env() -> None:
    """Capture the current environment as the baseline for project switching.

    Call this AFTER loading the root .env file but BEFORE loading any
    project-specific .env files. This establishes the baseline that
    restore_original_env() will restore to.
    """
    global _original_env
    _original_env = {var: os.environ[var] for var in _PROJECT_ENV_VARS if var in os.environ}


def restore_original_env() -> None:
    """Restore project-related env vars to their baseline values.

    Call this before loading a new project's .env to prevent settings
    from leaking between projects. Vars that were in the baseline are
    restored; vars that weren't are removed.
    """
    for var in _PROJECT_ENV_VARS:
        if var in _original_env:
            os.environ[var] = _original_env[var]
        else:
            os.environ.pop(var, None)


LLMProvider = Literal["anthropic", "ollama", "github", "openai"]
DBMode = Literal["duckdb", "sqlite", "postgres", "flightsql"]

# Mapping from database mode to SQL dialect for query generation
DB_MODE_TO_DIALECT: dict[str, str] = {
    "duckdb": "duckdb",
    "sqlite": "sqlite",
    "postgres": "postgres",
    "flightsql": "duckdb",  # Flight SQL uses DuckDB dialect
}


@dataclass
class LLMSettings:
    """Settings for LLM providers."""

    provider: LLMProvider = "anthropic"

    # Anthropic settings
    anthropic_api_key: str = ""
    anthropic_model: str = "claude-haiku-4-5-20251001"
    anthropic_base_url: str | None = None

    # Ollama settings
    ollama_base_url: str = "http://localhost:11434/v1"
    ollama_model: str = "qwen3:8b"

    # GitHub Models settings
    github_token: str = ""
    github_models_model: str = "gpt-4o"
    github_models_base_url: str = "https://models.github.ai/inference"

    # OpenAI settings
    openai_api_key: str = ""
    openai_model: str = "gpt-4o-mini"
    openai_base_url: str = "https://api.openai.com/v1"

    # Per-request HTTP timeout (seconds).
    timeout: float = 120.0

    @property
    def model(self) -> str:
        """Get the model name for the current provider."""
        match self.provider:
            case "ollama":
                return self.ollama_model
            case "github":
                return self.github_models_model
            case "openai":
                return self.openai_model
            case _:
                return self.anthropic_model

    @property
    def api_key(self) -> str:
        """Get the API key for the current provider."""
        match self.provider:
            case "ollama":
                return "ollama"
            case "github":
                return self.github_token
            case "openai":
                return self.openai_api_key
            case _:
                return self.anthropic_api_key

    @property
    def base_url(self) -> str | None:
        """Get the base URL for the current provider."""
        match self.provider:
            case "ollama":
                return self.ollama_base_url
            case "github":
                return self.github_models_base_url
            case "openai":
                return self.openai_base_url
            case _:
                return self.anthropic_base_url


@dataclass
class DatabaseSettings:
    """Settings for database connections."""

    mode: DBMode = "duckdb"
    path: str = "database.duckdb"

    # Flight SQL settings
    flight_uri: str = "grpc://localhost:31337"
    flight_token: str | None = None
    flight_username: str | None = None
    flight_password: str | None = None

    # PostgreSQL settings
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_database: str = ""
    postgres_user: str = ""
    postgres_password: str = ""
    postgres_url: str = ""
    postgres_sslmode: str = "prefer"

    @property
    def sql_dialect(self) -> str:
        """Get the SQL dialect for the current mode."""
        return DB_MODE_TO_DIALECT.get(self.mode, "duckdb")


@dataclass
class AppSettings:
    """Application-level settings."""

    port: int = 8084
    confirm_sql: bool = False
    explain_sql: bool = False
    clarify_sql: bool = True
    show_provenance: bool = False
    max_history_pairs: int = 10
    response_cache_max: int = 100
    sql_cache_max_bytes: int = 1 << 30  # 1 GiB; 0 disables
    max_cost_usd_per_turn: float | None = 1.0  # None disables the per-turn LLM cost budget
    max_output_tokens: int = 4096  # Output-token budget per LLM call


@dataclass
class Settings:
    """Root settings container."""

    llm: LLMSettings = field(default_factory=LLMSettings)
    database: DatabaseSettings = field(default_factory=DatabaseSettings)
    app: AppSettings = field(default_factory=AppSettings)

    @classmethod
    def from_env(cls, env_path: str | Path | None = None, *, override: bool = False) -> Settings:
        """Load settings from environment variables.

        Parameters
        ----------
        env_path:
            Optional path to a .env file. If provided, it will be loaded
            before reading environment variables.
        override:
            If False (default), existing env vars (e.g. shell exports) take
            precedence over .env file values - use for CLI commands where
            users may want to override project config via shell.
            If True, .env file values override existing env vars - use for
            web app project switching where project config should win.

        Returns
        -------
        A Settings instance populated from the environment.
        """
        if env_path:
            load_dotenv(env_path, override=override)

        # Parse and validate LLM provider
        llm_provider_raw = os.environ.get("LLM_PROVIDER", "anthropic")
        match llm_provider_raw:
            case "anthropic":
                llm_provider: LLMProvider = "anthropic"
            case "ollama":
                llm_provider = "ollama"
            case "github":
                llm_provider = "github"
            case "openai":
                llm_provider = "openai"
            case _:
                valid_providers = "anthropic, ollama, github, openai"
                raise ConfigurationError(
                    f"Invalid LLM_PROVIDER: {llm_provider_raw!r}. "
                    f"Valid providers: {valid_providers}"
                )

        # Normalize DB_MODE (accept 'local' as alias for 'duckdb')
        db_mode_raw = os.environ.get("DB_MODE", "duckdb")
        match db_mode_raw:
            case "local" | "duckdb":
                db_mode: DBMode = "duckdb"
            case "sqlite":
                db_mode = "sqlite"
            case "postgres":
                db_mode = "postgres"
            case "flightsql":
                db_mode = "flightsql"
            case _:
                valid_modes = "duckdb, sqlite, postgres, flightsql"
                raise ConfigurationError(
                    f"Invalid DB_MODE: {db_mode_raw!r}. Valid modes: {valid_modes}"
                )

        return cls(
            llm=LLMSettings(
                provider=llm_provider,
                anthropic_api_key=os.environ.get("ANTHROPIC_API_KEY", ""),
                anthropic_model=os.environ.get("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001"),
                anthropic_base_url=os.environ.get("ANTHROPIC_BASE_URL"),
                ollama_base_url=os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434/v1"),
                ollama_model=os.environ.get("OLLAMA_MODEL", "qwen3:8b"),
                github_token=os.environ.get("GITHUB_TOKEN", ""),
                github_models_model=os.environ.get("GITHUB_MODELS_MODEL", "gpt-4o"),
                github_models_base_url=os.environ.get(
                    "GITHUB_MODELS_BASE_URL", "https://models.github.ai/inference"
                ),
                openai_api_key=os.environ.get("OPENAI_API_KEY", ""),
                openai_model=os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
                openai_base_url=os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1"),
                timeout=_safe_float(os.environ.get("LLM_TIMEOUT", ""), 120.0),
            ),
            database=DatabaseSettings(
                mode=db_mode,
                path=os.environ.get("DB_PATH", "database.duckdb"),
                flight_uri=os.environ.get("FLIGHT_SQL_URI", "grpc://localhost:31337"),
                flight_token=os.environ.get("FLIGHT_SQL_TOKEN"),
                flight_username=os.environ.get("FLIGHT_SQL_USERNAME"),
                flight_password=os.environ.get("FLIGHT_SQL_PASSWORD"),
                postgres_host=os.environ.get("POSTGRES_HOST", "localhost"),
                postgres_port=_safe_int(os.environ.get("POSTGRES_PORT", ""), 5432),
                postgres_database=os.environ.get("POSTGRES_DATABASE", ""),
                postgres_user=os.environ.get("POSTGRES_USER", ""),
                postgres_password=os.environ.get("POSTGRES_PASSWORD", ""),
                postgres_url=os.environ.get("POSTGRES_URL", ""),
                postgres_sslmode=os.environ.get("POSTGRES_SSLMODE", "prefer"),
            ),
            app=AppSettings(
                port=_safe_int(os.environ.get("PORT", ""), 8084),
                confirm_sql=os.environ.get("CONFIRM_SQL", "").lower() in ("1", "true", "yes"),
                explain_sql=os.environ.get("EXPLAIN_SQL", "").lower() in ("1", "true", "yes"),
                clarify_sql=os.environ.get("CLARIFY_SQL", "1").lower() not in ("0", "false", "no"),
                show_provenance=os.environ.get("SHOW_PROVENANCE", "").lower()
                in ("1", "true", "yes"),
                sql_cache_max_bytes=_safe_int(os.environ.get("SQL_CACHE_MAX_BYTES", ""), 1 << 30),
                max_cost_usd_per_turn=_safe_optional_float(
                    os.environ.get("MAX_COST_USD_PER_TURN", ""), 1.0
                ),
                max_output_tokens=_safe_int(os.environ.get("MAX_OUTPUT_TOKENS", ""), 4096),
            ),
        )

    def validate(self) -> list[str]:
        """Validate settings and return a list of error messages (empty if valid)."""
        errors: list[str] = []

        # Check LLM API key
        if self.llm.provider == "anthropic" and not self.llm.anthropic_api_key:
            errors.append("ANTHROPIC_API_KEY is not set")
        elif self.llm.provider == "github" and not self.llm.github_token:
            errors.append("GITHUB_TOKEN is not set")
        elif self.llm.provider == "openai" and not self.llm.openai_api_key:
            errors.append("OPENAI_API_KEY is not set")

        # Check database path for file-based databases
        if self.database.mode in ("duckdb", "sqlite"):
            if not self.database.path:
                errors.append("DB_PATH is not set")

        return errors
