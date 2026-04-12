"""Shared helpers for test fixtures that need to isolate datasight env state."""

from __future__ import annotations

import os

# Env vars that datasight reads and that leak across tests when CLI commands
# invoke load_dotenv() or os.environ assignments directly. Any fixture that
# needs a clean, "no project loaded" environment should scrub these before
# instantiating TestClient(web_app.app).
DATASIGHT_ENV_VARS: tuple[str, ...] = (
    "DB_MODE",
    "DB_PATH",
    "LLM_PROVIDER",
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_MODEL",
    "ANTHROPIC_BASE_URL",
    "OLLAMA_MODEL",
    "OLLAMA_BASE_URL",
    "GITHUB_TOKEN",
    "GITHUB_MODEL",
    "FLIGHT_URI",
    "FLIGHT_TOKEN",
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_DATABASE",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_URL",
    "POSTGRES_SSLMODE",
    "DATASIGHT_AUTO_LOAD_PROJECT",
)


def scrub_datasight_env() -> None:
    """Remove all datasight-controlled env vars from os.environ."""
    for key in DATASIGHT_ENV_VARS:
        os.environ.pop(key, None)
