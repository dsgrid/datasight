"""Shared helpers used by both ``datasight.cli`` and its subcommand modules.

Subcommand modules under ``datasight.cli_commands`` were originally
importing these helpers from ``datasight.cli``, which made the static
import graph cyclic (``cli`` registers its subcommands, and the
subcommands turned around and imported helpers from ``cli``). The
runtime didn't care — ``_register_commands`` is lazy — but CodeQL
flagged the cycle and any new subcommand inherits the issue.

Pulling these tiny helpers into a dedicated module that imports nothing
from the CLI package breaks the back-edge for callers that switch over.
"""

from __future__ import annotations

import os
from pathlib import Path
from textwrap import dedent

from datasight.settings import Settings, load_global_env


def _epilog(text: str) -> str:
    """Normalize Click epilog text defined in indented decorators."""
    # Rich Click reflows epilog paragraphs. Treat each authored line as
    # its own paragraph so examples remain scannable in terminal help.
    return "\n\n".join(line.rstrip() for line in dedent(text).strip().splitlines())


def _resolve_settings(
    project_dir: str,
    model_override: str | None = None,
) -> tuple[Settings, str]:
    """Load settings from a project directory and apply CLI overrides.

    Parameters
    ----------
    project_dir:
        Path to the project directory containing ``.env``.
    model_override:
        Optional model name to override the value from settings.

    Returns
    -------
    Tuple of ``(settings, resolved_model)``.
    """
    from dotenv import load_dotenv

    env_path = os.path.join(project_dir, ".env")
    if os.path.exists(env_path):
        load_dotenv(env_path, override=False)
    load_global_env(override=False)
    settings = Settings.from_env()
    resolved_model = model_override if model_override else settings.llm.model
    return settings, resolved_model


def _resolve_db_path(settings: Settings, project_dir: str) -> str:
    """Resolve the configured database path, making relative paths absolute.

    Returns an empty string for non-file backends so callers can pass
    the result straight through to APIs that accept ``""`` as "no path."
    """
    if settings.database.mode not in ("duckdb", "sqlite"):
        return ""
    raw_path = settings.database.path
    if os.path.isabs(raw_path):
        return raw_path
    return str(Path(project_dir) / raw_path)
