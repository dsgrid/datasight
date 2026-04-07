"""
Manage recent project directories for the project switcher UI.

Stores a list of recently-used project directories in ~/.datasight/recent_projects.json.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

RECENT_PROJECTS_DIR = Path.home() / ".datasight"
RECENT_PROJECTS_FILE = RECENT_PROJECTS_DIR / "recent_projects.json"
MAX_RECENT_PROJECTS = 10


def _ensure_dir() -> None:
    """Ensure ~/.datasight directory exists."""
    RECENT_PROJECTS_DIR.mkdir(parents=True, exist_ok=True)


def load_recent_projects() -> list[dict[str, str]]:
    """Load the list of recent projects, sorted by last_used (most recent first)."""
    if not RECENT_PROJECTS_FILE.exists():
        return []
    try:
        data = json.loads(RECENT_PROJECTS_FILE.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            return []
        # Sort by last_used descending
        data.sort(key=lambda x: x.get("last_used", ""), reverse=True)
        return data
    except (json.JSONDecodeError, OSError):
        return []


def save_recent_projects(projects: list[dict[str, str]]) -> None:
    """Save the list of recent projects."""
    _ensure_dir()
    # Keep only most recent entries
    projects = projects[:MAX_RECENT_PROJECTS]
    RECENT_PROJECTS_FILE.write_text(json.dumps(projects, indent=2), encoding="utf-8")


def add_recent_project(project_path: str) -> list[dict[str, str]]:
    """Add or update a project in the recent list. Returns the updated list."""
    project_path = str(Path(project_path).resolve())
    projects = load_recent_projects()

    # Remove existing entry for this path if present
    projects = [p for p in projects if p.get("path") != project_path]

    # Add new entry at the front
    projects.insert(
        0,
        {
            "path": project_path,
            "last_used": datetime.now(timezone.utc).isoformat(),
        },
    )

    # Trim to max size
    projects = projects[:MAX_RECENT_PROJECTS]

    save_recent_projects(projects)
    return projects


def validate_project_dir(project_path: str) -> tuple[bool, str]:
    """
    Validate that a directory is a valid datasight project.

    Returns (is_valid, error_message).
    """
    path = Path(project_path)

    if not path.exists():
        return False, f"Directory does not exist: {project_path}"

    if not path.is_dir():
        return False, f"Not a directory: {project_path}"

    schema_file = path / "schema_description.md"
    if not schema_file.exists():
        return False, f"Missing schema_description.md in {project_path}"

    return True, ""


def get_project_name(project_path: str) -> str:
    """Get a display name for a project (basename of the path)."""
    return Path(project_path).name


def remove_recent_project(project_path: str) -> list[dict[str, str]]:
    """Remove a project from the recent list. Returns the updated list."""
    project_path = str(Path(project_path).resolve())
    projects = load_recent_projects()
    projects = [p for p in projects if p.get("path") != project_path]
    save_recent_projects(projects)
    return projects
