"""Tests for the recent_projects module."""

import json
from pathlib import Path

import pytest

from datasight.recent_projects import (
    MAX_RECENT_PROJECTS,
    add_recent_project,
    get_project_name,
    load_recent_projects,
    remove_recent_project,
    save_recent_projects,
    validate_project_dir,
)


@pytest.fixture
def mock_recent_projects_file(tmp_path, monkeypatch):
    """Mock the recent projects file location."""
    mock_dir = tmp_path / ".datasight"
    mock_file = mock_dir / "recent_projects.json"
    monkeypatch.setattr("datasight.recent_projects.RECENT_PROJECTS_DIR", mock_dir)
    monkeypatch.setattr("datasight.recent_projects.RECENT_PROJECTS_FILE", mock_file)
    return mock_file


def test_load_recent_projects_empty(mock_recent_projects_file):
    """Test loading when no file exists."""
    assert load_recent_projects() == []


def test_save_and_load_recent_projects(mock_recent_projects_file):
    """Test saving and loading projects."""
    projects = [
        {"path": "/path/to/project1", "last_used": "2026-04-05T10:00:00Z"},
        {"path": "/path/to/project2", "last_used": "2026-04-04T09:00:00Z"},
    ]
    save_recent_projects(projects)

    loaded = load_recent_projects()
    assert len(loaded) == 2
    # Should be sorted by last_used descending
    assert loaded[0]["path"] == "/path/to/project1"
    assert loaded[1]["path"] == "/path/to/project2"


def test_add_recent_project(mock_recent_projects_file, tmp_path):
    """Test adding a project to the recent list."""
    project_path = str(tmp_path / "my_project")
    Path(project_path).mkdir()

    result = add_recent_project(project_path)

    assert len(result) == 1
    assert result[0]["path"] == project_path
    assert "last_used" in result[0]


def test_add_recent_project_updates_existing(mock_recent_projects_file, tmp_path):
    """Test that adding an existing project updates its last_used time."""
    project_path = str(tmp_path / "my_project")
    Path(project_path).mkdir()

    add_recent_project(project_path)
    # Add another project
    other_path = str(tmp_path / "other_project")
    Path(other_path).mkdir()
    add_recent_project(other_path)

    # Add the first project again
    result = add_recent_project(project_path)

    assert len(result) == 2
    # The first project should now be at the top
    assert result[0]["path"] == project_path


def test_add_recent_project_limits_to_max(mock_recent_projects_file, tmp_path):
    """Test that the recent list is capped at MAX_RECENT_PROJECTS."""
    for i in range(MAX_RECENT_PROJECTS + 5):
        path = str(tmp_path / f"project_{i}")
        Path(path).mkdir()
        add_recent_project(path)

    result = load_recent_projects()
    assert len(result) == MAX_RECENT_PROJECTS


def test_remove_recent_project(mock_recent_projects_file, tmp_path):
    """Test removing a project from the recent list."""
    project_path = str(tmp_path / "my_project")
    Path(project_path).mkdir()

    add_recent_project(project_path)
    assert len(load_recent_projects()) == 1

    remove_recent_project(project_path)
    assert len(load_recent_projects()) == 0


def test_validate_project_dir_valid(tmp_path):
    """Test validation of a valid project directory."""
    (tmp_path / "schema_description.md").write_text("# Schema", encoding="utf-8")

    is_valid, error = validate_project_dir(str(tmp_path))

    assert is_valid is True
    assert error == ""


def test_validate_project_dir_missing_schema(tmp_path):
    """Test validation fails when schema_description.md is missing."""
    is_valid, error = validate_project_dir(str(tmp_path))

    assert is_valid is False
    assert "schema_description.md" in error


def test_validate_project_dir_not_exists():
    """Test validation fails for non-existent directory."""
    is_valid, error = validate_project_dir("/nonexistent/path")

    assert is_valid is False
    assert "does not exist" in error


def test_validate_project_dir_is_file(tmp_path):
    """Test validation fails when path is a file, not a directory."""
    file_path = tmp_path / "some_file.txt"
    file_path.write_text("content", encoding="utf-8")

    is_valid, error = validate_project_dir(str(file_path))

    assert is_valid is False
    assert "Not a directory" in error


def test_get_project_name():
    """Test getting project display name."""
    assert get_project_name("/path/to/my_project") == "my_project"
    assert get_project_name("/") == ""
    assert get_project_name("relative/path") == "path"


def test_load_recent_projects_handles_corrupt_json(mock_recent_projects_file):
    """Test that corrupt JSON files are handled gracefully."""
    mock_recent_projects_file.parent.mkdir(parents=True, exist_ok=True)
    mock_recent_projects_file.write_text("not valid json {{{", encoding="utf-8")

    assert load_recent_projects() == []


def test_load_recent_projects_handles_wrong_type(mock_recent_projects_file):
    """Test that wrong data type is handled gracefully."""
    mock_recent_projects_file.parent.mkdir(parents=True, exist_ok=True)
    mock_recent_projects_file.write_text(json.dumps({"not": "a list"}), encoding="utf-8")

    assert load_recent_projects() == []
