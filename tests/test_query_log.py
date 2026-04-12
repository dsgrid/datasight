"""Tests for QueryLogger (datasight.query_log)."""

from __future__ import annotations

import json


from datasight.query_log import QueryLogger


def test_log_writes_jsonl_entry(tmp_path):
    log_path = tmp_path / "subdir" / "queries.jsonl"
    logger = QueryLogger(log_path)
    logger.log(
        session_id="s1",
        user_question="q?",
        tool="sql",
        sql="SELECT 1",
        execution_time_ms=12.345,
        row_count=1,
        column_count=1,
    )
    lines = log_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    entry = json.loads(lines[0])
    assert entry["session_id"] == "s1"
    assert entry["execution_time_ms"] == 12.35
    assert entry["sql"] == "SELECT 1"
    assert entry["error"] is None


def test_log_cost_writes_entry(tmp_path):
    log_path = tmp_path / "queries.jsonl"
    logger = QueryLogger(log_path)
    logger.log_cost(
        session_id="s1",
        user_question="q?",
        api_calls=2,
        input_tokens=100,
        output_tokens=50,
        estimated_cost=0.01,
    )
    entries = logger.read_recent()
    assert len(entries) == 1
    assert entries[0]["type"] == "cost"
    assert entries[0]["api_calls"] == 2
    assert entries[0]["estimated_cost"] == 0.01


def test_read_recent_missing_file_returns_empty(tmp_path):
    log_path = tmp_path / "missing.jsonl"
    logger = QueryLogger(log_path)
    # Re-create the logger pointing at a truly missing file (parent was created)
    log_path.unlink(missing_ok=True)
    assert logger.read_recent() == []


def test_read_recent_tolerates_malformed_lines(tmp_path):
    log_path = tmp_path / "log.jsonl"
    # Create the file with a malformed line interleaved with valid entries
    log_path.write_text(
        '{"timestamp": "t1", "session_id": "s1"}\n'
        "this is not json\n"
        '{"timestamp": "t2", "session_id": "s2"}\n',
        encoding="utf-8",
    )
    logger = QueryLogger(log_path)
    entries = logger.read_recent()
    assert [e["session_id"] for e in entries] == ["s1", "s2"]


def test_read_recent_truncates_to_n(tmp_path):
    log_path = tmp_path / "log.jsonl"
    logger = QueryLogger(log_path)
    for i in range(5):
        logger.log(
            session_id=f"s{i}",
            user_question="q",
            tool="sql",
            sql="SELECT 1",
            execution_time_ms=0.0,
        )
    recent = logger.read_recent(n=2)
    assert [e["session_id"] for e in recent] == ["s3", "s4"]


def test_init_tolerates_unwritable_parent(tmp_path):
    # Create a regular file and try to use a path *under* it as the log dir,
    # which forces mkdir to raise OSError (NotADirectoryError is a subclass).
    blocker = tmp_path / "not-a-dir"
    blocker.write_text("blocker", encoding="utf-8")
    log_path = blocker / "nested" / "log.jsonl"
    # Should not raise — OSError is swallowed with a warning
    logger = QueryLogger(log_path)
    assert logger.path == log_path


def test_log_tolerates_oserror(tmp_path):
    # Same trick: path where open() will fail with OSError (parent is a file).
    blocker = tmp_path / "blocker"
    blocker.write_text("x", encoding="utf-8")
    log_path = blocker / "log.jsonl"
    logger = QueryLogger(log_path)
    # Must not raise, even though the open() call in log() will fail.
    logger.log(
        session_id="s1",
        user_question="q",
        tool="sql",
        sql="SELECT 1",
        execution_time_ms=1.0,
    )
    logger.log_cost(
        session_id="s1",
        user_question="q",
        api_calls=1,
        input_tokens=1,
        output_tokens=1,
    )


def test_read_recent_returns_all_when_fewer_than_n(tmp_path):
    log_path = tmp_path / "log.jsonl"
    logger = QueryLogger(log_path)
    logger.log(
        session_id="only",
        user_question="q",
        tool="sql",
        sql="SELECT 1",
        execution_time_ms=0.0,
    )
    entries = logger.read_recent(n=50)
    assert len(entries) == 1
