"""
Query logger for datasight.

Appends structured JSONL entries for every SQL query executed,
capturing timing, results, and errors for auditing and learning.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger


class QueryLogger:
    """Append-only JSONL query logger."""

    def __init__(self, path: str | Path, enabled: bool = False):
        self.path = Path(path)
        self.enabled = enabled

    def log(
        self,
        *,
        session_id: str,
        user_question: str,
        tool: str,
        sql: str,
        execution_time_ms: float,
        row_count: int | None = None,
        column_count: int | None = None,
        error: str | None = None,
    ) -> None:
        if not self.enabled:
            return
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "session_id": session_id,
            "user_question": user_question,
            "tool": tool,
            "sql": sql,
            "execution_time_ms": round(execution_time_ms, 2),
            "row_count": row_count,
            "column_count": column_count,
            "error": error,
        }
        try:
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
        except OSError as e:
            logger.error(f"Failed to write query log: {e}")

    def read_recent(self, n: int = 50) -> list[dict]:
        """Return the last *n* log entries."""
        if not self.path.exists():
            return []
        lines = self.path.read_text(encoding="utf-8").splitlines()
        recent = lines[-n:] if len(lines) > n else lines
        entries = []
        for line in recent:
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return entries
