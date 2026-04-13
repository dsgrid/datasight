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

    def __init__(self, path: str | Path):
        self.path = Path(path)
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            # Mirror the OSError-tolerant behavior of log()/log_cost(): a
            # read-only project directory must not crash callers that only
            # *might* end up logging. Subsequent log() calls will fail the
            # same way and be swallowed there.
            logger.warning(f"Could not create query log directory: {e}")

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
        turn_id: str | None = None,
    ) -> None:
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "session_id": session_id,
            "turn_id": turn_id,
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

    def log_cost(
        self,
        *,
        session_id: str,
        user_question: str,
        api_calls: int,
        input_tokens: int,
        output_tokens: int,
        estimated_cost: float | None = None,
        turn_id: str | None = None,
    ) -> None:
        """Log a turn-level cost summary entry."""
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "session_id": session_id,
            "turn_id": turn_id,
            "user_question": user_question,
            "type": "cost",
            "api_calls": api_calls,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "estimated_cost": estimated_cost,
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
