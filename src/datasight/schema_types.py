"""Dataclasses describing introspected schema.

Kept in a leaf module with no intra-package imports so both ``runner`` and
``schema`` can reference the types without introducing a cycle.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ColumnInfo:
    name: str
    dtype: str
    nullable: bool = True


@dataclass
class TableInfo:
    name: str
    columns: list[ColumnInfo] = field(default_factory=list)
    row_count: int | None = None
