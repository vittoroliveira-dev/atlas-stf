"""Data models for profiling reports."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field


@dataclass(frozen=True)
class ColumnProfile:
    name: str
    non_null_count: int
    null_count: int
    distinct_count: int
    sample_values: list[str] = field(default_factory=list)
    max_length: int = 0
    looks_like_key: bool = False
    looks_like_date: bool = False

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class DatasetProfile:
    filename: str
    row_count: int
    column_count: int
    duplicate_row_count: int
    columns: list[ColumnProfile]

    def to_dict(self) -> dict[str, object]:
        return {
            "filename": self.filename,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "duplicate_row_count": self.duplicate_row_count,
            "columns": [column.to_dict() for column in self.columns],
        }
