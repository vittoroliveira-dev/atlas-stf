from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class ServingBuildResult:
    database_url: str
    case_count: int
    alert_count: int
    counsel_count: int
    party_count: int
    source_count: int


@dataclass(frozen=True)
class SourceFile:
    label: str
    category: str
    path: Path


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def _dedupe_records_by_key(records: Iterable[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    unique_records: dict[str, dict[str, Any]] = {}
    for record in records:
        record_key = record.get(key)
        if not isinstance(record_key, str) or not record_key:
            continue
        unique_records.setdefault(record_key, record)
    return list(unique_records.values())


def _parse_date(value: Any) -> date | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _coerce_int(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return 0


def _coerce_bool(value: Any) -> bool:
    return bool(value) if value is not None else False


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value:
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _source_checksum(path: Path) -> str:
    stat = path.stat()
    signature = f"{path}:{stat.st_size}:{stat.st_mtime_ns}".encode("utf-8")
    return sha256(signature).hexdigest()[:16]


def _source_updated_at(path: Path) -> datetime:
    return datetime.fromtimestamp(path.stat().st_mtime).astimezone()


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)
