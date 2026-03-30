"""Core inspection logic for CSV and JSONL data files.

Streams through files computing per-column statistics without loading
full contents into memory.  Large JSONL files are sampled at even
intervals; CSVs are read in full (all target CSVs are < 200 MB).
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
import subprocess
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

VERSION = "1.0.0"

_MAX_DISTINCT = 10_000
_SAMPLE_VALUES_COUNT = 5
_HASH_CHUNK = 1_048_576  # 1 MB

_DATE_RE = re.compile(r"^\d{2}/\d{2}/\d{4}$|^\d{4}-\d{2}-\d{2}$")
_INT_RE = re.compile(r"^-?\d+$")
_FLOAT_RE = re.compile(r"^-?\d+[.,]\d+$")
_BOOLEANS = frozenset({"true", "false", "sim", "não", "nao", "s", "n", "0", "1", "yes", "no"})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _file_fingerprint(path: Path) -> str:
    h = hashlib.sha256()
    h.update(str(path.stat().st_size).encode())
    with open(path, "rb") as f:
        h.update(f.read(_HASH_CHUNK))
    return h.hexdigest()


def _detect_line_ending(path: Path) -> str:
    with open(path, "rb") as f:
        chunk = f.read(8192)
    if b"\r\n" in chunk:
        return "crlf"
    if b"\r" in chunk:
        return "cr"
    return "lf"


def _detect_encoding(path: Path) -> str:
    with open(path, "rb") as f:
        raw = f.read(65_536)
    try:
        raw.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        return "latin-1"


def _normalize_col(name: str) -> str:
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_only = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"[^a-zA-Z0-9]+", "_", ascii_only.lower()).strip("_")


def _count_lines_fast(path: Path) -> int:
    result = subprocess.run(
        ["wc", "-l", str(path)],
        capture_output=True,
        text=True,
        check=True,
    )
    return int(result.stdout.strip().split()[0])


def _infer_type(values: list[str]) -> str:
    if not values:
        return "empty"
    counts: dict[str, int] = {
        "string": 0,
        "integer": 0,
        "float": 0,
        "date": 0,
        "boolean": 0,
    }
    for v in values:
        vs = v.strip()
        if not vs:
            continue
        if vs.lower() in _BOOLEANS:
            counts["boolean"] += 1
        elif _DATE_RE.match(vs):
            counts["date"] += 1
        elif _INT_RE.match(vs):
            counts["integer"] += 1
        elif _FLOAT_RE.match(vs):
            counts["float"] += 1
        else:
            counts["string"] += 1
    total = sum(counts.values())
    if total == 0:
        return "empty"
    dominant = max(counts, key=lambda k: counts[k])
    return dominant if counts[dominant] / total >= 0.8 else "mixed"


# ---------------------------------------------------------------------------
# Column accumulator
# ---------------------------------------------------------------------------


@dataclass
class _Col:
    count: int = 0
    null_count: int = 0
    empty_count: int = 0
    distinct: set[str] = field(default_factory=set)
    distinct_saturated: bool = False
    samples: list[str] = field(default_factory=list)
    type_probe: list[str] = field(default_factory=list)
    min_len: int | None = None
    max_len: int | None = None

    def observe(self, value: str | None) -> None:
        self.count += 1
        if value is None:
            self.null_count += 1
            return
        stripped = value.strip()
        if stripped == "":
            self.empty_count += 1
        length = len(value)
        if self.min_len is None or length < self.min_len:
            self.min_len = length
        if self.max_len is None or length > self.max_len:
            self.max_len = length
        if not self.distinct_saturated:
            self.distinct.add(value)
            if len(self.distinct) > _MAX_DISTINCT:
                self.distinct_saturated = True
        if len(self.samples) < _SAMPLE_VALUES_COUNT and stripped:
            if value not in self.samples:
                self.samples.append(value)
        if len(self.type_probe) < 200:
            self.type_probe.append(value)

    def profile(self, col_name: str, position: int, notes: str | None) -> dict[str, Any]:
        return {
            "position": position,
            "observed_column_name": col_name,
            "normalized_column_name": _normalize_col(col_name),
            "observed_type": _infer_type(self.type_probe),
            "sample_values": self.samples,
            "null_rate": round(self.null_count / self.count, 6) if self.count else 0.0,
            "empty_rate": round(self.empty_count / self.count, 6) if self.count else 0.0,
            "distinct_count_estimate": len(self.distinct),
            "distinct_saturated": self.distinct_saturated,
            "min_length": self.min_len,
            "max_length": self.max_len,
            "notes": notes,
            "suspected_semantic_drift": None,
            "suspected_alias_group": None,
            "extraction_confidence": "high",
        }


def _meta_block() -> dict[str, str]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generator": "atlas_stf.contracts.inspect_sources",
        "generator_version": VERSION,
    }


# ---------------------------------------------------------------------------
# CSV inspector
# ---------------------------------------------------------------------------


def inspect_csv(
    path: Path,
    *,
    source: str,
    year_or_cycle: str,
    delimiter: str = ";",
    project_root: Path,
    notes: dict[str, str] | None = None,
) -> dict[str, Any]:
    encoding = _detect_encoding(path)
    line_ending = _detect_line_ending(path)
    fingerprint = _file_fingerprint(path)
    file_size = path.stat().st_size

    accumulators: dict[int, _Col] = {}
    header: list[str] = []
    total = 0

    with open(path, encoding=encoding, newline="") as f:
        reader = csv.reader(f, delimiter=delimiter, quotechar='"')
        first_row = next(reader)
        header = [c.strip() for c in first_row]
        for i in range(len(header)):
            accumulators[i] = _Col()

        for row in reader:
            total += 1
            for i, val in enumerate(row):
                if i not in accumulators:
                    accumulators[i] = _Col()
                accumulators[i].observe(val)
            for i in range(len(row), len(header)):
                if i in accumulators:
                    accumulators[i].observe(None)

    col_notes = notes or {}
    columns = [
        accumulators[i].profile(
            header[i] if i < len(header) else f"_col_{i}",
            i,
            col_notes.get(header[i] if i < len(header) else f"_col_{i}"),
        )
        for i in sorted(accumulators)
    ]

    return {
        "_meta": _meta_block(),
        "source": source,
        "file_name": path.name,
        "file_path_relative": str(path.relative_to(project_root)),
        "year_or_cycle": year_or_cycle,
        "format": "csv",
        "delimiter": delimiter,
        "encoding_detected": encoding,
        "line_ending": line_ending,
        "header_present": True,
        "total_records": total,
        "sample_size": total,
        "sample_coverage": 1.0,
        "file_size_bytes": file_size,
        "file_fingerprint_sha256_1mb": fingerprint,
        "columns": columns,
    }


# ---------------------------------------------------------------------------
# JSONL inspector
# ---------------------------------------------------------------------------


def inspect_jsonl(
    path: Path,
    *,
    source: str,
    year_or_cycle: str,
    project_root: Path,
    sample_size: int = 20_000,
    partition_key: str | None = None,
    notes: dict[str, str] | None = None,
) -> dict[str, Any]:
    encoding = _detect_encoding(path)
    fingerprint = _file_fingerprint(path)
    file_size = path.stat().st_size
    total = _count_lines_fast(path)
    step = max(1, total // sample_size)

    accumulators: dict[str, _Col] = {}
    partition_counts: dict[str, int] = {}
    all_keys: set[str] = set()
    sampled = 0

    with open(path, encoding=encoding) as f:
        for line_no, line in enumerate(f):
            if line_no % step != 0:
                continue
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            sampled += 1
            keys_in_record = set(record.keys())
            all_keys.update(keys_in_record)

            if partition_key and partition_key in record:
                pk = str(record[partition_key])
                partition_counts[pk] = partition_counts.get(pk, 0) + 1

            for key, value in record.items():
                if key not in accumulators:
                    accumulators[key] = _Col()
                accumulators[key].observe(str(value) if value is not None else None)

            for key in all_keys - keys_in_record:
                if key in accumulators:
                    accumulators[key].observe(None)

    col_notes = notes or {}
    columns = [accumulators[key].profile(key, i, col_notes.get(key)) for i, key in enumerate(sorted(accumulators))]

    coverage = round(sampled / total, 6) if total else 0.0
    for col in columns:
        if coverage < 1.0:
            col["extraction_confidence"] = "medium"

    result: dict[str, Any] = {
        "_meta": _meta_block(),
        "source": source,
        "file_name": path.name,
        "file_path_relative": str(path.relative_to(project_root)),
        "year_or_cycle": year_or_cycle,
        "format": "jsonl",
        "delimiter": None,
        "encoding_detected": encoding,
        "line_ending": None,
        "header_present": False,
        "total_records": total,
        "sample_size": sampled,
        "sample_coverage": coverage,
        "file_size_bytes": file_size,
        "file_fingerprint_sha256_1mb": fingerprint,
        "columns": columns,
    }
    if partition_counts:
        result["partition_key"] = partition_key
        result["partition_values_sampled"] = dict(sorted(partition_counts.items()))
    return result


# ---------------------------------------------------------------------------
# JSONL inspector — partitioned (one inventory per partition value)
# ---------------------------------------------------------------------------


def inspect_jsonl_partitioned(
    path: Path,
    *,
    source: str,
    project_root: Path,
    partition_key: str,
    max_per_partition: int = 3_000,
) -> dict[str, dict[str, Any]]:
    """Single-pass partitioned inspection of a JSONL file.

    Returns ``{partition_value: inventory_dict}`` with one inventory per
    observed value of *partition_key* (e.g. election year).  At most
    *max_per_partition* records are fully profiled per partition; remaining
    records are counted but not profiled.
    """
    encoding = _detect_encoding(path)
    fingerprint = _file_fingerprint(path)
    file_size = path.stat().st_size

    accs: dict[str, dict[str, _Col]] = {}
    keys_per: dict[str, set[str]] = {}
    counts: dict[str, int] = {}
    sampled: dict[str, int] = {}

    with open(path, encoding=encoding) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            pk = str(record.get(partition_key, "_unknown"))
            counts[pk] = counts.get(pk, 0) + 1

            if sampled.get(pk, 0) >= max_per_partition:
                continue
            sampled[pk] = sampled.get(pk, 0) + 1

            if pk not in accs:
                accs[pk] = {}
                keys_per[pk] = set()

            year_acc = accs[pk]
            rec_keys = set(record.keys())
            keys_per[pk].update(rec_keys)

            for key, value in record.items():
                if key not in year_acc:
                    year_acc[key] = _Col()
                year_acc[key].observe(str(value) if value is not None else None)

            for key in keys_per[pk] - rec_keys:
                if key in year_acc:
                    year_acc[key].observe(None)

    result: dict[str, dict[str, Any]] = {}
    for pk in sorted(accs):
        year_acc = accs[pk]
        year_total = counts.get(pk, 0)
        year_sampled = sampled.get(pk, 0)
        cov = round(year_sampled / year_total, 6) if year_total else 0.0
        cols = [year_acc[k].profile(k, i, None) for i, k in enumerate(sorted(year_acc))]
        if cov < 1.0:
            for c in cols:
                c["extraction_confidence"] = "medium"

        result[pk] = {
            "_meta": _meta_block(),
            "source": source,
            "file_name": path.name,
            "file_path_relative": str(path.relative_to(project_root)),
            "year_or_cycle": pk,
            "format": "jsonl",
            "delimiter": None,
            "encoding_detected": encoding,
            "line_ending": None,
            "header_present": False,
            "total_records": year_total,
            "sample_size": year_sampled,
            "sample_coverage": cov,
            "file_size_bytes": file_size,
            "file_fingerprint_sha256_1mb": fingerprint,
            "columns": cols,
        }
    return result
