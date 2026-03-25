from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable

logger = logging.getLogger(__name__)


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


_MAX_CONFLICT_SAMPLES = 20


@dataclass(frozen=True)
class _PkCheck:
    """Validation rule for a single JSONL artifact.

    Budget rationale (reviewed 2026-03-23):
    - donation_event.jsonl: TSE raw data has structural repetition from multiple
      receipt files covering overlapping candidates. 40% budget reflects current
      pipeline state; target is <5% after upstream dedup fix.
    - sanction_match.jsonl: CGU/CVM sources occasionally emit duplicate sanction
      IDs across snapshot refreshes. 5% budget is conservative safety margin.
    """

    category: str  # "curated" or "analytics"
    filename: str
    pk_field: str
    allow_exact_dupes: bool  # True only when loader has classified dedup
    max_exact_dupe_pct: float = 0.0  # hard fail above this rate (0.0 = zero tolerance)

    @property
    def label(self) -> str:
        return f"{self.category}/{self.filename}"


_PK_CHECKS: list[_PkCheck] = [
    # ── Curated ──
    _PkCheck("curated", "process.jsonl", "process_id", allow_exact_dupes=False),
    _PkCheck("curated", "decision_event.jsonl", "decision_event_id", allow_exact_dupes=False),
    _PkCheck("curated", "party.jsonl", "party_id", allow_exact_dupes=False),
    _PkCheck("curated", "process_party_link.jsonl", "link_id", allow_exact_dupes=False),
    _PkCheck("curated", "counsel.jsonl", "counsel_id", allow_exact_dupes=False),
    _PkCheck("curated", "process_counsel_link.jsonl", "link_id", allow_exact_dupes=False),
    _PkCheck("curated", "movement.jsonl", "movement_id", allow_exact_dupes=True, max_exact_dupe_pct=5.0),
    _PkCheck("curated", "session_event.jsonl", "session_event_id", allow_exact_dupes=False),
    _PkCheck("curated", "lawyer_entity.jsonl", "lawyer_id", allow_exact_dupes=False),
    _PkCheck("curated", "law_firm_entity.jsonl", "firm_id", allow_exact_dupes=False),
    _PkCheck("curated", "representation_edge.jsonl", "edge_id", allow_exact_dupes=False),
    _PkCheck("curated", "representation_event.jsonl", "event_id", allow_exact_dupes=False),
    _PkCheck("curated", "agenda_event.jsonl", "agenda_event_id", allow_exact_dupes=False),
    _PkCheck("curated", "agenda_coverage.jsonl", "coverage_id", allow_exact_dupes=False),
    # ── Analytics ──
    _PkCheck("analytics", "outlier_alert.jsonl", "alert_id", allow_exact_dupes=False),
    _PkCheck("analytics", "donation_event.jsonl", "event_id", allow_exact_dupes=True, max_exact_dupe_pct=40.0),
    _PkCheck("analytics", "economic_group.jsonl", "group_id", allow_exact_dupes=False),
    _PkCheck("analytics", "sanction_match.jsonl", "match_id", allow_exact_dupes=True, max_exact_dupe_pct=5.0),
    _PkCheck("analytics", "donation_match.jsonl", "match_id", allow_exact_dupes=False),
    _PkCheck("analytics", "compound_risk.jsonl", "pair_id", allow_exact_dupes=False),
    _PkCheck("analytics", "sanction_corporate_link.jsonl", "link_id", allow_exact_dupes=False),
    _PkCheck("analytics", "corporate_network.jsonl", "conflict_id", allow_exact_dupes=False),
    _PkCheck("analytics", "counsel_affinity.jsonl", "affinity_id", allow_exact_dupes=False),
    _PkCheck("analytics", "temporal_analysis.jsonl", "record_id", allow_exact_dupes=False),
    _PkCheck("analytics", "decision_velocity.jsonl", "velocity_id", allow_exact_dupes=False),
    _PkCheck("analytics", "rapporteur_change.jsonl", "change_id", allow_exact_dupes=False),
    _PkCheck("analytics", "counsel_network_cluster.jsonl", "cluster_id", allow_exact_dupes=False),
    _PkCheck("analytics", "counsel_sanction_profile.jsonl", "counsel_id", allow_exact_dupes=False),
    _PkCheck("analytics", "counsel_donation_profile.jsonl", "counsel_id", allow_exact_dupes=False),
    _PkCheck("analytics", "payment_counterparty.jsonl", "counterparty_id", allow_exact_dupes=False),
    _PkCheck("analytics", "agenda_exposure.jsonl", "exposure_id", allow_exact_dupes=False),
]


@dataclass
class _ConflictSample:
    """Detailed sample of a PK conflict for forensic analysis."""

    pk: str
    first_hash: str
    divergent_hash: str

    def to_dict(self) -> dict[str, str]:
        return {"pk": self.pk, "first_hash": self.first_hash, "divergent_hash": self.divergent_hash}


@dataclass
class _FileValidationResult:
    """Per-file validation result with classified findings."""

    filename: str
    pk_field: str
    total: int = 0
    empty_pks: int = 0
    exact_dupes: int = 0
    conflicts: int = 0
    unique: int = 0
    conflict_samples: list[_ConflictSample] | None = None

    @property
    def has_issues(self) -> bool:
        return self.empty_pks > 0 or self.exact_dupes > 0 or self.conflicts > 0

    @property
    def exact_dupe_pct(self) -> float:
        return (self.exact_dupes * 100.0 / self.total) if self.total > 0 else 0.0

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "filename": self.filename,
            "pk_field": self.pk_field,
            "total": self.total,
            "unique": self.unique,
            "empty_pks": self.empty_pks,
            "exact_dupes": self.exact_dupes,
            "exact_dupe_pct": round(self.exact_dupe_pct, 2),
            "conflicts": self.conflicts,
        }
        if self.conflict_samples:
            d["conflict_samples"] = [s.to_dict() for s in self.conflict_samples]
        return d


def _content_hash(record: dict[str, Any], pk_field: str) -> str:
    """Hash record content excluding the PK field, for duplicate classification."""
    filtered = {k: v for k, v in sorted(record.items()) if k != pk_field}
    return sha256(json.dumps(filtered, ensure_ascii=False, sort_keys=True).encode()).hexdigest()[:16]


def _validate_inputs(
    curated_dir: Path,
    analytics_dir: Path,
    *,
    report_path: Path | None = None,
) -> list[_FileValidationResult]:
    """Validate source artifacts before expensive build phases.

    Three classes of finding:
    - empty_pk: error always (broken data contract).
    - exact_duplicate (same PK + identical payload): warning if allow_exact_dupes
      and within budget; error if budget exceeded or not allowed.
    - conflict (same PK + divergent payload): error always.

    Persists a structured JSON report to report_path if provided.
    """
    dir_map = {"curated": curated_dir, "analytics": analytics_dir}
    results: list[_FileValidationResult] = []
    errors: list[str] = []
    warnings: list[str] = []

    for check in _PK_CHECKS:
        path = dir_map[check.category] / check.filename
        if not path.exists():
            continue

        res = _FileValidationResult(filename=check.filename, pk_field=check.pk_field)
        seen: dict[str, str] = {}  # pk -> content_hash
        conflict_samples: list[_ConflictSample] = []

        for record in _read_jsonl(path):
            res.total += 1
            pk = str(record.get(check.pk_field, ""))
            if not pk:
                res.empty_pks += 1
                continue
            if pk in seen:
                h = _content_hash(record, check.pk_field)
                if seen[pk] == h:
                    res.exact_dupes += 1
                else:
                    res.conflicts += 1
                    if len(conflict_samples) < _MAX_CONFLICT_SAMPLES:
                        conflict_samples.append(_ConflictSample(pk=pk, first_hash=seen[pk], divergent_hash=h))
            else:
                seen[pk] = _content_hash(record, check.pk_field)
                res.unique += 1

        if conflict_samples:
            res.conflict_samples = conflict_samples
        results.append(res)

        # ── Classify outcomes ──

        if res.empty_pks > 0:
            msg = f"{check.label}: {res.empty_pks} empty {check.pk_field} in {res.total} records"
            errors.append(msg)
            logger.error("Input validation: %s", msg)

        if res.conflicts > 0:
            sample_str = ", ".join(s.pk for s in conflict_samples[:5])
            msg = (
                f"{check.label}: {res.conflicts} conflicting duplicates "
                f"(same {check.pk_field}, different payload; samples: {sample_str})"
            )
            errors.append(msg)
            logger.error("Input validation: %s", msg)

        if res.exact_dupes > 0:
            if check.allow_exact_dupes:
                if check.max_exact_dupe_pct > 0 and res.exact_dupe_pct > check.max_exact_dupe_pct:
                    msg = (
                        f"{check.label}: exact duplicate rate {res.exact_dupe_pct:.1f}% "
                        f"exceeds budget {check.max_exact_dupe_pct:.0f}%"
                    )
                    errors.append(msg)
                    logger.error("Input validation: %s", msg)
                else:
                    warn_msg = (
                        f"{check.label}: {res.exact_dupes} exact duplicates "
                        f"({res.exact_dupe_pct:.1f}%, budget {check.max_exact_dupe_pct:.0f}%)"
                    )
                    warnings.append(warn_msg)
                    logger.warning("Input validation: %s (loader handles dedup)", warn_msg)
            else:
                msg = f"{check.label}: {res.exact_dupes} exact duplicates of {check.pk_field} in {res.total} records"
                errors.append(msg)
                logger.error("Input validation: %s", msg)

        if not res.has_issues:
            logger.info("Input validation: %s OK (%d records, unique %s)", check.label, res.total, check.pk_field)

    # ── Decision: ternary state ──
    if errors:
        decision = "abort"
    elif warnings:
        decision = "continue_with_warnings"
    else:
        decision = "continue_clean"

    # ── Persist structured report ──
    report: dict[str, Any] = {
        "decision": decision,
        "files_validated": len(results),
        "errors": errors,
        "warnings": warnings,
        "files": [r.to_dict() for r in results],
    }
    if report_path is not None:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("Input validation report written to %s (decision=%s)", report_path, decision)

    if errors:
        msg = "Input validation failed:\n  " + "\n  ".join(errors)
        raise ValueError(msg)

    logger.info("Input validation: %s (%d files verified)", decision, len(results))
    return results
