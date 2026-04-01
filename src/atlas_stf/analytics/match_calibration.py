"""Calibration harness for fuzzy matching thresholds.

Phased execution with checkpoint/resume and streaming diagnostics.
Aggregate phase peaks at O(donor_agg) (~3-4 GB for 4.7M donors).
Diagnostic phases peak at O(entity_index) — donors streamed from JSONL.

Phases: aggregate → party → counsel → consolidate
"""

from __future__ import annotations

import gc
import json
import logging
import os
import subprocess
import tempfile
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..schema_validate import validate_records
from ._atomic_io import AtomicJsonlWriter
from ._match_diagnostics import (
    CALIBRATION_CONFIGS,
    MatchDiagnostic,
    _derive_outcome,
    _diag_outcome_kwargs,
    match_entity_record_diagnostic,
)
from ._match_helpers import (
    EntityMatchIndex,
    build_entity_match_index,
    read_jsonl,
)

# Re-export public API (used by tests and other modules)
__all__ = [
    "CALIBRATION_CONFIGS",
    "MatchDiagnostic",
    "match_entity_record_diagnostic",
    "run_match_calibration",
]

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path("schemas/match_calibration_review.schema.json")
SUMMARY_SCHEMA_PATH = Path("schemas/match_calibration_summary.schema.json")

_MANIFEST_FILE = ".match_calibration_manifest.json"
_DONORS_JSONL_FILE = ".calibration_donors.jsonl"
_MAX_REVIEW_PER_REASON = 500


def _git_commit() -> str | None:
    try:
        result = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, timeout=5)
        return result.stdout.strip() or None
    except FileNotFoundError, subprocess.TimeoutExpired:
        return None


def _rss_mb() -> int:
    try:
        return int(open(f"/proc/{os.getpid()}/statm").read().split()[1]) * os.sysconf("SC_PAGE_SIZE") // (1024 * 1024)
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------


@dataclass
class _CalibrationManifest:
    started_at: str
    donor_count: int
    raw_count: int
    phases_completed: list[str] = field(default_factory=list)
    phase_results: dict[str, Any] = field(default_factory=dict)


def _load_manifest(output_dir: Path) -> _CalibrationManifest | None:
    path = output_dir / _MANIFEST_FILE
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return _CalibrationManifest(
            started_at=data["started_at"],
            donor_count=data["donor_count"],
            raw_count=data["raw_count"],
            phases_completed=data.get("phases_completed", []),
            phase_results=data.get("phase_results", {}),
        )
    except Exception:
        return None


def _save_manifest(output_dir: Path, manifest: _CalibrationManifest) -> None:
    (output_dir / _MANIFEST_FILE).write_text(
        json.dumps(asdict(manifest), ensure_ascii=False, indent=2), encoding="utf-8"
    )


# ---------------------------------------------------------------------------
# Streaming entity phase — O(entity_index) memory, not O(donors)
# ---------------------------------------------------------------------------


def _streaming_entity_phase(
    entity_type: str,
    name_field: str,
    index: EntityMatchIndex,
    donors_path: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Run diagnostics by streaming donors from JSONL. Never holds all diagnostics in memory."""
    from ..core.identity import canonicalize_entity_name, strip_accents

    def _has_accent(text: str | None) -> bool:
        return bool(text and strip_accents(text) != text)

    # Compact accumulators — O(1) memory
    cfg_matched: dict[str, int] = {n: 0 for n, _ in CALIBRATION_CONFIGS}
    cfg_ambiguous: dict[str, int] = {n: 0 for n, _ in CALIBRATION_CONFIGS}
    cfg_fuzzy: dict[str, int] = {n: 0 for n, _ in CALIBRATION_CONFIGS}
    cfg_strategy: dict[str, dict[str, int]] = {n: defaultdict(int) for n, _ in CALIBRATION_CONFIGS}
    jac_hist: dict[str, int] = defaultdict(int)
    lev_hist: dict[str, int] = defaultdict(int)
    accent_affected = 0
    # Review buffer — bounded at _MAX_REVIEW_PER_REASON × 3 reasons
    review_buf: dict[str, list[dict[str, Any]]] = defaultdict(list)
    review_omitted: int = 0
    diag_count = 0

    with open(donors_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            donor = json.loads(line)
            diag = match_entity_record_diagnostic(
                query_name=donor["donor_name_normalized"],
                query_tax_id=donor.get("donor_cpf_cnpj"),
                index=index,
                name_field=name_field,
            )
            diag_count += 1

            # 1. Config counters
            kw = _diag_outcome_kwargs(diag)
            for cfg_name, cfg_thresholds in CALIBRATION_CONFIGS:
                strategy, _, amb, _ = _derive_outcome(**kw, thresholds=cfg_thresholds)
                if strategy is not None:
                    cfg_matched[cfg_name] += 1
                    cfg_strategy[cfg_name][strategy] += 1
                    if amb:
                        cfg_ambiguous[cfg_name] += 1
                    if strategy in {"jaccard", "levenshtein"}:
                        cfg_fuzzy[cfg_name] += 1

            # 2. Histograms
            if diag.best_jaccard_score is not None:
                lo = int(diag.best_jaccard_score * 20) * 5
                jac_hist[f"{lo / 100:.2f}-{(lo + 5) / 100:.2f}"] += 1
            if diag.best_levenshtein_distance is not None:
                lev_hist[str(diag.best_levenshtein_distance)] += 1

            # 3. Accent impact (inline — avoids retaining list)
            query_canonical = canonicalize_entity_name(diag.query_name)
            if query_canonical and (
                _has_accent(query_canonical)
                or _has_accent(diag.best_jaccard_candidate)
                or _has_accent(diag.best_levenshtein_candidate)
            ):
                accent_affected += 1
                # Contrafactual omitted for brevity — counters stay 0 in streaming mode.
                # Full contrafactual requires raw_jaccard/raw_levenshtein which is expensive.
                # Accent impact was already 0 in production runs; this is a known limitation.

            # 4. Review candidate (bounded buffer)
            configs_match: list[str] = []
            configs_reject: list[str] = []
            for cn, ct in CALIBRATION_CONFIGS:
                s, _, _, _ = _derive_outcome(**kw, thresholds=ct)
                if s is not None and s != "ambiguous":
                    configs_match.append(cn)
                else:
                    configs_reject.append(cn)
            reason: str | None = None
            if configs_match and configs_reject:
                reason = "borderline_disagreement"
            elif diag.is_ambiguous:
                reason = "ambiguous"
            elif _has_accent(query_canonical) or _has_accent(diag.best_jaccard_candidate):
                reason = "accent_affected"
            if reason:
                if len(review_buf[reason]) >= _MAX_REVIEW_PER_REASON:
                    review_omitted += 1
                else:
                    review_buf[reason].append(
                        {
                            "entity_type": entity_type,
                            "query_name": diag.query_name,
                            "query_tax_id": diag.query_tax_id,
                            "best_jaccard_score": diag.best_jaccard_score,
                            "best_jaccard_candidate": diag.best_jaccard_candidate,
                            "best_levenshtein_distance": diag.best_levenshtein_distance,
                            "best_levenshtein_candidate": diag.best_levenshtein_candidate,
                            "accent_affected": reason == "accent_affected",
                            "current_winning_strategy": diag.winning_strategy,
                            "configs_that_match": configs_match,
                            "configs_that_reject": configs_reject,
                            "review_reason": reason,
                        }
                    )

            if diag_count % 500_000 == 0:
                logger.info("  [%s] %dk donors processed, RSS=%dMB", entity_type, diag_count // 1000, _rss_mb())

    # Assemble results
    config_results = {
        name: {
            "matched_count": cfg_matched[name],
            "by_strategy": dict(cfg_strategy[name]),
            "ambiguous_count": cfg_ambiguous[name],
            "fuzzy_accepted_count": cfg_fuzzy[name],
        }
        for name, _ in CALIBRATION_CONFIGS
    }
    review: list[dict[str, Any]] = []
    for recs in review_buf.values():
        review.extend(recs)

    entity_result: dict[str, Any] = {
        "index_size": len(index.records),
        "configs": config_results,
        "jaccard_score_histogram": dict(sorted(jac_hist.items())),
        "levenshtein_distance_histogram": dict(sorted(lev_hist.items())),
        "accent_impact": {
            "accent_affected_count": accent_affected,
            "accent_only_match_gain_count": None,  # contrafactual not computed in streaming mode
            "accent_only_ambiguous_gain_count": None,
            "accent_strategy_shift_count": None,
            "contrafactual_computed": False,
        },
        "review_total_written": len(review),
        "review_omitted_by_cap": review_omitted,
    }
    logger.info(
        "Calibration [%s]: %d diagnostics, %d review cases, RSS=%dMB", entity_type, diag_count, len(review), _rss_mb()
    )
    return entity_result, review


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------


def run_match_calibration(
    *,
    tse_dir: Path = Path("data/raw/tse"),
    party_path: Path = Path("data/curated/party.jsonl"),
    counsel_path: Path = Path("data/curated/counsel.jsonl"),
    output_dir: Path = Path("data/analytics"),
    alias_path: Path = Path("data/curated/entity_alias.jsonl"),
) -> Path:
    """Run calibration harness — streaming diagnostics, phased checkpoint/resume."""
    output_dir.mkdir(parents=True, exist_ok=True)
    from ._donation_aggregator import _stream_aggregate_donations

    donations_path = tse_dir / "donations_raw.jsonl"
    summary_path = output_dir / "match_calibration_summary.json"
    donors_jsonl = output_dir / _DONORS_JSONL_FILE

    if not donations_path.exists():
        msg = f"No donations_raw.jsonl found in {tse_dir} — cannot calibrate"
        logger.error(msg)
        raise FileNotFoundError(msg)

    manifest = _load_manifest(output_dir)

    # Phase 1 — aggregate: stream 20GB → compact JSONL (streamed on resume)
    if manifest and "aggregate" in manifest.phases_completed and donors_jsonl.exists():
        logger.info("Resume: aggregate checkpoint found (%d donors), RSS=%dMB", manifest.donor_count, _rss_mb())
        donor_count, raw_count = manifest.donor_count, manifest.raw_count
    else:
        logger.info("Phase aggregate: streaming %s, RSS=%dMB", donations_path, _rss_mb())
        donor_agg, raw_count, _ = _stream_aggregate_donations(donations_path)
        donor_count = len(donor_agg)
        logger.info("Phase aggregate: writing %d donors to JSONL checkpoint", donor_count)
        with open(donors_jsonl, "w", encoding="utf-8") as f:
            for d in donor_agg.values():
                f.write(json.dumps(d, ensure_ascii=False) + "\n")
        del donor_agg
        gc.collect()
        logger.info("Phase aggregate complete: %d donors, %d raw, RSS=%dMB", donor_count, raw_count, _rss_mb())
        manifest = _CalibrationManifest(
            started_at=datetime.now(timezone.utc).isoformat(),
            donor_count=donor_count,
            raw_count=raw_count,
            phases_completed=["aggregate"],
        )
        _save_manifest(output_dir, manifest)

    # Phase 2 — party (streaming diagnostics)
    if manifest and "party" in manifest.phases_completed:
        logger.info("Resume: party phase already done, RSS=%dMB", _rss_mb())
        party_entity_result = manifest.phase_results["party"]["entity_result"]
        party_review = manifest.phase_results["party"]["review"]
        party_index_size = party_entity_result.get("index_size", 0)
    else:
        logger.info("Phase party: building index, RSS=%dMB", _rss_mb())
        party_records = read_jsonl(party_path) if party_path.exists() else []
        party_index = build_entity_match_index(
            party_records,
            name_field="party_name_normalized",
            alias_path=alias_path,
            entity_kind="party",
        )
        party_index_size = len(party_records)
        del party_records
        gc.collect()
        logger.info(
            "Phase party: streaming %d donors against %d parties, RSS=%dMB",
            donor_count,
            len(party_index.records),
            _rss_mb(),
        )
        party_entity_result, party_review = _streaming_entity_phase(
            "party",
            "party_name_normalized",
            party_index,
            donors_jsonl,
        )
        del party_index
        gc.collect()
        manifest.phases_completed.append("party")
        manifest.phase_results["party"] = {"entity_result": party_entity_result, "review": party_review}
        _save_manifest(output_dir, manifest)
        logger.info("Phase party saved to checkpoint, RSS=%dMB", _rss_mb())

    # Phase 3 — counsel (streaming diagnostics)
    if manifest and "counsel" in manifest.phases_completed:
        logger.info("Resume: counsel phase already done, RSS=%dMB", _rss_mb())
        counsel_entity_result = manifest.phase_results["counsel"]["entity_result"]
        counsel_review = manifest.phase_results["counsel"]["review"]
        counsel_index_size = counsel_entity_result.get("index_size", 0)
    else:
        logger.info("Phase counsel: building index, RSS=%dMB", _rss_mb())
        counsel_records = read_jsonl(counsel_path) if counsel_path.exists() else []
        counsel_index = build_entity_match_index(
            counsel_records,
            name_field="counsel_name_normalized",
            alias_path=alias_path,
            entity_kind="counsel",
        )
        counsel_index_size = len(counsel_records)
        del counsel_records
        gc.collect()
        logger.info(
            "Phase counsel: streaming %d donors against %d counsels, RSS=%dMB",
            donor_count,
            len(counsel_index.records),
            _rss_mb(),
        )
        counsel_entity_result, counsel_review = _streaming_entity_phase(
            "counsel",
            "counsel_name_normalized",
            counsel_index,
            donors_jsonl,
        )
        del counsel_index
        gc.collect()
        manifest.phases_completed.append("counsel")
        manifest.phase_results["counsel"] = {"entity_result": counsel_entity_result, "review": counsel_review}
        _save_manifest(output_dir, manifest)
        logger.info("Phase counsel saved to checkpoint, RSS=%dMB", _rss_mb())

    # Phase 4 — consolidate
    logger.info("Phase consolidate, RSS=%dMB", _rss_mb())
    all_review = party_review + counsel_review
    summary: dict[str, Any] = {
        "total_donors_evaluated": donor_count,
        "entity_types": {"party": party_entity_result, "counsel": counsel_entity_result},
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "git_commit": _git_commit(),
        "source_dataset": str(donations_path),
        "input_files": {
            "donations_raw": {"path": str(donations_path), "raw_records": raw_count},
            "party": {"path": str(party_path), "index_size": party_index_size},
            "counsel": {"path": str(counsel_path), "index_size": counsel_index_size},
            "alias": {"path": str(alias_path), "exists": alias_path.exists()},
        },
        "thresholds_evaluated": [
            {"name": n, "jaccard_min": t.jaccard_min, "levenshtein_max": t.levenshtein_max}
            for n, t in CALIBRATION_CONFIGS
        ],
        "execution_status": "complete",
    }
    validate_records([summary], SUMMARY_SCHEMA_PATH)

    tmp_fd, tmp_path = tempfile.mkstemp(dir=output_dir, suffix=".json.tmp", prefix=".summary_")
    try:
        with open(tmp_fd, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(summary, ensure_ascii=False, indent=2))
        Path(tmp_path).replace(summary_path)
    except Exception:
        Path(tmp_path).unlink(missing_ok=True)
        raise

    validate_records(all_review, SCHEMA_PATH)
    review_path = output_dir / "match_calibration_review.jsonl"
    with AtomicJsonlWriter(review_path) as fh:
        for rec in all_review:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

    # Cleanup
    (output_dir / _MANIFEST_FILE).unlink(missing_ok=True)
    donors_jsonl.unlink(missing_ok=True)

    logger.info(
        "Calibration complete: %s + %s (%d review), RSS=%dMB", summary_path, review_path, len(all_review), _rss_mb()
    )
    return summary_path
