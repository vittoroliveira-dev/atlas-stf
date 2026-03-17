"""Sanction corporate link bridge helpers: loading, hashing, STF entity index."""

from __future__ import annotations

import hashlib
import json
import logging
from collections import Counter
from pathlib import Path
from typing import Any

from ..core.identity import normalize_entity_name
from ._match_helpers import read_jsonl
from .donor_corporate_link import _iter_jsonl

logger = logging.getLogger(__name__)


def _canonical_json(record: dict[str, Any]) -> str:
    """Serialize record to canonical JSON (sorted keys, no whitespace)."""
    return json.dumps(record, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _record_hash(record: dict[str, Any]) -> str:
    """Compute SHA-256 of the canonical payload (excluding hash and timestamp)."""
    payload = {k: v for k, v in record.items() if k not in {"record_hash", "generated_at"}}
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _load_sanctions(cgu_dir: Path, cvm_dir: Path) -> list[dict[str, Any]]:
    """Load sanctions from CGU and CVM raw files."""
    sanctions: list[dict[str, Any]] = []
    cgu_path = cgu_dir / "sanctions_raw.jsonl"
    if cgu_path.exists():
        for rec in _iter_jsonl(cgu_path):
            rec.setdefault("sanction_source_origin", "cgu")
            sanctions.append(rec)
    cvm_path = cvm_dir / "sanctions_raw.jsonl"
    if cvm_path.exists():
        for rec in _iter_jsonl(cvm_path):
            rec.setdefault("sanction_source_origin", "cvm")
            sanctions.append(rec)
    return sanctions


def _load_economic_groups(analytics_dir: Path) -> dict[str, dict[str, Any]]:
    """Load economic groups indexed by cnpj_basico -> group record."""
    eg_path = analytics_dir / "economic_group.jsonl"
    if not eg_path.exists():
        return {}
    index: dict[str, dict[str, Any]] = {}
    for record in read_jsonl(eg_path):
        for cnpj in record.get("member_cnpjs", []):
            index.setdefault(cnpj, record)
    return index


def _build_stf_entity_index(
    curated_dir: Path,
) -> tuple[list[dict[str, Any]], dict[str, str], dict[str, str]]:
    """Build combined party+counsel records for entity matching.

    Returns (combined_records, party_id_to_name, counsel_id_to_name).
    """
    party_path = curated_dir / "party.jsonl"
    counsel_path = curated_dir / "counsel.jsonl"

    combined: list[dict[str, Any]] = []
    party_id_to_name: dict[str, str] = {}
    counsel_id_to_name: dict[str, str] = {}

    if party_path.exists():
        for rec in read_jsonl(party_path):
            pid = rec.get("party_id", "")
            name = normalize_entity_name(rec.get("party_name_normalized") or rec.get("party_name_raw", ""))
            if pid and name:
                party_id_to_name[pid] = name
                combined.append(
                    {
                        "entity_id": pid,
                        "entity_type": "party",
                        "entity_name_normalized": name,
                        "entity_tax_id": rec.get("entity_tax_id"),
                    }
                )

    if counsel_path.exists():
        for rec in read_jsonl(counsel_path):
            cid = rec.get("counsel_id", "")
            name = normalize_entity_name(rec.get("counsel_name_normalized") or rec.get("counsel_name_raw", ""))
            if cid and name:
                counsel_id_to_name[cid] = name
                combined.append(
                    {
                        "entity_id": cid,
                        "entity_type": "counsel",
                        "entity_name_normalized": name,
                        "entity_tax_id": rec.get("entity_tax_id"),
                    }
                )

    return combined, party_id_to_name, counsel_id_to_name


def _compute_modal_class_jb(
    process_ids: list[str],
    process_class_map: dict[str, str],
    process_jb_map: dict[str, str],
) -> tuple[str | None, str | None]:
    """Compute the modal (process_class, jb_category) pair."""
    pairs: list[tuple[str, str]] = []
    for pid in process_ids:
        pc = process_class_map.get(pid)
        if pc:
            jb = process_jb_map.get(pid, "incerto")
            pairs.append((pc, jb))
    if not pairs:
        return None, None
    (modal_class, modal_jb), _count = Counter(pairs).most_common(1)[0]
    return modal_class, modal_jb
