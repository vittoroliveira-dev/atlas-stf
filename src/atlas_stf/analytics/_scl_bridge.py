"""Sanction corporate link bridge helpers: loading, hashing, STF entity index."""

from __future__ import annotations

import hashlib
import json
import logging
from collections import Counter
from pathlib import Path
from typing import Any

from ..core.identity import is_valid_cnpj, normalize_entity_name, normalize_tax_id
from ._match_io import read_jsonl
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


def _collect_sanction_cnpjs(sanctions: list[dict[str, Any]]) -> set[str]:
    """Extract unique CNPJ basicos from sanctions (8-digit prefix of CNPJs)."""
    result: set[str] = set()
    for sanction in sanctions:
        raw = sanction.get("entity_cnpj_cpf") or sanction.get("cpf_cnpj_sancionado")
        tax_id = normalize_tax_id(raw)
        if not tax_id:
            continue
        if len(tax_id) == 14 and is_valid_cnpj(tax_id):
            result.add(tax_id[:8])
    return result


def _collect_sanction_tax_ids(sanctions: list[dict[str, Any]]) -> set[str]:
    """Extract all normalized tax IDs from sanctions (both CPF and CNPJ)."""
    result: set[str] = set()
    for sanction in sanctions:
        raw = sanction.get("entity_cnpj_cpf") or sanction.get("cpf_cnpj_sancionado")
        tax_id = normalize_tax_id(raw)
        if tax_id:
            result.add(tax_id)
    return result


def _stream_partners_for_cnpjs(
    path: Path,
    target_cnpjs: set[str],
    target_tax_ids: set[str] | None = None,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
    """Stream partners_raw.jsonl, keeping only those matching target CNPJs or tax IDs.

    Returns (partner_by_cnpj_basico, partner_by_doc).
    partner_by_cnpj_basico: cnpj_basico -> list of partner records
    partner_by_doc: normalized tax_id -> list of partner records where tax_id is the partner's CPF/CNPJ
    """
    partner_by_cnpj_basico: dict[str, list[dict[str, Any]]] = {}
    partner_by_doc: dict[str, list[dict[str, Any]]] = {}
    if not path.exists():
        return partner_by_cnpj_basico, partner_by_doc
    for record in _iter_jsonl(path):
        cnpj_basico = record.get("cnpj_basico", "")
        partner_cpf_cnpj = normalize_tax_id(record.get("partner_cpf_cnpj", "")) or ""
        in_cnpjs = cnpj_basico in target_cnpjs
        in_tax_ids = bool(target_tax_ids and partner_cpf_cnpj and partner_cpf_cnpj in target_tax_ids)
        if not (in_cnpjs or in_tax_ids):
            continue
        partner_by_cnpj_basico.setdefault(cnpj_basico, []).append(record)
        if partner_cpf_cnpj:
            partner_by_doc.setdefault(partner_cpf_cnpj, []).append(record)
    return partner_by_cnpj_basico, partner_by_doc


def _expand_cnpjs_via_groups(
    eg_path: Path,
    seed_cnpjs: set[str],
    *,
    ctx: Any | None = None,
) -> tuple[set[str], dict[str, dict[str, Any]]]:
    """Stream economic_group.jsonl and expand seed CNPJs via group membership.

    Returns (expanded_cnpjs, eg_index).
    expanded_cnpjs: all CNPJs reachable from seeds via economic groups
    eg_index: cnpj_basico -> group record (only for relevant groups)
    """
    if not eg_path.exists():
        return seed_cnpjs.copy(), {}
    expanded: set[str] = seed_cnpjs.copy()
    eg_index: dict[str, dict[str, Any]] = {}
    for record in _iter_jsonl(eg_path):
        member_cnpjs: list[str] = record.get("member_cnpjs", [])
        if not any(cnpj in seed_cnpjs for cnpj in member_cnpjs):
            continue
        if len(member_cnpjs) > 1000 and ctx is not None:
            ctx.log_memory(f"large_group: {record.get('group_id', '?')} ({len(member_cnpjs)} members)")
        for cnpj in member_cnpjs:
            expanded.add(cnpj)
            eg_index[cnpj] = record
    return expanded, eg_index


def _stream_companies_for_cnpjs(path: Path, target_cnpjs: set[str]) -> dict[str, dict[str, Any]]:
    """Stream companies_raw.jsonl keeping only matching cnpj_basico."""
    result: dict[str, dict[str, Any]] = {}
    if not path.exists():
        return result
    for record in _iter_jsonl(path):
        cnpj_basico = record.get("cnpj_basico", "")
        if cnpj_basico in target_cnpjs:
            result[cnpj_basico] = record
    return result


def _stream_establishments_for_cnpjs(path: Path, target_cnpjs: set[str]) -> dict[str, dict[str, Any]]:
    """Stream establishments_raw.jsonl keeping only matching cnpj_basico (first = HQ)."""
    result: dict[str, dict[str, Any]] = {}
    if not path.exists():
        return result
    for record in _iter_jsonl(path):
        cnpj_basico = record.get("cnpj_basico", "")
        if cnpj_basico in target_cnpjs and cnpj_basico not in result:
            result[cnpj_basico] = record
    return result


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
