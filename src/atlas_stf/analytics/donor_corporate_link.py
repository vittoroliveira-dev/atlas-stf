"""Build donor → corporate links using deterministic CPF/CNPJ join against RFB data."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.identity import (
    is_valid_cnpj,
    is_valid_cpf,
    normalize_tax_id,
    stable_id,
)
from ._atomic_io import AtomicJsonlWriter
from ._donor_identity import donor_identity_key

logger = logging.getLogger(__name__)

DEFAULT_TSE_DIR = Path("data/raw/tse")
DEFAULT_RFB_DIR = Path("data/raw/rfb")
DEFAULT_OUTPUT_DIR = Path("data/analytics")

_CONFIDENCE_MAP: dict[str, str] = {
    "exact_cnpj_basico": "deterministic",
    "exact_partner_cpf": "deterministic",
    "exact_partner_cnpj": "deterministic",
    "not_in_rfb_corpus": "low",
    "masked_cpf": "unresolved",
    "missing_document": "unresolved",
    "invalid_document": "unresolved",
}


def _iter_jsonl(path: Path):
    """Yield JSONL records one at a time (never loads full file)."""
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def _classify_document(raw_doc: str) -> tuple[str, str | None, bool, str | None]:
    """Classify a donor document.

    Returns (document_type, tax_id_normalized, tax_id_valid, cnpj_basico).
    """
    if not raw_doc or not raw_doc.strip():
        return "unknown", None, False, None

    normalized = normalize_tax_id(raw_doc)
    if not normalized:
        # Has content but no digits (e.g. masked ***.***.***-**)
        return "unknown", None, False, None

    if len(normalized) == 14:
        valid = is_valid_cnpj(normalized)
        return "cnpj", normalized, valid, normalized[:8] if valid else None
    elif len(normalized) == 11:
        valid = is_valid_cpf(normalized)
        return "cpf", normalized, valid, None
    else:
        return "unknown", normalized, False, None


def _is_masked(raw_doc: str) -> bool:
    """Check if a document is masked (e.g. ***.***.***-**)."""
    return bool(raw_doc) and "*" in raw_doc


def _make_link_id(donor_key: str, company_cnpj_basico: str | None, link_basis: str) -> str:
    return stable_id("dcl-", f"{donor_key}:{company_cnpj_basico or ''}:{link_basis}")


def _build_partner_index(
    partners_path: Path,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
    """Build partner indexes: by normalized partner_cpf_cnpj, and by cnpj_basico."""
    by_doc: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_cnpj_basico: dict[str, list[dict[str, Any]]] = defaultdict(list)

    if not partners_path.exists():
        return by_doc, by_cnpj_basico

    for record in _iter_jsonl(partners_path):
        partner_doc = normalize_tax_id(record.get("partner_cpf_cnpj", ""))
        if partner_doc:
            by_doc[partner_doc].append(record)
        cnpj_b = record.get("cnpj_basico", "")
        if cnpj_b:
            by_cnpj_basico[cnpj_b].append(record)

    return by_doc, by_cnpj_basico


def _build_company_index(companies_path: Path) -> dict[str, dict[str, Any]]:
    """Index companies by cnpj_basico."""
    index: dict[str, dict[str, Any]] = {}
    if not companies_path.exists():
        return index
    for record in _iter_jsonl(companies_path):
        cnpj_b = record.get("cnpj_basico", "")
        if cnpj_b:
            index.setdefault(cnpj_b, record)
    return index


def _build_establishment_index(establishments_path: Path) -> dict[str, dict[str, Any]]:
    """Index establishments by cnpj_basico, preferring matriz (matriz_filial=='1')."""
    index: dict[str, dict[str, Any]] = {}
    if not establishments_path.exists():
        return index
    for record in _iter_jsonl(establishments_path):
        cnpj_b = record.get("cnpj_basico", "")
        if not cnpj_b:
            continue
        existing = index.get(cnpj_b)
        if existing is None:
            index[cnpj_b] = record
        elif record.get("matriz_filial") == "1" and existing.get("matriz_filial") != "1":
            index[cnpj_b] = record
    return index


def _make_record(
    donor_key: str,
    donor_name: str,
    donor_cpf_cnpj: str,
    doc_type: str,
    tax_id_normalized: str | None,
    tax_id_valid: bool,
    donor_cnpj_basico: str | None,
    link_basis: str,
    company: dict[str, Any] | None,
    establishment: dict[str, Any] | None,
    partner: dict[str, Any] | None,
    generated_at: str,
) -> dict[str, Any]:
    company_cnpj_basico = company.get("cnpj_basico") if company else None
    confidence = _CONFIDENCE_MAP.get(link_basis, "low")

    return {
        "link_id": _make_link_id(donor_key, company_cnpj_basico, link_basis),
        "donor_identity_key": donor_key,
        "donor_name": donor_name,
        "donor_cpf_cnpj": donor_cpf_cnpj,
        "donor_document_type": doc_type,
        "donor_tax_id_normalized": tax_id_normalized,
        "donor_tax_id_valid": tax_id_valid,
        "donor_cnpj_basico": donor_cnpj_basico,
        "link_basis": link_basis,
        "company_cnpj_basico": company_cnpj_basico,
        "company_name": company.get("razao_social") if company else None,
        "company_natureza_juridica": company.get("natureza_juridica") if company else None,
        "company_capital_social": company.get("capital_social") if company else None,
        "establishment_cnpj_full": establishment.get("cnpj_full") if establishment else None,
        "establishment_uf": establishment.get("uf") if establishment else None,
        "establishment_municipio": establishment.get("municipio_label") if establishment else None,
        "establishment_cnae_fiscal": establishment.get("cnae_fiscal") if establishment else None,
        "establishment_situacao_cadastral": (
            establishment.get("situacao_cadastral_label") or establishment.get("situacao_cadastral")
            if establishment
            else None
        ),
        "partner_name": partner.get("partner_name") if partner else None,
        "partner_role": partner.get("qualification_label") if partner else None,
        "qualification_code": partner.get("qualification_code") if partner else None,
        "confidence": confidence,
        "review_note": None,
        "generated_at": generated_at,
    }


def build_donor_corporate_links(
    *,
    tse_dir: Path = DEFAULT_TSE_DIR,
    rfb_dir: Path = DEFAULT_RFB_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> Path:
    """Build donor → corporate links from TSE donations + RFB data.

    Every donor_identity_key generates at least one output record.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    donations_path = tse_dir / "donations_raw.jsonl"
    if not donations_path.exists():
        logger.warning("No donations_raw.jsonl found in %s — skipping", tse_dir)
        return output_dir

    now_iso = datetime.now(timezone.utc).isoformat()

    # Aggregate unique donors
    donor_agg: dict[str, dict[str, Any]] = {}
    for d in _iter_jsonl(donations_path):
        name = d.get("donor_name_normalized", "")
        if not name:
            continue
        cpf_cnpj = d.get("donor_cpf_cnpj", "")
        key = donor_identity_key(name, cpf_cnpj)
        if key not in donor_agg:
            donor_agg[key] = {
                "donor_name": name,
                "donor_cpf_cnpj": cpf_cnpj,
            }

    logger.info("Aggregated %d unique donors from TSE data", len(donor_agg))

    # Build RFB indexes
    partners_path = rfb_dir / "partners_raw.jsonl"
    companies_path = rfb_dir / "companies_raw.jsonl"
    establishments_path = rfb_dir / "establishments_raw.jsonl"

    partner_by_doc, _partner_by_cnpj = _build_partner_index(partners_path)
    company_index = _build_company_index(companies_path)
    establishment_index = _build_establishment_index(establishments_path)

    # Resolve links
    output_records: list[dict[str, Any]] = []
    seen_keys: set[str] = set()  # dedup: (donor_key, company_cnpj_basico, link_basis)

    for donor_key, donor_info in donor_agg.items():
        raw_doc = donor_info["donor_cpf_cnpj"]
        donor_name = donor_info["donor_name"]
        doc_type, tax_id_normalized, tax_id_valid, donor_cnpj_basico = _classify_document(raw_doc)
        resolved = False

        # --- Caminho A: PJ empresa própria ---
        if doc_type == "cnpj" and tax_id_valid and donor_cnpj_basico:
            company = company_index.get(donor_cnpj_basico)
            establishment = establishment_index.get(donor_cnpj_basico)
            if company or establishment:
                dedup_key = f"{donor_key}:{donor_cnpj_basico}:exact_cnpj_basico"
                if dedup_key not in seen_keys:
                    seen_keys.add(dedup_key)
                    output_records.append(
                        _make_record(
                            donor_key,
                            donor_name,
                            raw_doc,
                            doc_type,
                            tax_id_normalized,
                            tax_id_valid,
                            donor_cnpj_basico,
                            "exact_cnpj_basico",
                            company,
                            establishment,
                            None,
                            now_iso,
                        )
                    )
                    resolved = True

        # --- Caminho B: PJ sócia de outra empresa ---
        if doc_type == "cnpj" and tax_id_valid and tax_id_normalized:
            partner_records = partner_by_doc.get(tax_id_normalized, [])
            for partner in partner_records:
                cnpj_b = partner.get("cnpj_basico", "")
                if not cnpj_b:
                    continue
                dedup_key = f"{donor_key}:{cnpj_b}:exact_partner_cnpj"
                if dedup_key in seen_keys:
                    continue
                seen_keys.add(dedup_key)
                company = company_index.get(cnpj_b)
                establishment = establishment_index.get(cnpj_b)
                output_records.append(
                    _make_record(
                        donor_key,
                        donor_name,
                        raw_doc,
                        doc_type,
                        tax_id_normalized,
                        tax_id_valid,
                        donor_cnpj_basico,
                        "exact_partner_cnpj",
                        company,
                        establishment,
                        partner,
                        now_iso,
                    )
                )
                resolved = True

        # --- PF: sócio via CPF ---
        if doc_type == "cpf" and tax_id_valid and tax_id_normalized:
            partner_records = partner_by_doc.get(tax_id_normalized, [])
            for partner in partner_records:
                cnpj_b = partner.get("cnpj_basico", "")
                if not cnpj_b:
                    continue
                dedup_key = f"{donor_key}:{cnpj_b}:exact_partner_cpf"
                if dedup_key in seen_keys:
                    continue
                seen_keys.add(dedup_key)
                company = company_index.get(cnpj_b)
                establishment = establishment_index.get(cnpj_b)
                output_records.append(
                    _make_record(
                        donor_key,
                        donor_name,
                        raw_doc,
                        doc_type,
                        tax_id_normalized,
                        tax_id_valid,
                        None,
                        "exact_partner_cpf",
                        company,
                        establishment,
                        partner,
                        now_iso,
                    )
                )
                resolved = True

        # --- Unresolved cases ---
        if not resolved:
            if _is_masked(raw_doc):
                link_basis = "masked_cpf"
            elif not raw_doc or not raw_doc.strip():
                link_basis = "missing_document"
            elif not tax_id_valid:
                link_basis = "invalid_document"
            else:
                link_basis = "not_in_rfb_corpus"

            dedup_key = f"{donor_key}::{link_basis}"
            if dedup_key not in seen_keys:
                seen_keys.add(dedup_key)
                output_records.append(
                    _make_record(
                        donor_key,
                        donor_name,
                        raw_doc,
                        doc_type,
                        tax_id_normalized,
                        tax_id_valid,
                        donor_cnpj_basico,
                        link_basis,
                        None,
                        None,
                        None,
                        now_iso,
                    )
                )

    # Write output
    output_path = output_dir / "donor_corporate_link.jsonl"
    with AtomicJsonlWriter(output_path) as fh:
        for rec in output_records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

    # Build summary
    counts_by_basis: dict[str, int] = defaultdict(int)
    counts_by_confidence: dict[str, int] = defaultdict(int)
    counts_by_doc_type: dict[str, int] = defaultdict(int)
    resolved_count = 0
    unresolved_count = 0

    for rec in output_records:
        counts_by_basis[rec["link_basis"]] += 1
        counts_by_confidence[rec["confidence"]] += 1
        counts_by_doc_type[rec["donor_document_type"]] += 1
        if rec["confidence"] == "unresolved":
            unresolved_count += 1
        else:
            resolved_count += 1

    summary = {
        "total_donors": len(donor_agg),
        "total_output_records": len(output_records),
        "resolved_record_count": resolved_count,
        "unresolved_record_count": unresolved_count,
        "counts_by_link_basis": dict(counts_by_basis),
        "counts_by_confidence": dict(counts_by_confidence),
        "counts_by_document_type": dict(counts_by_doc_type),
        "generated_at": now_iso,
    }
    summary_path = output_dir / "donor_corporate_link_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info(
        "Built %d donor corporate links (%d resolved, %d unresolved) for %d donors",
        len(output_records),
        resolved_count,
        unresolved_count,
        len(donor_agg),
    )
    return output_path
