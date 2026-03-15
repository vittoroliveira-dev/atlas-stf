"""Enrich RFB records with decoded labels from reference tables."""

from __future__ import annotations

from typing import Any


def enrich_partner_record(record: dict[str, Any], qualificacoes: dict[str, str]) -> dict[str, Any]:
    """Add decoded qualification labels to a partner record."""
    record["qualification_label"] = qualificacoes.get(
        record.get("qualification_code", ""), ""
    )
    record["representative_qualification_label"] = qualificacoes.get(
        record.get("representative_qualification", ""), ""
    )
    return record


def enrich_company_record(record: dict[str, Any], naturezas: dict[str, str]) -> dict[str, Any]:
    """Add decoded natureza juridica label to a company record."""
    record["natureza_juridica_label"] = naturezas.get(
        record.get("natureza_juridica", ""), ""
    )
    return record


def enrich_establishment_record(
    record: dict[str, Any],
    cnaes: dict[str, str],
    municipios: dict[str, str],
    motivos: dict[str, str],
) -> dict[str, Any]:
    """Add decoded CNAE, municipio, and motivo labels to an establishment record."""
    record["cnae_fiscal_label"] = cnaes.get(record.get("cnae_fiscal", ""), "")
    record["municipio_label"] = municipios.get(record.get("municipio", ""), "")
    record["motivo_situacao_label"] = motivos.get(
        record.get("motivo_situacao_cadastral", ""), ""
    )
    # Decode secondary CNAEs
    sec = record.get("cnae_fiscal_secundaria", [])
    record["cnae_secundaria_labels"] = [
        cnaes.get(c.strip(), c.strip()) for c in sec if c.strip()
    ]
    return record
