"""Build payment counterparty rollup analytics from party organ finance data."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.identity import build_identity_key, normalize_tax_id, stable_id
from ._atomic_io import AtomicJsonlWriter

logger = logging.getLogger(__name__)

DEFAULT_TSE_DIR = Path("data/raw/tse")
DEFAULT_OUTPUT_DIR = Path("data/analytics")


def _infer_document_type(tax_id: str) -> str:
    digits = normalize_tax_id(tax_id)
    if digits is None:
        return ""
    if len(digits) == 11:
        return "cpf"
    if len(digits) == 14:
        return "cnpj"
    return ""


class _Accumulator:
    __slots__ = (
        "total_received_brl",
        "payment_count",
        "election_years",
        "payer_parties",
        "states",
        "cnae_codes",
        "best_name",
        "tax_id_raw",
        "identity_basis",
        "min_date",
        "max_date",
        "source_files_seen",
        "ingest_runs_seen",
        "min_collected_at",
        "max_collected_at",
    )

    def __init__(self, *, identity_basis: str, tax_id_raw: str, best_name: str) -> None:
        self.total_received_brl: float = 0.0
        self.payment_count: int = 0
        self.election_years: set[int] = set()
        self.payer_parties: set[str] = set()
        self.states: set[str] = set()
        self.cnae_codes: set[str] = set()
        self.best_name: str = best_name
        self.tax_id_raw: str = tax_id_raw
        self.identity_basis: str = identity_basis
        self.min_date: str | None = None
        self.max_date: str | None = None
        self.source_files_seen: set[str] = set()
        self.ingest_runs_seen: set[str] = set()
        self.min_collected_at: str | None = None
        self.max_collected_at: str | None = None


def _update_best_name(acc: _Accumulator, record: dict[str, Any]) -> None:
    rfb = record.get("counterparty_name_rfb", "")
    if rfb:
        acc.best_name = rfb
        return
    raw = record.get("counterparty_name", "")
    if raw and not acc.best_name:
        acc.best_name = raw


def _update_date_range(acc: _Accumulator, date_str: str) -> None:
    if not date_str:
        return
    if acc.min_date is None or date_str < acc.min_date:
        acc.min_date = date_str
    if acc.max_date is None or date_str > acc.max_date:
        acc.max_date = date_str


def _update_provenance(acc: _Accumulator, record: dict[str, Any]) -> None:
    sf = record.get("source_file", "")
    if sf:
        acc.source_files_seen.add(sf)
    ir = record.get("ingest_run_id", "")
    if ir:
        acc.ingest_runs_seen.add(ir)
    ca = record.get("collected_at", "")
    if ca:
        if acc.min_collected_at is None or ca < acc.min_collected_at:
            acc.min_collected_at = ca
        if acc.max_collected_at is None or ca > acc.max_collected_at:
            acc.max_collected_at = ca


def build_payment_counterparties(
    *,
    tse_dir: Path = DEFAULT_TSE_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Build payment counterparty rollup from party_org_finance_raw.jsonl."""
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_path = tse_dir / "party_org_finance_raw.jsonl"

    if not raw_path.exists():
        logger.warning("Arquivo %s não encontrado; emitindo artefatos vazios.", raw_path)
        out_path = output_dir / "payment_counterparty.jsonl"
        out_path.write_text("", encoding="utf-8")
        summary_path = output_dir / "payment_counterparty_summary.json"
        summary_path.write_text(
            json.dumps(
                {
                    "total_expense_records": 0,
                    "skipped_non_expense_records": 0,
                    "skipped_no_identity_records": 0,
                    "aggregated_counterparties": 0,
                    "aggregated_by_tax_id_count": 0,
                    "aggregated_by_name_fallback_count": 0,
                    "total_received_brl": 0.0,
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        return out_path

    accumulators: dict[str, _Accumulator] = {}
    total_expense = 0
    skipped_non_expense = 0
    skipped_no_identity = 0

    with raw_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)

            if record.get("record_kind") != "expense":
                skipped_non_expense += 1
                continue

            total_expense += 1
            name_normalized = record.get("counterparty_name_normalized", "")
            tax_id = record.get("counterparty_tax_id", "")
            identity_key = build_identity_key(name_normalized, entity_tax_id=tax_id)

            if identity_key is None:
                skipped_no_identity += 1
                continue

            identity_basis = "tax_id" if identity_key.startswith("tax:") else "name_fallback"

            if identity_key not in accumulators:
                best_name = (
                    record.get("counterparty_name_rfb", "")
                    or record.get("counterparty_name", "")
                    or name_normalized
                )
                accumulators[identity_key] = _Accumulator(
                    identity_basis=identity_basis,
                    tax_id_raw=tax_id,
                    best_name=best_name,
                )

            acc = accumulators[identity_key]
            acc.total_received_brl += record.get("transaction_amount", 0.0)
            acc.payment_count += 1

            year = record.get("election_year")
            if year is not None:
                acc.election_years.add(int(year))

            party = record.get("org_party_abbrev", "")
            if party:
                acc.payer_parties.add(party)

            state = record.get("state", "")
            if state:
                acc.states.add(state)

            cnae = record.get("counterparty_cnae_code", "")
            if cnae:
                acc.cnae_codes.add(cnae)

            _update_date_range(acc, record.get("transaction_date", ""))
            _update_best_name(acc, record)
            _update_provenance(acc, record)

    now_iso = datetime.now(timezone.utc).isoformat()
    out_path = output_dir / "payment_counterparty.jsonl"
    by_tax_id = 0
    by_name = 0
    total_brl = 0.0

    with AtomicJsonlWriter(out_path) as fh:
        for key, acc in accumulators.items():
            counterparty_id = stable_id("pc-", key)
            normalized_tid = normalize_tax_id(acc.tax_id_raw)
            doc_type = _infer_document_type(acc.tax_id_raw)

            if acc.identity_basis == "tax_id":
                by_tax_id += 1
            else:
                by_name += 1
            total_brl += acc.total_received_brl

            row: dict[str, Any] = {
                "counterparty_id": counterparty_id,
                "counterparty_identity_key": key,
                "identity_basis": acc.identity_basis,
                "counterparty_name": acc.best_name,
                "counterparty_tax_id": acc.tax_id_raw,
                "counterparty_tax_id_normalized": normalized_tid or "",
                "counterparty_document_type": doc_type,
                "total_received_brl": acc.total_received_brl,
                "payment_count": acc.payment_count,
                "election_years": sorted(acc.election_years),
                "payer_parties": sorted(acc.payer_parties),
                "payer_actor_type": "party_org",
                "first_payment_date": acc.min_date,
                "last_payment_date": acc.max_date,
                "states": sorted(acc.states),
                "cnae_codes": sorted(acc.cnae_codes),
                "provenance": {
                    "source_file_count": len(acc.source_files_seen),
                    "ingest_run_count": len(acc.ingest_runs_seen),
                    "first_collected_at": acc.min_collected_at,
                    "last_collected_at": acc.max_collected_at,
                },
                "generated_at": now_iso,
            }
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    summary = {
        "total_expense_records": total_expense,
        "skipped_non_expense_records": skipped_non_expense,
        "skipped_no_identity_records": skipped_no_identity,
        "aggregated_counterparties": len(accumulators),
        "aggregated_by_tax_id_count": by_tax_id,
        "aggregated_by_name_fallback_count": by_name,
        "total_received_brl": total_brl,
        "generated_at": now_iso,
    }
    summary_path = output_dir / "payment_counterparty_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info(
        "Contrapartes de pagamento: %d expense → %d contrapartes (%d tax_id, %d name)",
        total_expense,
        len(accumulators),
        by_tax_id,
        by_name,
    )
    return out_path
