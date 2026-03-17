"""Shared helpers and setup functions for sanction_corporate_link tests."""

from __future__ import annotations

import json
from pathlib import Path


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")


def _read_jsonl(path: Path) -> list[dict]:
    records: list[dict] = []
    if not path.exists():
        return records
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def _setup_rfb(
    rfb_dir: Path,
    *,
    partners: list[dict] | None = None,
    companies: list[dict] | None = None,
    establishments: list[dict] | None = None,
) -> None:
    rfb_dir.mkdir(parents=True, exist_ok=True)
    if partners is not None:
        _write_jsonl(rfb_dir / "partners_raw.jsonl", partners)
    if companies is not None:
        _write_jsonl(rfb_dir / "companies_raw.jsonl", companies)
    if establishments is not None:
        _write_jsonl(rfb_dir / "establishments_raw.jsonl", establishments)


def _setup_curated(
    curated_dir: Path,
    *,
    parties: list[dict] | None = None,
    counsels: list[dict] | None = None,
    processes: list[dict] | None = None,
    decision_events: list[dict] | None = None,
    process_party_links: list[dict] | None = None,
    process_counsel_links: list[dict] | None = None,
) -> None:
    _write_jsonl(curated_dir / "party.jsonl", parties or [])
    _write_jsonl(curated_dir / "counsel.jsonl", counsels or [])
    _write_jsonl(curated_dir / "process.jsonl", processes or [])
    _write_jsonl(curated_dir / "decision_event.jsonl", decision_events or [])
    _write_jsonl(curated_dir / "process_party_link.jsonl", process_party_links or [])
    _write_jsonl(curated_dir / "process_counsel_link.jsonl", process_counsel_links or [])


def _setup_sanctions(cgu_dir: Path, sanctions: list[dict]) -> None:
    _write_jsonl(cgu_dir / "sanctions_raw.jsonl", sanctions)


# Shared partner fixture: co-partner "JOAO DA SILVA" (CPF 52998224725) at company 44556677.
# That name matches party p1 = "JOAO DA SILVA" in curated.
#
# Sanction CNPJ 11222333000181 → company 11222333 → partner list includes JOAO DA SILVA.
# So path A: sanction CNPJ → company 11222333 → co-partner JOAO DA SILVA = party STF.

PARTY_NAME = "JOAO DA SILVA"
PARTY_CPF = "52998224725"

# Valid CNPJ: 11222333000181
SANCTION_CNPJ = "11222333000181"
SANCTION_CNPJ_BASICO = "11222333"

# Another company where sanction appears as PJ partner
OTHER_COMPANY_CNPJ_BASICO = "99887766"


def _base_curated(curated_dir: Path, *, process_count: int = 4) -> None:
    """Set up curated data so JOAO DA SILVA is a party with `process_count` processes."""
    processes = [{"process_id": f"proc{i}", "process_class": "RE"} for i in range(process_count)]
    links = [{"link_id": f"ppl{i}", "process_id": f"proc{i}", "party_id": "p1"} for i in range(process_count)]
    events = [
        {
            "decision_event_id": f"de{i}",
            "process_id": f"proc{i}",
            "decision_progress": "Provido",
            "judging_body": "Segunda Turma",
            "is_collegiate": True,
        }
        for i in range(process_count)
    ]
    _setup_curated(
        curated_dir,
        parties=[{"party_id": "p1", "party_name_normalized": PARTY_NAME}],
        processes=processes,
        decision_events=events,
        process_party_links=links,
    )
