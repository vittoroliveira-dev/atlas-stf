"""Curate subparsers: curate (process, decision-event, subject, party, counsel, etc.)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from . import (
    DEFAULT_JURIS_DIR,
    DEFAULT_STAGING_DIR,
)


def _add_curate_parsers(subparsers: Any) -> None:
    curate = subparsers.add_parser("curate", help="Build curated canonical entities")
    curate_sub = curate.add_subparsers(dest="curate_target", required=True)

    curate_process = curate_sub.add_parser("process", help="Build canonical process records")
    curate_process.add_argument("--staging-dir", type=Path, default=DEFAULT_STAGING_DIR, help="Staging CSV directory")
    curate_process.add_argument(
        "--output",
        type=Path,
        default=Path("data/curated/process.jsonl"),
        help="Output JSONL path",
    )
    curate_process.add_argument(
        "--juris-dir",
        type=Path,
        default=DEFAULT_JURIS_DIR,
        help="Jurisprudencia JSONL directory for enrichment (auto-detected)",
    )

    curate_decision_event = curate_sub.add_parser("decision-event", help="Build canonical decision-event records")
    curate_decision_event.add_argument(
        "--staging-file",
        type=Path,
        default=DEFAULT_STAGING_DIR / "decisoes.csv",
        help="Staging decisions CSV",
    )
    curate_decision_event.add_argument(
        "--output",
        type=Path,
        default=Path("data/curated/decision_event.jsonl"),
        help="Output JSONL path",
    )
    curate_decision_event.add_argument(
        "--juris-dir",
        type=Path,
        default=DEFAULT_JURIS_DIR,
        help="Jurisprudencia JSONL directory for enrichment (auto-detected)",
    )

    curate_subject = curate_sub.add_parser("subject", help="Build canonical subject records")
    curate_subject.add_argument(
        "--process-path",
        type=Path,
        default=Path("data/curated/process.jsonl"),
        help="Curated process JSONL path",
    )
    curate_subject.add_argument(
        "--output",
        type=Path,
        default=Path("data/curated/subject.jsonl"),
        help="Output JSONL path",
    )

    curate_party = curate_sub.add_parser("party", help="Build canonical party records")
    curate_party.add_argument(
        "--process-path",
        type=Path,
        default=Path("data/curated/process.jsonl"),
        help="Curated process JSONL path",
    )
    curate_party.add_argument(
        "--output",
        type=Path,
        default=Path("data/curated/party.jsonl"),
        help="Output JSONL path",
    )

    curate_counsel = curate_sub.add_parser("counsel", help="Build canonical counsel records")
    curate_counsel.add_argument(
        "--process-path",
        type=Path,
        default=Path("data/curated/process.jsonl"),
        help="Curated process JSONL path",
    )
    curate_counsel.add_argument(
        "--output",
        type=Path,
        default=Path("data/curated/counsel.jsonl"),
        help="Output JSONL path",
    )

    curate_representation = curate_sub.add_parser(
        "representation",
        help="Build representation network (lawyer entities, firms, edges, events, evidence)",
    )
    curate_representation.add_argument(
        "--process-path",
        type=Path,
        default=Path("data/curated/process.jsonl"),
        help="Curated process JSONL path",
    )
    curate_representation.add_argument(
        "--portal-dir",
        type=Path,
        default=Path("data/raw/stf_portal"),
        help="STF portal JSONL directory",
    )
    curate_representation.add_argument(
        "--curated-dir",
        type=Path,
        default=Path("data/curated"),
        help="Output directory for curated JSONL files",
    )

    curate_entity_identifier = curate_sub.add_parser(
        "entity-identifier",
        help="Extract experimental CPF/CNPJ occurrences from raw jurisprudencia text",
    )
    curate_entity_identifier.add_argument(
        "--process-path",
        type=Path,
        default=Path("data/curated/process.jsonl"),
        help="Curated process JSONL path",
    )
    curate_entity_identifier.add_argument(
        "--juris-dir",
        type=Path,
        default=DEFAULT_JURIS_DIR,
        help="Jurisprudencia JSONL directory",
    )
    curate_entity_identifier.add_argument(
        "--output",
        type=Path,
        default=Path("data/curated/entity_identifier.jsonl"),
        help="Output JSONL path",
    )

    curate_entity_reconciliation = curate_sub.add_parser(
        "entity-reconciliation",
        help="Reconcile STF tax-identifier occurrences against process-local party/counsel records",
    )
    curate_entity_reconciliation.add_argument(
        "--entity-identifier-path",
        type=Path,
        default=Path("data/curated/entity_identifier.jsonl"),
        help="Entity identifier JSONL path",
    )
    curate_entity_reconciliation.add_argument(
        "--party-path",
        type=Path,
        default=Path("data/curated/party.jsonl"),
        help="Party JSONL path",
    )
    curate_entity_reconciliation.add_argument(
        "--counsel-path",
        type=Path,
        default=Path("data/curated/counsel.jsonl"),
        help="Counsel JSONL path",
    )
    curate_entity_reconciliation.add_argument(
        "--process-party-link-path",
        type=Path,
        default=Path("data/curated/process_party_link.jsonl"),
        help="Process-party link JSONL path",
    )
    curate_entity_reconciliation.add_argument(
        "--process-counsel-link-path",
        type=Path,
        default=Path("data/curated/process_counsel_link.jsonl"),
        help="Process-counsel link JSONL path",
    )
    curate_entity_reconciliation.add_argument(
        "--output",
        type=Path,
        default=Path("data/curated/entity_identifier_reconciliation.jsonl"),
        help="Output JSONL path",
    )

    curate_links = curate_sub.add_parser("links", help="Build canonical process links")
    curate_links.add_argument(
        "--process-path",
        type=Path,
        default=Path("data/curated/process.jsonl"),
        help="Curated process JSONL path",
    )
    curate_links.add_argument(
        "--party-output",
        type=Path,
        default=Path("data/curated/process_party_link.jsonl"),
        help="Output JSONL path for process-party links",
    )
    curate_links.add_argument(
        "--counsel-output",
        type=Path,
        default=Path("data/curated/process_counsel_link.jsonl"),
        help="Output JSONL path for process-counsel links",
    )

    curate_movement = curate_sub.add_parser("movement", help="Build movement records from STF portal data")
    curate_movement.add_argument(
        "--portal-dir",
        type=Path,
        default=Path("data/raw/stf_portal"),
        help="STF portal JSONL directory",
    )
    curate_movement.add_argument(
        "--output",
        type=Path,
        default=Path("data/curated/movement.jsonl"),
        help="Output JSONL path",
    )

    curate_session_event = curate_sub.add_parser("session-event", help="Build session event records")
    curate_session_event.add_argument(
        "--movement-path",
        type=Path,
        default=Path("data/curated/movement.jsonl"),
        help="Curated movement JSONL path",
    )
    curate_session_event.add_argument(
        "--portal-dir",
        type=Path,
        default=Path("data/raw/stf_portal"),
        help="STF portal JSONL directory",
    )
    curate_session_event.add_argument(
        "--output",
        type=Path,
        default=Path("data/curated/session_event.jsonl"),
        help="Output JSONL path",
    )

    curate_all = curate_sub.add_parser("all", help="Build all currently supported curated entities")
    curate_all.add_argument("--staging-dir", type=Path, default=DEFAULT_STAGING_DIR, help="Staging CSV directory")
    curate_all.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/curated"),
        help="Output directory for curated JSONL files",
    )
    curate_all.add_argument(
        "--juris-dir",
        type=Path,
        default=DEFAULT_JURIS_DIR,
        help="Jurisprudencia JSONL directory for enrichment (auto-detected)",
    )
