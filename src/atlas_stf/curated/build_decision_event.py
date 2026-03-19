"""Build canonical decision-event records from staged decisions.

Primary source: ``decisoes.csv``.
Supplementary sources (auto-detected in the same directory):
  - ``plenario_virtual.csv`` — virtual plenary session decisions
  - ``decisoes_covid.csv``   — COVID-preference decisions
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from ..core.identity import normalize_process_code
from ..core.parsers import (
    as_optional_str,
    infer_process_number,
    parse_bool_collegiate,
    parse_decision_year,
)
from ..schema_validate import validate_records
from .common import (
    DECISION_EVENT_NORMALIZATION_VERSION,
    SOURCE_ID,
    stable_id,
    utc_now_iso,
    write_jsonl,
)

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path("schemas/decision_event.schema.json")
DEFAULT_STAGING_FILE = Path("data/staging/transparencia/decisoes.csv")
DEFAULT_OUTPUT_PATH = Path("data/curated/decision_event.jsonl")

# ---------------------------------------------------------------------------
# Column mappings per supplementary CSV
# ---------------------------------------------------------------------------
# Each mapping translates source-specific column names to the canonical names
# used by decisoes.csv.  Missing keys → None in the output record.

_PLENARIO_VIRTUAL_COL_MAP = {
    "decision_date": "data_decisao",
    "decision_year": None,  # derived from data_decisao
    "rapporteur": "relator_atual",
    "decision_origin": None,
    "decision_type": "tipo_decisao",
    "decision_progress": "descricao_andamento",
    "decision_note": "observacao_andamento",
    "panel_indicator": None,  # all rows are collegiate
    "is_collegiate_override": True,
    "judging_body": "orgao_julgador",
    "source_row_id": "cod_andamento",
}

_DECISOES_COVID_COL_MAP = {
    "decision_date": "data_decisao",
    "decision_year": None,
    "rapporteur": "relator",
    "decision_origin": None,
    "decision_type": "tipo_decisao",
    "decision_progress": "tipo_decisao",
    "decision_note": "observacao_decisao",
    "panel_indicator": None,
    "is_collegiate_override": None,
    "judging_body": None,
    "source_row_id": None,
}

_SUPPLEMENTARY_SOURCES: list[tuple[str, dict[str, Any]]] = [
    ("plenario_virtual.csv", _PLENARIO_VIRTUAL_COL_MAP),
    ("decisoes_covid.csv", _DECISOES_COVID_COL_MAP),
]


def _lookup_decision_enrichment(
    decision_index: dict[str, list[dict[str, Any]]],
    process_number: str,
    decision_date: str | None,
) -> dict[str, Any]:
    """Find best matching jurisprudencia doc for a decision event."""
    empty = {
        "juris_doc_id": None,
        "juris_decisao_texto": None,
        "juris_ementa_texto": None,
        "juris_inteiro_teor_url": None,
        "juris_publicacao_data": None,
    }
    if not decision_date:
        return empty
    key = normalize_process_code(process_number)
    lookup_key = f"{key}::{decision_date}"
    matches = decision_index.get(lookup_key)
    if not matches:
        return empty
    return matches[0]


_EMPTY_JURIS: dict[str, Any] = {
    "juris_doc_id": None,
    "juris_decisao_texto": None,
    "juris_ementa_texto": None,
    "juris_inteiro_teor_url": None,
    "juris_publicacao_data": None,
}


def _resolve_juris(
    decision_index: dict[str, list[dict[str, Any]]] | None,
    process_number: str,
    decision_date: str | None,
) -> dict[str, Any]:
    if decision_index is not None:
        return _lookup_decision_enrichment(decision_index, process_number, decision_date)
    return dict(_EMPTY_JURIS)


def _parse_records_decisoes(
    staging_file: Path,
    decision_index: dict[str, list[dict[str, Any]]] | None,
    timestamp: str,
) -> list[dict[str, Any]]:
    """Parse the primary decisoes.csv into decision-event records."""
    df = pd.read_csv(staging_file, dtype=str, low_memory=False)
    records: list[dict[str, Any]] = []
    filename = staging_file.name

    columns = list(df.columns)
    for row_number, values in enumerate(df.itertuples(index=False, name=None), start=1):
        row = dict(zip(columns, values))
        process_number = infer_process_number(row)
        if not process_number:
            continue
        decision_date = as_optional_str(row.get("data_da_decisao"))
        juris_fields = _resolve_juris(decision_index, process_number, decision_date)

        records.append(
            {
                "decision_event_id": stable_id(
                    "de_",
                    f"{filename}:{row.get('idfatodecisao', '')}:{row_number}:{process_number}",
                ),
                "source_row_id": as_optional_str(row.get("idfatodecisao")),
                "process_id": stable_id("proc_", process_number),
                "decision_date": decision_date,
                "decision_year": parse_decision_year(row.get("ano_da_decisao")),
                "current_rapporteur": as_optional_str(row.get("relator_atual")),
                "decision_origin": as_optional_str(row.get("origem_decisao")),
                "decision_type": as_optional_str(row.get("tipo_decisao")),
                "decision_progress": as_optional_str(row.get("andamento_decisao")),
                "decision_note": as_optional_str(row.get("observacao_do_andamento")),
                "panel_indicator_raw": as_optional_str(row.get("indicador_colegiado")),
                "is_collegiate": parse_bool_collegiate(row.get("indicador_colegiado")),
                "judging_body": as_optional_str(row.get("orgao_julgador")),
                "time_bucket": decision_date[:7] if decision_date and len(decision_date) >= 7 else None,
                **juris_fields,
                "raw_fields": {
                    "source_file": staging_file.name,
                    "source_row_number": row_number,
                    "process_number": process_number,
                },
                "normalization_version": DECISION_EVENT_NORMALIZATION_VERSION,
                "source_id": SOURCE_ID,
                "created_at": timestamp,
                "updated_at": timestamp,
            }
        )

    return records


def _parse_records_supplementary(
    csv_path: Path,
    col_map: dict[str, Any],
    decision_index: dict[str, list[dict[str, Any]]] | None,
    timestamp: str,
) -> list[dict[str, Any]]:
    """Parse a supplementary CSV using its column mapping."""
    df = pd.read_csv(csv_path, dtype=str, low_memory=False)
    records: list[dict[str, Any]] = []
    filename = csv_path.name

    columns = list(df.columns)
    for row_number, values in enumerate(df.itertuples(index=False, name=None), start=1):
        row = dict(zip(columns, values))
        process_number = infer_process_number(row)
        if not process_number:
            continue

        date_col = col_map.get("decision_date")
        decision_date = as_optional_str(row.get(date_col)) if date_col else None

        year_col = col_map.get("decision_year")
        if year_col:
            decision_year = parse_decision_year(row.get(year_col))
        elif decision_date and len(decision_date) >= 4:
            decision_year = parse_decision_year(decision_date[:4])
        else:
            decision_year = None

        rap_col = col_map.get("rapporteur")
        rapporteur = as_optional_str(row.get(rap_col)) if rap_col else None

        type_col = col_map.get("decision_type")
        decision_type = as_optional_str(row.get(type_col)) if type_col else None

        progress_col = col_map.get("decision_progress")
        decision_progress = as_optional_str(row.get(progress_col)) if progress_col else None

        note_col = col_map.get("decision_note")
        decision_note = as_optional_str(row.get(note_col)) if note_col else None

        panel_col = col_map.get("panel_indicator")
        panel_raw = as_optional_str(row.get(panel_col)) if panel_col else None

        is_col_override = col_map.get("is_collegiate_override")
        if is_col_override is not None:
            is_collegiate: bool | None = bool(is_col_override)
        else:
            is_collegiate = parse_bool_collegiate(panel_raw) if panel_raw else None

        body_col = col_map.get("judging_body")
        judging_body = as_optional_str(row.get(body_col)) if body_col else None

        src_id_col = col_map.get("source_row_id")
        source_row_id = as_optional_str(row.get(src_id_col)) if src_id_col else None

        juris_fields = _resolve_juris(decision_index, process_number, decision_date)

        de_id_seed = f"{filename}:{source_row_id or ''}:{row_number}:{process_number}"
        records.append(
            {
                "decision_event_id": stable_id("de_", de_id_seed),
                "source_row_id": source_row_id,
                "process_id": stable_id("proc_", process_number),
                "decision_date": decision_date,
                "decision_year": decision_year,
                "current_rapporteur": rapporteur,
                "decision_origin": None,
                "decision_type": decision_type,
                "decision_progress": decision_progress,
                "decision_note": decision_note,
                "panel_indicator_raw": panel_raw,
                "is_collegiate": is_collegiate,
                "judging_body": judging_body,
                "time_bucket": decision_date[:7] if decision_date and len(decision_date) >= 7 else None,
                **juris_fields,
                "raw_fields": {
                    "source_file": filename,
                    "source_row_number": row_number,
                    "process_number": process_number,
                },
                "normalization_version": DECISION_EVENT_NORMALIZATION_VERSION,
                "source_id": SOURCE_ID,
                "created_at": timestamp,
                "updated_at": timestamp,
            }
        )

    return records


def build_decision_event_records(
    staging_file: Path = DEFAULT_STAGING_FILE,
    decision_index: dict[str, list[dict[str, Any]]] | None = None,
) -> list[dict[str, Any]]:
    timestamp = utc_now_iso()

    # Primary source
    records = _parse_records_decisoes(staging_file, decision_index, timestamp)
    logger.info("Loaded %d decision events from %s", len(records), staging_file.name)

    # Supplementary sources (auto-detected in the same directory)
    staging_dir = staging_file.parent
    for filename, col_map in _SUPPLEMENTARY_SOURCES:
        csv_path = staging_dir / filename
        if not csv_path.exists():
            continue
        supplementary = _parse_records_supplementary(csv_path, col_map, decision_index, timestamp)
        logger.info("Loaded %d decision events from %s", len(supplementary), filename)
        records.extend(supplementary)

    validate_records(records, SCHEMA_PATH)
    return records


def build_decision_event_jsonl(
    staging_file: Path = DEFAULT_STAGING_FILE,
    output_path: Path = DEFAULT_OUTPUT_PATH,
    decision_index: dict[str, list[dict[str, Any]]] | None = None,
) -> Path:
    records = build_decision_event_records(staging_file=staging_file, decision_index=decision_index)
    return write_jsonl(records, output_path)
