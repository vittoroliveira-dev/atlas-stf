"""Build canonical process records from staged datasets."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from ..core.identity import (
    infer_process_class_from_number,
    normalize_process_code,
    stable_id,
)
from ..core.parsers import (
    as_optional_str,
    first_non_null,
    infer_process_number,
    split_subjects,
)
from ..schema_validate import validate_records
from .common import (
    PROCESS_NORMALIZATION_VERSION,
    PROCESS_SOURCE_FILES,
    SOURCE_ID,
    stable_record_hash,
    utc_now_iso,
    write_jsonl,
)

SCHEMA_PATH = Path("schemas/process.schema.json")
DEFAULT_STAGING_DIR = Path("data/staging/transparencia")
DEFAULT_OUTPUT_PATH = Path("data/curated/process.jsonl")


def _empty_process(process_number: str) -> dict[str, Any]:
    timestamp = utc_now_iso()
    return {
        "process_id": stable_id("proc_", process_number),
        "process_number": process_number,
        "process_class": None,
        "filing_date": None,
        "closing_date": None,
        "origin_description": None,
        "origin_court_or_body": None,
        "branch_of_law": None,
        "subjects_raw": None,
        "subjects_normalized": None,
        "case_environment": None,
        "procedural_status": None,
        "raw_fields": {"source_files": []},
        "normalization_version": PROCESS_NORMALIZATION_VERSION,
        "source_id": SOURCE_ID,
        "source_record_hash": None,
        "created_at": timestamp,
        "updated_at": timestamp,
        "juris_inteiro_teor_url": None,
        "juris_partes": None,
        "juris_legislacao_citada": None,
        "juris_procedencia": None,
        "juris_classe_extenso": None,
        "juris_doc_count": None,
        "juris_has_acordao": None,
        "juris_has_decisao_monocratica": None,
    }


def _merge_process(record: dict[str, Any], row: dict[str, Any], filename: str, row_number: int) -> None:
    if record["source_record_hash"] is None:
        record["source_record_hash"] = stable_record_hash(filename, row_number, record["process_number"])
        record["raw_fields"]["representative_source_file"] = filename
        record["raw_fields"]["representative_row_number"] = row_number

    if filename not in record["raw_fields"]["source_files"]:
        record["raw_fields"]["source_files"].append(filename)

    field_map = {
        "process_class": first_non_null(row, "classe", "classe_processo", "tipo_classe")
        or infer_process_class_from_number(record["process_number"]),
        "filing_date": first_non_null(row, "data_autuacao", "data_de_autuacao"),
        "closing_date": as_optional_str(row.get("data_baixa")),
        "origin_description": first_non_null(row, "procedencia", "descricao_procedencia_processo"),
        "origin_court_or_body": first_non_null(row, "orgao_origem", "descricao_orgao_origem"),
        "branch_of_law": first_non_null(row, "ramo_do_direito", "ramo_direito", "ramos_do_direito"),
        "case_environment": first_non_null(row, "meio_processo", "ambiente_julgamento"),
        "procedural_status": first_non_null(
            row,
            "situacao_processual",
            "em_tramitacao",
            "indicador_de_tramitacao",
            "situacao_processo_paradigma",
        ),
    }
    for field, value in field_map.items():
        value_text = as_optional_str(value)
        if record[field] is None and value_text is not None:
            record[field] = value_text

    subjects_value = first_non_null(
        row,
        "assuntos",
        "assunto",
        "assunto_completo",
        "assuntos_do_processo",
        "assunto_relacionado",
    )
    if record["subjects_raw"] is None and as_optional_str(subjects_value) is not None:
        subjects = split_subjects(subjects_value)
        record["subjects_raw"] = subjects
        record["subjects_normalized"] = subjects


def _enrich_with_jurisprudencia(
    process_map: dict[str, dict[str, Any]],
    juris_index: dict[str, dict[str, Any]],
) -> int:
    """Apply jurisprudencia enrichment fields to matching processes. Returns match count."""
    matched = 0
    for process_number, record in process_map.items():
        key = normalize_process_code(process_number)
        entry = juris_index.get(key)
        if entry is None:
            continue
        matched += 1
        for field, value in entry.items():
            record[field] = value
    return matched


def build_process_records(
    staging_dir: Path = DEFAULT_STAGING_DIR,
    juris_index: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    process_map: dict[str, dict[str, Any]] = {}

    for filename in PROCESS_SOURCE_FILES:
        path = staging_dir / filename
        if not path.exists():
            continue
        df = pd.read_csv(path, dtype=str, low_memory=False)
        for row_number, (_, series) in enumerate(df.iterrows(), start=1):
            row = series.to_dict()
            process_number = infer_process_number(row)
            if not process_number:
                continue
            record = process_map.setdefault(process_number, _empty_process(process_number))
            _merge_process(record, row, filename, row_number)

    if juris_index:
        _enrich_with_jurisprudencia(process_map, juris_index)

    records = sorted(process_map.values(), key=lambda item: item["process_number"])
    validate_records(records, SCHEMA_PATH)
    return records


def build_process_jsonl(
    staging_dir: Path = DEFAULT_STAGING_DIR,
    output_path: Path = DEFAULT_OUTPUT_PATH,
    juris_index: dict[str, dict[str, Any]] | None = None,
) -> Path:
    records = build_process_records(staging_dir=staging_dir, juris_index=juris_index)
    return write_jsonl(records, output_path)
