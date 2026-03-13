"""Extract experimental CPF/CNPJ occurrences from raw jurisprudencia text."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from ..core.identity import (
    is_valid_cnpj,
    is_valid_cpf,
    normalize_entity_name,
    normalize_process_code,
    normalize_tax_id,
    stable_id,
)
from ..schema_validate import validate_records
from .common import read_jsonl_records, utc_now_iso, write_jsonl

SCHEMA_PATH = Path("schemas/entity_identifier.schema.json")
DEFAULT_PROCESS_PATH = Path("data/curated/process.jsonl")
DEFAULT_JURIS_DIR = Path("data/raw/jurisprudencia")
DEFAULT_OUTPUT_PATH = Path("data/curated/entity_identifier.jsonl")
SOURCE_FIELDS = (
    "partes_lista_texto",
    "decisao_texto",
    "inteiro_teor_texto",
    "documental_observacao_texto",
)
LABELED_TAX_ID_RE = re.compile(
    r"(?i)\b(?P<label>CPF|CNPJ)\s*(?:N[ºO]\s*)?[:\-]?\s*(?P<value>\d{11}|\d{14}|\d{3}\.\d{3}\.\d{3}-\d{2}|\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})"
)
FORMATTED_TAX_ID_RE = re.compile(r"(?<!\d)(?P<value>\d{3}\.\d{3}\.\d{3}-\d{2}|\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})(?!\d)")
ENTITY_HINT_RE = re.compile(
    r"(?P<name>[A-ZÀ-Ü][A-ZÀ-Ü\s.'-]{3,})\s+(?:CPF|CNPJ)\b",
    flags=re.UNICODE,
)


def _iter_jsonl_files(directory: Path) -> list[Path]:
    if not directory.exists():
        return []
    return sorted(directory.glob("*.jsonl"))


def _context_snippet(text: str, start: int, end: int, radius: int = 80) -> str:
    snippet = text[max(0, start - radius) : min(len(text), end + radius)]
    return re.sub(r"\s+", " ", snippet).strip()


def _infer_entity_name_hint(text: str, end: int) -> str | None:
    window = text[max(0, end - 160) : min(len(text), end + 20)]
    matches = list(ENTITY_HINT_RE.finditer(window))
    if not matches:
        return None
    return normalize_entity_name(matches[-1].group("name"))


def _classify_tax_id(raw_value: str) -> tuple[str, str] | None:
    normalized = normalize_tax_id(raw_value)
    if normalized is None:
        return None
    if len(normalized) == 11 and is_valid_cpf(normalized):
        return ("cpf", normalized)
    if len(normalized) == 14 and is_valid_cnpj(normalized):
        return ("cnpj", normalized)
    return None


def _extract_from_text(
    *,
    text: str,
    process_id: str | None,
    process_number: str | None,
    source_doc_type: str,
    source_file: str,
    source_field: str,
    juris_doc_id: str | None,
    source_url: str | None,
    created_at: str,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    seen: set[tuple[str | None, str, str, str]] = set()

    for match in LABELED_TAX_ID_RE.finditer(text):
        raw_value = match.group("value")
        classified = _classify_tax_id(raw_value)
        if classified is None:
            continue
        identifier_kind, normalized_value = classified
        key = (juris_doc_id, source_field, identifier_kind, normalized_value)
        if key in seen:
            continue
        seen.add(key)
        context = _context_snippet(text, match.start("value"), match.end("value"))
        records.append(
            {
                "identifier_occurrence_id": stable_id(
                    "eid_",
                    f"{process_number}:{juris_doc_id}:{source_field}:{normalized_value}:{match.start('value')}",
                ),
                "process_id": process_id,
                "process_number": process_number,
                "juris_doc_id": juris_doc_id,
                "source_doc_type": source_doc_type,
                "source_file": source_file,
                "source_field": source_field,
                "source_url": source_url,
                "identifier_kind": identifier_kind,
                "identifier_value_raw": raw_value,
                "identifier_value_normalized": normalized_value,
                "context_snippet": context,
                "entity_name_hint": _infer_entity_name_hint(text, match.end()),
                "extraction_confidence": 0.95,
                "extraction_method": "regex_labeled_tax_id",
                "uncertainty_note": None,
                "created_at": created_at,
            }
        )

    for match in FORMATTED_TAX_ID_RE.finditer(text):
        raw_value = match.group("value")
        if LABELED_TAX_ID_RE.search(text[max(0, match.start() - 30) : min(len(text), match.end() + 10)]):
            continue
        classified = _classify_tax_id(raw_value)
        if classified is None:
            continue
        identifier_kind, normalized_value = classified
        key = (juris_doc_id, source_field, identifier_kind, normalized_value)
        if key in seen:
            continue
        seen.add(key)
        records.append(
            {
                "identifier_occurrence_id": stable_id(
                    "eid_",
                    f"{process_number}:{juris_doc_id}:{source_field}:{normalized_value}:{match.start('value')}",
                ),
                "process_id": process_id,
                "process_number": process_number,
                "juris_doc_id": juris_doc_id,
                "source_doc_type": source_doc_type,
                "source_file": source_file,
                "source_field": source_field,
                "source_url": source_url,
                "identifier_kind": identifier_kind,
                "identifier_value_raw": raw_value,
                "identifier_value_normalized": normalized_value,
                "context_snippet": _context_snippet(text, match.start("value"), match.end("value")),
                "entity_name_hint": _infer_entity_name_hint(text, match.end()),
                "extraction_confidence": 0.8,
                "extraction_method": "regex_formatted_tax_id",
                "uncertainty_note": "INCERTO",
                "created_at": created_at,
            }
        )

    return records


def build_entity_identifier_records(
    *,
    process_path: Path = DEFAULT_PROCESS_PATH,
    juris_dir: Path = DEFAULT_JURIS_DIR,
) -> list[dict[str, Any]]:
    process_map: dict[str, dict[str, Any]] = {}
    for record in read_jsonl_records(process_path):
        process_number = record.get("process_number")
        if process_number:
            process_map[normalize_process_code(str(process_number))] = record

    created_at = utc_now_iso()
    records: list[dict[str, Any]] = []
    for source_doc_type in ("decisoes", "acordaos"):
        source_dir = juris_dir / source_doc_type
        for jsonl_path in _iter_jsonl_files(source_dir):
            with jsonl_path.open(encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    doc = json.loads(line)
                    process_number = doc.get("processo_codigo_completo")
                    if not process_number:
                        continue
                    normalized_process = normalize_process_code(str(process_number))
                    process_record = process_map.get(normalized_process, {})
                    process_id = process_record.get("process_id")
                    for source_field in SOURCE_FIELDS:
                        text = doc.get(source_field)
                        if not isinstance(text, str) or not text.strip():
                            continue
                        records.extend(
                            _extract_from_text(
                                text=text,
                                process_id=process_id,
                                process_number=normalized_process,
                                source_doc_type=source_doc_type,
                                source_file=jsonl_path.name,
                                source_field=source_field,
                                juris_doc_id=doc.get("_id"),
                                source_url=doc.get("inteiro_teor_url"),
                                created_at=created_at,
                            )
                        )

    validate_records(records, SCHEMA_PATH)
    return records


def build_entity_identifier_jsonl(
    *,
    process_path: Path = DEFAULT_PROCESS_PATH,
    juris_dir: Path = DEFAULT_JURIS_DIR,
    output_path: Path = DEFAULT_OUTPUT_PATH,
) -> Path:
    records = build_entity_identifier_records(process_path=process_path, juris_dir=juris_dir)
    return write_jsonl(records, output_path)
