"""Build in-memory lookup indices from jurisprudencia JSONL files."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from ..core.identity import normalize_process_code

logger = logging.getLogger(__name__)

DEFAULT_JURIS_DIR = Path("data/raw/jurisprudencia")


def _iter_jsonl_files(directory: Path) -> list[Path]:
    if not directory.exists():
        return []
    return sorted(directory.glob("*.jsonl"))


def _safe_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _merge_first(target: dict[str, Any], key: str, value: Any) -> None:
    """Set key only if not already set and value is non-empty."""
    if target.get(key) is None:
        val = _safe_str(value)
        if val is not None:
            target[key] = val


def build_process_index(juris_dir: Path = DEFAULT_JURIS_DIR) -> dict[str, dict[str, Any]]:
    """Build a per-process enrichment dict from all jurisprudencia JSONL files.

    Returns a dict keyed by normalized process code ('SIGLA NUMERO')
    with enrichment fields from the first matching document.
    """
    index: dict[str, dict[str, Any]] = {}

    for subdir_name in ("decisoes", "acordaos"):
        subdir = juris_dir / subdir_name
        is_acordao = subdir_name == "acordaos"
        for jsonl_path in _iter_jsonl_files(subdir):
            with jsonl_path.open(encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    doc = json.loads(line)
                    raw_code = doc.get("processo_codigo_completo")
                    if not raw_code:
                        continue
                    key = normalize_process_code(raw_code)
                    if key not in index:
                        index[key] = {
                            "juris_inteiro_teor_url": None,
                            "juris_partes": None,
                            "juris_legislacao_citada": None,
                            "juris_procedencia": None,
                            "juris_classe_extenso": None,
                            "juris_doc_count": 0,
                            "juris_has_acordao": False,
                            "juris_has_decisao_monocratica": False,
                        }
                    entry = index[key]
                    entry["juris_doc_count"] += 1
                    if is_acordao:
                        entry["juris_has_acordao"] = True
                    else:
                        entry["juris_has_decisao_monocratica"] = True
                    _merge_first(entry, "juris_inteiro_teor_url", doc.get("inteiro_teor_url"))
                    _merge_first(entry, "juris_partes", doc.get("partes_lista_texto"))
                    _merge_first(entry, "juris_legislacao_citada", doc.get("documental_legislacao_citada_texto"))
                    _merge_first(entry, "juris_procedencia", doc.get("procedencia_geografica_completo"))
                    _merge_first(entry, "juris_classe_extenso", doc.get("processo_classe_processual_unificada_extenso"))

    logger.info("Jurisprudencia process index: %d processes indexed", len(index))
    return index


def build_decision_index(juris_dir: Path = DEFAULT_JURIS_DIR) -> dict[str, list[dict[str, Any]]]:
    """Build a per-(process, date) lookup for decision text enrichment.

    Returns a dict keyed by 'SIGLA NUMERO::YYYY-MM-DD' with a list of
    matching documents (there may be multiple decisions on the same day).
    """
    index: dict[str, list[dict[str, Any]]] = {}

    for subdir_name in ("decisoes", "acordaos"):
        subdir = juris_dir / subdir_name
        is_acordao = subdir_name == "acordaos"
        for jsonl_path in _iter_jsonl_files(subdir):
            with jsonl_path.open(encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    doc = json.loads(line)
                    raw_code = doc.get("processo_codigo_completo")
                    julgamento_data = doc.get("julgamento_data")
                    if not raw_code or not julgamento_data:
                        continue
                    process_key = normalize_process_code(raw_code)
                    date_str = str(julgamento_data).strip()[:10]
                    lookup_key = f"{process_key}::{date_str}"

                    entry: dict[str, Any] = {
                        "juris_doc_id": _safe_str(doc.get("_id")),
                        "juris_inteiro_teor_url": _safe_str(doc.get("inteiro_teor_url")),
                    }
                    if is_acordao:
                        entry["juris_ementa_texto"] = _safe_str(doc.get("ementa_texto"))
                        entry["juris_decisao_texto"] = None
                    else:
                        entry["juris_decisao_texto"] = _safe_str(doc.get("decisao_texto"))
                        entry["juris_ementa_texto"] = None

                    index.setdefault(lookup_key, []).append(entry)

    logger.info("Jurisprudencia decision index: %d (process, date) keys", len(index))
    return index
