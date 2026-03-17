"""Parse and classify STF ministerial agenda events."""

from __future__ import annotations

import re
from datetime import date as date_type
from datetime import time as time_type
from typing import Any

from ..core.identity import stable_id

PROCESS_REF_RE = re.compile(
    r"\b(ADI|ADPF|ADC|ADO|RE|ARE|HC|RHC|MS|RMS|MI|AP|Inq|Rcl|AC|AO|Pet|PPE|Ext|SE|"
    r"SIRDR|CC|AI|AImp|AOE|AR|AS|Cm|EI|EL|PSV|RC|RHD|RMI|RvC|SL|SS|STA|STP|TPA|EP|IF|HD)"
    r"\s*n?[o.]?\s*"
    r"(\d[\d./-]*)",
    re.IGNORECASE,
)

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")

_CORE_KW = [
    "sessao plenaria",
    "sessao administrativa",
    "sessao virtual",
    "plenario virtual",
    "julgamento",
    "sessao do tse",
]
_EXTERNAL_KW = [
    "agu",
    "pgr",
    "procurador-geral",
    "embaixador",
    "onu",
    "posse",
    "homenagem",
    "seminario",
    "congresso",
    "oab",
    "secretaria-geral",
]
_PRIVATE_KW = ["assunto:", "dr.", "dra.", "escritorio", "advogad"]
_PUBLIC_ACTOR_KW = [
    "agu",
    "pgr",
    "procurador-geral",
    "ministerio",
    "secretaria",
    "embaixador",
    "onu",
    "oab",
    "tribunal",
    "orgao",
    "governo",
]
_PRIVATE_ACTOR_KW = ["dr.", "dra.", "escritorio", "advogad", "banca"]
_CEREMONY_KW = ["posse", "homenagem", "titulo", "cerimonia", "entrega"]
_ACADEMIC_KW = ["seminario", "congresso", "palestra", "simposio"]
_ADMIN_KW = ["administrativa", "administrativo", "reuniao interna"]
_COURTESY_KW = ["cortesia", "visita de cortesia"]


def _clean_html(text: str | None) -> str:
    if not text:
        return ""
    cleaned = _HTML_TAG_RE.sub("", text)
    return _WHITESPACE_RE.sub(" ", cleaned).strip()


def _parse_time(raw: str | None) -> time_type | None:
    if not raw:
        return None
    raw = raw.strip()
    match = re.match(r"(\d{1,2})h(\d{2})", raw)
    if match:
        try:
            return time_type(int(match.group(1)), int(match.group(2)))
        except ValueError:
            return None
    match = re.match(r"(\d{1,2}):(\d{2})", raw)
    if match:
        try:
            return time_type(int(match.group(1)), int(match.group(2)))
        except ValueError:
            return None
    return None


def canonicalize_process_ref(raw: str) -> tuple[str, str]:
    match = PROCESS_REF_RE.search(raw)
    if not match:
        cleaned = raw.strip().upper()
        parts = cleaned.split(None, 1)
        if len(parts) == 2:
            return parts[0], re.sub(r"[^0-9]", "", parts[1])
        return cleaned, ""
    cls = match.group(1).upper()
    num_raw = match.group(2)
    num_clean = re.sub(r"/[A-Za-z]{2}$", "", num_raw)
    num_clean = re.sub(r"[./-]", "", num_clean)
    return cls, num_clean


def extract_process_refs(text: str) -> list[dict[str, str]]:
    refs: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for match in PROCESS_REF_RE.finditer(text):
        raw = match.group(0)
        cls, num = canonicalize_process_ref(raw)
        key = (cls, num)
        if key not in seen:
            seen.add(key)
            refs.append({"class": cls, "number": num, "raw": raw.strip()})
    return refs


def _has_keyword(text: str, keywords: list[str]) -> bool:
    text_lower = text.lower()
    return any(kw in text_lower for kw in keywords)


def classify_event_category(
    title: str,
    description: str,
) -> tuple[str, str, float, bool, bool]:
    combined = f"{title} {description}".strip()
    contains_public = _has_keyword(combined, _PUBLIC_ACTOR_KW)
    contains_private = _has_keyword(combined, _PRIVATE_ACTOR_KW)
    nature = classify_meeting_nature(title, description)

    if contains_public and contains_private:
        return "unclear", nature, 0.4, True, True
    if _has_keyword(combined, _CORE_KW):
        return "institutional_core", nature, 0.9, contains_public, contains_private
    if contains_public and not contains_private and _has_keyword(combined, _EXTERNAL_KW):
        return "institutional_external_actor", nature, 0.8, True, False
    if contains_private and not contains_public:
        return "private_advocacy", nature, 0.7, False, True
    if _has_keyword(combined, _PRIVATE_KW) and PROCESS_REF_RE.search(combined):
        return "private_advocacy", nature, 0.6, False, True
    if contains_public:
        return "institutional_external_actor", nature, 0.6, True, False
    return "unclear", nature, 0.3, contains_public, contains_private


def classify_meeting_nature(title: str, description: str) -> str:
    combined = f"{title} {description}".strip()
    if _has_keyword(combined, _CORE_KW):
        return "session"
    if _has_keyword(combined, _CEREMONY_KW):
        return "ceremony"
    if _has_keyword(combined, _ACADEMIC_KW):
        return "academic_event"
    if _has_keyword(combined, _COURTESY_KW):
        return "courtesy_visit"
    if _has_keyword(combined, _ADMIN_KW):
        return "administrative_event"
    if "audiencia" in combined.lower():
        return "hearing"
    if _has_keyword(combined, _PRIVATE_ACTOR_KW):
        return "private_meeting"
    if "reuniao" in combined.lower() and _has_keyword(combined, _PUBLIC_ACTOR_KW):
        return "official_meeting"
    return "other"


def extract_participants(description: str) -> tuple[list[str], list[str]]:
    participants: list[str] = []
    organizations: list[str] = []
    for match in re.finditer(r"(Dr[a]?\.?\s+[A-Z][a-zA-Z\s]+?)(?=[,;.\-]|$)", description):
        name = match.group(1).strip()
        if len(name) > 5:
            participants.append(name)
    for match in re.finditer(r"(AGU|PGR|OAB|Escritorio\s+[\w\s]+?)(?=[,;.\-]|$)", description):
        organizations.append(match.group(1).strip())
    return participants, organizations


def determine_owner_scope_and_role(
    minister_name_raw: str,
    event_date: date_type | None,
    president_mapping: list[dict[str, str]],
) -> tuple[str, str, str, str]:
    name_upper = minister_name_raw.strip().upper()
    non_ministerial = [
        "CENTRO DE ESTUDOS",
        "SECRETARIO-GERAL",
        "SECRETARIA-GERAL",
        "DIRETORA-GERAL",
        "DIRETOR-GERAL",
    ]
    for marker in non_ministerial:
        if marker in name_upper:
            slug = _slugify(minister_name_raw)
            role = "administrative" if "DIRETOR" in name_upper else "institutional_body"
            return slug, "non_ministerial", role, minister_name_raw

    if name_upper == "PRESIDENTE" or name_upper.startswith("PRESIDENTE "):
        if event_date:
            for p in president_mapping:
                start = date_type.fromisoformat(p["start_date"])
                end = date_type.fromisoformat(p["end_date"])
                if start <= event_date <= end:
                    return p["minister_slug"], "ministerial", "president", p["minister_name"]
        return "presidente", "ministerial", "president", minister_name_raw

    return _slugify(minister_name_raw), "ministerial", "minister", minister_name_raw


def _slugify(name: str) -> str:
    clean = re.sub(r"^MIN\.?\s*", "", name.strip().upper())
    parts = clean.split()
    return parts[-1].lower() if parts else clean.lower()


def normalize_raw_day(
    day_data: dict[str, Any],
    president_mapping: list[dict[str, str]],
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    date_raw = day_data.get("data", "")
    event_date: date_type | None = None
    date_match = re.match(r"(\d{2})/(\d{2})/(\d{4})", str(date_raw))
    if date_match:
        try:
            event_date = date_type(
                int(date_match.group(3)),
                int(date_match.group(2)),
                int(date_match.group(1)),
            )
        except ValueError:
            pass

    date_str = str(event_date) if event_date else ""
    fetched_at = day_data.get("fetched_at", "")

    for ministro in day_data.get("ministro") or []:
        minister_name_raw = ministro.get("nomeMinistro", "")
        minister_slug, owner_scope, owner_role, resolved_name = determine_owner_scope_and_role(
            minister_name_raw, event_date, president_mapping
        )
        for evento in ministro.get("eventos") or []:
            titulo = _clean_html(evento.get("titulo", ""))
            descricao = ""
            hora_raw = evento.get("hora", "")
            time_obj = _parse_time(hora_raw)

            cat, nature, conf, pub, priv = classify_event_category(titulo, descricao)
            process_refs = extract_process_refs(f"{titulo} {descricao}")
            has_process_ref = len(process_refs) > 0
            participants_raw, organizations_raw = extract_participants(descricao)
            relevance_track = "A" if has_process_ref else ("B" if participants_raw else "none")

            title_norm = titulo[:80].lower().strip()
            desc_short = descricao[:40].lower().strip()
            event_id = stable_id(
                "agd_",
                f"{minister_slug}:{date_str}:{hora_raw}:{title_norm}:{desc_short}",
            )
            events.append(
                {
                    "event_id": event_id,
                    "minister_name": resolved_name,
                    "minister_slug": minister_slug,
                    "owner_scope": owner_scope,
                    "owner_role": owner_role,
                    "event_date": date_str,
                    "event_time_local": str(time_obj) if time_obj else None,
                    "source_time_raw": hora_raw or None,
                    "event_title": titulo,
                    "event_description": descricao or None,
                    "event_category": cat,
                    "meeting_nature": nature,
                    "process_refs": process_refs,
                    "has_process_ref": has_process_ref,
                    "contains_public_actor": pub,
                    "contains_private_actor": priv,
                    "actor_count": len(participants_raw) + len(organizations_raw),
                    "classification_confidence": conf,
                    "participants_raw": participants_raw,
                    "participant_entities": [],
                    "participant_resolution_confidence": 0.0,
                    "organizations_raw": organizations_raw,
                    "relevance_track": relevance_track,
                    "source_date_raw": date_raw,
                    "fetched_at": fetched_at,
                    "coverage_scope": "public_agenda_partial",
                }
            )
    return events
