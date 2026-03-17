"""Parse STF portal process pages into structured data.

The STF portal (portal.stf.jus.br) uses ASP.NET server-rendered HTML with
div-based layouts (not tables). Each tab is fetched individually and returns
an HTML fragment.

Tab structure (validated against live portal 2026-03):
- abaAndamentos: ``<div class="andamento-data">DD/MM/YYYY</div>`` +
  ``<h5 class="andamento-nome">description</h5>``
- abaPartes: ``<div class="detalhe-parte">role</div>`` +
  ``<div class="nome-parte">NAME (OAB/UF)</div>``
- abaPeticoes: ``<span class="processo-detalhes-bold">protocol</span>`` +
  ``Peticionado em DD/MM/YYYY`` + ``Recebido em ...``
- abaDeslocamentos: ``<span class="processo-detalhes-bold">destination</span>``
  + ``Enviado por ... em DD/MM/YYYY`` + ``Guia ...`` + ``Recebido em ...``
- abaInformacoes: label/value div pairs with ``processo-detalhes-bold``
- abaSessao: JS-rendered voting widget (limited static content)
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from typing import Any


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _clean_text(text: str | None) -> str | None:
    """Strip HTML tags and normalize whitespace."""
    if not text:
        return None
    cleaned = re.sub(r"<[^>]+>", "", text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or None


def _parse_date(text: str | None) -> str | None:
    """Extract a date in YYYY-MM-DD from various PT-BR formats."""
    if not text:
        return None
    text = text.strip()

    # Try DD/MM/YYYY
    match = re.search(r"(\d{2})/(\d{2})/(\d{4})", text)
    if match:
        day, month, year = match.groups()
        return f"{year}-{month}-{day}"

    # Try YYYY-MM-DD already
    match = re.search(r"(\d{4})-(\d{2})-(\d{2})", text)
    if match:
        return match.group(0)

    return None


# ---------------------------------------------------------------------------
# Andamentos
# ---------------------------------------------------------------------------

_ANDAMENTO_RE = re.compile(
    r'<div\s+class="andamento-data[^"]*">\s*([\d/]+)\s*</div>'
    r".*?"
    r'<h5\s+class="andamento-nome[^"]*">\s*(.*?)\s*</h5>',
    re.DOTALL,
)

# Detail text lives in a sibling div: <div class="col-md-9 p-0">text</div>
_ANDAMENTO_DETAIL_RE = re.compile(
    r'<div\s+class="col-md-9\s+p-0\s*">\s*(.*?)\s*</div>',
    re.DOTALL,
)


def parse_andamentos_html(html: str) -> list[dict[str, Any]]:
    """Parse andamentos (procedural events) from abaAndamentos HTML.

    Real structure:
    ``<div class="andamento-data ">18/06/2025</div>``
    ``<h5 class="andamento-nome ">Conclusos ao(à) Relator(a)</h5>``
    ``<div class="col-md-9 p-0">detail text</div>``
    """
    events: list[dict[str, Any]] = []
    for match in _ANDAMENTO_RE.finditer(html):
        date_text = match.group(1).strip()
        desc_text = _clean_text(match.group(2))
        parsed_date = _parse_date(date_text)
        if not parsed_date or not desc_text:
            continue
        # Extract detail from the next col-md-9 div after this match
        detail: str | None = None
        after = html[match.end() : match.end() + 2000]
        detail_match = _ANDAMENTO_DETAIL_RE.search(after)
        if detail_match:
            detail = _clean_text(detail_match.group(1))
        events.append(
            {
                "date": parsed_date,
                "description": desc_text,
                "detail": detail,
                "tab_name": "Andamentos",
            }
        )
    return events


# ---------------------------------------------------------------------------
# Deslocamentos
# ---------------------------------------------------------------------------

_DESLOCAMENTO_RE = re.compile(
    r'<div\s+class="col-md-12\s+lista-dados\s+p-r-0\s+p-l-0">'
    r'.*?<span\s+class="processo-detalhes-bold">\s*(.*?)\s*</span>'
    r".*?Enviado por\s+(.*?)\s+em\s+([\d/]+)"
    r".*?Guia\s+([\d/]+)"
    r".*?Recebido em\s+([\d/]+)",
    re.DOTALL,
)


def parse_deslocamentos_html(html: str) -> list[dict[str, Any]]:
    """Parse deslocamentos (transfers) from abaDeslocamentos HTML.

    Real structure: div blocks with destination, sender, sent date, guia,
    and received date.
    """
    events: list[dict[str, Any]] = []
    for match in _DESLOCAMENTO_RE.finditer(html):
        destination = _clean_text(match.group(1))
        origin = _clean_text(match.group(2))
        sent_date = _parse_date(match.group(3))
        guia = match.group(4).strip()
        received_date = _parse_date(match.group(5))

        if not sent_date:
            continue
        events.append(
            {
                "date": sent_date,
                "origin": origin,
                "destination": destination,
                "guia": guia,
                "received_date": received_date,
                "tab_name": "Deslocamentos",
            }
        )
    return events


# ---------------------------------------------------------------------------
# Petições
# ---------------------------------------------------------------------------

_PETICAO_RE = re.compile(
    r'<span\s+class="processo-detalhes-bold">\s*([\d/]+)\s*</span>\s*'
    r'<span\s+class="processo-detalhes">\s*Peticionado em ([\d/]+)\s*</span>',
    re.DOTALL,
)

_PETICAO_RECEBIDO_RE = re.compile(
    r"Recebido em ([\d/]+\s*[\d:]*)\s+por\s+(.*?)(?:</span>|$)",
    re.DOTALL,
)


def parse_peticoes_html(html: str) -> list[dict[str, Any]]:
    """Parse petições from abaPeticoes HTML.

    Real structure:
    ``<span class="processo-detalhes-bold">84897/2025</span>``
    ``<span class="processo-detalhes">Peticionado em 18/06/2025</span>``
    """
    events: list[dict[str, Any]] = []
    for match in _PETICAO_RE.finditer(html):
        protocol = match.group(1).strip()
        date_text = match.group(2).strip()
        parsed_date = _parse_date(date_text)
        if not parsed_date:
            continue

        # Try to find the "Recebido em" line nearby
        receiver: str | None = None
        after_text = html[match.end() : match.end() + 300]
        rec_match = _PETICAO_RECEBIDO_RE.search(after_text)
        if rec_match:
            receiver = _clean_text(rec_match.group(2))

        events.append(
            {
                "date": parsed_date,
                "protocol": protocol,
                "receiver": receiver,
                "tab_name": "Peticoes",
            }
        )
    return events


# ---------------------------------------------------------------------------
# Sessão Virtual
# ---------------------------------------------------------------------------


def parse_sessao_virtual_html(html: str) -> list[dict[str, Any]]:
    """Parse sessão virtual data from abaSessao HTML.

    The abaSessao tab uses JS to load voting data from an external API.
    Static parsing captures minimal data; returns empty if no static content.
    """
    # The abaSessao tab is largely JS-rendered; static HTML has script tags
    # with AJAX calls to sistemas.stf.jus.br/repgeral/votacao. We cannot
    # extract structured data without executing JS.
    return []


# ---------------------------------------------------------------------------
# Informações
# ---------------------------------------------------------------------------

_INFO_LABEL_RE = re.compile(
    r'<div[^>]*class="[^"]*processo-detalhes-bold[^"]*"[^>]*>\s*'
    r"(.*?)\s*</div>\s*"
    r'<div[^>]*class="[^"]*processo-detalhes(?:-bold)?[^"]*"[^>]*>\s*'
    r"(.*?)\s*</div>",
    re.DOTALL,
)

_INFO_ASSUNTO_RE = re.compile(
    r'<div[^>]*class="[^"]*informacoes__assunto[^"]*"[^>]*>.*?<ul>(.*?)</ul>',
    re.DOTALL,
)


def parse_informacoes_html(html: str) -> dict[str, Any]:
    """Parse informações (process metadata) from abaInformacoes HTML.

    Real structure: div pairs with ``processo-detalhes-bold`` labels and
    ``processo-detalhes`` values. Assunto is a ``<ul>`` of ``<li>`` items.
    """
    info: dict[str, Any] = {"tab_name": "Informacoes"}

    field_map: dict[str, str] = {
        "data de protocolo": "data_protocolo",
        "órgão de origem": "orgao_origem",
        "orgao de origem": "orgao_origem",
        "origem": "origem",
        "número de origem": "numero_origem",
        "numero de origem": "numero_origem",
    }

    for match in _INFO_LABEL_RE.finditer(html):
        label = _clean_text(match.group(1))
        value = _clean_text(match.group(2))
        if not label or not value:
            continue
        label_lower = label.lower().rstrip(":")
        for key, field_name in field_map.items():
            if key in label_lower and field_name not in info:
                info[field_name] = value
                break

    # Extract assuntos
    assunto_match = _INFO_ASSUNTO_RE.search(html)
    if assunto_match:
        items = re.findall(r"<li>(.*?)</li>", assunto_match.group(1), re.DOTALL)
        info["assuntos"] = [_clean_text(item) for item in items if _clean_text(item)]

    # Extract orgao-procedencia and descricao-procedencia spans
    proc_match = re.search(r'<span\s+id="orgao-procedencia">\s*(.*?)\s*</span>', html, re.DOTALL)
    if proc_match:
        val = _clean_text(proc_match.group(1))
        if val:
            info["orgao_procedencia"] = val

    desc_match = re.search(r'<span\s+id="descricao-procedencia">\s*(.*?)\s*</span>', html, re.DOTALL)
    if desc_match:
        val = _clean_text(desc_match.group(1))
        if val:
            info["descricao_procedencia"] = val

    return info


# ---------------------------------------------------------------------------
# Partes e Representantes
# ---------------------------------------------------------------------------

_PARTE_RE = re.compile(
    r'<div\s+class="detalhe-parte">\s*(.*?)\s*</div>\s*'
    r'<div\s+class="nome-parte">\s*(.*?)\s*</div>',
    re.DOTALL,
)

_OAB_INLINE_RE = re.compile(r"\((\d{1,6})/([A-Z]{2})\)")


def parse_partes_representantes_html(html: str) -> list[dict[str, Any]]:
    """Extract parties and representatives from abaPartes HTML.

    Real structure:
    ``<div class="detalhe-parte">REQTE.(S)</div>``
    ``<div class="nome-parte">NOME (12345/SP)</div>``

    OAB numbers are inline in the name for ADV entries: ``NAME (12345/UF)``.
    """
    results: list[dict[str, Any]] = []
    for match in _PARTE_RE.finditer(html):
        role = _clean_text(match.group(1))
        name_raw = _clean_text(match.group(2))
        if not role or not name_raw:
            continue

        oab_number: str | None = None
        oab_state: str | None = None
        name = name_raw

        oab_match = _OAB_INLINE_RE.search(name_raw)
        if oab_match:
            oab_number = f"{oab_match.group(1)}/{oab_match.group(2)}"
            oab_state = oab_match.group(2)
            name = name_raw[: oab_match.start()].strip()

        results.append(
            {
                "party_name": name,
                "party_role": role,
                "oab_number": oab_number,
                "oab_state": oab_state,
            }
        )
    return results


# ---------------------------------------------------------------------------
# Petições detalhadas (reusa parse_peticoes_html)
# ---------------------------------------------------------------------------


def parse_peticoes_detailed_html(html: str) -> list[dict[str, Any]]:
    """Extended petition parser — same data as parse_peticoes_html.

    The portal does not expose petitioner names in the HTML fragment.
    Returns the same structure as parse_peticoes_html.
    """
    return parse_peticoes_html(html)


# ---------------------------------------------------------------------------
# Sustentação Oral
# ---------------------------------------------------------------------------


def parse_oral_argument_html(html: str) -> list[dict[str, Any]]:
    """Extract oral argument data from abaSessao HTML.

    The abaSessao tab is JS-rendered; oral argument data is not available
    in the static HTML. Returns empty list.
    """
    return []


# ---------------------------------------------------------------------------
# Document assembly
# ---------------------------------------------------------------------------


def build_process_document(
    process_number: str,
    source_url: str,
    raw_html: str,
    andamentos: list[dict[str, Any]],
    deslocamentos: list[dict[str, Any]],
    peticoes: list[dict[str, Any]],
    sessao_virtual: list[dict[str, Any]],
    informacoes: dict[str, Any],
    *,
    representantes: list[dict[str, Any]] | None = None,
    peticoes_detailed: list[dict[str, Any]] | None = None,
    oral_arguments: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Assemble a complete process document with provenance metadata."""
    doc: dict[str, Any] = {
        "process_number": process_number,
        "source_system": "stf_portal",
        "source_url": source_url,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "raw_html_hash": _sha256(raw_html),
        "andamentos": andamentos,
        "deslocamentos": deslocamentos,
        "peticoes": peticoes,
        "sessao_virtual": sessao_virtual,
        "informacoes": informacoes,
    }
    if representantes:
        doc["representantes"] = representantes
    if peticoes_detailed:
        doc["peticoes_detailed"] = peticoes_detailed
    if oral_arguments:
        doc["oral_arguments"] = oral_arguments
    return doc
