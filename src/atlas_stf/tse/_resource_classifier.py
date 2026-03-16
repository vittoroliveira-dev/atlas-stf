"""Deterministic classification of TSE donation resource types (donation_description).

Pure module (zero I/O).  All lookup keys and keywords are stored pre-normalized
(uppercase, no accents).  The classifier applies a two-stage normalisation to the
raw input before matching:
  1. strip + upper + null-marker check  (preserves '#' for #NULO)
  2. full normalisation (remove accents, collapse whitespace, strip edge punctuation)
"""

from __future__ import annotations

import re
import unicodedata
from typing import NamedTuple

# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


class ResourceClassification(NamedTuple):
    category: str  # empty | payment_method | source_type | in_kind | unknown
    subtype: str
    confidence: str  # high | medium | none
    rule: str


# ---------------------------------------------------------------------------
# Null markers (checked BEFORE full normalisation)
# ---------------------------------------------------------------------------

_NULL_MARKERS: frozenset[str] = frozenset({"#NULO", "NULO", "NULL", "#NULO#"})

# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------


def _normalize_for_classification(raw: str) -> str:
    """Deterministic text normalisation: upper, strip accents, collapse ws."""
    text = raw.strip().upper()
    # Remove accents via NFKD decomposition
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    # Strip edge punctuation (but not inner)
    text = text.strip(".,;:-–—/\\()[]{}\"'")
    return text


# ---------------------------------------------------------------------------
# Exact-match lookup (source_type + payment_method)
# ---------------------------------------------------------------------------

# source_type values
_SOURCE_TYPE_LOOKUP: dict[str, str] = {
    "RECURSOS DE PESSOAS FISICAS": "individual",
    "RECURSOS DE PESSOAS JURIDICAS": "corporate",
    "RECURSOS PROPRIOS": "own_resources",
    "RECURSOS PROPRIO": "own_resources",
    "RECURSOS DE ORIGENS NAO IDENTIFICADAS": "unidentified_source",
    "RECURSOS DE PARTIDO POLITICO": "party_transfer",
    "RECURSOS DE OUTROS CANDIDATOS/COMITES": "committee_transfer",
    "RECURSOS DE COMITES": "committee_transfer",
    "RECURSOS DE OUTROS CANDIDATOS": "committee_transfer",
    "COMERCIALIZACAO DE BENS OU REALIZACAO DE EVENTOS": "events_commerce",
    "COMERCIALIZACAO DE BENS E REALIZACAO DE EVENTOS": "events_commerce",
    "FUNDO PARTIDARIO": "party_fund",
    "FUNDO ESPECIAL DE FINANCIAMENTO DE CAMPANHA": "campaign_fund",
    "FUNDO ESPECIAL": "campaign_fund",
    "FEFC": "campaign_fund",
    "FINANCIAMENTO COLETIVO POR MEIO DA INTERNET": "internet",
    "RENDIMENTOS DE APLICACOES FINANCEIRAS": "events_commerce",
    "DOACAO PELA INTERNET": "internet",
    "DOACOES PELA INTERNET": "internet",
    "RECURSOS DE FINANCIAMENTO COLETIVO": "internet",
}

# payment_method (text variants)
_PAYMENT_METHOD_LOOKUP: dict[str, str] = {
    "EM ESPECIE": "cash",
    "DINHEIRO": "cash",
    "CHEQUE": "check",
    "ESTIMADO": "estimated",
    "ESTIMAVEL": "estimated",
    "NAO INFORMADO": "not_informed",
    "NAO IDENTIFICADO": "not_informed",
}

# payment_method (numeric codes — only 0, 1, 2 are accepted)
_NUMERIC_CODE_MAP: dict[str, str] = {
    "0": "cash",
    "1": "check",
    "2": "estimated",
}

# ---------------------------------------------------------------------------
# Keyword rules for in_kind sub-typing (ordered by priority)
# ---------------------------------------------------------------------------

_IN_KIND_KEYWORDS: list[tuple[str, list[str]]] = [
    (
        "campaign_material",
        [
            "SANTINHO",
            "ADESIVO",
            "BANDEIRA",
            "BANNER",
            "FAIXA",
            "PANFLETO",
            "CAMISETA",
            "BONE",
            "BROCHE",
            "FLYER",
            "MATERIAL DE CAMPANHA",
            "MATERIAL DE PROPAGANDA",
            "PROPAGANDA ELEITORAL",
            "MATERIAL GRAFICO",
            "MATERIAL PUBLICITARIO",
            "PLACA",
        ],
    ),
    (
        "professional_service",
        [
            "SERVICO",
            "CONSULTORIA",
            "ASSESSORIA",
            "ADVOCACIA",
            "CONTABILIDADE",
            "CONTABIL",
            "JURIDICO",
            "PESQUISA",
            "MARKETING",
            "PUBLICIDADE",
            "PRESTACAO DE SERVICO",
            "HONORARIO",
            "LOCACAO DE MAO DE OBRA",
        ],
    ),
    (
        "transport_fuel",
        [
            "COMBUSTIVEL",
            "GASOLINA",
            "ETANOL",
            "DIESEL",
            "TRANSPORTE",
            "FRETE",
            "PASSAGEM",
            "DESLOCAMENTO",
            "VEICULO",
            "TAXI",
            "UBER",
        ],
    ),
    (
        "media_communication",
        [
            "RADIO",
            "TELEVISAO",
            "TV",
            "JORNAL",
            "INSERCAO",
            "MIDIA",
            "HORARIO ELEITORAL",
            "COMUNICACAO",
            "TELEFONE",
            "INTERNET",
            "SITE",
            "REDE SOCIAL",
        ],
    ),
    (
        "rental_property",
        [
            "ALUGUEL",
            "LOCACAO DE IMOVEL",
            "LOCACAO DE ESPACO",
            "CESSAO DE ESPACO",
            "CESSAO DE IMOVEL",
            "SALA",
            "IMOVEL",
            "COMITE",
        ],
    ),
    (
        "campaign_worker",
        [
            "PESSOAL",
            "FUNCIONARIO",
            "CABO ELEITORAL",
            "MILITANTE",
            "COORDENADOR",
            "FISCAL",
            "MESARIO",
            "FOLHA DE PAGAMENTO",
            "SALARIO",
        ],
    ),
    (
        "printing",
        [
            "GRAFICA",
            "IMPRESSAO",
            "TIPOGRAFIA",
            "COPIA",
            "XEROX",
        ],
    ),
    (
        "food_beverage",
        [
            "ALIMENTACAO",
            "ALIMENTO",
            "REFEICAO",
            "AGUA",
            "BEBIDA",
            "CAFE",
            "LANCHE",
            "COMIDA",
        ],
    ),
    (
        "volunteer_work",
        [
            "TRABALHO VOLUNTARIO",
            "CESSAO DE MAO DE OBRA",
        ],
    ),
    (
        "other_item",
        [
            "DOACAO",
            "ESTIMAVEL EM DINHEIRO",
            "DOACAO EM BENS",
            "DOACAO ESTIMADA",
            "CONTRIBUICAO",
            "RECURSO",
        ],
    ),
]


# ---------------------------------------------------------------------------
# Main classifier
# ---------------------------------------------------------------------------


def classify_resource_type(description: str | None) -> ResourceClassification:
    """Classify a TSE donation_description into a structured taxonomy.

    Returns a :class:`ResourceClassification` named-tuple with
    ``(category, subtype, confidence, rule)``.
    """
    if description is None:
        return ResourceClassification("empty", "blank", "none", "empty:blank")

    # Stage 1: strip + upper (preserves # for null markers)
    raw_upper = description.strip().upper()
    if raw_upper == "" or raw_upper in _NULL_MARKERS:
        subtype = "null_marker" if raw_upper in _NULL_MARKERS else "blank"
        rule_suffix = raw_upper if raw_upper else "blank"
        return ResourceClassification("empty", subtype, "none", f"empty:{rule_suffix}")

    # Stage 2: numeric code check (before full normalisation)
    if raw_upper in _NUMERIC_CODE_MAP:
        subtype = _NUMERIC_CODE_MAP[raw_upper]
        return ResourceClassification("payment_method", subtype, "high", f"code:{raw_upper}")

    # Stage 3: full normalisation
    normalised = _normalize_for_classification(description)
    if not normalised:
        return ResourceClassification("empty", "blank", "none", "empty:blank")

    # Stage 4: exact match — source_type
    if normalised in _SOURCE_TYPE_LOOKUP:
        subtype = _SOURCE_TYPE_LOOKUP[normalised]
        return ResourceClassification("source_type", subtype, "high", f"exact:{normalised}")

    # Stage 4b: exact match — payment_method text
    if normalised in _PAYMENT_METHOD_LOOKUP:
        subtype = _PAYMENT_METHOD_LOOKUP[normalised]
        return ResourceClassification("payment_method", subtype, "high", f"exact:{normalised}")

    # Stage 5: keyword search — in_kind
    for subtype, keywords in _IN_KIND_KEYWORDS:
        for kw in keywords:
            if kw in normalised:
                return ResourceClassification("in_kind", subtype, "medium", f"keyword:{kw}")

    # Stage 6: fallback
    return ResourceClassification("unknown", "unclassified", "none", "unclassified")
