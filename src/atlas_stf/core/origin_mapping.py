"""Pure mapping functions: origin fields → DataJud API indices."""

from __future__ import annotations

import unicodedata

STATE_TO_UF: dict[str, str] = {
    "ACRE": "AC",
    "ALAGOAS": "AL",
    "AMAPA": "AP",
    "AMAZONAS": "AM",
    "BAHIA": "BA",
    "CEARA": "CE",
    "DISTRITO FEDERAL": "DF",
    "ESPIRITO SANTO": "ES",
    "GOIAS": "GO",
    "MARANHAO": "MA",
    "MATO GROSSO": "MT",
    "MATO GROSSO DO SUL": "MS",
    "MINAS GERAIS": "MG",
    "PARA": "PA",
    "PARAIBA": "PB",
    "PARANA": "PR",
    "PERNAMBUCO": "PE",
    "PIAUI": "PI",
    "RIO DE JANEIRO": "RJ",
    "RIO GRANDE DO NORTE": "RN",
    "RIO GRANDE DO SUL": "RS",
    "RONDONIA": "RO",
    "RORAIMA": "RR",
    "SANTA CATARINA": "SC",
    "SAO PAULO": "SP",
    "SERGIPE": "SE",
    "TOCANTINS": "TO",
}

UF_TO_TRF: dict[str, int] = {
    "AC": 1,
    "AM": 1,
    "AP": 1,
    "BA": 1,
    "DF": 1,
    "GO": 1,
    "MA": 1,
    "MG": 6,
    "MT": 1,
    "PA": 1,
    "PI": 1,
    "RO": 1,
    "RR": 1,
    "TO": 1,
    "ES": 2,
    "RJ": 2,
    "MS": 3,
    "SP": 3,
    "AL": 5,
    "CE": 5,
    "PB": 5,
    "PE": 5,
    "RN": 5,
    "SE": 5,
    "PR": 4,
    "RS": 4,
    "SC": 4,
}

UF_TO_TJ_INDEX: dict[str, str] = {
    "AC": "api_publica_tjac",
    "AL": "api_publica_tjal",
    "AM": "api_publica_tjam",
    "AP": "api_publica_tjap",
    "BA": "api_publica_tjba",
    "CE": "api_publica_tjce",
    "DF": "api_publica_tjdft",
    "ES": "api_publica_tjes",
    "GO": "api_publica_tjgo",
    "MA": "api_publica_tjma",
    "MG": "api_publica_tjmg",
    "MS": "api_publica_tjms",
    "MT": "api_publica_tjmt",
    "PA": "api_publica_tjpa",
    "PB": "api_publica_tjpb",
    "PE": "api_publica_tjpe",
    "PI": "api_publica_tjpi",
    "PR": "api_publica_tjpr",
    "RJ": "api_publica_tjrj",
    "RN": "api_publica_tjrn",
    "RO": "api_publica_tjro",
    "RR": "api_publica_tjrr",
    "RS": "api_publica_tjrs",
    "SC": "api_publica_tjsc",
    "SE": "api_publica_tjse",
    "SP": "api_publica_tjsp",
    "TO": "api_publica_tjto",
}

SUPERIOR_COURT_INDICES: dict[str, str] = {
    "STJ": "api_publica_stj",
    "TST": "api_publica_tst",
    "TSE": "api_publica_tse",
    "STM": "api_publica_stm",
}


def normalize_state_description(desc: str | None) -> str | None:
    """Extract UF from free-text origin description."""
    if not desc:
        return None

    normalized = unicodedata.normalize("NFD", desc.upper())
    cleaned = "".join(c for c in normalized if unicodedata.category(c) != "Mn")
    if cleaned in STATE_TO_UF.values():
        return cleaned
    exact_match = STATE_TO_UF.get(cleaned)
    if exact_match is not None:
        return exact_match
    for state_name, uf in sorted(STATE_TO_UF.items(), key=lambda item: len(item[0]), reverse=True):
        if state_name in cleaned:
            return uf
    return None


def map_origin_to_datajud_indices(
    origin_court: str | None,
    origin_state: str | None,
) -> list[str]:
    """Map origin court type + state to DataJud API index names.

    Returns a list because a single UF may map to both TJ and TRF indices.
    """
    uf = normalize_state_description(origin_state) if origin_state else None

    if origin_court:
        court_upper = origin_court.upper()

        for label, index in SUPERIOR_COURT_INDICES.items():
            if label in court_upper:
                return [index]

        if uf:
            if "REGIONAL FEDERAL" in court_upper:
                trf_num = UF_TO_TRF.get(uf)
                return [f"api_publica_trf{trf_num}"] if trf_num else []
            if "JUSTICA ESTADUAL" in court_upper or "TRIBUNAL DE JUSTICA" in court_upper:
                tj = UF_TO_TJ_INDEX.get(uf)
                return [tj] if tj else []

    if uf:
        indices: list[str] = []
        tj = UF_TO_TJ_INDEX.get(uf)
        if tj:
            indices.append(tj)
        trf_num = UF_TO_TRF.get(uf)
        if trf_num:
            indices.append(f"api_publica_trf{trf_num}")
        return indices

    return []


def index_to_tribunal_label(index: str) -> str:
    """Convert an API index name to a human-readable tribunal label."""
    name = index.removeprefix("api_publica_")
    return name.upper()
