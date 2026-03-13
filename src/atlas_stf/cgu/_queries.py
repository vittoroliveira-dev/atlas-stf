"""Query builders and response normalizers for CGU CEIS/CNEP API."""

from __future__ import annotations


def build_ceis_name_params(name: str, page: int = 1) -> dict:
    """Build query params for CEIS search by entity name."""
    return {"nomeSancionado": name, "pagina": page}


def build_cnep_name_params(name: str, page: int = 1) -> dict:
    """Build query params for CNEP search by entity name."""
    return {"nomeSancionado": name, "pagina": page}


def normalize_ceis_record(raw: dict) -> dict:
    """Map CEIS API response fields to internal schema."""
    person = raw.get("sancionado", {})
    sanction = raw.get("sancionado", {})
    return {
        "sanction_source": "ceis",
        "sanction_id": str(raw.get("id", "")),
        "entity_name": person.get("nome", ""),
        "entity_cnpj_cpf": person.get("cnpjFormatado") or person.get("cpfFormatado") or "",
        "sanctioning_body": raw.get("orgaoSancionador", {}).get("nome", ""),
        "sanction_type": raw.get("tipoSancao", {}).get("descricaoResumida", ""),
        "sanction_start_date": raw.get("dataInicioSancao", ""),
        "sanction_end_date": raw.get("dataFimSancao", ""),
        "sanction_description": raw.get("textoPublicacao", ""),
        "uf_sancionado": sanction.get("ufSancionado", ""),
    }


def normalize_cnep_record(raw: dict) -> dict:
    """Map CNEP API response fields to internal schema."""
    person = raw.get("sancionado", {})
    return {
        "sanction_source": "cnep",
        "sanction_id": str(raw.get("id", "")),
        "entity_name": person.get("nome", ""),
        "entity_cnpj_cpf": person.get("cnpjFormatado") or person.get("cpfFormatado") or "",
        "sanctioning_body": raw.get("orgaoSancionador", {}).get("nome", ""),
        "sanction_type": raw.get("tipoSancao", {}).get("descricaoResumida", ""),
        "sanction_start_date": raw.get("dataInicioSancao", ""),
        "sanction_end_date": raw.get("dataFimSancao", ""),
        "sanction_description": raw.get("textoPublicacao", ""),
        "uf_sancionado": person.get("ufSancionado", ""),
    }


def build_leniencia_name_params(name: str, page: int = 1) -> dict:
    """Build query params for acordos de leniência search by entity name."""
    return {"nomeInformadoOrgaoResponsavel": name, "pagina": page}


def normalize_leniencia_record(raw: dict) -> dict:
    """Map Leniência API response fields to internal schema."""
    sancoes = raw.get("sancoes", [])
    empresa = sancoes[0] if sancoes else {}
    name = empresa.get("nomeInformadoOrgaoResponsavel") or empresa.get("razaoSocial", "")
    cnpj = empresa.get("cnpjFormatado") or empresa.get("cnpj", "")
    return {
        "sanction_source": "leniencia",
        "sanction_id": str(raw.get("id", "")),
        "entity_name": name,
        "entity_cnpj_cpf": cnpj,
        "sanctioning_body": raw.get("orgaoResponsavel", ""),
        "sanction_type": raw.get("situacaoAcordo", ""),
        "sanction_start_date": raw.get("dataInicioAcordo", ""),
        "sanction_end_date": raw.get("dataFimAcordo", ""),
        "sanction_description": raw.get("situacaoAcordo", ""),
        "uf_sancionado": "",
    }
