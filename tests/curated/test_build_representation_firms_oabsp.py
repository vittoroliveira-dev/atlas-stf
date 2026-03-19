"""Tests for OAB/SP enrichment in law firm entity builder (Source 3)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from atlas_stf.curated._build_representation_firms import build_law_firm_entity_records


def _write_deoab_vinculo(deoab_dir: Path, records: list[dict]) -> None:
    path = deoab_dir / "oab_sociedade_vinculo.jsonl"
    with path.open("w", encoding="utf-8") as fh:
        for rec in fh if False else records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")


def _write_oab_sp_detalhe(oab_sp_dir: Path, records: list[dict]) -> None:
    path = oab_sp_dir / "sociedade_detalhe.jsonl"
    with path.open("w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")


@pytest.fixture()
def workspace(tmp_path: Path) -> dict[str, Path]:
    """Create workspace dirs and minimal process.jsonl."""
    process_path = tmp_path / "curated" / "process.jsonl"
    process_path.parent.mkdir(parents=True)
    process_path.write_text("", encoding="utf-8")

    portal_dir = tmp_path / "portal"
    portal_dir.mkdir()

    curated_dir = tmp_path / "curated"

    deoab_dir = tmp_path / "deoab"
    deoab_dir.mkdir()

    oab_sp_dir = tmp_path / "oab_sp"
    oab_sp_dir.mkdir()

    return {
        "process_path": process_path,
        "portal_dir": portal_dir,
        "curated_dir": curated_dir,
        "deoab_dir": deoab_dir,
        "oab_sp_dir": oab_sp_dir,
    }


def test_oabsp_enrichment(workspace: dict[str, Path]) -> None:
    """Firms with matching cnsa_number get OAB/SP fields populated."""
    _write_deoab_vinculo(
        workspace["deoab_dir"],
        [
            {
                "sociedade_nome": "SILVA E ASSOCIADOS SOCIEDADE DE ADVOGADOS",
                "sociedade_registro": "18554",
                "seccional": "SP",
                "oab_numero": "12345",
                "oab_seccional": "SP",
                "advogado_nome": None,
                "data_publicacao": "2024-01-15",
                "source_url": "https://deoab.example.com",
            },
        ],
    )
    _write_oab_sp_detalhe(
        workspace["oab_sp_dir"],
        [
            {
                "registration_number": "18554",
                "oab_sp_param": "19640",
                "firm_name": "SILVA E ASSOCIADOS SOCIEDADE DE ADVOGADOS",
                "address": "Rua Augusta 1234",
                "neighborhood": "Consolação",
                "zip_code": "01305100",
                "city": "São Paulo",
                "state": "SP",
                "email": "contato@silva.adv.br",
                "phone": "(11) 3333-4444",
                "society_type": "sociedade_advogados",
                "detail_url": "https://www2.oabsp.org.br/asp/consultaSociedades/consultaSociedades03.asp?param=19640",
                "fetched_at": "2026-03-17T00:00:00+00:00",
                "parser_version": 1,
            },
        ],
    )

    records = build_law_firm_entity_records(
        workspace["process_path"],
        workspace["portal_dir"],
        workspace["curated_dir"],
        deoab_dir=workspace["deoab_dir"],
        oab_sp_dir=workspace["oab_sp_dir"],
    )

    assert len(records) == 1
    firm = records[0]
    assert firm["oab_sp_firm_name"] == "SILVA E ASSOCIADOS SOCIEDADE DE ADVOGADOS"
    assert firm["address"] == "Rua Augusta 1234"
    assert firm["neighborhood"] == "Consolação"
    assert firm["zip_code"] == "01305100"
    assert firm["city"] == "São Paulo"
    assert firm["state"] == "SP"
    assert firm["email"] == "contato@silva.adv.br"
    assert firm["phone"] == "(11) 3333-4444"
    assert firm["society_type"] == "sociedade_advogados"
    assert "oab_sp" in firm["source_systems"]
    assert "deoab" in firm["source_systems"]


def test_oabsp_no_artifact(workspace: dict[str, Path]) -> None:
    """Without OAB/SP artifact, firms have None for new fields, no error."""
    _write_deoab_vinculo(
        workspace["deoab_dir"],
        [
            {
                "sociedade_nome": "PEREIRA ADVOCACIA",
                "sociedade_registro": "28651",
                "seccional": "SP",
                "oab_numero": "67890",
                "oab_seccional": "SP",
                "advogado_nome": None,
                "data_publicacao": "2024-02-20",
                "source_url": "https://deoab.example.com",
            },
        ],
    )

    records = build_law_firm_entity_records(
        workspace["process_path"],
        workspace["portal_dir"],
        workspace["curated_dir"],
        deoab_dir=workspace["deoab_dir"],
        oab_sp_dir=workspace["oab_sp_dir"],
    )

    assert len(records) == 1
    firm = records[0]
    assert firm.get("oab_sp_firm_name") is None
    assert firm.get("address") is None
    assert "oab_sp" not in firm["source_systems"]


def test_oabsp_in_source_systems(workspace: dict[str, Path]) -> None:
    """oab_sp appears in source_systems when enriched."""
    _write_deoab_vinculo(
        workspace["deoab_dir"],
        [
            {
                "sociedade_nome": "EXEMPLO ADVOGADOS",
                "sociedade_registro": "99999",
                "seccional": "SP",
                "oab_numero": "11111",
                "oab_seccional": "SP",
                "advogado_nome": None,
                "data_publicacao": "2024-03-01",
                "source_url": "https://deoab.example.com",
            },
        ],
    )
    _write_oab_sp_detalhe(
        workspace["oab_sp_dir"],
        [
            {
                "registration_number": "99999",
                "firm_name": "EXEMPLO ADVOGADOS LTDA",
                "address": None,
                "neighborhood": None,
                "zip_code": None,
                "city": None,
                "state": "SP",
                "email": None,
                "phone": None,
                "society_type": "individual",
            },
        ],
    )

    records = build_law_firm_entity_records(
        workspace["process_path"],
        workspace["portal_dir"],
        workspace["curated_dir"],
        deoab_dir=workspace["deoab_dir"],
        oab_sp_dir=workspace["oab_sp_dir"],
    )

    assert len(records) == 1
    assert "oab_sp" in records[0]["source_systems"]
    assert "deoab" in records[0]["source_systems"]


def test_firm_name_raw_preserved(workspace: dict[str, Path]) -> None:
    """firm_name_raw stays as DEOAB original; oab_sp_firm_name is separate."""
    _write_deoab_vinculo(
        workspace["deoab_dir"],
        [
            {
                "sociedade_nome": "NOME DEOAB ORIGINAL",
                "sociedade_registro": "55555",
                "seccional": "SP",
                "oab_numero": "22222",
                "oab_seccional": "SP",
                "advogado_nome": None,
                "data_publicacao": "2024-04-10",
                "source_url": "https://deoab.example.com",
            },
        ],
    )
    _write_oab_sp_detalhe(
        workspace["oab_sp_dir"],
        [
            {
                "registration_number": "55555",
                "firm_name": "NOME CANONICO OABSP",
                "address": "Av Paulista 100",
                "neighborhood": "Bela Vista",
                "zip_code": "01310100",
                "city": "São Paulo",
                "state": "SP",
                "email": None,
                "phone": None,
                "society_type": "sociedade_advogados",
            },
        ],
    )

    records = build_law_firm_entity_records(
        workspace["process_path"],
        workspace["portal_dir"],
        workspace["curated_dir"],
        deoab_dir=workspace["deoab_dir"],
        oab_sp_dir=workspace["oab_sp_dir"],
    )

    assert len(records) == 1
    firm = records[0]
    assert firm["firm_name_raw"] == "NOME DEOAB ORIGINAL"
    assert firm["oab_sp_firm_name"] == "NOME CANONICO OABSP"
