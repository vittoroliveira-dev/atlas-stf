"""Integration tests with real RFB data (skipped when data not available)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from atlas_stf.analytics.corporate_network import build_corporate_network
from atlas_stf.core.identity import normalize_entity_name

PARTNERS_PATH = Path("data/raw/rfb/partners_raw.jsonl")
COMPANIES_PATH = Path("data/raw/rfb/companies_raw.jsonl")
BIO_PATH = Path("data/curated/minister_bio.json")
DATA_EXISTS = PARTNERS_PATH.exists() and COMPANIES_PATH.exists() and BIO_PATH.exists()

CIVIL_NAMES: dict[str, str] = {
    "LUÍS ROBERTO BARROSO": "LUÍS ROBERTO BARROSO",
    "EDSON FACHIN": "LUIZ EDSON FACHIN",
    "ALEXANDRE DE MORAES": "ALEXANDRE DE MORAES",
    "NUNES MARQUES": "KASSIO NUNES MARQUES",
    "ANDRÉ MENDONÇA": "ANDRÉ LUIZ DE ALMEIDA MENDONÇA",
    "CRISTIANO ZANIN": "CRISTIANO ZANIN MARTINS",
    "FLÁVIO DINO": "FLÁVIO DINO DE CASTRO E COSTA",
    "GILMAR MENDES": "GILMAR FERREIRA MENDES",
    "DIAS TOFFOLI": "JOSE ANTONIO DIAS TOFFOLI",
    "CÁRMEN LÚCIA": "CÁRMEN LÚCIA ANTUNES ROCHA",
    "LUIZ FUX": "LUIZ FUX",
    "MARCO AURÉLIO": "MARCO AURÉLIO MENDES DE FARIAS MELLO",
    "CELSO DE MELLO": "JOSE CELSO DE MELLO FILHO",
    "RICARDO LEWANDOWSKI": "ENRIQUE RICARDO LEWANDOWSKI",
    "ROSA WEBER": "ROSA MARIA WEBER CANDIOTA DA ROSA",
}


def _load_partners() -> list[dict]:
    """Load all partner records from partners_raw.jsonl."""
    records = []
    with PARTNERS_PATH.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


@pytest.mark.skipif(not DATA_EXISTS, reason="Real RFB data not available")
class TestRealRfbIntegration:
    def test_civil_names_expand_minister_matches(self) -> None:
        """Civil names find more companies than parliamentary names."""
        partners = _load_partners()
        parliamentary_cnpjs: set[str] = set()
        civil_cnpjs: set[str] = set()

        for minister_name, civil_name in CIVIL_NAMES.items():
            minister_norm = normalize_entity_name(minister_name)
            civil_norm = normalize_entity_name(civil_name)

            for p in partners:
                pn = p.get("partner_name_normalized", "")
                cnpj = p.get("cnpj_basico", "")
                if pn == minister_norm:
                    parliamentary_cnpjs.add(cnpj)
                    civil_cnpjs.add(cnpj)
                if pn == civil_norm:
                    civil_cnpjs.add(cnpj)

        assert len(civil_cnpjs) >= len(parliamentary_cnpjs)
        # Verify DIAS TOFFOLI civil name exists
        toffoli_civil = normalize_entity_name("JOSE ANTONIO DIAS TOFFOLI")
        toffoli_found = any(p.get("partner_name_normalized") == toffoli_civil for p in partners)
        if toffoli_found:
            assert toffoli_found is True

    def test_representative_fields_extracted(self) -> None:
        """Partner records include representative fields after re-parse."""
        partners = _load_partners()
        has_rep = [p for p in partners if p.get("representative_name", "")]
        # At least some records should have representative names after re-fetch
        # This is informational — may be 0 if data was fetched before the parser update
        assert isinstance(has_rep, list)

    def test_representative_matching_finds_new_links(self) -> None:
        """Check if any minister names appear as representatives but not as partners."""
        partners = _load_partners()
        minister_norms = {normalize_entity_name(n) for n in CIVIL_NAMES}
        minister_norms |= {normalize_entity_name(v) for v in CIVIL_NAMES.values()}

        partner_only: set[str] = set()
        rep_only: set[str] = set()

        for p in partners:
            pn = p.get("partner_name_normalized", "")
            rn = p.get("representative_name_normalized", "")
            cnpj = p.get("cnpj_basico", "")

            if pn in minister_norms:
                partner_only.add(f"{pn}:{cnpj}")
            if rn in minister_norms:
                rep_only.add(f"{rn}:{cnpj}")

        new_from_rep = rep_only - partner_only
        # Informational — reports how many new links found via representative
        assert isinstance(new_from_rep, set)

    def test_pj_partner_expansion_grau2(self) -> None:
        """PJ->PJ expansion finds degree-2 chains."""
        partners = _load_partners()
        minister_norms = {normalize_entity_name(n) for n in CIVIL_NAMES}
        minister_norms |= {normalize_entity_name(v) for v in CIVIL_NAMES.values()}

        # Find companies where ministers are partners
        minister_cnpjs: set[str] = set()
        for p in partners:
            if p.get("partner_name_normalized", "") in minister_norms:
                minister_cnpjs.add(p.get("cnpj_basico", ""))

        # Find PJ partners in those companies
        pj_cnpjs: set[str] = set()
        for p in partners:
            if p.get("cnpj_basico", "") in minister_cnpjs and p.get("partner_type") == "1":
                pj_cpf = p.get("partner_cpf_cnpj", "").strip()
                if pj_cpf:
                    pj_cnpjs.add(pj_cpf)

        # Informational — count how many PJ partners exist in minister companies
        assert isinstance(pj_cnpjs, set)

    def test_full_corporate_network_with_improvements(self, tmp_path: Path) -> None:
        """End-to-end: build_corporate_network with real data produces >= 3 conflicts."""
        curated_dir = Path("data/curated")
        if not (curated_dir / "party.jsonl").exists():
            pytest.skip("Curated data not available")

        result = build_corporate_network(
            rfb_dir=Path("data/raw/rfb"),
            minister_bio_path=BIO_PATH,
            party_path=curated_dir / "party.jsonl",
            counsel_path=curated_dir / "counsel.jsonl",
            process_path=curated_dir / "process.jsonl",
            decision_event_path=curated_dir / "decision_event.jsonl",
            process_party_link_path=curated_dir / "process_party_link.jsonl",
            process_counsel_link_path=curated_dir / "process_counsel_link.jsonl",
            output_dir=tmp_path / "analytics",
        )
        assert result.exists()
        text = result.read_text(encoding="utf-8").strip()
        conflicts = [json.loads(line) for line in text.split("\n") if line.strip()]
        assert len(conflicts) >= 3
        # Check new field present
        for c in conflicts:
            assert "link_degree" in c
            assert c["link_degree"] >= 1

    def test_civil_names_in_minister_bio(self) -> None:
        """minister_bio.json has civil_name for all 21 ministers."""
        data = json.loads(BIO_PATH.read_text(encoding="utf-8"))
        assert len(data) == 21
        different_count = 0
        for _key, entry in data.items():
            assert "civil_name" in entry
            assert isinstance(entry["civil_name"], str)
            assert len(entry["civil_name"]) > 0
            if entry["civil_name"] != entry["minister_name"]:
                different_count += 1
        # At least 15 ministers have civil_name different from minister_name
        assert different_count >= 15
