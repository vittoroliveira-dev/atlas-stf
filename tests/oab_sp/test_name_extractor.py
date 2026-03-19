"""Tests for OAB/SP lawyer name extractor from firm names."""

from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.oab_sp._name_extractor import build_lookup_candidates, extract_lawyer_name_from_firm

# ---------------------------------------------------------------------------
# extract_lawyer_name_from_firm
# ---------------------------------------------------------------------------


def test_extract_individual_firm() -> None:
    """Sociedade individual de advocacia yields the lawyer name."""
    result = extract_lawyer_name_from_firm("JOÃO DA SILVA SOCIEDADE INDIVIDUAL DE ADVOCACIA")
    assert result == "JOÃO DA SILVA"


def test_extract_strips_ltda() -> None:
    """LTDA suffix is stripped to reveal the base name."""
    result = extract_lawyer_name_from_firm("MARIA SANTOS LTDA")
    assert result == "MARIA SANTOS"


def test_extract_strips_advogados() -> None:
    """Stripping 'ADVOGADOS' from a single-word remainder returns None."""
    result = extract_lawyer_name_from_firm("PEREIRA ADVOGADOS")
    assert result is None


def test_extract_too_short() -> None:
    """Names with 2 or fewer characters return None."""
    result = extract_lawyer_name_from_firm("AB")
    assert result is None


def test_extract_single_word() -> None:
    """A single-word result without spaces is not a person name → None."""
    result = extract_lawyer_name_from_firm("SILVA")
    assert result is None


def test_extract_preserves_accents() -> None:
    """Accented characters in lawyer names are preserved."""
    result = extract_lawyer_name_from_firm("JOSÉ ANTÔNIO SOCIEDADE INDIVIDUAL DE ADVOCACIA")
    assert result == "JOSÉ ANTÔNIO"


# ---------------------------------------------------------------------------
# build_lookup_candidates
# ---------------------------------------------------------------------------


def _write_sociedade_detalhe(path: Path, records: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")


def test_build_candidates_filters_non_individual(tmp_path: Path) -> None:
    """Only records with society_type='individual' produce lookup candidates."""
    oab_sp_dir = tmp_path / "oab_sp"
    oab_sp_dir.mkdir()
    _write_sociedade_detalhe(
        oab_sp_dir / "sociedade_detalhe.jsonl",
        [
            {
                "registration_number": "11111",
                "firm_name": "CARLOS ANDRADE SOCIEDADE INDIVIDUAL DE ADVOCACIA",
                "society_type": "individual",
                "city": "São Paulo",
            },
            {
                "registration_number": "22222",
                "firm_name": "SANTOS E ASSOCIADOS ADVOGADOS",
                "society_type": "sociedade_advogados",
                "city": "São Paulo",
            },
            {
                "registration_number": "33333",
                "firm_name": "ANA LIMA SOCIEDADE INDIVIDUAL DE ADVOCACIA",
                "society_type": "individual",
                "city": "Campinas",
            },
        ],
    )
    candidates = build_lookup_candidates(oab_sp_dir, {})
    regs = {c["registration_number"] for c in candidates}
    assert "11111" in regs
    assert "33333" in regs
    assert "22222" not in regs
