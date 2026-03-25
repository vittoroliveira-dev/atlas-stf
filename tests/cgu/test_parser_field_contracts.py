"""Parser field contracts for CGU CEIS/CNEP/Leniência."""

from __future__ import annotations

from pathlib import Path

import pytest

from atlas_stf.cgu._runner_csv import (
    _CEIS_EXPECTED_HEADER,
    _LENIENCIA_EXPECTED_HEADER,
    _load_csv_sanctions,
    _load_leniencia_csv,
    _validate_header,
)
from tests.cgu._runner_helpers import (
    _make_ceis_csv,
    _make_cnep_csv,
    _make_leniencia_csv,
)


class TestCeisFieldContract:
    def test_happy_path_24_columns(self, tmp_path: Path) -> None:
        row = [
            "CEIS",  # 0: CADASTRO
            "S001",  # 1: CÓDIGO DA SANÇÃO
            "J",  # 2: TIPO DE PESSOA
            "12345678000100",  # 3: CPF OU CNPJ
            "EMPRESA TESTE LTDA",  # 4: NOME
            "EMPRESA TESTE",  # 5: NOME INFORMADO
            "EMPRESA TESTE LTDA",  # 6: RAZÃO SOCIAL
            "TESTE",  # 7: FANTASIA
            "12345/2024",  # 8: NÚMERO DO PROCESSO
            "Inidoneidade",  # 9: CATEGORIA
            "01/01/2024",  # 10: DATA INÍCIO
            "31/12/2024",  # 11: DATA FIM
            "02/01/2024",  # 12: DATA PUBLICAÇÃO
            "DOU",  # 13: PUBLICAÇÃO
            "Seção 3",  # 14: DETALHAMENTO
            "",  # 15: TRÂNSITO EM JULGADO
            "Nacional",  # 16: ABRAGÊNCIA
            "CGU",  # 17: ÓRGÃO
            "DF",  # 18: UF ÓRGÃO
            "Federal",  # 19: ESFERA
            "Lei 8666",  # 20: FUNDAMENTAÇÃO
            "03/01/2024",  # 21: DATA ORIGEM
            "Portal CGU",  # 22: ORIGEM
            "",  # 23: OBSERVAÇÕES
        ]
        csv_text = _make_ceis_csv([row])
        csv_path = tmp_path / "ceis.csv"
        csv_path.write_text(csv_text, encoding="utf-8")

        records = _load_csv_sanctions(csv_path, "ceis")
        assert len(records) == 1
        rec = records[0]
        assert rec["sanction_source"] == "ceis"
        assert rec["entity_name"] == "EMPRESA TESTE LTDA"
        assert rec["entity_cnpj_cpf"] != ""  # normalized
        assert rec["entity_type_pf_pj"] == "PJ"
        assert rec["sanction_start_date"] == "2024-01-01"
        assert rec["sanction_end_date"] == "2024-12-31"
        assert rec["sanctioning_body"] == "CGU"
        assert rec["uf_sancionado"] == "DF"


class TestCnepFieldContract:
    def test_happy_path_25_columns(self, tmp_path: Path) -> None:
        row = [
            "CNEP",  # 0: CADASTRO
            "S002",  # 1: CÓDIGO
            "J",  # 2: TIPO
            "98765432000199",  # 3: CNPJ
            "CONSTRUTORA XYZ SA",  # 4: NOME
            "CONSTRUTORA XYZ",  # 5: NOME INFORMADO
            "CONSTRUTORA XYZ SA",  # 6: RAZÃO
            "XYZ",  # 7: FANTASIA
            "54321/2024",  # 8: PROCESSO
            "Multa",  # 9: CATEGORIA
            "50000.00",  # 10: VALOR DA MULTA
            "15/06/2024",  # 11: DATA INÍCIO
            "15/06/2025",  # 12: DATA FIM
            "16/06/2024",  # 13: DATA PUB
            "DOU",  # 14: PUBLICAÇÃO
            "Seção 2",  # 15: DETALHAMENTO
            "",  # 16: TRÂNSITO
            "Nacional",  # 17: ABRAGÊNCIA
            "CGU",  # 18: ÓRGÃO
            "DF",  # 19: UF
            "Federal",  # 20: ESFERA
            "Lei 12846",  # 21: FUNDAMENTAÇÃO
            "17/06/2024",  # 22: DATA ORIGEM
            "Portal CGU",  # 23: ORIGEM
            "",  # 24: OBS
        ]
        csv_text = _make_cnep_csv([row])
        csv_path = tmp_path / "cnep.csv"
        csv_path.write_text(csv_text, encoding="utf-8")

        records = _load_csv_sanctions(csv_path, "cnep")
        assert len(records) == 1
        rec = records[0]
        assert rec["sanction_source"] == "cnep"
        assert rec["entity_name"] == "CONSTRUTORA XYZ SA"
        assert rec["sanction_start_date"] == "2024-06-15"
        assert rec["sanctioning_body"] == "CGU"


class TestLenienciaFieldContract:
    def test_happy_path_11_columns(self, tmp_path: Path) -> None:
        row = [
            "1",  # 0: ID DO ACORDO
            "33000167000101",  # 1: CNPJ
            "ODEBRECHT SA",  # 2: RAZÃO SOCIAL
            "ODEBRECHT",  # 3: FANTASIA
            "01/01/2018",  # 4: INÍCIO
            "31/12/2028",  # 5: FIM
            "Em execução",  # 6: SITUAÇÃO
            "15/03/2018",  # 7: DATA INFO
            "08012.001111/2018-01",  # 8: PROCESSO
            "Cooperação",  # 9: TERMOS
            "CGU",  # 10: ÓRGÃO
        ]
        csv_text = _make_leniencia_csv([row])
        csv_path = tmp_path / "leniencia.csv"
        csv_path.write_text(csv_text, encoding="utf-8")

        records = _load_leniencia_csv(csv_path)
        assert len(records) == 1
        rec = records[0]
        assert rec["sanction_source"] == "leniencia"
        assert rec["entity_name"] == "ODEBRECHT SA"
        assert rec["entity_type_pf_pj"] == "PJ"
        assert rec["sanction_start_date"] == "2018-01-01"


class TestCguHeaderValidation:
    def test_wrong_header_raises(self) -> None:
        wrong = ["COL1", "COL2", "COL3"]
        with pytest.raises(ValueError, match="header length mismatch"):
            _validate_header(wrong, _CEIS_EXPECTED_HEADER, "ceis")

    def test_old_simplified_fixture_rejected(self) -> None:
        """18-column fixture that old tests used should be rejected."""
        old_header = [
            "CADASTRO",
            "CÓDIGO",
            "TIPO",
            "CPF/CNPJ",
            "NOME",
            "NOME ORG",
            "RAZAO",
            "FANTASIA",
            "PROCESSO",
            "CATEGORIA",
            "DATA INÍCIO",
            "DATA FIM",
            "DATA PUB",
            "PUBLICAÇÃO",
            "DETALHE",
            "TRANSITO",
            "ABRANGENCIA",
            "ÓRGÃO",
        ]
        with pytest.raises(ValueError, match="header length mismatch"):
            _validate_header(old_header, _CEIS_EXPECTED_HEADER, "ceis")

    def test_known_alias_passes(self) -> None:
        """Real typo 'LENIÊNICA' passes via alias table."""
        header_with_typo = list(_LENIENCIA_EXPECTED_HEADER)
        for i, h in enumerate(header_with_typo):
            header_with_typo[i] = h.replace("LENIÊNCIA", "LENIÊNICA")
        # Should not raise
        _validate_header(header_with_typo, _LENIENCIA_EXPECTED_HEADER, "leniencia")

    def test_unknown_variation_fails(self) -> None:
        """Variation not in alias table should fail."""
        header_with_bad = list(_LENIENCIA_EXPECTED_HEADER)
        for i, h in enumerate(header_with_bad):
            header_with_bad[i] = h.replace("LENIÊNCIA", "LENIÊNCIA ESPECIAL")
        with pytest.raises(ValueError, match="column mismatch"):
            _validate_header(header_with_bad, _LENIENCIA_EXPECTED_HEADER, "leniencia")
