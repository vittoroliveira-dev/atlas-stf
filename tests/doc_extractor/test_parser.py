"""Tests for doc_extractor/_parser.py."""

from __future__ import annotations

from atlas_stf.doc_extractor._parser import (
    extract_cnpj_from_text,
    extract_cpf_from_text,
    extract_lawyer_names_from_header,
    extract_oab_from_text,
    parse_petition_header,
    parse_procuracao_text,
)


class TestExtractOabFromText:
    def test_oab_slash_format(self) -> None:
        result = extract_oab_from_text("Advogado inscrito na OAB/SP 123456")
        assert len(result) == 1
        assert result[0]["oab_number"] == "123456"
        assert result[0]["oab_state"] == "SP"

    def test_oab_numero_format(self) -> None:
        result = extract_oab_from_text("OAB no 654.321 RJ")
        assert len(result) == 1
        assert result[0]["oab_number"] == "654321"
        assert result[0]["oab_state"] == "RJ"

    def test_oab_dash_format(self) -> None:
        result = extract_oab_from_text("OAB/DF 12345")
        assert len(result) == 1
        assert result[0]["oab_number"] == "12345"
        assert result[0]["oab_state"] == "DF"

    def test_multiple_oabs(self) -> None:
        text = "OAB/SP 111222 e OAB/RJ 333444"
        result = extract_oab_from_text(text)
        assert len(result) == 2
        assert result[0]["oab_state"] == "SP"
        assert result[1]["oab_state"] == "RJ"

    def test_deduplicates(self) -> None:
        text = "OAB/SP 111222 inscrito OAB/SP 111222"
        result = extract_oab_from_text(text)
        assert len(result) == 1

    def test_empty_text(self) -> None:
        assert extract_oab_from_text("") == []

    def test_none_like_empty(self) -> None:
        assert extract_oab_from_text("") == []

    def test_no_match(self) -> None:
        assert extract_oab_from_text("nenhuma inscricao aqui") == []


class TestExtractCnpjFromText:
    def test_valid_cnpj(self) -> None:
        result = extract_cnpj_from_text("CNPJ: 12.345.678/0001-90")
        assert result == ["12.345.678/0001-90"]

    def test_multiple_cnpjs(self) -> None:
        text = "CNPJ 12.345.678/0001-90 e 98.765.432/0001-00"
        result = extract_cnpj_from_text(text)
        assert len(result) == 2

    def test_deduplicates(self) -> None:
        text = "12.345.678/0001-90 e 12.345.678/0001-90"
        result = extract_cnpj_from_text(text)
        assert len(result) == 1

    def test_empty_text(self) -> None:
        assert extract_cnpj_from_text("") == []

    def test_no_match(self) -> None:
        assert extract_cnpj_from_text("sem cnpj aqui") == []


class TestExtractCpfFromText:
    def test_valid_cpf(self) -> None:
        result = extract_cpf_from_text("CPF: 123.456.789-00")
        assert result == ["123.456.789-00"]

    def test_multiple_cpfs(self) -> None:
        text = "CPF 123.456.789-00 e 987.654.321-00"
        result = extract_cpf_from_text(text)
        assert len(result) == 2

    def test_deduplicates(self) -> None:
        text = "123.456.789-00 novamente 123.456.789-00"
        result = extract_cpf_from_text(text)
        assert len(result) == 1

    def test_empty_text(self) -> None:
        assert extract_cpf_from_text("") == []


class TestExtractLawyerNamesFromHeader:
    def test_constitui_procurador(self) -> None:
        text = "constitui como seu procurador o Dr. JOSE DA SILVA, inscrito"
        result = extract_lawyer_names_from_header(text)
        assert len(result) == 1
        assert result[0] == "JOSE DA SILVA"

    def test_nomeia_advogado(self) -> None:
        text = "nomeia advogado MARIA SOUZA PEREIRA para"
        result = extract_lawyer_names_from_header(text)
        assert len(result) == 1
        assert result[0] == "MARIA SOUZA PEREIRA"

    def test_empty_text(self) -> None:
        assert extract_lawyer_names_from_header("") == []

    def test_no_match(self) -> None:
        assert extract_lawyer_names_from_header("texto sem advogado") == []


class TestParseProcuracaoText:
    def test_full_procuracao(self) -> None:
        text = (
            "O outorgante EMPRESA BETA constitui como seu procurador "
            "o Dr. CARLOS MENDES, inscrito na OAB/SP 123456, "
            "do escritorio MENDES E ASSOCIADOS, "
            "CNPJ: 12.345.678/0001-90"
        )
        result = parse_procuracao_text(text)
        assert result["lawyer_name"] == "CARLOS MENDES"
        assert result["oab_number"] == "123456"
        assert result["oab_state"] == "SP"
        assert result["firm_name"] == "MENDES E ASSOCIADOS"
        assert result["cnpj"] == "12.345.678/0001-90"

    def test_empty_text(self) -> None:
        result = parse_procuracao_text("")
        assert result["lawyer_name"] is None
        assert result["oab_number"] is None
        assert result["oab_state"] is None
        assert result["firm_name"] is None
        assert result["cnpj"] is None
        assert result["party_represented"] is None

    def test_partial_data(self) -> None:
        text = "Advogado com OAB/MG 789012"
        result = parse_procuracao_text(text)
        assert result["oab_number"] == "789012"
        assert result["oab_state"] == "MG"
        assert result["lawyer_name"] is None

    def test_all_keys_present(self) -> None:
        result = parse_procuracao_text("qualquer texto")
        expected_keys = {"lawyer_name", "oab_number", "oab_state", "firm_name", "cnpj", "party_represented"}
        assert set(result.keys()) == expected_keys


class TestParsePetitionHeader:
    def test_recurso_extraordinario(self) -> None:
        text = "RECURSO EXTRAORDINARIO\nRECORRENTE: ESTADO DE SAO PAULO, representado\nAdvogado OAB/SP 111222"
        result = parse_petition_header(text)
        assert result["document_type"] == "RECURSO EXTRAORDINARIO"
        assert result["petitioner_name"] == "ESTADO DE SAO PAULO"
        assert result["oab_number"] == "111222"
        assert result["oab_state"] == "SP"

    def test_empty_text(self) -> None:
        result = parse_petition_header("")
        assert result["petitioner_name"] is None
        assert result["document_type"] is None
        assert result["oab_number"] is None
        assert result["oab_state"] is None

    def test_all_keys_present(self) -> None:
        result = parse_petition_header("qualquer texto")
        expected_keys = {"petitioner_name", "document_type", "oab_number", "oab_state"}
        assert set(result.keys()) == expected_keys

    def test_habeas_corpus(self) -> None:
        text = "HABEAS CORPUS\nIMPETRANTE: JOAO DA SILVA, brasileiro"
        result = parse_petition_header(text)
        assert result["document_type"] == "HABEAS CORPUS"
        assert result["petitioner_name"] == "JOAO DA SILVA"
