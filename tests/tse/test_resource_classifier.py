"""Tests for core/resource_classifier.py — deterministic resource type classification."""

from __future__ import annotations

import pytest

from atlas_stf.core.resource_classifier import (
    ResourceClassification,
    _normalize_for_classification,
    classify_resource_type,
)


class TestNormalization:
    def test_strips_whitespace(self) -> None:
        assert _normalize_for_classification("  hello  ") == "HELLO"

    def test_uppercases(self) -> None:
        assert _normalize_for_classification("Hello World") == "HELLO WORLD"

    def test_removes_accents(self) -> None:
        assert _normalize_for_classification("Doação em espécie") == "DOACAO EM ESPECIE"
        assert _normalize_for_classification("CONTRIBUIÇÃO") == "CONTRIBUICAO"

    def test_collapses_whitespace(self) -> None:
        assert _normalize_for_classification("a   b\tc") == "A B C"

    def test_strips_edge_punctuation(self) -> None:
        assert _normalize_for_classification("(Doacao)") == "DOACAO"
        assert _normalize_for_classification("--Valor--") == "VALOR"

    def test_empty_string(self) -> None:
        assert _normalize_for_classification("") == ""

    def test_preserves_inner_punctuation(self) -> None:
        assert _normalize_for_classification("A/B") == "A/B"


class TestEmptyClassification:
    def test_none_input(self) -> None:
        rc = classify_resource_type(None)
        assert rc == ResourceClassification("empty", "blank", "none", "empty:blank")

    def test_empty_string(self) -> None:
        rc = classify_resource_type("")
        assert rc.category == "empty"
        assert rc.subtype == "blank"

    def test_whitespace_only(self) -> None:
        rc = classify_resource_type("   ")
        assert rc.category == "empty"
        assert rc.subtype == "blank"

    @pytest.mark.parametrize("marker", ["#NULO", "#NULO#", "NULO", "NULL", "#nulo", "nulo"])
    def test_null_markers(self, marker: str) -> None:
        rc = classify_resource_type(marker)
        assert rc.category == "empty"
        assert rc.subtype == "null_marker"
        assert rc.confidence == "none"


class TestNumericCodes:
    @pytest.mark.parametrize(
        ("code", "expected_subtype"),
        [("0", "cash"), ("1", "check"), ("2", "estimated")],
    )
    def test_valid_numeric_codes(self, code: str, expected_subtype: str) -> None:
        rc = classify_resource_type(code)
        assert rc.category == "payment_method"
        assert rc.subtype == expected_subtype
        assert rc.confidence == "high"
        assert rc.rule == f"code:{code}"

    @pytest.mark.parametrize("code", ["3", "10", "999", "00", "01"])
    def test_invalid_numeric_codes_not_payment_method(self, code: str) -> None:
        """Only 0, 1, 2 should be classified as payment_method codes."""
        rc = classify_resource_type(code)
        assert rc.category != "payment_method" or rc.rule.startswith("code:") is False


class TestSourceType:
    @pytest.mark.parametrize(
        ("description", "expected_subtype"),
        [
            ("Recursos de Pessoas Físicas", "individual"),
            ("RECURSOS DE PESSOAS FISICAS", "individual"),
            ("Recursos de Pessoas Jurídicas", "corporate"),
            ("Recursos Próprios", "own_resources"),
            ("RECURSOS PROPRIO", "own_resources"),
            ("Recursos de Partido Político", "party_transfer"),
            ("RECURSOS DE OUTROS CANDIDATOS/COMITES", "committee_transfer"),
            ("RECURSOS DE COMITES", "committee_transfer"),
            ("Comercialização de Bens ou Realização de Eventos", "events_commerce"),
            ("FUNDO PARTIDARIO", "party_fund"),
            ("Fundo Especial de Financiamento de Campanha", "campaign_fund"),
            ("FEFC", "campaign_fund"),
            ("FINANCIAMENTO COLETIVO POR MEIO DA INTERNET", "internet"),
            ("DOACAO PELA INTERNET", "internet"),
            ("Recursos de Origens Não Identificadas", "unidentified_source"),
        ],
    )
    def test_source_type_exact_match(self, description: str, expected_subtype: str) -> None:
        rc = classify_resource_type(description)
        assert rc.category == "source_type", f"Failed for {description!r}: got {rc}"
        assert rc.subtype == expected_subtype
        assert rc.confidence == "high"
        assert rc.rule.startswith("exact:")


class TestPaymentMethodText:
    @pytest.mark.parametrize(
        ("description", "expected_subtype"),
        [
            ("Em espécie", "cash"),
            ("Dinheiro", "cash"),
            ("Cheque", "check"),
            ("Estimado", "estimated"),
            ("Não informado", "not_informed"),
        ],
    )
    def test_payment_method_text(self, description: str, expected_subtype: str) -> None:
        rc = classify_resource_type(description)
        assert rc.category == "payment_method", f"Failed for {description!r}: got {rc}"
        assert rc.subtype == expected_subtype
        assert rc.confidence == "high"


class TestInKindKeywords:
    @pytest.mark.parametrize(
        ("description", "expected_subtype"),
        [
            ("Santinhos para campanha", "campaign_material"),
            ("Adesivos de propaganda", "campaign_material"),
            ("Material de campanha", "campaign_material"),
            ("Serviço de contabilidade", "professional_service"),
            ("Consultoria em marketing", "professional_service"),
            ("Prestação de serviço", "professional_service"),
            ("Combustível para transporte", "transport_fuel"),
            ("Gasolina para campanha", "transport_fuel"),
            ("Aluguel de imóvel", "rental_property"),
            ("Cessão de espaço", "rental_property"),
            ("Inserção em rádio", "media_communication"),
            ("Programa de televisão", "media_communication"),
            ("Cabo eleitoral", "campaign_worker"),
            ("Pessoal de campanha", "campaign_worker"),
            ("Gráfica offset", "printing"),
            ("Impressão de material", "printing"),
            ("Alimentação dos cabos", "food_beverage"),
            ("Água e café", "food_beverage"),
            ("Trabalho voluntário", "volunteer_work"),
            ("Doação em bens", "other_item"),
        ],
    )
    def test_in_kind_keyword_match(self, description: str, expected_subtype: str) -> None:
        rc = classify_resource_type(description)
        assert rc.category == "in_kind", f"Failed for {description!r}: got {rc}"
        assert rc.subtype == expected_subtype
        assert rc.confidence == "medium"
        assert rc.rule.startswith("keyword:")


class TestUnknown:
    def test_unclassifiable_text(self) -> None:
        rc = classify_resource_type("xyzzy foobar qux")
        assert rc.category == "unknown"
        assert rc.subtype == "unclassified"
        assert rc.confidence == "none"
        assert rc.rule == "unclassified"


class TestPriority:
    def test_exact_match_beats_keyword(self) -> None:
        """'RECURSOS DE PESSOAS FISICAS' should be source_type, not in_kind via 'RECURSO'."""
        rc = classify_resource_type("RECURSOS DE PESSOAS FISICAS")
        assert rc.category == "source_type"
        assert rc.subtype == "individual"

    def test_numeric_code_beats_normalisation(self) -> None:
        """'0' should be payment_method via code, not proceed to normalisation."""
        rc = classify_resource_type("0")
        assert rc.category == "payment_method"
        assert rc.rule == "code:0"

    def test_null_marker_beats_everything(self) -> None:
        """'#NULO' should be empty, not proceed to keyword search."""
        rc = classify_resource_type("#NULO")
        assert rc.category == "empty"


class TestEdgeCases:
    def test_accented_source_type(self) -> None:
        """Accented version should match after normalisation."""
        rc = classify_resource_type("Recursos de Pessoas Físicas")
        assert rc.category == "source_type"
        assert rc.subtype == "individual"

    def test_extra_whitespace(self) -> None:
        rc = classify_resource_type("  RECURSOS   DE   PESSOAS   FISICAS  ")
        assert rc.category == "source_type"
        assert rc.subtype == "individual"

    def test_mixed_case(self) -> None:
        rc = classify_resource_type("recursos de pessoas fisicas")
        assert rc.category == "source_type"
        assert rc.subtype == "individual"

    def test_classification_is_namedtuple(self) -> None:
        rc = classify_resource_type("0")
        assert isinstance(rc, tuple)
        assert rc.category == "payment_method"
        assert rc.subtype == "cash"
        assert rc.confidence == "high"
        assert rc.rule == "code:0"


class TestCorpusTopValues:
    """Classify the most common real-world values from the TSE corpus."""

    @pytest.mark.parametrize(
        ("value", "expected_category"),
        [
            ("", "empty"),
            ("#NULO#", "empty"),
            ("#NULO", "empty"),
            ("0", "payment_method"),
            ("1", "payment_method"),
            ("2", "payment_method"),
            ("Em espécie", "payment_method"),
            ("Cheque", "payment_method"),
            ("Estimado", "payment_method"),
            ("Não informado", "payment_method"),
            ("RECURSOS DE PESSOAS FISICAS", "source_type"),
            ("RECURSOS DE PESSOAS JURIDICAS", "source_type"),
            ("RECURSOS PROPRIOS", "source_type"),
            ("RECURSOS DE PARTIDO POLITICO", "source_type"),
            ("FUNDO PARTIDARIO", "source_type"),
            ("FUNDO ESPECIAL DE FINANCIAMENTO DE CAMPANHA", "source_type"),
            ("FEFC", "source_type"),
            ("Santinhos", "in_kind"),
            ("Serviço de contabilidade", "in_kind"),
            ("Combustível", "in_kind"),
            ("Aluguel de sala", "in_kind"),
        ],
    )
    def test_corpus_value(self, value: str, expected_category: str) -> None:
        rc = classify_resource_type(value)
        assert rc.category == expected_category, f"Failed for {value!r}: got {rc}"
