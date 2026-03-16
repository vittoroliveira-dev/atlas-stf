"""Tests for core identity functions."""

import pytest

from atlas_stf.core.identity import (
    build_identity_key,
    canonicalize_entity_name,
    infer_process_class_from_number,
    is_valid_cnpj,
    is_valid_cpf,
    jaccard_similarity,
    levenshtein_distance,
    normalize_entity_name,
    normalize_process_code,
    normalize_tax_id,
    stable_id,
    strip_accents,
)


class TestStableId:
    def test_deterministic(self):
        assert stable_id("proc_", "ADI 1234") == stable_id("proc_", "ADI 1234")

    def test_different_inputs_produce_different_ids(self):
        assert stable_id("proc_", "ADI 1234") != stable_id("proc_", "ADI 5678")

    def test_prefix(self):
        result = stable_id("proc_", "ADI 1234")
        assert result.startswith("proc_")

    def test_custom_length(self):
        result = stable_id("x_", "test", length=8)
        assert len(result) == 2 + 8  # prefix + hash


class TestNormalizeEntityName:
    @pytest.mark.parametrize(
        "input_val,expected",
        [
            ("  João  da   Silva  ", "JOÃO DA SILVA"),
            ("maria souza", "MARIA SOUZA"),
            (None, None),
            ("", None),
            ("   ", None),
        ],
    )
    def test_normalization(self, input_val, expected):
        assert normalize_entity_name(input_val) == expected


class TestCanonicalizeEntityName:
    def test_removes_common_corporate_suffixes(self):
        assert canonicalize_entity_name("Petróleo Brasileiro S.A.") == "PETRÓLEO BRASILEIRO"

    def test_preserves_meaningful_tokens(self):
        assert canonicalize_entity_name("João da Silva") == "JOÃO DA SILVA"


class TestNormalizeTaxId:
    @pytest.mark.parametrize(
        "input_val,expected",
        [
            ("12.345.678/0001-99", "12345678000199"),
            ("123.456.789-01", "12345678901"),
            ("", None),
            (None, None),
        ],
    )
    def test_normalization(self, input_val, expected):
        assert normalize_tax_id(input_val) == expected


class TestTaxIdValidation:
    def test_valid_cpf(self):
        assert is_valid_cpf("529.982.247-25") is True

    def test_invalid_cpf(self):
        assert is_valid_cpf("111.111.111-11") is False

    def test_valid_cnpj(self):
        assert is_valid_cnpj("04.252.011/0001-10") is True

    def test_invalid_cnpj(self):
        assert is_valid_cnpj("11.111.111/1111-11") is False


class TestBuildIdentityKey:
    def test_prefers_tax_id_when_available(self):
        assert build_identity_key("Acme Corp", entity_tax_id="12.345.678/0001-99") == "tax:12345678000199"

    def test_falls_back_to_canonical_name(self):
        assert build_identity_key("Acme Corp Ltda") == "name:ACME CORP"


class TestSimilarityHelpers:
    def test_jaccard_similarity_matches_initials_for_compound_names(self):
        assert jaccard_similarity("JOAO SILVA", "J. SILVA") >= 0.8

    def test_levenshtein_distance_accepts_small_typos(self):
        assert levenshtein_distance("PETROBRAS", "PETROBRAX") == 1


class TestNormalizeProcessCode:
    @pytest.mark.parametrize(
        "input_code,expected",
        [
            ("adi 1234", "ADI 1234"),
            ("ADI 1234/DF", "ADI 1234"),
            ("  adi  1234  ", "ADI 1234"),
            ("SOMETHING", "SOMETHING"),
        ],
    )
    def test_normalization(self, input_code, expected):
        assert normalize_process_code(input_code) == expected


class TestStripAccents:
    def test_removes_accents(self):
        assert strip_accents("JOÃO") == "JOAO"
        assert strip_accents("JOSÉ") == "JOSE"
        assert strip_accents("CAÇÃO") == "CACAO"

    def test_preserves_plain_ascii(self):
        assert strip_accents("SILVA") == "SILVA"

    def test_empty_string(self):
        assert strip_accents("") == ""

    def test_mixed_accents(self):
        assert strip_accents("ANDRÉ MÜLLER") == "ANDRE MULLER"


class TestJaccardWithAccents:
    def test_accented_vs_plain_match(self):
        assert jaccard_similarity("JOÃO SILVA", "JOAO SILVA") == 1.0

    def test_partial_accent_match(self):
        score = jaccard_similarity("JOÃO JOSÉ DA SILVA", "JOAO JOSE DA SILVA")
        assert score == 1.0


class TestLevenshteinWithAccents:
    def test_accented_vs_plain_distance_zero(self):
        assert levenshtein_distance("JOÃO SILVA", "JOAO SILVA") == 0

    def test_accent_plus_typo(self):
        dist = levenshtein_distance("JOÃO SILVAA", "JOAO SILVA")
        assert dist == 1


class TestInferProcessClass:
    @pytest.mark.parametrize(
        "input_number,expected",
        [
            ("ADI 1234", "ADI"),
            ("ADPF 567", "ADPF"),
            ("adi 999", "ADI"),
            (None, None),
            ("", None),
            ("1234", None),
        ],
    )
    def test_inference(self, input_number, expected):
        assert infer_process_class_from_number(input_number) == expected
