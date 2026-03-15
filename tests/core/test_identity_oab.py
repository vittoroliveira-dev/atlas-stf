"""Tests for OAB, CNSA and representation network identity helpers."""

from __future__ import annotations

import pytest

from atlas_stf.core.identity import (
    VALID_UF_CODES,
    build_firm_identity_key,
    build_lawyer_identity_key,
    is_valid_oab_format,
    normalize_cnsa_number,
    normalize_oab_number,
)


class TestNormalizeOabNumber:
    def test_valid_simple(self):
        assert normalize_oab_number("123456/SP") == "123456/SP"

    def test_with_dots(self):
        assert normalize_oab_number("123.456/SP") == "123456/SP"

    def test_with_spaces(self):
        assert normalize_oab_number("  123456 / SP  ") == "123456/SP"

    def test_lowercase_uf(self):
        assert normalize_oab_number("123456/sp") == "123456/SP"

    def test_leading_zeros(self):
        assert normalize_oab_number("1/SP") == "1/SP"

    def test_six_digits(self):
        assert normalize_oab_number("999999/RJ") == "999999/RJ"

    def test_none_input(self):
        assert normalize_oab_number(None) is None

    def test_empty_string(self):
        assert normalize_oab_number("") is None

    def test_no_slash(self):
        assert normalize_oab_number("123456SP") is None

    def test_invalid_uf(self):
        assert normalize_oab_number("123456/XX") is None

    def test_seven_digits_invalid(self):
        assert normalize_oab_number("1234567/SP") is None

    def test_no_digits_before_slash(self):
        assert normalize_oab_number("/SP") is None

    def test_dots_and_spaces(self):
        assert normalize_oab_number("12.345/RJ") == "12345/RJ"


class TestIsValidOabFormat:
    @pytest.mark.parametrize("uf", sorted(VALID_UF_CODES))
    def test_all_27_ufs_are_valid(self, uf: str):
        assert is_valid_oab_format(f"1/{uf}") is True

    def test_exactly_27_valid_ufs(self):
        assert len(VALID_UF_CODES) == 27

    def test_invalid_uf_returns_false(self):
        assert is_valid_oab_format("123/ZZ") is False

    def test_invalid_number_returns_false(self):
        assert is_valid_oab_format("/SP") is False

    def test_none_returns_false(self):
        assert is_valid_oab_format(None) is False


class TestNormalizeCnsaNumber:
    def test_digits_only(self):
        assert normalize_cnsa_number("12345") == "12345"

    def test_with_formatting(self):
        assert normalize_cnsa_number("12.345-6") == "123456"

    def test_with_letters_and_digits(self):
        assert normalize_cnsa_number("CNSA-001") == "001"

    def test_none(self):
        assert normalize_cnsa_number(None) is None

    def test_empty_string(self):
        assert normalize_cnsa_number("") is None

    def test_no_digits(self):
        assert normalize_cnsa_number("abc") is None


class TestBuildLawyerIdentityKey:
    def test_priority_oab_over_tax(self):
        result = build_lawyer_identity_key(
            name="Joao Silva",
            oab_number="123456/SP",
            tax_id="529.982.247-25",
        )
        assert result == "oab:123456/SP"

    def test_priority_tax_over_name(self):
        result = build_lawyer_identity_key(
            name="Joao Silva",
            tax_id="529.982.247-25",
        )
        assert result == "tax:52998224725"

    def test_fallback_to_name(self):
        result = build_lawyer_identity_key(name="Joao Silva")
        assert result == "name:JOAO SILVA"

    def test_all_none(self):
        result = build_lawyer_identity_key(name=None)
        assert result is None

    def test_invalid_oab_falls_to_tax(self):
        result = build_lawyer_identity_key(
            name="Maria",
            oab_number="invalid",
            tax_id="12345678901",
        )
        assert result == "tax:12345678901"

    def test_invalid_oab_and_no_tax_falls_to_name(self):
        result = build_lawyer_identity_key(
            name="Maria Santos",
            oab_number="invalid",
        )
        assert result == "name:MARIA SANTOS"


class TestBuildFirmIdentityKey:
    def test_priority_cnpj_over_cnsa(self):
        result = build_firm_identity_key(
            name="Silva Advogados",
            cnpj="04.252.011/0001-10",
            cnsa_number="12345",
        )
        assert result == "tax:04252011000110"

    def test_priority_cnsa_over_name(self):
        result = build_firm_identity_key(
            name="Silva Advogados",
            cnsa_number="12345",
        )
        assert result == "cnsa:12345"

    def test_fallback_to_name(self):
        result = build_firm_identity_key(name="Silva Advogados Ltda")
        assert result == "name:SILVA ADVOGADOS"

    def test_all_none(self):
        result = build_firm_identity_key(name=None)
        assert result is None

    def test_invalid_cnpj_falls_to_cnsa(self):
        result = build_firm_identity_key(
            name="Firm",
            cnpj="",
            cnsa_number="999",
        )
        assert result == "cnsa:999"

    def test_no_cnpj_no_cnsa_falls_to_name(self):
        result = build_firm_identity_key(
            name="Escritorio ABC",
            cnpj=None,
            cnsa_number=None,
        )
        assert result == "name:ESCRITORIO ABC"
