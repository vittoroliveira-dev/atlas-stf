"""Tests for oab/_providers.py."""

from __future__ import annotations

from atlas_stf.oab._config import OabValidationConfig
from atlas_stf.oab._providers import (
    CnaProvider,
    CnsaProvider,
    FormatOnlyProvider,
    NullOabProvider,
    OabValidationResult,
    select_provider,
)


class TestOabValidationResult:
    def test_fields(self) -> None:
        result = OabValidationResult(
            oab_number="12345",
            oab_state="SP",
            oab_status="ativo",
            oab_source="cna",
            oab_validation_method="api",
            oab_last_checked_at="2026-01-01T00:00:00Z",
            cna_name="JOAO DA SILVA",
            cna_firm_name="SILVA ADVOGADOS",
            cna_firm_cnsa="999",
        )
        assert result.oab_number == "12345"
        assert result.oab_state == "SP"
        assert result.oab_status == "ativo"
        assert result.oab_source == "cna"
        assert result.oab_validation_method == "api"
        assert result.oab_last_checked_at == "2026-01-01T00:00:00Z"
        assert result.cna_name == "JOAO DA SILVA"
        assert result.cna_firm_name == "SILVA ADVOGADOS"
        assert result.cna_firm_cnsa == "999"

    def test_optional_fields_default_none(self) -> None:
        result = OabValidationResult(
            oab_number="1",
            oab_state="RJ",
            oab_status=None,
            oab_source="null",
            oab_validation_method="none",
            oab_last_checked_at=None,
        )
        assert result.cna_name is None
        assert result.cna_firm_name is None
        assert result.cna_firm_cnsa is None


class TestNullOabProvider:
    def test_validate_returns_null_status(self) -> None:
        provider = NullOabProvider()
        result = provider.validate("12345", "SP")
        assert result.oab_number == "12345"
        assert result.oab_state == "SP"
        assert result.oab_status is None
        assert result.oab_source == "null"
        assert result.oab_validation_method == "none"
        assert result.oab_last_checked_at is None

    def test_validate_preserves_input(self) -> None:
        provider = NullOabProvider()
        result = provider.validate("999999", "DF")
        assert result.oab_number == "999999"
        assert result.oab_state == "DF"

    def test_validate_batch_returns_list(self) -> None:
        provider = NullOabProvider()
        entries = [("12345", "SP"), ("67890", "RJ"), ("111", "MG")]
        results = provider.validate_batch(entries)
        assert len(results) == 3
        assert all(r.oab_source == "null" for r in results)
        assert results[0].oab_number == "12345"
        assert results[1].oab_number == "67890"
        assert results[2].oab_number == "111"

    def test_validate_batch_empty(self) -> None:
        provider = NullOabProvider()
        results = provider.validate_batch([])
        assert results == []


class TestFormatOnlyProvider:
    def test_validate_valid_format(self) -> None:
        provider = FormatOnlyProvider()
        result = provider.validate("12345", "SP")
        assert result.oab_status == "format_valid"
        assert result.oab_source == "format_only"
        assert result.oab_validation_method == "format"

    def test_validate_invalid_format_bad_state(self) -> None:
        provider = FormatOnlyProvider()
        result = provider.validate("12345", "XX")
        assert result.oab_status is None
        assert result.oab_source == "format_only"
        assert result.oab_validation_method == "format"

    def test_validate_invalid_format_too_long(self) -> None:
        provider = FormatOnlyProvider()
        result = provider.validate("1234567", "SP")
        assert result.oab_status is None

    def test_validate_invalid_format_empty_number(self) -> None:
        provider = FormatOnlyProvider()
        result = provider.validate("", "SP")
        assert result.oab_status is None

    def test_validate_batch_mixed(self) -> None:
        provider = FormatOnlyProvider()
        entries = [("12345", "SP"), ("99", "XX"), ("1", "RJ")]
        results = provider.validate_batch(entries)
        assert len(results) == 3
        assert results[0].oab_status == "format_valid"
        assert results[1].oab_status is None
        assert results[2].oab_status == "format_valid"


class TestCnaProvider:
    def test_falls_back_to_format(self) -> None:
        provider = CnaProvider(api_key="test-key")
        result = provider.validate("12345", "SP")
        assert result.oab_source == "cna"
        assert result.oab_number == "12345"
        assert result.oab_state == "SP"

    def test_validate_batch(self) -> None:
        provider = CnaProvider(api_key="test-key")
        entries = [("100", "SP"), ("200", "RJ")]
        results = provider.validate_batch(entries)
        assert len(results) == 2
        assert all(r.oab_source == "cna" for r in results)

    def test_custom_rate_limit(self) -> None:
        provider = CnaProvider(api_key="key", rate_limit_seconds=5.0, max_retries=5)
        assert provider._rate_limit == 5.0
        assert provider._max_retries == 5


class TestCnsaProvider:
    def test_returns_unavailable(self) -> None:
        provider = CnsaProvider()
        result = provider.validate("12345", "SP")
        assert result.oab_status is None
        assert result.oab_source == "cnsa_unavailable"
        assert result.oab_validation_method == "none"

    def test_validate_batch(self) -> None:
        provider = CnsaProvider()
        entries = [("100", "SP"), ("200", "RJ")]
        results = provider.validate_batch(entries)
        assert len(results) == 2
        assert all(r.oab_source == "cnsa_unavailable" for r in results)


class TestSelectProvider:
    def test_null_default(self) -> None:
        config = OabValidationConfig()
        provider = select_provider(config)
        assert isinstance(provider, NullOabProvider)

    def test_null_explicit(self) -> None:
        config = OabValidationConfig(provider="null")
        provider = select_provider(config)
        assert isinstance(provider, NullOabProvider)

    def test_format(self) -> None:
        config = OabValidationConfig(provider="format")
        provider = select_provider(config)
        assert isinstance(provider, FormatOnlyProvider)

    def test_cna_with_key(self) -> None:
        config = OabValidationConfig(provider="cna", api_key="my-key")
        provider = select_provider(config)
        assert isinstance(provider, CnaProvider)

    def test_cna_without_key_falls_back(self) -> None:
        config = OabValidationConfig(provider="cna", api_key=None)
        provider = select_provider(config)
        assert isinstance(provider, FormatOnlyProvider)

    def test_cnsa(self) -> None:
        config = OabValidationConfig(provider="cnsa")
        provider = select_provider(config)
        assert isinstance(provider, CnsaProvider)

    def test_unknown_defaults_to_null(self) -> None:
        config = OabValidationConfig(provider="unknown")
        provider = select_provider(config)
        assert isinstance(provider, NullOabProvider)
