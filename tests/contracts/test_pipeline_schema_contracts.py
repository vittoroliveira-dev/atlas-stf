"""Cross-boundary schema contract tests.

Verifies that fields produced by each pipeline stage match what the next
stage expects — raw→analytics, analytics→serving.  These tests read sample
data from disk and fail if a required field is absent.

Unlike parser field contracts (which test individual parsers with synthetic
data), these tests verify the producer-consumer contract across module
boundaries using real artifacts.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_RAW = Path("data/raw")
_ANALYTICS = Path("data/analytics")


def _read_first_record(path: Path) -> dict | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                return json.loads(line)
    return None


def _read_keys(path: Path) -> set[str] | None:
    rec = _read_first_record(path)
    return set(rec.keys()) if rec else None


def _skip_if_missing(path: Path) -> None:
    if not path.exists():
        pytest.skip(f"{path} not found (source not fetched)")


def _search_jsonl(path: Path, key: str, value: object, max_lines: int = 500_000) -> dict | None:
    """Find first record where record[key] == value."""
    with path.open("r", encoding="utf-8") as fh:
        for i, line in enumerate(fh):
            if i >= max_lines:
                break
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if rec.get(key) == value:
                return rec
    return None


# ---------------------------------------------------------------------------
# Raw → Analytics contracts
# ---------------------------------------------------------------------------


class TestRawToAnalyticsSanctionMatch:
    """CGU/CVM sanctions_raw.jsonl must have fields that sanction_match.py reads."""

    _REQUIRED = {
        "entity_name",
        "entity_cnpj_cpf",
        "sanction_source",
        "sanction_id",
        "sanction_type",
        "sanction_start_date",
    }

    def test_cgu_has_required_fields(self) -> None:
        path = _RAW / "cgu" / "sanctions_raw.jsonl"
        _skip_if_missing(path)
        keys = _read_keys(path)
        assert keys is not None
        missing = self._REQUIRED - keys
        assert not missing, f"CGU sanctions_raw missing fields for sanction_match: {missing}"

    def test_cvm_has_required_fields(self) -> None:
        path = _RAW / "cvm" / "sanctions_raw.jsonl"
        _skip_if_missing(path)
        keys = _read_keys(path)
        assert keys is not None
        missing = self._REQUIRED - keys
        assert not missing, f"CVM sanctions_raw missing fields for sanction_match: {missing}"


class TestRawToAnalyticsDonationMatch:
    """TSE donations_raw.jsonl must have fields that donation_match.py reads.

    TSE has 6 CSV schema generations (2002-2024).  Early years lack fields
    like ``donation_date`` that are present from 2018+.  We split into
    universal fields (all years) and modern fields (2018+).
    """

    _UNIVERSAL = {
        "donor_name_normalized",
        "donor_cpf_cnpj",
        "election_year",
        "donation_amount",
        "candidate_name",
        "party_abbrev",
        "position",
        "state",
    }
    # Optional fields: donation_date, donor_name_originator — present only when
    # the source CSV has the corresponding column.  Analytics consumers use
    # .get() with fallback, so absence is valid.

    def test_tse_donations_has_required_fields(self) -> None:
        path = _RAW / "tse" / "donations_raw.jsonl"
        _skip_if_missing(path)
        keys = _read_keys(path)
        assert keys is not None
        missing = self._UNIVERSAL - keys
        assert not missing, f"TSE donations_raw missing universal fields: {missing}"


class TestRawToAnalyticsEconomicGroup:
    """RFB partners_raw.jsonl must have fields that economic_group.py reads."""

    _REQUIRED = {
        "cnpj_basico",
        "partner_cpf_cnpj",
        "partner_type",
        "partner_name_normalized",
    }

    def test_rfb_partners_has_required_fields(self) -> None:
        path = _RAW / "rfb" / "partners_raw.jsonl"
        _skip_if_missing(path)
        keys = _read_keys(path)
        assert keys is not None
        missing = self._REQUIRED - keys
        assert not missing, f"RFB partners_raw missing fields for economic_group: {missing}"


class TestRawToAnalyticsCorporateLink:
    """RFB companies/establishments must have fields that sanction_corporate_link.py reads."""

    _COMPANIES_REQUIRED = {"cnpj_basico", "razao_social", "capital_social"}
    _ESTABLISHMENTS_REQUIRED = {"cnpj_basico", "uf", "situacao_cadastral", "cnae_fiscal"}

    def test_rfb_companies_has_required_fields(self) -> None:
        path = _RAW / "rfb" / "companies_raw.jsonl"
        _skip_if_missing(path)
        keys = _read_keys(path)
        assert keys is not None
        missing = self._COMPANIES_REQUIRED - keys
        assert not missing, f"RFB companies_raw missing fields: {missing}"

    def test_rfb_establishments_has_required_fields(self) -> None:
        path = _RAW / "rfb" / "establishments_raw.jsonl"
        _skip_if_missing(path)
        keys = _read_keys(path)
        assert keys is not None
        missing = self._ESTABLISHMENTS_REQUIRED - keys
        assert not missing, f"RFB establishments_raw missing fields: {missing}"


class TestRawToAnalyticsOriginContext:
    """DataJud JSON must have fields that origin_context.py reads."""

    _REQUIRED = {"index", "total_processes", "top_assuntos", "top_orgaos_julgadores", "class_distribution"}

    def test_datajud_has_required_fields(self) -> None:
        datajud_dir = _RAW / "datajud"
        _skip_if_missing(datajud_dir)
        jsons = sorted(datajud_dir.glob("api_publica_*.json"))
        if not jsons:
            pytest.skip("No DataJud JSON files found")
        data = json.loads(jsons[0].read_text(encoding="utf-8"))
        keys = set(data.keys())
        missing = self._REQUIRED - keys
        assert not missing, f"DataJud JSON missing fields for origin_context: {missing}"


# ---------------------------------------------------------------------------
# Analytics → Serving contracts
# ---------------------------------------------------------------------------


class TestAnalyticsToServingSanctionMatch:
    """sanction_match.jsonl must have fields that serving loader expects."""

    _REQUIRED = {
        "match_id",
        "entity_type",
        "sanction_source",
        "sanction_id",
        "match_strategy",
        "match_score",
        "red_flag",
        "stf_case_count",
        "favorable_rate",
    }

    def test_sanction_match_has_required_fields(self) -> None:
        path = _ANALYTICS / "sanction_match.jsonl"
        _skip_if_missing(path)
        keys = _read_keys(path)
        assert keys is not None
        missing = self._REQUIRED - keys
        assert not missing, f"sanction_match.jsonl missing fields for serving: {missing}"


class TestAnalyticsToServingDonationMatch:
    """donation_match.jsonl must have fields that serving loader expects."""

    _REQUIRED = {
        "match_id",
        "entity_type",
        "donor_cpf_cnpj",
        "total_donated_brl",
        "donation_count",
        "match_strategy",
        "match_score",
        "red_flag",
        "donor_identity_key",
    }

    def test_donation_match_has_required_fields(self) -> None:
        path = _ANALYTICS / "donation_match.jsonl"
        _skip_if_missing(path)
        keys = _read_keys(path)
        assert keys is not None
        missing = self._REQUIRED - keys
        assert not missing, f"donation_match.jsonl missing fields for serving: {missing}"


class TestAnalyticsToServingEconomicGroup:
    """economic_group.jsonl must have fields that serving loader expects."""

    _REQUIRED = {
        "group_id",
        "member_cnpjs",
        "razoes_sociais",
        "member_count",
        "is_law_firm_group",
    }

    def test_economic_group_has_required_fields(self) -> None:
        path = _ANALYTICS / "economic_group.jsonl"
        _skip_if_missing(path)
        keys = _read_keys(path)
        assert keys is not None
        missing = self._REQUIRED - keys
        assert not missing, f"economic_group.jsonl missing fields for serving: {missing}"


class TestAnalyticsToServingCompoundRisk:
    """compound_risk.jsonl must have fields that serving loader expects."""

    _REQUIRED = {
        "pair_id",
        "minister_name",
        "entity_type",
        "entity_id",
        "entity_name",
        "signal_count",
        "signals",
        "red_flag",
        "max_alert_score",
    }

    def test_compound_risk_has_required_fields(self) -> None:
        path = _ANALYTICS / "compound_risk.jsonl"
        _skip_if_missing(path)
        keys = _read_keys(path)
        assert keys is not None
        missing = self._REQUIRED - keys
        assert not missing, f"compound_risk.jsonl missing fields for serving: {missing}"


class TestAnalyticsToServingCounselAffinity:
    """counsel_affinity.jsonl must have fields that serving loader expects."""

    _REQUIRED = {
        "affinity_id",
        "rapporteur",
        "counsel_id",
        "shared_case_count",
        "pair_favorable_rate",
        "red_flag",
    }

    def test_counsel_affinity_has_required_fields(self) -> None:
        path = _ANALYTICS / "counsel_affinity.jsonl"
        _skip_if_missing(path)
        keys = _read_keys(path)
        assert keys is not None
        missing = self._REQUIRED - keys
        assert not missing, f"counsel_affinity.jsonl missing fields for serving: {missing}"


class TestAnalyticsToServingCorporateNetwork:
    """corporate_network.jsonl must have fields that serving loader expects."""

    _REQUIRED = {
        "conflict_id",
        "minister_name",
        "company_cnpj_basico",
        "linked_entity_type",
        "linked_entity_id",
        "risk_score",
        "red_flag",
    }

    def test_corporate_network_has_required_fields(self) -> None:
        path = _ANALYTICS / "corporate_network.jsonl"
        _skip_if_missing(path)
        keys = _read_keys(path)
        if keys is None:
            pytest.skip("corporate_network.jsonl is empty (no corporate conflicts detected)")
        missing = self._REQUIRED - keys
        assert not missing, f"corporate_network.jsonl missing fields for serving: {missing}"
