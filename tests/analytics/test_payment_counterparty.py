"""Tests for analytics/payment_counterparty.py."""

from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics.payment_counterparty import build_payment_counterparties


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def _make_expense(
    *,
    name: str = "FORNECEDOR X",
    name_normalized: str = "FORNECEDOR X",
    tax_id: str = "12345678000190",
    amount: float = 1000.0,
    year: int = 2022,
    state: str = "SP",
    party_abbrev: str = "PT",
    date: str = "2022-06-15",
    name_rfb: str = "",
    cnae_code: str = "",
    source_file: str = "receitas_2022.csv",
    ingest_run_id: str = "run-001",
    collected_at: str = "2026-01-01T00:00:00",
    record_kind: str = "expense",
) -> dict:
    return {
        "record_kind": record_kind,
        "actor_kind": "organ",
        "election_year": year,
        "state": state,
        "org_scope": "estadual",
        "org_party_name": "PARTIDO",
        "org_party_abbrev": party_abbrev,
        "org_cnpj": "00000000000100",
        "counterparty_name": name,
        "counterparty_name_normalized": name_normalized,
        "counterparty_tax_id": tax_id,
        "counterparty_name_rfb": name_rfb,
        "counterparty_cnae_code": cnae_code,
        "counterparty_cnae_desc": "",
        "transaction_amount": amount,
        "transaction_date": date,
        "transaction_description": "Pagamento",
        "record_hash": "abc123",
        "source_file": source_file,
        "source_url": "https://example.com",
        "collected_at": collected_at,
        "ingest_run_id": ingest_run_id,
    }


class TestTaxIdConsolidation:
    """Same tax_id formatted differently → same identity key."""

    def test_same_tax_id_different_formatting(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        output_dir = tmp_path / "analytics"
        _write_jsonl(
            tse_dir / "party_org_finance_raw.jsonl",
            [
                _make_expense(tax_id="12.345.678/0001-90", amount=500.0, year=2022),
                _make_expense(tax_id="12345678000190", amount=300.0, year=2024),
            ],
        )
        build_payment_counterparties(tse_dir=tse_dir, output_dir=output_dir)
        rows = _read_jsonl(output_dir / "payment_counterparty.jsonl")
        assert len(rows) == 1
        assert rows[0]["identity_basis"] == "tax_id"
        assert rows[0]["counterparty_tax_id_normalized"] == "12345678000190"
        assert rows[0]["total_received_brl"] == 800.0
        assert rows[0]["payment_count"] == 2
        assert sorted(rows[0]["election_years"]) == [2022, 2024]


class TestNameFallback:
    """Fallback by name when tax_id is empty."""

    def test_name_fallback_when_no_tax_id(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        output_dir = tmp_path / "analytics"
        _write_jsonl(
            tse_dir / "party_org_finance_raw.jsonl",
            [
                _make_expense(tax_id="", name_normalized="FORNECEDOR ALPHA", amount=200.0),
            ],
        )
        build_payment_counterparties(tse_dir=tse_dir, output_dir=output_dir)
        rows = _read_jsonl(output_dir / "payment_counterparty.jsonl")
        assert len(rows) == 1
        assert rows[0]["identity_basis"] == "name_fallback"
        assert rows[0]["counterparty_tax_id_normalized"] == ""


class TestDistinctDocuments:
    """Same name, different tax_ids → 2 separate records."""

    def test_no_fusion_with_different_tax_ids(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        output_dir = tmp_path / "analytics"
        _write_jsonl(
            tse_dir / "party_org_finance_raw.jsonl",
            [
                _make_expense(name_normalized="ACME", tax_id="11111111000100", amount=100.0),
                _make_expense(name_normalized="ACME", tax_id="22222222000200", amount=200.0),
            ],
        )
        build_payment_counterparties(tse_dir=tse_dir, output_dir=output_dir)
        rows = _read_jsonl(output_dir / "payment_counterparty.jsonl")
        assert len(rows) == 2


class TestExpenseFilter:
    """Only expense records are processed; revenue is skipped."""

    def test_revenue_records_skipped(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        output_dir = tmp_path / "analytics"
        _write_jsonl(
            tse_dir / "party_org_finance_raw.jsonl",
            [
                _make_expense(record_kind="revenue", amount=999.0),
                _make_expense(record_kind="expense", amount=100.0),
            ],
        )
        build_payment_counterparties(tse_dir=tse_dir, output_dir=output_dir)
        rows = _read_jsonl(output_dir / "payment_counterparty.jsonl")
        assert len(rows) == 1
        assert rows[0]["total_received_brl"] == 100.0

        summary = json.loads((output_dir / "payment_counterparty_summary.json").read_text())
        assert summary["skipped_non_expense_records"] == 1
        assert summary["total_expense_records"] == 1


class TestDateRangeTracking:
    """Date range tracks min/max, ignoring empty strings."""

    def test_date_range(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        output_dir = tmp_path / "analytics"
        _write_jsonl(
            tse_dir / "party_org_finance_raw.jsonl",
            [
                _make_expense(date="2020-03-10", amount=100.0),
                _make_expense(date="", amount=50.0),
                _make_expense(date="2022-11-01", amount=75.0),
            ],
        )
        build_payment_counterparties(tse_dir=tse_dir, output_dir=output_dir)
        rows = _read_jsonl(output_dir / "payment_counterparty.jsonl")
        assert len(rows) == 1
        assert rows[0]["first_payment_date"] == "2020-03-10"
        assert rows[0]["last_payment_date"] == "2022-11-01"


class TestNamePriority:
    """counterparty_name_rfb > counterparty_name > counterparty_name_normalized."""

    def test_rfb_name_preferred(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        output_dir = tmp_path / "analytics"
        _write_jsonl(
            tse_dir / "party_org_finance_raw.jsonl",
            [
                _make_expense(
                    name="RAW NAME",
                    name_normalized="NORMALIZED",
                    name_rfb="",
                    amount=100.0,
                ),
                _make_expense(
                    name="RAW NAME",
                    name_normalized="NORMALIZED",
                    name_rfb="RFB NAME",
                    amount=100.0,
                ),
            ],
        )
        build_payment_counterparties(tse_dir=tse_dir, output_dir=output_dir)
        rows = _read_jsonl(output_dir / "payment_counterparty.jsonl")
        assert len(rows) == 1
        assert rows[0]["counterparty_name"] == "RFB NAME"


class TestOptionalFields:
    """Optional fields absent → empty lists."""

    def test_empty_cnae_when_absent(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        output_dir = tmp_path / "analytics"
        _write_jsonl(
            tse_dir / "party_org_finance_raw.jsonl",
            [_make_expense(cnae_code="", amount=100.0)],
        )
        build_payment_counterparties(tse_dir=tse_dir, output_dir=output_dir)
        rows = _read_jsonl(output_dir / "payment_counterparty.jsonl")
        assert rows[0]["cnae_codes"] == []

    def test_cnae_collected_when_present(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        output_dir = tmp_path / "analytics"
        _write_jsonl(
            tse_dir / "party_org_finance_raw.jsonl",
            [_make_expense(cnae_code="4110700", amount=100.0)],
        )
        build_payment_counterparties(tse_dir=tse_dir, output_dir=output_dir)
        rows = _read_jsonl(output_dir / "payment_counterparty.jsonl")
        assert rows[0]["cnae_codes"] == ["4110700"]


class TestProvenanceSummarized:
    """Provenance stores counters, not raw lists."""

    def test_provenance_counters(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        output_dir = tmp_path / "analytics"
        _write_jsonl(
            tse_dir / "party_org_finance_raw.jsonl",
            [
                _make_expense(source_file="a.csv", ingest_run_id="r1", collected_at="2026-01-01T00:00:00"),
                _make_expense(source_file="b.csv", ingest_run_id="r2", collected_at="2026-02-01T00:00:00"),
            ],
        )
        build_payment_counterparties(tse_dir=tse_dir, output_dir=output_dir)
        rows = _read_jsonl(output_dir / "payment_counterparty.jsonl")
        prov = rows[0]["provenance"]
        assert prov["source_file_count"] == 2
        assert prov["ingest_run_count"] == 2
        assert prov["first_collected_at"] == "2026-01-01T00:00:00"
        assert prov["last_collected_at"] == "2026-02-01T00:00:00"


class TestSummaryJson:
    """Summary JSON has expected fields."""

    def test_summary_fields(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        output_dir = tmp_path / "analytics"
        _write_jsonl(
            tse_dir / "party_org_finance_raw.jsonl",
            [
                _make_expense(tax_id="11111111000100", amount=100.0),
                _make_expense(tax_id="", name_normalized="SEM DOC", amount=50.0),
                _make_expense(record_kind="revenue"),
            ],
        )
        build_payment_counterparties(tse_dir=tse_dir, output_dir=output_dir)
        summary = json.loads((output_dir / "payment_counterparty_summary.json").read_text())
        assert summary["total_expense_records"] == 2
        assert summary["skipped_non_expense_records"] == 1
        assert summary["skipped_no_identity_records"] == 0
        assert summary["aggregated_counterparties"] == 2
        assert summary["aggregated_by_tax_id_count"] == 1
        assert summary["aggregated_by_name_fallback_count"] == 1
        assert summary["total_received_brl"] == 150.0
        assert "generated_at" in summary


class TestTaxIdPresentNameEmpty:
    """Tax_id present with empty name → consolidates by tax_id."""

    def test_tax_id_with_empty_name(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        output_dir = tmp_path / "analytics"
        _write_jsonl(
            tse_dir / "party_org_finance_raw.jsonl",
            [
                _make_expense(
                    name="",
                    name_normalized="",
                    tax_id="99999999000100",
                    name_rfb="",
                    amount=100.0,
                ),
            ],
        )
        build_payment_counterparties(tse_dir=tse_dir, output_dir=output_dir)
        rows = _read_jsonl(output_dir / "payment_counterparty.jsonl")
        assert len(rows) == 1
        assert rows[0]["identity_basis"] == "tax_id"
        assert rows[0]["counterparty_tax_id_normalized"] == "99999999000100"


class TestDeterminism:
    """Same input → same counterparty_id."""

    def test_deterministic_id(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        records = [_make_expense(tax_id="11111111000100", amount=100.0)]
        _write_jsonl(tse_dir / "party_org_finance_raw.jsonl", records)

        out1 = tmp_path / "out1"
        build_payment_counterparties(tse_dir=tse_dir, output_dir=out1)
        out2 = tmp_path / "out2"
        build_payment_counterparties(tse_dir=tse_dir, output_dir=out2)

        rows1 = _read_jsonl(out1 / "payment_counterparty.jsonl")
        rows2 = _read_jsonl(out2 / "payment_counterparty.jsonl")
        assert rows1[0]["counterparty_id"] == rows2[0]["counterparty_id"]


class TestEmptyOrAbsentFile:
    """Empty/absent file → empty output, no exception."""

    def test_absent_file(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        tse_dir.mkdir(parents=True)
        output_dir = tmp_path / "analytics"
        build_payment_counterparties(tse_dir=tse_dir, output_dir=output_dir)
        rows = _read_jsonl(output_dir / "payment_counterparty.jsonl")
        assert rows == []
        summary = json.loads((output_dir / "payment_counterparty_summary.json").read_text())
        assert summary["aggregated_counterparties"] == 0

    def test_empty_file(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        tse_dir.mkdir(parents=True)
        (tse_dir / "party_org_finance_raw.jsonl").write_text("", encoding="utf-8")
        output_dir = tmp_path / "analytics"
        build_payment_counterparties(tse_dir=tse_dir, output_dir=output_dir)
        rows = _read_jsonl(output_dir / "payment_counterparty.jsonl")
        assert rows == []


class TestSkippedNoIdentity:
    """Records with neither tax_id nor name are counted as skipped."""

    def test_skipped_no_identity(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        output_dir = tmp_path / "analytics"
        _write_jsonl(
            tse_dir / "party_org_finance_raw.jsonl",
            [_make_expense(tax_id="", name_normalized="", amount=50.0)],
        )
        build_payment_counterparties(tse_dir=tse_dir, output_dir=output_dir)
        rows = _read_jsonl(output_dir / "payment_counterparty.jsonl")
        assert rows == []
        summary = json.loads((output_dir / "payment_counterparty_summary.json").read_text())
        assert summary["skipped_no_identity_records"] == 1


class TestDocumentType:
    """Infers CPF/CNPJ from digit count."""

    def test_cpf_inferred(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        output_dir = tmp_path / "analytics"
        _write_jsonl(
            tse_dir / "party_org_finance_raw.jsonl",
            [_make_expense(tax_id="12345678901", amount=100.0)],
        )
        build_payment_counterparties(tse_dir=tse_dir, output_dir=output_dir)
        rows = _read_jsonl(output_dir / "payment_counterparty.jsonl")
        assert rows[0]["counterparty_document_type"] == "cpf"

    def test_cnpj_inferred(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        output_dir = tmp_path / "analytics"
        _write_jsonl(
            tse_dir / "party_org_finance_raw.jsonl",
            [_make_expense(tax_id="12345678000190", amount=100.0)],
        )
        build_payment_counterparties(tse_dir=tse_dir, output_dir=output_dir)
        rows = _read_jsonl(output_dir / "payment_counterparty.jsonl")
        assert rows[0]["counterparty_document_type"] == "cnpj"


class TestPayerActorType:
    """payer_actor_type is always 'party_org' in this phase."""

    def test_payer_actor_type_fixed(self, tmp_path: Path) -> None:
        tse_dir = tmp_path / "tse"
        output_dir = tmp_path / "analytics"
        _write_jsonl(
            tse_dir / "party_org_finance_raw.jsonl",
            [_make_expense(amount=100.0)],
        )
        build_payment_counterparties(tse_dir=tse_dir, output_dir=output_dir)
        rows = _read_jsonl(output_dir / "payment_counterparty.jsonl")
        assert rows[0]["payer_actor_type"] == "party_org"
