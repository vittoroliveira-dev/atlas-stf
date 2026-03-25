"""Governance validator tests — column_governance.json e join_matrix.json."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from atlas_stf.contracts._governance import classify_join_fitness, scope_declaration
from atlas_stf.contracts._governance_validator import (
    validate_all,
    validate_governance,
    validate_matrix,
)

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_GOVERNANCE_DIR = _PROJECT_ROOT / "data" / "contracts" / "governance"
_OBSERVED_DIR = _PROJECT_ROOT / "data" / "contracts" / "observed"
_GOVERNANCE_PATH = _GOVERNANCE_DIR / "column_governance.json"
_MATRIX_PATH = _GOVERNANCE_DIR / "join_matrix.json"

_REQUIRED_COLUMN_FIELDS = {
    "canonical_name",
    "concept",
    "aliases",
    "join_role",
    "usability",
    "lossiness",
    "validation_status",
    "normalization_required",
}

_skip_no_governance = pytest.mark.skipif(
    not _GOVERNANCE_DIR.exists(),
    reason="governance dir ausente — execute inspect_sources primeiro",
)


# ---------------------------------------------------------------------------
# TestGovernanceFileParsing
# ---------------------------------------------------------------------------


class TestGovernanceFileParsing:
    @_skip_no_governance
    def test_column_governance_parses(self) -> None:
        raw = json.loads(_GOVERNANCE_PATH.read_text(encoding="utf-8"))
        assert "canonical_columns" in raw
        assert len(raw["canonical_columns"]) >= 10
        assert "enums" in raw
        assert "schema_version" in raw

    @_skip_no_governance
    def test_join_matrix_parses(self) -> None:
        raw = json.loads(_MATRIX_PATH.read_text(encoding="utf-8"))
        assert "matrix" in raw
        assert len(raw["matrix"]) >= 15
        assert "summary" in raw
        assert "join_policies" in raw

    @_skip_no_governance
    def test_all_canonical_columns_have_required_fields(self) -> None:
        raw = json.loads(_GOVERNANCE_PATH.read_text(encoding="utf-8"))
        for entry in raw["canonical_columns"]:
            missing = _REQUIRED_COLUMN_FIELDS - entry.keys()
            assert not missing, f"canonical_name={entry.get('canonical_name')!r} faltando campos: {missing}"


# ---------------------------------------------------------------------------
# TestGovernanceConsistency
# ---------------------------------------------------------------------------


class TestGovernanceConsistency:
    @_skip_no_governance
    def test_no_critical_violations(self) -> None:
        violations = validate_governance(_GOVERNANCE_PATH, _OBSERVED_DIR)
        critical = [v for v in violations if v.severity == "critical"]
        if critical:
            lines = [str(v) for v in critical]
            pytest.fail("Violações críticas de governança:\n" + "\n".join(lines))

    @_skip_no_governance
    def test_matrix_consistent_with_governance(self) -> None:
        violations = validate_matrix(_MATRIX_PATH, _GOVERNANCE_PATH)
        critical = [v for v in violations if v.severity == "critical"]
        if critical:
            lines = [str(v) for v in critical]
            pytest.fail("Violações críticas na matriz:\n" + "\n".join(lines))

    @_skip_no_governance
    def test_validate_all_clean(self) -> None:
        violations = validate_all(_GOVERNANCE_DIR, _OBSERVED_DIR)
        critical = [v for v in violations if v.severity == "critical"]
        if critical:
            lines = [str(v) for v in critical]
            pytest.fail("Violações críticas em validate_all:\n" + "\n".join(lines))


# ---------------------------------------------------------------------------
# TestGovernanceSemantics
# ---------------------------------------------------------------------------


class TestGovernanceSemantics:
    @_skip_no_governance
    def test_cvm_tax_id_declared_absent(self) -> None:
        raw = json.loads(_GOVERNANCE_PATH.read_text(encoding="utf-8"))
        tax_id_entry = next(
            (e for e in raw["canonical_columns"] if e["canonical_name"] == "entity_tax_id"),
            None,
        )
        assert tax_id_entry is not None, "entity_tax_id ausente em canonical_columns"
        aliases: dict[str, object] = tax_id_entry["aliases"]
        assert "cvm/processo_sancionador_acusado.csv" in aliases, (
            "CVM acusado não declarado em aliases de entity_tax_id"
        )
        assert aliases["cvm/processo_sancionador_acusado.csv"] is None, (
            "entity_tax_id para CVM acusado deve ser null (ausente)"
        )

    @_skip_no_governance
    def test_cvm_blocked_in_matrix(self) -> None:
        raw = json.loads(_MATRIX_PATH.read_text(encoding="utf-8"))
        cvm_blocked = [
            e
            for e in raw["matrix"]
            if e.get("source_a") == "cvm/processo_sancionador_acusado.csv"
            and e.get("concept") == "entity_tax_id"
            and e.get("join_type") == "blocked"
        ]
        assert cvm_blocked, "Nenhuma entrada bloqueada (CVM→RFB via entity_tax_id) encontrada na matriz"

    @_skip_no_governance
    def test_all_matrix_concepts_in_governance(self) -> None:
        governance = json.loads(_GOVERNANCE_PATH.read_text(encoding="utf-8"))
        matrix = json.loads(_MATRIX_PATH.read_text(encoding="utf-8"))
        canonical_names = {e["canonical_name"] for e in governance["canonical_columns"]}
        unknown = {e["concept"] for e in matrix["matrix"] if e.get("concept") not in canonical_names}
        assert not unknown, f"Conceitos na matriz não declarados em governance: {unknown}"

    @_skip_no_governance
    def test_stf_process_aliases_match_inventories(self) -> None:
        raw = json.loads(_GOVERNANCE_PATH.read_text(encoding="utf-8"))
        process_entry = next(
            (e for e in raw["canonical_columns"] if e["canonical_name"] == "process_number"),
            None,
        )
        assert process_entry is not None, "process_number ausente em canonical_columns"
        aliases: dict[str, object] = process_entry["aliases"]
        assert aliases.get("stf/decisoes.csv") == "processo"
        assert aliases.get("stf/plenario_virtual.csv") == "processo"
        assert aliases.get("stf/distribuidos.csv") == "no_do_processo"

    @_skip_no_governance
    def test_blocked_entries_have_null_field_a(self) -> None:
        raw = json.loads(_MATRIX_PATH.read_text(encoding="utf-8"))
        blocked = [e for e in raw["matrix"] if e.get("join_type") == "blocked"]
        assert blocked, "Nenhuma entrada bloqueada encontrada — verificar o JSON"
        for entry in blocked:
            assert entry.get("field_a") is None, (
                f"Entrada bloqueada {entry['id']!r} tem field_a não-nulo: {entry['field_a']!r}"
            )

    @_skip_no_governance
    def test_summary_counts_match(self) -> None:
        raw = json.loads(_MATRIX_PATH.read_text(encoding="utf-8"))
        matrix: list[dict[str, object]] = raw["matrix"]
        summary: dict[str, int] = raw["summary"]
        assert summary["total_pairs"] == len(matrix)
        assert summary["viable_strict"] == sum(1 for e in matrix if e.get("join_type") == "strict")
        assert summary["viable_broad"] == sum(1 for e in matrix if e.get("join_type") == "broad")
        assert summary["blocked"] == sum(1 for e in matrix if e.get("join_type") == "blocked")


# ---------------------------------------------------------------------------
# TestGovernanceNegative
# ---------------------------------------------------------------------------


class TestGovernanceNegative:
    @_skip_no_governance
    def test_wrong_alias_detected(self, tmp_path: Path) -> None:
        raw = json.loads(_GOVERNANCE_PATH.read_text(encoding="utf-8"))
        # Inject a wrong alias for process_number in decisoes.csv
        for entry in raw["canonical_columns"]:
            if entry["canonical_name"] == "process_number":
                entry["aliases"]["stf/decisoes.csv"] = "coluna_inexistente_xyzzy"
                break
        bad_gov = tmp_path / "column_governance.json"
        bad_gov.write_text(json.dumps(raw), encoding="utf-8")
        violations = validate_governance(bad_gov, _OBSERVED_DIR)
        critical = [v for v in violations if v.severity == "critical"]
        assert critical, "Alias errado deveria gerar violação crítica mas nenhuma foi reportada"

    @_skip_no_governance
    def test_unknown_concept_detected(self, tmp_path: Path) -> None:
        raw = json.loads(_MATRIX_PATH.read_text(encoding="utf-8"))
        raw["matrix"].append(
            {
                "id": "test_unknown_concept",
                "source_a": "cgu/ceis.csv",
                "field_a": "CPF OU CNPJ DO SANCIONADO",
                "source_b": "rfb/partners_raw.jsonl",
                "field_b": "partner_cpf_cnpj",
                "concept": "conceito_completamente_inexistente_abc123",
                "join_type": "strict",
            }
        )
        bad_matrix = tmp_path / "join_matrix.json"
        bad_matrix.write_text(json.dumps(raw), encoding="utf-8")
        violations = validate_matrix(bad_matrix, _GOVERNANCE_PATH)
        critical = [v for v in violations if v.severity == "critical"]
        assert any("conceito_completamente_inexistente_abc123" in v.message for v in critical), (
            "Conceito desconhecido na matriz deveria gerar violação crítica"
        )


# ---------------------------------------------------------------------------
# TestClassifyJoinFitness — pure-function tests (no filesystem required)
# ---------------------------------------------------------------------------


def _make_inventory(
    source: str,
    file_name: str,
    columns: list[dict[str, Any]],
) -> dict[str, Any]:
    return {"source": source, "file_name": file_name, "columns": columns}


def _make_col(name: str, null_rate: float = 0.0, empty_rate: float = 0.0) -> dict[str, Any]:
    return {"observed_column_name": name, "null_rate": null_rate, "empty_rate": empty_rate}


class TestClassifyJoinFitness:
    def test_unknown_source_is_skipped(self) -> None:
        inv = _make_inventory("unknown_source", "file.csv", [_make_col("CPF")])
        result = classify_join_fitness([inv])
        assert result == {}

    def test_deterministic_key_no_degradation(self) -> None:
        # CGU CPF with low null/empty rates — should remain deterministic
        inv = _make_inventory(
            "cgu",
            "ceis.csv",
            [
                _make_col("CPF OU CNPJ DO SANCIONADO", null_rate=0.01, empty_rate=0.01),
                _make_col("NOME DO SANCIONADO", null_rate=0.05, empty_rate=0.0),
                _make_col("CNPJ DO SANCIONADO", null_rate=0.02, empty_rate=0.0),
            ],
        )
        result = classify_join_fitness([inv])
        key = "cgu/ceis.csv"
        assert key in result
        cpf_entry = result[key]["CPF OU CNPJ DO SANCIONADO"]
        assert cpf_entry["expected"] == "deterministic_key"
        assert cpf_entry["actual"] == "deterministic_key"
        assert cpf_entry["degradation"] is None

    def test_deterministic_key_degrades_to_probabilistic_on_high_null(self) -> None:
        # CGU CPF with null_rate > 0.30 → should degrade
        inv = _make_inventory(
            "cgu",
            "ceis.csv",
            [
                _make_col("CPF OU CNPJ DO SANCIONADO", null_rate=0.50, empty_rate=0.0),
                _make_col("NOME DO SANCIONADO", null_rate=0.0, empty_rate=0.0),
                _make_col("CNPJ DO SANCIONADO", null_rate=0.0, empty_rate=0.0),
            ],
        )
        result = classify_join_fitness([inv])
        entry = result["cgu/ceis.csv"]["CPF OU CNPJ DO SANCIONADO"]
        assert entry["expected"] == "deterministic_key"
        assert entry["actual"] == "probabilistic_key"
        assert entry["degradation"] is not None
        assert "null=" in entry["degradation"]

    def test_deterministic_key_degrades_on_high_empty_rate(self) -> None:
        inv = _make_inventory(
            "cgu",
            "ceis.csv",
            [
                _make_col("CPF OU CNPJ DO SANCIONADO", null_rate=0.0, empty_rate=0.40),
                _make_col("NOME DO SANCIONADO", null_rate=0.0, empty_rate=0.0),
                _make_col("CNPJ DO SANCIONADO", null_rate=0.0, empty_rate=0.0),
            ],
        )
        result = classify_join_fitness([inv])
        entry = result["cgu/ceis.csv"]["CPF OU CNPJ DO SANCIONADO"]
        assert entry["actual"] == "probabilistic_key"

    def test_absent_column_reported_correctly(self) -> None:
        # Inventory for CGU but without the CNPJ column
        inv = _make_inventory(
            "cgu",
            "ceis.csv",
            [
                _make_col("CPF OU CNPJ DO SANCIONADO", null_rate=0.01, empty_rate=0.0),
                _make_col("NOME DO SANCIONADO", null_rate=0.05, empty_rate=0.0),
                # CNPJ DO SANCIONADO absent
            ],
        )
        result = classify_join_fitness([inv])
        entry = result["cgu/ceis.csv"]["CNPJ DO SANCIONADO"]
        assert entry["actual"] == "absent"
        assert entry["null_rate"] is None
        assert entry["empty_rate"] is None
        assert "ceis.csv" in entry["degradation"]

    def test_probabilistic_key_not_downgraded_further(self) -> None:
        # NOME DO SANCIONADO is probabilistic — high null rate should NOT downgrade it
        # (only deterministic keys get downgraded by the rule)
        inv = _make_inventory(
            "cgu",
            "ceis.csv",
            [
                _make_col("CPF OU CNPJ DO SANCIONADO", null_rate=0.01, empty_rate=0.0),
                _make_col("NOME DO SANCIONADO", null_rate=0.80, empty_rate=0.0),
                _make_col("CNPJ DO SANCIONADO", null_rate=0.0, empty_rate=0.0),
            ],
        )
        result = classify_join_fitness([inv])
        entry = result["cgu/ceis.csv"]["NOME DO SANCIONADO"]
        assert entry["expected"] == "probabilistic_key"
        assert entry["actual"] == "probabilistic_key"
        assert entry["degradation"] is None

    def test_tse_source_produces_correct_key(self) -> None:
        inv = _make_inventory(
            "tse",
            "donations_raw.jsonl",
            [
                _make_col("donor_cpf_cnpj", null_rate=0.05, empty_rate=0.0),
                _make_col("donor_name_normalized", null_rate=0.10, empty_rate=0.0),
                _make_col("candidate_cpf", null_rate=0.0, empty_rate=0.0),
                _make_col("supplier_tax_id", null_rate=0.0, empty_rate=0.0),
                _make_col("supplier_name_normalized", null_rate=0.0, empty_rate=0.0),
            ],
        )
        result = classify_join_fitness([inv])
        key = "tse/donations_raw.jsonl"
        assert key in result
        assert "donor_cpf_cnpj" in result[key]

    def test_multiple_inventories_produce_multiple_keys(self) -> None:
        inv_cgu = _make_inventory(
            "cgu",
            "ceis.csv",
            [
                _make_col("CPF OU CNPJ DO SANCIONADO"),
                _make_col("NOME DO SANCIONADO"),
                _make_col("CNPJ DO SANCIONADO"),
            ],
        )
        inv_cvm = _make_inventory(
            "cvm",
            "processo.csv",
            [
                _make_col("NUP"),
                _make_col("Nome_Acusado"),
                _make_col("CPF_CNPJ"),
            ],
        )
        result = classify_join_fitness([inv_cgu, inv_cvm])
        assert "cgu/ceis.csv" in result
        assert "cvm/processo.csv" in result

    def test_empty_inventories_list_returns_empty(self) -> None:
        assert classify_join_fitness([]) == {}

    def test_result_contains_null_and_empty_rate_values(self) -> None:
        inv = _make_inventory(
            "rfb",
            "partners.jsonl",
            [
                _make_col("cnpj_basico", null_rate=0.12, empty_rate=0.03),
                _make_col("partner_cpf_cnpj", null_rate=0.05, empty_rate=0.01),
                _make_col("partner_name_normalized", null_rate=0.20, empty_rate=0.0),
                _make_col("razao_social", null_rate=0.0, empty_rate=0.0),
                _make_col("representative_name_normalized", null_rate=0.0, empty_rate=0.0),
            ],
        )
        result = classify_join_fitness([inv])
        entry = result["rfb/partners.jsonl"]["cnpj_basico"]
        assert entry["null_rate"] == pytest.approx(0.12)
        assert entry["empty_rate"] == pytest.approx(0.03)


# ---------------------------------------------------------------------------
# TestScopeDeclaration — pure function, no I/O required
# ---------------------------------------------------------------------------


class TestScopeDeclaration:
    def test_returns_dict(self) -> None:
        result = scope_declaration()
        assert isinstance(result, dict)

    def test_layer_is_a(self) -> None:
        result = scope_declaration()
        assert result["layer"] == "A"

    def test_required_keys_present(self) -> None:
        result = scope_declaration()
        required = {"layer", "statement", "sources_inspected", "sources_deferred", "deferred_rationale"}
        assert required <= result.keys()

    def test_sources_inspected_contains_all_expected(self) -> None:
        result = scope_declaration()
        assert set(result["sources_inspected"]) == {"cgu", "cvm", "rfb", "tse", "stf"}

    def test_sources_deferred_non_empty(self) -> None:
        result = scope_declaration()
        assert len(result["sources_deferred"]) >= 1

    def test_inspected_and_deferred_disjoint(self) -> None:
        result = scope_declaration()
        overlap = set(result["sources_inspected"]) & set(result["sources_deferred"])
        assert overlap == set(), f"Fontes em ambas as listas: {overlap}"

    def test_statement_is_non_empty_string(self) -> None:
        result = scope_declaration()
        assert isinstance(result["statement"], str)
        assert len(result["statement"]) > 10

    def test_deferred_rationale_is_non_empty_string(self) -> None:
        result = scope_declaration()
        assert isinstance(result["deferred_rationale"], str)
        assert len(result["deferred_rationale"]) > 10
