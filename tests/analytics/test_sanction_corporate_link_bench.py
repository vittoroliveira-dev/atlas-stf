"""Benchmark tests for sanction_corporate_link (deterministic, no time assertions).

Marked with @pytest.mark.perf — excluded from `make test` (run with `pytest -m perf`).
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from atlas_stf.analytics.sanction_corporate_link import build_sanction_corporate_links
from tests.analytics._scl_helpers import (
    PARTY_CPF,
    PARTY_NAME,
    SANCTION_CNPJ,
    SANCTION_CNPJ_BASICO,
    _base_curated,
    _read_jsonl,
    _setup_rfb,
    _setup_sanctions,
    _write_jsonl,
)


def _read_summary(output_dir: Path) -> dict:
    return json.loads((output_dir / "sanction_corporate_link_summary.json").read_text())


@pytest.mark.perf
class TestSimpleCase:
    def test_simple_case(self, tmp_path: Path) -> None:
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        output_dir = tmp_path / "output"

        _setup_sanctions(cgu_dir, [{"sanction_id": "s1", "entity_cnpj_cpf": SANCTION_CNPJ}])
        _setup_rfb(
            rfb_dir,
            partners=[
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "partner_type": "2",
                 "partner_cpf_cnpj": PARTY_CPF,
                 "partner_name_normalized": PARTY_NAME},
            ],
            companies=[
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "razao_social": "SIMPLE CO"},
            ],
        )
        _base_curated(curated_dir)
        _write_jsonl(analytics_dir / "economic_group.jsonl", [])

        t0 = time.monotonic()
        build_sanction_corporate_links(
            cgu_dir=cgu_dir, cvm_dir=tmp_path / "cvm", rfb_dir=rfb_dir,
            curated_dir=curated_dir, analytics_dir=analytics_dir, output_dir=output_dir,
        )
        elapsed = time.monotonic() - t0

        records = _read_jsonl(output_dir / "sanction_corporate_link.jsonl")
        summary = _read_summary(output_dir)

        print(f"  elapsed={elapsed:.3f}s records={len(records)} summary={json.dumps(summary, indent=2)}")

        assert len(records) >= 1
        assert summary["truncated_sanctions_count"] == 0
        for r in records:
            assert r["estimated_degree3_count"] is not None


@pytest.mark.perf
class TestIntermediateCase:
    def test_intermediate_case(self, tmp_path: Path) -> None:
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        output_dir = tmp_path / "output"

        sanctions = [
            {"sanction_id": f"s{i}", "entity_cnpj_cpf": SANCTION_CNPJ}
            for i in range(5)
        ]
        _setup_sanctions(cgu_dir, sanctions)
        _setup_rfb(
            rfb_dir,
            partners=[
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "partner_type": "2",
                 "partner_cpf_cnpj": PARTY_CPF,
                 "partner_name_normalized": PARTY_NAME},
            ],
            companies=[
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "razao_social": "SHARED CO"},
            ],
        )
        _base_curated(curated_dir)
        _write_jsonl(analytics_dir / "economic_group.jsonl", [])

        t0 = time.monotonic()
        build_sanction_corporate_links(
            cgu_dir=cgu_dir, cvm_dir=tmp_path / "cvm", rfb_dir=rfb_dir,
            curated_dir=curated_dir, analytics_dir=analytics_dir, output_dir=output_dir,
        )
        elapsed = time.monotonic() - t0

        records = _read_jsonl(output_dir / "sanction_corporate_link.jsonl")
        summary = _read_summary(output_dir)

        print(f"  elapsed={elapsed:.3f}s records={len(records)} summary={json.dumps(summary, indent=2)}")

        assert summary["total_links"] > 0
        degree_counts = summary["degree_counts"]
        total_from_degrees = sum(degree_counts.values())
        assert total_from_degrees == summary["total_links"]


@pytest.mark.perf
class TestPathologicalTruncation:
    def test_pathological_truncation(self, tmp_path: Path) -> None:
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        output_dir = tmp_path / "output"

        _setup_sanctions(cgu_dir, [{"sanction_id": "s1", "entity_cnpj_cpf": SANCTION_CNPJ}])
        group_members = [f"{i:08d}" for i in range(6000)]
        group_members[0] = SANCTION_CNPJ_BASICO

        _setup_rfb(
            rfb_dir,
            partners=[
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "partner_type": "2",
                 "partner_cpf_cnpj": PARTY_CPF,
                 "partner_name_normalized": PARTY_NAME},
            ],
            companies=[
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "razao_social": "PATHO CO"},
            ],
        )
        _base_curated(curated_dir)
        _write_jsonl(analytics_dir / "economic_group.jsonl", [
            {
                "group_id": "eg-huge",
                "member_cnpjs": group_members,
                "member_count": len(group_members),
                "is_law_firm_group": False,
            },
        ])

        t0 = time.monotonic()
        build_sanction_corporate_links(
            cgu_dir=cgu_dir, cvm_dir=tmp_path / "cvm", rfb_dir=rfb_dir,
            curated_dir=curated_dir, analytics_dir=analytics_dir, output_dir=output_dir,
        )
        elapsed = time.monotonic() - t0

        records = _read_jsonl(output_dir / "sanction_corporate_link.jsonl")
        summary = _read_summary(output_dir)

        print(f"  elapsed={elapsed:.3f}s records={len(records)} summary={json.dumps(summary, indent=2)}")

        assert summary["truncated_sanctions_count"] >= 1
        for r in records:
            assert r["truncated"] is True
            assert r["estimated_degree3_count"] > 5000
            assert r["post_truncation_cnpj_count"] < r["pre_truncation_cnpj_count"]

        # Degree-3 members should NOT be materialized
        degree3 = [r for r in records if r["link_degree"] == 3]
        assert len(degree3) == 0


@pytest.mark.perf
class TestCacheEffectiveness:
    def test_cache_effectiveness(self, tmp_path: Path) -> None:
        cgu_dir = tmp_path / "cgu"
        rfb_dir = tmp_path / "rfb"
        curated_dir = tmp_path / "curated"
        analytics_dir = tmp_path / "analytics"
        output_dir = tmp_path / "output"

        # 50 sanctions sharing the same bridge company with 3 distinct partners
        sanctions = [
            {"sanction_id": f"s{i}", "entity_cnpj_cpf": SANCTION_CNPJ}
            for i in range(50)
        ]
        _setup_sanctions(cgu_dir, sanctions)
        _setup_rfb(
            rfb_dir,
            partners=[
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "partner_type": "2",
                 "partner_cpf_cnpj": PARTY_CPF,
                 "partner_name_normalized": PARTY_NAME},
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "partner_type": "2",
                 "partner_cpf_cnpj": "11111111111",
                 "partner_name_normalized": "MARIA SOUZA"},
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "partner_type": "2",
                 "partner_cpf_cnpj": "22222222222",
                 "partner_name_normalized": "PEDRO SANTOS"},
            ],
            companies=[
                {"cnpj_basico": SANCTION_CNPJ_BASICO, "razao_social": "SHARED CO"},
            ],
        )
        _base_curated(curated_dir)
        _write_jsonl(analytics_dir / "economic_group.jsonl", [])

        t0 = time.monotonic()
        build_sanction_corporate_links(
            cgu_dir=cgu_dir, cvm_dir=tmp_path / "cvm", rfb_dir=rfb_dir,
            curated_dir=curated_dir, analytics_dir=analytics_dir, output_dir=output_dir,
        )
        elapsed = time.monotonic() - t0

        summary = _read_summary(output_dir)

        print(f"  elapsed={elapsed:.3f}s summary={json.dumps(summary, indent=2)}")

        cache = summary["cache_stats"]
        assert cache["misses"] <= 3  # one miss per distinct partner
        assert cache["hits"] > 0  # proves cache reuse
