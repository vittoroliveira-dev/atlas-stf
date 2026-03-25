"""Build economic group analytics using Union-Find on corporate ownership chains."""

from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.identity import stable_id
from ._atomic_io import AtomicJsonlWriter
from ._match_helpers import read_jsonl

logger = logging.getLogger(__name__)


class _UnionFind:
    """Weighted Union-Find with path compression."""

    def __init__(self) -> None:
        self._parent: dict[str, str] = {}
        self._rank: dict[str, int] = {}

    def make_set(self, x: str) -> None:
        if x not in self._parent:
            self._parent[x] = x
            self._rank[x] = 0

    def find(self, x: str) -> str:
        root = x
        while self._parent[root] != root:
            root = self._parent[root]
        # Path compression
        while self._parent[x] != root:
            self._parent[x], x = root, self._parent[x]
        return root

    def union(self, x: str, y: str) -> None:
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return
        if self._rank[rx] < self._rank[ry]:
            rx, ry = ry, rx
        self._parent[ry] = rx
        if self._rank[rx] == self._rank[ry]:
            self._rank[rx] += 1

    def components(self) -> dict[str, list[str]]:
        groups: dict[str, list[str]] = defaultdict(list)
        for x in self._parent:
            groups[self.find(x)].append(x)
        return dict(groups)


def _derive_pj_partner_basico(
    raw_cpf_cnpj: str,
    *,
    discard_counter: dict[str, int],
) -> str | None:
    """Extract cnpj_basico (8 digits) from a PJ partner's CPF/CNPJ field."""
    digits = re.sub(r"\D", "", raw_cpf_cnpj)
    if len(digits) == 14:
        return digits[:8]
    if len(digits) == 8:
        return digits
    discard_counter["invalid_pj_cpf_cnpj_length"] = discard_counter.get("invalid_pj_cpf_cnpj_length", 0) + 1
    logger.debug(
        "Discarding PJ partner with invalid cpf_cnpj length: %d digits",
        len(digits),
    )
    return None


def build_economic_groups(
    *,
    rfb_dir: Path = Path("data/raw/rfb"),
    output_dir: Path = Path("data/analytics"),
    minister_bio_path: Path = Path("data/curated/minister_bio.json"),
    party_path: Path = Path("data/curated/party.jsonl"),
    counsel_path: Path = Path("data/curated/counsel.jsonl"),
) -> Path:
    """Build economic group analytics from RFB partner data using Union-Find."""
    output_dir.mkdir(parents=True, exist_ok=True)

    partners_path = rfb_dir / "partners_raw.jsonl"
    companies_path = rfb_dir / "companies_raw.jsonl"
    establishments_path = rfb_dir / "establishments_raw.jsonl"

    if not partners_path.exists():
        logger.warning("No partners_raw.jsonl found in %s", rfb_dir)
        return output_dir

    # Load all data
    partners: list[dict[str, Any]] = list(read_jsonl(partners_path))
    companies_by_cnpj: dict[str, dict[str, Any]] = {}
    if companies_path.exists():
        for record in read_jsonl(companies_path):
            cnpj = record.get("cnpj_basico", "")
            if cnpj:
                companies_by_cnpj.setdefault(cnpj, record)

    estab_by_cnpj: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if establishments_path.exists():
        for record in read_jsonl(establishments_path):
            cnpj = record.get("cnpj_basico", "")
            if cnpj:
                estab_by_cnpj[cnpj].append(record)

    # Build partner indexes
    partners_by_cnpj: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for p in partners:
        cnpj = p.get("cnpj_basico", "")
        if cnpj:
            partners_by_cnpj[cnpj].append(p)

    # Step 1: Initialize Union-Find
    uf = _UnionFind()
    all_cnpjs: set[str] = set()
    for cnpj in partners_by_cnpj:
        uf.make_set(cnpj)
        all_cnpjs.add(cnpj)
    for cnpj in companies_by_cnpj:
        uf.make_set(cnpj)
        all_cnpjs.add(cnpj)

    # Step 2: Union PJ partners with their companies
    discard_counter: dict[str, int] = {}
    for cnpj, partner_list in partners_by_cnpj.items():
        for p in partner_list:
            if p.get("partner_type") != "1":
                continue
            pj_cpf_cnpj = p.get("partner_cpf_cnpj", "").strip()
            if not pj_cpf_cnpj:
                continue
            pj_basico = _derive_pj_partner_basico(pj_cpf_cnpj, discard_counter=discard_counter)
            if pj_basico is None:
                continue
            uf.make_set(pj_basico)
            uf.union(cnpj, pj_basico)

    if discard_counter.get("invalid_pj_cpf_cnpj_length", 0) > 0:
        logger.info(
            "PJ normalization: %d records discarded due to invalid cpf_cnpj length",
            discard_counter["invalid_pj_cpf_cnpj_length"],
        )

    # Step 3: Extract connected components
    components = uf.components()

    # Load minister/party/counsel names for flag detection
    minister_norms: set[str] = set()
    if minister_bio_path.exists():
        from ..core.identity import normalize_entity_name

        bio_data = json.loads(minister_bio_path.read_text(encoding="utf-8"))
        for entry in bio_data.values():
            for name_field in ("minister_name", "civil_name"):
                name = entry.get(name_field, "")
                if name:
                    norm = normalize_entity_name(name)
                    if norm:
                        minister_norms.add(norm)

    party_norms: set[str] = set()
    if party_path.exists():
        from ..core.identity import normalize_entity_name

        for record in read_jsonl(party_path):
            norm = record.get("party_name_normalized", "")
            if norm:
                party_norms.add(norm)

    counsel_norms: set[str] = set()
    if counsel_path.exists():
        from ..core.identity import normalize_entity_name

        for record in read_jsonl(counsel_path):
            norm = record.get("counsel_name_normalized", "")
            if norm:
                counsel_norms.add(norm)

    now_iso = datetime.now(timezone.utc).isoformat()
    groups: list[dict[str, Any]] = []

    for _root, member_cnpjs in components.items():
        member_cnpjs_sorted = sorted(member_cnpjs)

        # Aggregate data from companies
        razoes_sociais: list[str] = []
        total_capital = 0.0
        cnae_labels: list[str] = []
        for cnpj in member_cnpjs_sorted:
            company = companies_by_cnpj.get(cnpj, {})
            razao = company.get("razao_social", "")
            if razao:
                razoes_sociais.append(razao)
            capital = company.get("capital_social", 0.0)
            if isinstance(capital, (int, float)):
                total_capital += capital
            cnae_label = company.get("cnae_fiscal_label") or ""
            if cnae_label and cnae_label not in cnae_labels:
                cnae_labels.append(cnae_label)

        # Aggregate establishment data
        ufs: list[str] = []
        active_count = 0
        total_estab_count = 0
        law_firm_cnpjs: set[str] = set()
        for cnpj in member_cnpjs_sorted:
            for estab in estab_by_cnpj.get(cnpj, []):
                total_estab_count += 1
                if estab.get("situacao_cadastral") == "02":
                    active_count += 1
                uf = estab.get("uf", "")
                if uf and uf not in ufs:
                    ufs.append(uf)
                cnae = estab.get("cnae_fiscal", "")
                if cnae.startswith("6911"):
                    law_firm_cnpjs.add(cnpj)
        law_firm_member_count = len(law_firm_cnpjs)
        law_firm_member_ratio = (
            round(law_firm_member_count / len(member_cnpjs_sorted), 4)
            if member_cnpjs_sorted else 0.0
        )
        is_law_firm = law_firm_member_ratio > 0.5

        # Check partner flags
        has_minister = False
        has_party = False
        has_counsel = False
        for cnpj in member_cnpjs_sorted:
            for p in partners_by_cnpj.get(cnpj, []):
                pnorm = p.get("partner_name_normalized", "")
                rnorm = p.get("representative_name_normalized", "")
                for norm in (pnorm, rnorm):
                    if not norm:
                        continue
                    if norm in minister_norms:
                        has_minister = True
                    if norm in party_norms:
                        has_party = True
                    if norm in counsel_norms:
                        has_counsel = True

        group_id = stable_id("eg-", ",".join(member_cnpjs_sorted))

        if len(member_cnpjs_sorted) > 200:
            logger.warning(
                "Large economic group detected: %d members (group_id=%s)",
                len(member_cnpjs_sorted),
                group_id,
            )

        groups.append(
            {
                "group_id": group_id,
                "member_cnpjs": member_cnpjs_sorted,
                "razoes_sociais": razoes_sociais,
                "member_count": len(member_cnpjs_sorted),
                "total_capital_social": total_capital,
                "cnae_labels": cnae_labels,
                "ufs": ufs,
                "active_establishment_count": active_count,
                "total_establishment_count": total_estab_count,
                "is_law_firm_group": is_law_firm,
                "law_firm_member_count": law_firm_member_count,
                "law_firm_member_ratio": law_firm_member_ratio,
                "has_minister_partner": has_minister,
                "has_party_partner": has_party,
                "has_counsel_partner": has_counsel,
                "generated_at": now_iso,
            }
        )

    # Write groups
    output_path = output_dir / "economic_group.jsonl"
    with AtomicJsonlWriter(output_path) as fh:
        for g in groups:
            fh.write(json.dumps(g, ensure_ascii=False) + "\n")

    # Write summary
    multi_member = [g for g in groups if g["member_count"] > 1]
    summary: dict[str, Any] = {
        "total_groups": len(groups),
        "singleton_count": sum(1 for g in groups if g["member_count"] == 1),
        "multi_member_count": len(multi_member),
        "avg_member_count": (sum(g["member_count"] for g in multi_member) / len(multi_member) if multi_member else 0),
        "max_member_count": max((g["member_count"] for g in groups), default=0),
        "law_firm_groups": sum(1 for g in groups if g["is_law_firm_group"]),
        "generated_at": now_iso,
    }
    summary_path = output_dir / "economic_group_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info(
        "Built economic groups: %d total (%d multi-member, %d law firm groups)",
        len(groups),
        len(multi_member),
        summary["law_firm_groups"],
    )
    return output_path
