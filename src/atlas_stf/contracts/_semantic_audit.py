"""Semantic audit: classify known ambiguities and dangerous column uses.

Produces explicit classifications for dual-use columns, semantic mismatches,
fallback conflations, masked identifiers, and high-null join risks.  The
known issues are deliberately hardcoded — they represent reviewed knowledge,
not auto-discovery.  A supplementary dynamic check catches governance gaps.
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ._coverage import _read_inventory

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

_CATEGORIES = {"dual_use", "semantic_mismatch", "fallback_conflation", "masked_identifier", "high_null_join_risk"}
_STATUSES = {"safe", "degraded", "blocked", "requires_manual_review"}
_SEVERITIES = {"critical", "warning", "info"}


@dataclass(frozen=True)
class SemanticFinding:
    """A semantic ambiguity or dangerous column use."""

    finding_id: str
    source: str
    columns: list[str]
    category: str  # dual_use | semantic_mismatch | fallback_conflation | masked_identifier | high_null_join_risk
    status: str  # safe | degraded | blocked | requires_manual_review
    severity: str  # critical | warning | info
    description: str
    impact: str
    recommendation: str


# ---------------------------------------------------------------------------
# Reviewed knowledge — static findings
# ---------------------------------------------------------------------------

_KNOWN_ISSUES: list[dict[str, Any]] = [
    {
        "finding_id": "covid_tipo_decisao_dual_use",
        "source": "stf/decisoes_covid.csv",
        "columns": ["tipo_decisao"],
        "category": "dual_use",
        "status": "degraded",
        "severity": "warning",
        "description": (
            "tipo_decisao is mapped to BOTH decision_type AND decision_progress "
            "in build_decision_event.py col_map. Single source column populates "
            "two semantic fields."
        ),
        "impact": (
            "decision_progress for covid events is semantically identical to "
            "decision_type — loses the progression dimension that decisoes.csv "
            "captures via andamento_decisao."
        ),
        "recommendation": (
            "Accept as degraded: decision_progress for covid events should be "
            "treated as less informative than for decisoes.csv events. Flag in "
            "downstream analytics."
        ),
    },
    {
        "finding_id": "tse_sg_uf_vs_sg_ue",
        "source": "tse/donations_raw.jsonl",
        "columns": ["SG_UF", "SG_UE"],
        "category": "semantic_mismatch",
        "status": "degraded",
        "severity": "warning",
        "description": (
            "SG_UF (state abbreviation, 2 chars) and SG_UE (electoral unit, "
            "can be municipality code) are treated as equivalent aliases for "
            "'state' in the parser."
        ),
        "impact": (
            "For years using SG_UE (2004), the state field may contain "
            "municipality codes instead of state abbreviations, producing "
            "incorrect geographic attribution."
        ),
        "recommendation": (
            "Validate SG_UE values against known state abbreviations. Flag non-matching values as degraded."
        ),
    },
    {
        "finding_id": "tse_expense_amount_semantics",
        "source": "tse/campaign_expenses_raw.jsonl",
        "columns": ["VR_DESPESA", "VR_DESPESA_CONTRATADA"],
        "category": "semantic_mismatch",
        "status": "degraded",
        "severity": "warning",
        "description": (
            "VR_DESPESA (total expense, Gen1-Gen4) and VR_DESPESA_CONTRATADA "
            "(contracted expense, Gen6) are normalized to the same "
            "expense_amount field. Semantic difference: contracted vs realized."
        ),
        "impact": (
            "Cross-generation comparisons of expense_amount mix different "
            "accounting concepts. Sums across generations are not strictly "
            "comparable."
        ),
        "recommendation": (
            "Accept for magnitude estimation. Do not use for precise "
            "cross-generation comparisons without schema_generation annotation."
        ),
    },
    {
        "finding_id": "tse_donor_name_originator_fallback",
        "source": "tse/donations_raw.jsonl",
        "columns": ["donor_name", "donor_name_originator"],
        "category": "fallback_conflation",
        "status": "degraded",
        "severity": "warning",
        "description": (
            "When donor_name is empty, parser falls back to "
            "donor_name_originator. These have different provenance: originator "
            "is the actual source of funds (e.g., party committee), not the "
            "direct donor."
        ),
        "impact": (
            "Name-based matching may link to the wrong entity when the "
            "originator (e.g., party fund) is used as the donor name."
        ),
        "recommendation": (
            "In graph traversal, reduce match_score when donor_name was "
            "populated via originator fallback. Track provenance_field in "
            "matching."
        ),
    },
    {
        "finding_id": "rfb_partner_cpf_masked",
        "source": "rfb/partners_raw.jsonl",
        "columns": ["partner_cpf_cnpj"],
        "category": "masked_identifier",
        "status": "degraded",
        "severity": "warning",
        "description": (
            "RFB public data masks CPFs as ***NNNNNN** (only 6 central digits "
            "visible). CNPJs (14 digits) are NOT masked. Column appears "
            "deterministic but CPF portion is unusable for exact matching."
        ),
        "impact": (
            "Joins via partner_cpf_cnpj work for PJ (CNPJ) but fail for PF "
            "(CPF) due to masking. False negative rate is high for PF entities."
        ),
        "recommendation": (
            "Split matching strategy: CNPJ (14 digits, no mask) -> strict "
            "join. CPF (masked) -> blocked for deterministic join, fallback to "
            "name matching only."
        ),
    },
    {
        "finding_id": "rfb_representative_high_null",
        "source": "rfb/partners_raw.jsonl",
        "columns": ["representative_name", "representative_name_normalized"],
        "category": "high_null_join_risk",
        "status": "blocked",
        "severity": "critical",
        "description": (
            "representative_name is 94.5% empty. Column exists and might "
            "appear usable but is effectively null for the vast majority of "
            "records."
        ),
        "impact": (
            "Any join or filter using representative_name will miss 94.5% of "
            "records. Using it as a join key would create extreme selection "
            "bias."
        ),
        "recommendation": (
            "Blocked for any join role. Descriptive enrichment only when "
            "present. Governance already marks this as unusable."
        ),
    },
]


# ---------------------------------------------------------------------------
# Dynamic check — high-null columns with active join roles
# ---------------------------------------------------------------------------

_HIGH_NULL_THRESHOLD = 0.50
_PASSIVE_JOIN_ROLES = {"descriptive_only", "blocked"}


def _dynamic_high_null_findings(
    governance: dict[str, Any],
    observed_dir: Path,
) -> list[SemanticFinding]:
    """Flag columns with >50% null+empty that governance assigns an active join role."""
    # Build lookup: (source, observed_col) -> max(null_rate + empty_rate)
    null_rates: dict[tuple[str, str], float] = {}

    if observed_dir.is_dir():
        for path in sorted(observed_dir.rglob("*.json")):
            if path.name.startswith("_") or "by_year" in path.parts:
                continue
            inv = _read_inventory(path)
            if inv is None:
                continue
            source = inv.get("source", "")
            file_name = inv.get("file_name", "")
            key_prefix = f"{source}/{file_name}" if source else file_name
            for col in inv.get("columns", []):
                col_name = col.get("observed_column_name", "")
                rate = (col.get("null_rate") or 0.0) + (col.get("empty_rate") or 0.0)
                key = (key_prefix, col_name)
                null_rates[key] = max(null_rates.get(key, 0.0), rate)

    # Check governance canonical columns with active join roles
    known_ids = {f["finding_id"] for f in _KNOWN_ISSUES}
    findings: list[SemanticFinding] = []

    for canon in governance.get("canonical_columns", []):
        join_role = canon.get("join_role", "descriptive_only")
        if join_role in _PASSIVE_JOIN_ROLES:
            continue
        aliases: dict[str, Any] = canon.get("aliases", {})
        for source_file, col_ref in aliases.items():
            if col_ref is None:
                continue
            col_names = col_ref if isinstance(col_ref, list) else [col_ref]
            for col_name in col_names:
                rate = null_rates.get((source_file, col_name), 0.0)
                if rate <= _HIGH_NULL_THRESHOLD:
                    continue
                fid = f"dynamic_high_null__{source_file.replace('/', '_')}__{col_name}"
                if fid in known_ids:
                    continue
                findings.append(
                    SemanticFinding(
                        finding_id=fid,
                        source=source_file,
                        columns=[col_name],
                        category="high_null_join_risk",
                        status="requires_manual_review",
                        severity="warning",
                        description=(
                            f"{col_name} has null+empty rate {rate:.1%} but governance assigns join_role={join_role}."
                        ),
                        impact=(
                            f"Joins using {col_name} will miss a significant fraction "
                            f"of records, creating selection bias."
                        ),
                        recommendation=(
                            "Review whether join_role should be downgraded to "
                            "descriptive_only or blocked in column_governance.json."
                        ),
                    )
                )

    return findings


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_semantic_audit(
    governance_dir: Path,
    observed_dir: Path,
) -> dict[str, Any]:
    """Run all semantic audit checks and return a structured report.

    Parameters
    ----------
    governance_dir:
        Directory containing ``column_governance.json``.
    observed_dir:
        Directory containing per-source observed inventories.

    Returns
    -------
    dict ready for JSON serialisation.
    """
    gov_file = governance_dir / "column_governance.json"
    governance: dict[str, Any] = {}
    try:
        governance = json.loads(gov_file.read_text(encoding="utf-8"))
    except OSError, json.JSONDecodeError:
        pass

    # Static findings
    findings = [SemanticFinding(**issue) for issue in _KNOWN_ISSUES]

    # Dynamic findings
    findings.extend(_dynamic_high_null_findings(governance, observed_dir))

    # Build summary
    by_status: Counter[str] = Counter()
    by_severity: Counter[str] = Counter()
    blocked_columns: list[str] = []

    for f in findings:
        by_status[f.status] += 1
        by_severity[f.severity] += 1
        if f.status == "blocked":
            blocked_columns.extend(f.columns)

    # Coverage note: findings cover only known issues (static rules) plus
    # dynamic high-null detection. Columns without findings are NOT confirmed
    # safe — they are "unaudited" for semantic issues beyond null rates.
    total_gov_columns = len(governance.get("canonical_columns", []))
    audited_concepts = {f.finding_id.split("_")[0] for f in findings}

    return {
        "schema_version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "findings": [asdict(f) for f in findings],
        "summary": {
            "total_findings": len(findings),
            "by_status": {s: by_status.get(s, 0) for s in sorted(_STATUSES)},
            "by_severity": {s: by_severity.get(s, 0) for s in sorted(_SEVERITIES)},
            "blocked_columns": sorted(set(blocked_columns)),
            "coverage_note": (
                f"{len(findings)} findings from {len(audited_concepts)} source contexts "
                f"out of {total_gov_columns} governance columns. "
                "Columns without findings are unaudited for semantic issues, not confirmed safe."
            ),
        },
    }


def write_semantic_audit(report: dict[str, Any], output_path: Path) -> Path:
    """Serialise *report* to *output_path* (full file path)."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_path
