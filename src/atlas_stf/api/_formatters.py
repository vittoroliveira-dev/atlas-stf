from __future__ import annotations

import json
from datetime import date
from math import isfinite

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..serving.models import ServingAlert, ServingCase, ServingMetric, ServingSourceAudit
from .schemas import AlertSummaryItem, CaseSummaryItem, SourceAuditItem


def _collegiate_label(value: bool | None) -> str:
    if value is True:
        return "Colegial"
    if value is False:
        return "Monocrático"
    return "INCERTO"


def _format_date(value: date | None) -> str:
    return value.isoformat() if value else "INCERTO"


def _note_snippet(value: str | None) -> str:
    if not value:
        return "Sem nota decisória materializada."
    compact = " ".join(value.split())
    return compact[:220] + ("…" if len(compact) > 220 else "")


def _case_to_summary(case: ServingCase) -> CaseSummaryItem:
    return CaseSummaryItem(
        process_id=case.process_id,
        process_number=case.process_number or "INCERTO",
        process_class=case.process_class or "INCERTO",
        decision_event_id=case.decision_event_id,
        decision_date=_format_date(case.decision_date),
        decision_type=case.decision_type or "INCERTO",
        decision_progress=case.decision_progress or "INCERTO",
        judging_body=case.judging_body or "INCERTO",
        collegiate_label=_collegiate_label(case.is_collegiate),
        branch_of_law=case.branch_of_law or "INCERTO",
        first_subject=case.thematic_key or "INCERTO",
        inteiro_teor_url=case.inteiro_teor_url,
        doc_count_label=str(case.juris_doc_count),
        acordao_label="Com acórdão" if case.juris_has_acordao else "Sem acórdão",
        monocratic_decision_label=(
            "Com decisão monocrática" if case.juris_has_decisao_monocratica else "Sem decisão monocrática"
        ),
        origin_description=case.origin_description or "INCERTO",
        decision_note_snippet=_note_snippet(case.decision_note),
    )


def _alert_to_summary(
    alert: ServingAlert,
    case: ServingCase,
    *,
    ensemble_score: float | None = None,
) -> AlertSummaryItem:
    case_item = _case_to_summary(case)
    return AlertSummaryItem(
        alert_id=alert.alert_id,
        process_id=alert.process_id,
        decision_event_id=alert.decision_event_id,
        comparison_group_id=alert.comparison_group_id,
        alert_type=alert.alert_type,
        alert_score=alert.alert_score,
        ensemble_score=ensemble_score,
        expected_pattern=alert.expected_pattern,
        observed_pattern=alert.observed_pattern,
        evidence_summary=alert.evidence_summary,
        uncertainty_note=alert.uncertainty_note,
        status=alert.status,
        risk_signal_count=alert.risk_signal_count,
        risk_signals=json.loads(alert.risk_signals_json) if alert.risk_signals_json else [],
        created_at=alert.created_at,
        updated_at=alert.updated_at,
        process_number=case_item.process_number,
        process_class=case_item.process_class,
        decision_date=case_item.decision_date,
        decision_type=case_item.decision_type,
        decision_progress=case_item.decision_progress,
        judging_body=case_item.judging_body,
        collegiate_label=case_item.collegiate_label,
        inteiro_teor_url=case_item.inteiro_teor_url,
        doc_count_label=case_item.doc_count_label,
        acordao_label=case_item.acordao_label,
        monocratic_decision_label=case_item.monocratic_decision_label,
        branch_of_law=case_item.branch_of_law,
        first_subject=case_item.first_subject,
        origin_description=case_item.origin_description,
        decision_note_snippet=case_item.decision_note_snippet,
    )


def _source_files(session: Session) -> list[SourceAuditItem]:
    stmt = select(ServingSourceAudit).order_by(
        ServingSourceAudit.category,
        ServingSourceAudit.label,
    )
    rows = session.scalars(stmt).all()
    return [
        SourceAuditItem(
            label=row.label,
            category=row.category,
            path=row.relative_path,
            checksum=row.checksum,
            updated_at=row.updated_at,
        )
        for row in rows
    ]


def _metrics(session: Session) -> dict[str, int | float]:
    result: dict[str, int | float] = {}
    rows = session.scalars(select(ServingMetric)).all()
    for row in rows:
        if row.value_integer is not None:
            result[row.key] = row.value_integer
        elif row.value_float is not None and isfinite(row.value_float):
            result[row.key] = row.value_float
    return result
