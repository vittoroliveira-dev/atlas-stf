from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime, Float, Index, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class ServingCase(Base):
    __tablename__ = "serving_case"

    decision_event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    process_id: Mapped[str] = mapped_column(String(64), index=True)
    process_number: Mapped[str | None] = mapped_column(String(128), index=True)
    process_class: Mapped[str | None] = mapped_column(String(64), index=True)
    branch_of_law: Mapped[str | None] = mapped_column(String(256))
    thematic_key: Mapped[str | None] = mapped_column(String(256), index=True)
    origin_description: Mapped[str | None] = mapped_column(String(256))
    inteiro_teor_url: Mapped[str | None] = mapped_column(Text())
    acompanhamento_url: Mapped[str | None] = mapped_column(Text())
    juris_doc_count: Mapped[int] = mapped_column(Integer, default=0)
    juris_has_acordao: Mapped[bool] = mapped_column(Boolean, default=False)
    juris_has_decisao_monocratica: Mapped[bool] = mapped_column(Boolean, default=False)
    decision_date: Mapped[date | None] = mapped_column(Date, index=True)
    period: Mapped[str | None] = mapped_column(String(7), index=True)
    current_rapporteur: Mapped[str | None] = mapped_column(String(256), index=True)
    decision_type: Mapped[str | None] = mapped_column(String(256), index=True)
    decision_progress: Mapped[str | None] = mapped_column(String(256), index=True)
    decision_origin: Mapped[str | None] = mapped_column(String(256))
    judging_body: Mapped[str | None] = mapped_column(String(256), index=True)
    is_collegiate: Mapped[bool | None] = mapped_column(Boolean, index=True)
    decision_note: Mapped[str | None] = mapped_column(Text())
    first_distribution_date: Mapped[str | None] = mapped_column(String(10), nullable=True)


Index(
    "ix_serving_case_filter_bundle",
    ServingCase.current_rapporteur,
    ServingCase.period,
    ServingCase.is_collegiate,
    ServingCase.judging_body,
    ServingCase.process_class,
)


class ServingAlert(Base):
    __tablename__ = "serving_alert"

    alert_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    process_id: Mapped[str] = mapped_column(String(64), index=True)
    decision_event_id: Mapped[str] = mapped_column(String(64), index=True)
    comparison_group_id: Mapped[str] = mapped_column(String(64), index=True)
    alert_type: Mapped[str] = mapped_column(String(64), index=True)
    alert_score: Mapped[float] = mapped_column(Float, index=True)
    expected_pattern: Mapped[str] = mapped_column(Text())
    observed_pattern: Mapped[str] = mapped_column(Text())
    evidence_summary: Mapped[str] = mapped_column(Text())
    uncertainty_note: Mapped[str | None] = mapped_column(Text())
    status: Mapped[str] = mapped_column(String(32), index=True)
    risk_signal_count: Mapped[int] = mapped_column(Integer, default=0)
    risk_signals_json: Mapped[str | None] = mapped_column(Text())
    created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ServingCounsel(Base):
    __tablename__ = "serving_counsel"

    counsel_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    counsel_name_raw: Mapped[str] = mapped_column(Text())
    counsel_name_normalized: Mapped[str] = mapped_column(String(512), index=True)
    notes: Mapped[str | None] = mapped_column(String(128))


class ServingProcessCounsel(Base):
    __tablename__ = "serving_process_counsel"

    link_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    process_id: Mapped[str] = mapped_column(String(64), index=True)
    counsel_id: Mapped[str] = mapped_column(String(64), index=True)
    side_in_case: Mapped[str | None] = mapped_column(String(128), index=True)
    source_id: Mapped[str | None] = mapped_column(String(128))


class ServingParty(Base):
    __tablename__ = "serving_party"

    party_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    party_name_raw: Mapped[str] = mapped_column(Text())
    party_name_normalized: Mapped[str] = mapped_column(String(512), index=True)
    notes: Mapped[str | None] = mapped_column(String(128))


class ServingProcessParty(Base):
    __tablename__ = "serving_process_party"

    link_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    process_id: Mapped[str] = mapped_column(String(64), index=True)
    party_id: Mapped[str] = mapped_column(String(64), index=True)
    role_in_case: Mapped[str | None] = mapped_column(String(128), index=True)
    source_id: Mapped[str | None] = mapped_column(String(128))


class ServingSourceAudit(Base):
    __tablename__ = "serving_source_audit"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    label: Mapped[str] = mapped_column(String(64), index=True)
    category: Mapped[str] = mapped_column(String(32), index=True)
    relative_path: Mapped[str] = mapped_column(String(512), unique=True)
    checksum: Mapped[str] = mapped_column(String(64))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ServingMetric(Base):
    __tablename__ = "serving_metric"

    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value_integer: Mapped[int | None] = mapped_column(Integer)
    value_float: Mapped[float | None] = mapped_column(Float)
    value_text: Mapped[str | None] = mapped_column(String(256))


class ServingSchemaMeta(Base):
    __tablename__ = "serving_schema_meta"

    singleton_key: Mapped[str] = mapped_column(String(32), primary_key=True, default="serving")
    schema_version: Mapped[int] = mapped_column(Integer)
    schema_fingerprint: Mapped[str] = mapped_column(String(64))
    built_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


# Re-export analytics and timeline models for backward compatibility.
# All existing imports from .models continue to work unchanged.
from ._models_analytics import *  # noqa: E402, F401, F403
from ._models_timeline import *  # noqa: E402, F401, F403
