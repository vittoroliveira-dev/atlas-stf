from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime, Float, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from .models import Base


class ServingLawyerEntity(Base):
    __tablename__ = "serving_lawyer_entity"

    lawyer_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    lawyer_name_raw: Mapped[str] = mapped_column(Text())
    lawyer_name_normalized: Mapped[str | None] = mapped_column(String(512), index=True)
    canonical_name_normalized: Mapped[str | None] = mapped_column(String(512))
    oab_number: Mapped[str | None] = mapped_column(String(16), index=True)
    oab_state: Mapped[str | None] = mapped_column(String(2), index=True)
    oab_status: Mapped[str | None] = mapped_column(String(16))
    oab_source: Mapped[str | None] = mapped_column(String(32))
    oab_validation_method: Mapped[str | None] = mapped_column(String(16))
    oab_last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    entity_tax_id: Mapped[str | None] = mapped_column(String(20))
    identity_key: Mapped[str | None] = mapped_column(String(256), index=True)
    identity_strategy: Mapped[str | None] = mapped_column(String(16))
    source_systems_json: Mapped[str | None] = mapped_column(Text())
    firm_id: Mapped[str | None] = mapped_column(String(64), index=True)
    notes: Mapped[str | None] = mapped_column(Text())
    process_count: Mapped[int] = mapped_column(Integer, default=0)
    event_count: Mapped[int] = mapped_column(Integer, default=0)
    first_seen_date: Mapped[date | None] = mapped_column(Date)
    last_seen_date: Mapped[date | None] = mapped_column(Date)


class ServingLawFirmEntity(Base):
    __tablename__ = "serving_law_firm_entity"

    firm_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    firm_name_raw: Mapped[str] = mapped_column(Text())
    firm_name_normalized: Mapped[str | None] = mapped_column(String(512), index=True)
    canonical_name_normalized: Mapped[str | None] = mapped_column(String(512))
    cnpj: Mapped[str | None] = mapped_column(String(20), index=True)
    cnpj_valid: Mapped[bool | None] = mapped_column(Boolean)
    cnsa_number: Mapped[str | None] = mapped_column(String(16), index=True)
    identity_key: Mapped[str | None] = mapped_column(String(256), index=True)
    identity_strategy: Mapped[str | None] = mapped_column(String(16))
    source_systems_json: Mapped[str | None] = mapped_column(Text())
    member_lawyer_ids_json: Mapped[str | None] = mapped_column(Text())
    member_count: Mapped[int] = mapped_column(Integer, default=0)
    process_count: Mapped[int] = mapped_column(Integer, default=0)
    first_seen_date: Mapped[date | None] = mapped_column(Date)
    last_seen_date: Mapped[date | None] = mapped_column(Date)
    oab_sp_firm_name: Mapped[str | None] = mapped_column(Text())
    address: Mapped[str | None] = mapped_column(Text())
    neighborhood: Mapped[str | None] = mapped_column(String(256))
    zip_code: Mapped[str | None] = mapped_column(String(16))
    city: Mapped[str | None] = mapped_column(String(256))
    state: Mapped[str | None] = mapped_column(String(2))
    email: Mapped[str | None] = mapped_column(String(256))
    phone: Mapped[str | None] = mapped_column(String(64))
    society_type: Mapped[str | None] = mapped_column(String(32))


class ServingProcessLawyer(Base):
    __tablename__ = "serving_process_lawyer"

    link_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    process_id: Mapped[str] = mapped_column(String(64), index=True)
    lawyer_id: Mapped[str] = mapped_column(String(64), index=True)
    side_in_case: Mapped[str | None] = mapped_column(String(128), index=True)
    source_id: Mapped[str | None] = mapped_column(String(128))


class ServingRepresentationEdge(Base):
    __tablename__ = "serving_representation_edge"

    edge_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    process_id: Mapped[str] = mapped_column(String(64), index=True)
    representative_entity_id: Mapped[str] = mapped_column(String(64), index=True)
    representative_kind: Mapped[str | None] = mapped_column(String(16), index=True)
    role_type: Mapped[str | None] = mapped_column(String(64))
    lawyer_id: Mapped[str | None] = mapped_column(String(64), index=True)
    firm_id: Mapped[str | None] = mapped_column(String(64), index=True)
    party_id: Mapped[str | None] = mapped_column(String(64), index=True)
    event_count: Mapped[int] = mapped_column(Integer, default=0)
    start_date: Mapped[date | None] = mapped_column(Date)
    end_date: Mapped[date | None] = mapped_column(Date)
    confidence: Mapped[float | None] = mapped_column(Float)
    source_systems_json: Mapped[str | None] = mapped_column(Text())


class ServingRepresentationEvent(Base):
    __tablename__ = "serving_representation_event"

    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    process_id: Mapped[str] = mapped_column(String(64), index=True)
    edge_id: Mapped[str | None] = mapped_column(String(64), index=True)
    lawyer_id: Mapped[str | None] = mapped_column(String(64), index=True)
    firm_id: Mapped[str | None] = mapped_column(String(64), index=True)
    event_type: Mapped[str | None] = mapped_column(String(32), index=True)
    event_date: Mapped[date | None] = mapped_column(Date, index=True)
    event_description: Mapped[str | None] = mapped_column(Text())
    protocol_number: Mapped[str | None] = mapped_column(String(64))
    document_type: Mapped[str | None] = mapped_column(String(128))
    source_system: Mapped[str | None] = mapped_column(String(32))
    source_url: Mapped[str | None] = mapped_column(Text())
    confidence: Mapped[float | None] = mapped_column(Float)
