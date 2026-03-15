"""Pydantic schemas for representation network endpoints."""

from __future__ import annotations

from pydantic import BaseModel


class LawyerEntityItem(BaseModel):
    lawyer_id: str
    lawyer_name_raw: str
    lawyer_name_normalized: str | None = None
    canonical_name_normalized: str | None = None
    oab_number: str | None = None
    oab_state: str | None = None
    oab_status: str | None = None
    oab_source: str | None = None
    identity_key: str | None = None
    identity_strategy: str | None = None
    firm_id: str | None = None
    process_count: int = 0
    event_count: int = 0
    first_seen_date: str | None = None
    last_seen_date: str | None = None


class LawFirmEntityItem(BaseModel):
    firm_id: str
    firm_name_raw: str
    firm_name_normalized: str | None = None
    canonical_name_normalized: str | None = None
    cnpj: str | None = None
    cnpj_valid: bool | None = None
    cnsa_number: str | None = None
    identity_key: str | None = None
    identity_strategy: str | None = None
    member_count: int = 0
    process_count: int = 0
    first_seen_date: str | None = None
    last_seen_date: str | None = None


class RepresentationEdgeItem(BaseModel):
    edge_id: str
    process_id: str
    representative_entity_id: str
    representative_kind: str | None = None
    role_type: str | None = None
    lawyer_id: str | None = None
    firm_id: str | None = None
    party_id: str | None = None
    event_count: int = 0
    start_date: str | None = None
    end_date: str | None = None
    confidence: float | None = None


class RepresentationEventItem(BaseModel):
    event_id: str
    process_id: str
    edge_id: str | None = None
    lawyer_id: str | None = None
    firm_id: str | None = None
    event_type: str | None = None
    event_date: str | None = None
    event_description: str | None = None
    protocol_number: str | None = None
    document_type: str | None = None
    source_system: str | None = None
    confidence: float | None = None


class PaginatedLawyersResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[LawyerEntityItem]


class PaginatedFirmsResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[LawFirmEntityItem]


class LawyerDetailResponse(BaseModel):
    lawyer: LawyerEntityItem
    edges: list[RepresentationEdgeItem] = []
    events: list[RepresentationEventItem] = []


class FirmDetailResponse(BaseModel):
    firm: LawFirmEntityItem
    lawyers: list[LawyerEntityItem] = []


class ProcessRepresentationResponse(BaseModel):
    process_id: str
    edges: list[RepresentationEdgeItem] = []
    events: list[RepresentationEventItem] = []


class RepresentationNetworkSummary(BaseModel):
    total_lawyers: int = 0
    total_firms: int = 0
    total_edges: int = 0
    total_events: int = 0
    lawyers_with_oab: int = 0
    lawyers_with_firm: int = 0
