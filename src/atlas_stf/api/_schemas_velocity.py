"""Pydantic schemas for decision velocity, rapporteur change, and counsel network endpoints."""

from __future__ import annotations

from pydantic import BaseModel

# --- Decision Velocity ---


class DecisionVelocityItem(BaseModel):
    velocity_id: str
    decision_event_id: str
    process_id: str
    current_rapporteur: str | None = None
    decision_date: str | None = None
    filing_date: str | None = None
    days_to_decision: int
    process_class: str | None = None
    thematic_key: str | None = None
    decision_year: int | None = None
    group_size: int | None = None
    p5_days: float | None = None
    p10_days: float | None = None
    median_days: float | None = None
    p90_days: float | None = None
    p95_days: float | None = None
    velocity_flag: str | None = None
    velocity_z_score: float | None = None


class PaginatedDecisionVelocityResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[DecisionVelocityItem]


class DecisionVelocityRedFlagsResponse(BaseModel):
    items: list[DecisionVelocityItem]
    total: int


# --- Rapporteur Change ---


class RapporteurChangeItem(BaseModel):
    change_id: str
    process_id: str
    process_class: str | None = None
    previous_rapporteur: str
    new_rapporteur: str
    change_date: str | None = None
    decision_event_id: str | None = None
    post_change_decision_count: int
    post_change_favorable_rate: float | None = None
    new_rapporteur_baseline_rate: float | None = None
    delta_vs_baseline: float | None = None
    red_flag: bool


class PaginatedRapporteurChangeResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[RapporteurChangeItem]


class RapporteurChangeRedFlagsResponse(BaseModel):
    items: list[RapporteurChangeItem]
    total: int


# --- Counsel Network Cluster ---


class CounselNetworkClusterItem(BaseModel):
    cluster_id: str
    counsel_ids: list[str] = []
    counsel_names: list[str] = []
    cluster_size: int
    shared_client_count: int
    shared_process_count: int
    minister_names: list[str] = []
    cluster_favorable_rate: float | None = None
    cluster_case_count: int
    red_flag: bool


class PaginatedCounselNetworkResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[CounselNetworkClusterItem]


class CounselNetworkRedFlagsResponse(BaseModel):
    items: list[CounselNetworkClusterItem]
    total: int
