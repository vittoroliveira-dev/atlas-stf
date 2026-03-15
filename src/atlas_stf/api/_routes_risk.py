from __future__ import annotations

from typing import Annotated

from fastapi import FastAPI, HTTPException, Query

from .schemas import (
    CompoundRiskHeatmapResponse,
    CompoundRiskRedFlagsResponse,
    CorporateConflictItem,
    CorporateConflictRedFlagsResponse,
    CounselAffinityItem,
    CounselAffinityRedFlagsResponse,
    CounselDonationProfileItem,
    CounselNetworkRedFlagsResponse,
    CounselSanctionProfileItem,
    DecisionVelocityRedFlagsResponse,
    DonationMatchItem,
    DonationRedFlagsResponse,
    EconomicGroupItem,
    PaginatedCompoundRiskResponse,
    PaginatedCorporateConflictsResponse,
    PaginatedCounselAffinityResponse,
    PaginatedCounselNetworkResponse,
    PaginatedDecisionVelocityResponse,
    PaginatedDonationsResponse,
    PaginatedEconomicGroupResponse,
    PaginatedRapporteurChangeResponse,
    PaginatedSanctionsResponse,
    RapporteurChangeRedFlagsResponse,
    SanctionMatchItem,
    SanctionRedFlagsResponse,
)

PositiveInt = Annotated[int, Query(ge=1)]
PageSize = Annotated[int, Query(ge=1, le=100)]
RedFlagLimit = Annotated[int, Query(ge=1, le=100)]
CompoundRiskLimit = Annotated[int, Query(ge=1, le=500)]
ListLimit = Annotated[int, Query(ge=1, le=500)]


def register_risk_routes(
    app: FastAPI,
    factory: ...,
    build_filters: ...,
    get_base_filters: ...,
) -> None:
    @app.get("/sanctions", response_model=PaginatedSanctionsResponse)
    def sanctions(
        page: PositiveInt = 1,
        page_size: PageSize = 20,
        source: str | None = Query(default=None, pattern="^(ceis|cnep|cvm|leniencia)$"),
        red_flag_only: bool = Query(default=False),
        entity_type: str | None = Query(default=None, pattern="^(party|counsel)$"),
    ) -> PaginatedSanctionsResponse:
        from ._sanctions import get_sanctions as _get_sanctions

        with factory() as session:
            return _get_sanctions(
                session, page, page_size, source=source, red_flag_only=red_flag_only, entity_type=entity_type
            )

    @app.get("/sanctions/red-flags", response_model=SanctionRedFlagsResponse)
    def sanction_red_flags(limit: RedFlagLimit = 100) -> SanctionRedFlagsResponse:
        from ._sanctions import get_sanction_red_flags

        with factory() as session:
            return get_sanction_red_flags(session, limit=limit)

    @app.get("/parties/{party_id}/sanctions", response_model=list[SanctionMatchItem])
    def party_sanctions(party_id: str, limit: ListLimit = 100) -> list[SanctionMatchItem]:
        from ._sanctions import get_party_sanctions

        with factory() as session:
            return get_party_sanctions(session, party_id, limit=limit)

    @app.get("/counsels/{counsel_id}/sanction-profile", response_model=CounselSanctionProfileItem)
    def counsel_sanction_profile(counsel_id: str) -> CounselSanctionProfileItem:
        from ._sanctions import get_counsel_sanction_profile

        with factory() as session:
            result = get_counsel_sanction_profile(session, counsel_id)
        if result is None:
            raise HTTPException(status_code=404, detail="counsel_sanction_profile_not_found")
        return result

    @app.get("/donations", response_model=PaginatedDonationsResponse)
    def donations(
        page: PositiveInt = 1,
        page_size: PageSize = 20,
        red_flag_only: bool = Query(default=False),
        entity_type: str | None = Query(default=None, pattern="^(party|counsel)$"),
    ) -> PaginatedDonationsResponse:
        from ._donations import get_donations as _get_donations

        with factory() as session:
            return _get_donations(session, page, page_size, red_flag_only=red_flag_only, entity_type=entity_type)

    @app.get("/donations/red-flags", response_model=DonationRedFlagsResponse)
    def donation_red_flags(limit: RedFlagLimit = 100) -> DonationRedFlagsResponse:
        from ._donations import get_donation_red_flags

        with factory() as session:
            return get_donation_red_flags(session, limit=limit)

    @app.get("/parties/{party_id}/donations", response_model=list[DonationMatchItem])
    def party_donations(party_id: str, limit: ListLimit = 100) -> list[DonationMatchItem]:
        from ._donations import get_party_donations

        with factory() as session:
            return get_party_donations(session, party_id, limit=limit)

    @app.get("/counsels/{counsel_id}/donation-profile", response_model=CounselDonationProfileItem)
    def counsel_donation_profile(counsel_id: str) -> CounselDonationProfileItem:
        from ._donations import get_counsel_donation_profile

        with factory() as session:
            result = get_counsel_donation_profile(session, counsel_id)
        if result is None:
            raise HTTPException(status_code=404, detail="counsel_donation_profile_not_found")
        return result

    @app.get("/corporate-network", response_model=PaginatedCorporateConflictsResponse)
    def corporate_network(
        page: PositiveInt = 1,
        page_size: PageSize = 20,
        minister: str | None = Query(default=None),
        red_flag_only: bool = Query(default=False),
        link_degree: int | None = Query(default=None, ge=1),
    ) -> PaginatedCorporateConflictsResponse:
        from ._corporate_network import get_corporate_conflicts

        with factory() as session:
            return get_corporate_conflicts(
                session,
                page,
                page_size,
                minister=minister,
                red_flag_only=red_flag_only,
                link_degree=link_degree,
            )

    @app.get("/corporate-network/red-flags", response_model=CorporateConflictRedFlagsResponse)
    def corporate_network_red_flags(limit: RedFlagLimit = 100) -> CorporateConflictRedFlagsResponse:
        from ._corporate_network import get_corporate_conflict_red_flags

        with factory() as session:
            return get_corporate_conflict_red_flags(session, limit=limit)

    @app.get("/ministers/{minister}/corporate-conflicts", response_model=list[CorporateConflictItem])
    def minister_corporate_conflicts(minister: str, limit: ListLimit = 100) -> list[CorporateConflictItem]:
        from ._corporate_network import get_minister_corporate_conflicts

        with factory() as session:
            return get_minister_corporate_conflicts(session, minister, limit=limit)

    @app.get("/counsel-affinity", response_model=PaginatedCounselAffinityResponse)
    def counsel_affinity(
        page: PositiveInt = 1,
        page_size: PageSize = 20,
        minister: str | None = Query(default=None),
        red_flag_only: bool = Query(default=False),
    ) -> PaginatedCounselAffinityResponse:
        from ._counsel_affinity import get_counsel_affinities

        with factory() as session:
            return get_counsel_affinities(session, page, page_size, minister=minister, red_flag_only=red_flag_only)

    @app.get("/counsel-affinity/red-flags", response_model=CounselAffinityRedFlagsResponse)
    def counsel_affinity_red_flags(limit: RedFlagLimit = 100) -> CounselAffinityRedFlagsResponse:
        from ._counsel_affinity import get_counsel_affinity_red_flags

        with factory() as session:
            return get_counsel_affinity_red_flags(session, limit=limit)

    @app.get("/ministers/{minister}/counsel-affinity", response_model=list[CounselAffinityItem])
    def minister_counsel_affinity(minister: str, limit: ListLimit = 100) -> list[CounselAffinityItem]:
        from ._counsel_affinity import get_minister_counsel_affinities

        with factory() as session:
            return get_minister_counsel_affinities(session, minister, limit=limit)

    @app.get("/counsels/{counsel_id}/minister-affinity", response_model=list[CounselAffinityItem])
    def counsel_minister_affinity(counsel_id: str, limit: ListLimit = 100) -> list[CounselAffinityItem]:
        from ._counsel_affinity import get_counsel_minister_affinities

        with factory() as session:
            return get_counsel_minister_affinities(session, counsel_id, limit=limit)

    @app.get("/compound-risk", response_model=PaginatedCompoundRiskResponse)
    def compound_risk(
        page: PositiveInt = 1,
        page_size: PageSize = 20,
        minister: str | None = Query(default=None),
        entity_type: str | None = Query(default=None, pattern="^(party|counsel)$"),
        red_flag_only: bool = Query(default=False),
    ) -> PaginatedCompoundRiskResponse:
        from ._compound_risk import get_compound_risks

        with factory() as session:
            return get_compound_risks(
                session,
                page,
                page_size,
                minister=minister,
                entity_type=entity_type,
                red_flag_only=red_flag_only,
            )

    @app.get("/compound-risk/red-flags", response_model=CompoundRiskRedFlagsResponse)
    def compound_risk_red_flags(
        limit: CompoundRiskLimit = 100,
        minister: str | None = Query(default=None),
        entity_type: str | None = Query(default=None, pattern="^(party|counsel)$"),
    ) -> CompoundRiskRedFlagsResponse:
        from ._compound_risk import get_compound_risk_red_flags

        with factory() as session:
            return get_compound_risk_red_flags(
                session,
                minister=minister,
                entity_type=entity_type,
                limit=limit,
            )

    @app.get("/compound-risk/heatmap", response_model=CompoundRiskHeatmapResponse)
    def compound_risk_heatmap(
        limit: RedFlagLimit = 20,
        minister: str | None = Query(default=None),
        entity_type: str | None = Query(default=None, pattern="^(party|counsel)$"),
        red_flag_only: bool = Query(default=False),
    ) -> CompoundRiskHeatmapResponse:
        from ._compound_risk import get_compound_risk_heatmap

        with factory() as session:
            return get_compound_risk_heatmap(
                session,
                limit=limit,
                minister=minister,
                entity_type=entity_type,
                red_flag_only=red_flag_only,
            )

    # --- Decision Velocity ---

    @app.get("/decision-velocity", response_model=PaginatedDecisionVelocityResponse)
    def decision_velocity(
        page: PositiveInt = 1,
        page_size: PageSize = 20,
        minister: str | None = Query(default=None),
        flag_only: bool = Query(default=False),
        velocity_flag: str | None = Query(
            default=None, pattern="^(queue_jump|stalled)$"
        ),
        process_class: str | None = Query(default=None),
    ) -> PaginatedDecisionVelocityResponse:
        from ._decision_velocity import get_decision_velocities

        with factory() as session:
            return get_decision_velocities(
                session,
                page,
                page_size,
                minister=minister,
                flag_only=flag_only,
                velocity_flag=velocity_flag,
                process_class=process_class,
            )

    @app.get(
        "/decision-velocity/flags",
        response_model=DecisionVelocityRedFlagsResponse,
    )
    def decision_velocity_flags(
        limit: RedFlagLimit = 100,
    ) -> DecisionVelocityRedFlagsResponse:
        from ._decision_velocity import get_decision_velocity_flags

        with factory() as session:
            return get_decision_velocity_flags(session, limit=limit)

    # --- Rapporteur Change ---

    @app.get(
        "/rapporteur-change",
        response_model=PaginatedRapporteurChangeResponse,
    )
    def rapporteur_change(
        page: PositiveInt = 1,
        page_size: PageSize = 20,
        minister: str | None = Query(default=None),
        red_flag_only: bool = Query(default=False),
    ) -> PaginatedRapporteurChangeResponse:
        from ._rapporteur_change import get_rapporteur_changes

        with factory() as session:
            return get_rapporteur_changes(
                session,
                page,
                page_size,
                minister=minister,
                red_flag_only=red_flag_only,
            )

    @app.get(
        "/rapporteur-change/red-flags",
        response_model=RapporteurChangeRedFlagsResponse,
    )
    def rapporteur_change_red_flags(
        limit: RedFlagLimit = 100,
    ) -> RapporteurChangeRedFlagsResponse:
        from ._rapporteur_change import get_rapporteur_change_red_flags

        with factory() as session:
            return get_rapporteur_change_red_flags(session, limit=limit)

    # --- Counsel Network ---

    @app.get(
        "/counsel-network",
        response_model=PaginatedCounselNetworkResponse,
    )
    def counsel_network_clusters(
        page: PositiveInt = 1,
        page_size: PageSize = 20,
        red_flag_only: bool = Query(default=False),
    ) -> PaginatedCounselNetworkResponse:
        from ._counsel_network import get_counsel_network_clusters

        with factory() as session:
            return get_counsel_network_clusters(
                session,
                page,
                page_size,
                red_flag_only=red_flag_only,
            )

    @app.get(
        "/counsel-network/red-flags",
        response_model=CounselNetworkRedFlagsResponse,
    )
    def counsel_network_red_flags(
        limit: RedFlagLimit = 100,
    ) -> CounselNetworkRedFlagsResponse:
        from ._counsel_network import get_counsel_network_red_flags

        with factory() as session:
            return get_counsel_network_red_flags(session, limit=limit)

    # --- Economic Groups ---

    @app.get(
        "/economic-groups",
        response_model=PaginatedEconomicGroupResponse,
    )
    def economic_groups(
        page: PositiveInt = 1,
        page_size: PageSize = 20,
        minister_only: bool = Query(default=False),
        party_only: bool = Query(default=False),
        counsel_only: bool = Query(default=False),
        law_firm_only: bool = Query(default=False),
    ) -> PaginatedEconomicGroupResponse:
        from ._economic_groups import get_economic_groups

        with factory() as session:
            return get_economic_groups(
                session,
                page,
                page_size,
                minister_only=minister_only,
                party_only=party_only,
                counsel_only=counsel_only,
                law_firm_only=law_firm_only,
            )

    @app.get(
        "/economic-groups/{group_id}",
        response_model=EconomicGroupItem,
    )
    def economic_group_detail(group_id: str) -> EconomicGroupItem:
        from ._economic_groups import get_economic_group_by_id

        with factory() as session:
            result = get_economic_group_by_id(session, group_id)
        if result is None:
            raise HTTPException(status_code=404, detail="economic_group_not_found")
        return result
