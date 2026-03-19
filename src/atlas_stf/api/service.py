"""Public service re-exports — logic lives in _service_* submodules."""

from __future__ import annotations

from ..core.constants import CollegiateFilter  # noqa: F401
from ._aggregation import (  # noqa: F401
    _entity_listing_page,
    _minister_profiles,
    _top_entities,
)

# Re-exports for app.py compatibility
from ._filters import (  # noqa: F401
    EntityKind,
    QueryFilters,
    ResolvedFilters,
    _apply_case_filters,
    _paginate,
    resolve_filters,
)
from ._formatters import (  # noqa: F401
    _alert_to_summary,
    _case_to_summary,
    _metrics,
    _source_files,
)
from ._service_alerts_cases import (
    get_alert_detail,
    get_alerts,
    get_case_detail,
    get_case_ml_outlier,
    get_cases,
    get_related_alerts_for_case,
)
from ._service_analytics import (
    get_assignment_audit,
    get_health,
    get_minister_bio,
    get_minister_profile_data,
    get_minister_sequential,
    get_origin_context,
    get_sources_audit,
)
from ._service_dashboard import get_dashboard
from ._service_entities import (
    get_counsel_detail,
    get_counsel_ministers,
    get_counsels,
    get_minister_counsels,
    get_minister_parties,
    get_parties,
    get_party_detail,
    get_party_ministers,
)
from ._service_flow import get_minister_flow
from ._temporal_analysis import get_temporal_analysis_minister, get_temporal_analysis_overview

__all__ = [
    "CollegiateFilter",
    "EntityKind",
    "QueryFilters",
    "ResolvedFilters",
    "get_alert_detail",
    "get_alerts",
    "get_assignment_audit",
    "get_case_detail",
    "get_case_ml_outlier",
    "get_cases",
    "get_counsel_detail",
    "get_counsel_ministers",
    "get_counsels",
    "get_dashboard",
    "get_health",
    "get_minister_bio",
    "get_minister_counsels",
    "get_minister_flow",
    "get_minister_parties",
    "get_minister_profile_data",
    "get_minister_sequential",
    "get_origin_context",
    "get_parties",
    "get_party_detail",
    "get_party_ministers",
    "get_related_alerts_for_case",
    "get_sources_audit",
    "get_temporal_analysis_minister",
    "get_temporal_analysis_overview",
    "resolve_filters",
]
