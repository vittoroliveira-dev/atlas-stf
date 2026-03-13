from __future__ import annotations

import os
import time
from threading import Lock
from typing import TYPE_CHECKING

from fastapi import Depends, FastAPI, Query
from sqlalchemy import text

from .schemas import DashboardResponse, FilterOptionsResponse, HealthResponse, MinisterFlowResponse
from .service import QueryFilters, get_dashboard, get_health, get_minister_flow, resolve_filters

if TYPE_CHECKING:
    from typing import Annotated

    from sqlalchemy.orm import sessionmaker

    PositiveInt = Annotated[int, Query(ge=1)]
    PageSize = Annotated[int, Query(ge=1, le=100)]


DEFAULT_DASHBOARD_CACHE_TTL_SECONDS_ENV = "ATLAS_STF_DASHBOARD_CACHE_TTL_SECONDS"


_DASHBOARD_CACHE_MAX_ENTRIES = 256

_CacheKey = tuple[str | None, str | None, str, str | None, str | None]


class _DashboardCache:
    def __init__(self, *, ttl_seconds: float, max_entries: int = _DASHBOARD_CACHE_MAX_ENTRIES) -> None:
        self._ttl_seconds = ttl_seconds
        self._max_entries = max_entries
        self._entries: dict[_CacheKey, tuple[float, DashboardResponse]] = {}
        self._lock = Lock()

    def get(self, key: _CacheKey) -> DashboardResponse | None:
        now = time.monotonic()
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            expires_at, payload = entry
            if expires_at <= now:
                self._entries.pop(key, None)
                return None
            return payload.model_copy(deep=True)

    def set(self, key: _CacheKey, payload: DashboardResponse) -> None:
        with self._lock:
            if len(self._entries) >= self._max_entries and key not in self._entries:
                oldest_key = min(self._entries, key=lambda k: self._entries[k][0])
                del self._entries[oldest_key]
            self._entries[key] = (time.monotonic() + self._ttl_seconds, payload.model_copy(deep=True))


def _get_dashboard_cache_ttl_seconds() -> float:
    try:
        return max(float(os.getenv(DEFAULT_DASHBOARD_CACHE_TTL_SECONDS_ENV, "10")), 0.0)
    except ValueError:
        return 10.0


def register_core_routes(
    app: FastAPI,
    factory: sessionmaker,
    build_filters: ...,
    get_base_filters: ...,
) -> None:
    dashboard_cache = _DashboardCache(ttl_seconds=_get_dashboard_cache_ttl_seconds())

    @app.get("/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        with factory() as session:
            session.execute(text("select 1"))
        return get_health(app.state.database_url)

    @app.get("/filters/options", response_model=FilterOptionsResponse)
    def filters_options(
        minister: str | None = Query(default=None),
        base_filters: QueryFilters = Depends(get_base_filters),
    ) -> FilterOptionsResponse:
        filters = build_filters(
            minister=minister,
            period=base_filters.period,
            collegiate=base_filters.collegiate,
            judging_body=base_filters.judging_body,
            process_class=base_filters.process_class,
        )
        with factory() as session:
            return resolve_filters(session, filters).options

    @app.get("/dashboard", response_model=DashboardResponse)
    def dashboard(
        minister: str | None = Query(default=None),
        base_filters: QueryFilters = Depends(get_base_filters),
    ) -> DashboardResponse:
        filters = build_filters(
            minister=minister,
            period=base_filters.period,
            collegiate=base_filters.collegiate,
            judging_body=base_filters.judging_body,
            process_class=base_filters.process_class,
        )
        cache_key = (
            filters.minister,
            filters.period,
            filters.collegiate,
            filters.judging_body,
            filters.process_class,
        )
        cached = dashboard_cache.get(cache_key)
        if cached is not None:
            return cached

        with factory() as session:
            payload = get_dashboard(session, filters)
        dashboard_cache.set(cache_key, payload)
        return payload

    @app.get("/ministers/{minister}/flow", response_model=MinisterFlowResponse)
    def minister_flow(
        minister: str,
        period: str | None = Query(default=None),
        collegiate: str = Query(default="all", pattern="^(all|colegiado|monocratico)$"),
        judging_body: str | None = Query(default=None),
        process_class: str | None = Query(default=None),
    ) -> MinisterFlowResponse:
        filters = build_filters(
            minister=minister,
            period=period,
            collegiate=collegiate,
            judging_body=judging_body,
            process_class=process_class,
        )
        with factory() as session:
            return get_minister_flow(session, filters)
