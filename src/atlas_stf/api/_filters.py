from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import Callable, Literal

from sqlalchemy import Select, func, select
from sqlalchemy.orm import Session

from ..core.constants import QueryFilters
from ..serving.models import ServingCase
from .schemas import AppliedFilters, FilterOptionsResponse


class _FilterOptionsCache:
    """Cache DISTINCT filter options per engine (values only change on DB rebuild)."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._store: dict[int, tuple[list[str], list[str], list[str], list[str]]] = {}

    def get(self, session: Session) -> tuple[list[str], list[str], list[str], list[str]]:
        engine_id = id(session.bind)
        with self._lock:
            cached = self._store.get(engine_id)
        if cached is not None:
            return cached
        periods = _all_periods(session)
        judging_bodies = _all_distinct_strings(session, ServingCase.judging_body)
        process_classes = _all_distinct_strings(session, ServingCase.process_class)
        ministers = _all_distinct_strings(session, ServingCase.current_rapporteur)
        result = (periods, judging_bodies, process_classes, ministers)
        with self._lock:
            self._store[engine_id] = result
        return result

    def invalidate(self) -> None:
        with self._lock:
            self._store.clear()


_filter_options_cache = _FilterOptionsCache()

EntityKind = Literal["counsel", "party"]
CaseSelector = Callable[[ServingCase], str | None]


@dataclass(frozen=True)
class ResolvedFilters:
    options: FilterOptionsResponse
    filters: QueryFilters


def _normalized_like(column, value: str):
    escaped = value.lower().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    lowered = f"%{escaped}%"
    return func.lower(func.coalesce(column, "")).like(lowered, escape="\\")


def _apply_case_filters(stmt: Select, filters: QueryFilters) -> Select:
    if filters.minister:
        stmt = stmt.where(_normalized_like(ServingCase.current_rapporteur, filters.minister))
    if filters.period:
        stmt = stmt.where(ServingCase.period == filters.period)
    if filters.collegiate == "colegiado":
        stmt = stmt.where(ServingCase.is_collegiate.is_(True))
    elif filters.collegiate == "monocratico":
        stmt = stmt.where(ServingCase.is_collegiate.is_(False))
    if filters.judging_body:
        stmt = stmt.where(ServingCase.judging_body == filters.judging_body)
    if filters.process_class:
        stmt = stmt.where(ServingCase.process_class == filters.process_class)
    return stmt


def _all_distinct_strings(session: Session, column) -> list[str]:
    stmt = select(column).where(column.is_not(None)).distinct().order_by(column)
    return [value for value in session.scalars(stmt) if value]


def _all_periods(session: Session) -> list[str]:
    stmt = select(ServingCase.period).where(ServingCase.period.is_not(None)).distinct()
    values = [value for value in session.scalars(stmt) if value]
    return sorted(values, reverse=True)


def resolve_filters(session: Session, raw_filters: QueryFilters, *, auto_select_period: bool = True) -> ResolvedFilters:
    periods, judging_bodies, process_classes, all_ministers = _filter_options_cache.get(session)

    if raw_filters.period == "__all__":
        selected_period = None
    elif raw_filters.period in periods:
        selected_period = raw_filters.period
    elif auto_select_period:
        selected_period = periods[0] if periods else ""
    else:
        selected_period = None
    selected_judging_body = raw_filters.judging_body if raw_filters.judging_body in judging_bodies else None
    selected_process_class = raw_filters.process_class if raw_filters.process_class in process_classes else None
    if raw_filters.collegiate in {"all", "colegiado", "monocratico"}:
        selected_collegiate = raw_filters.collegiate
    else:
        selected_collegiate = "all"

    selected_minister = raw_filters.minister if raw_filters.minister else None

    applied_period = "__all__" if raw_filters.period == "__all__" else selected_period

    options = FilterOptionsResponse(
        ministers=all_ministers,
        periods=periods,
        judging_bodies=judging_bodies,
        process_classes=process_classes,
        applied=AppliedFilters(
            minister=selected_minister,
            period=applied_period,
            collegiate=selected_collegiate,
            judging_body=selected_judging_body,
            process_class=selected_process_class,
        ),
    )
    return ResolvedFilters(
        options=options,
        filters=QueryFilters(
            minister=selected_minister,
            period=selected_period,
            collegiate=selected_collegiate,
            judging_body=selected_judging_body,
            process_class=selected_process_class,
        ),
    )


def _paginate(page: int, page_size: int) -> tuple[int, int]:
    safe_page = max(page, 1)
    safe_size = min(max(page_size, 1), 100)
    return safe_page, safe_size
