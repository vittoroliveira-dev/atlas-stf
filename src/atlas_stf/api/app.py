from __future__ import annotations

import logging
import math
import os
import time
from asyncio import timeout as asyncio_timeout
from collections import defaultdict, deque
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from threading import Lock

from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from starlette.responses import JSONResponse

from .service import QueryFilters

logger = logging.getLogger("atlas_stf.api")

DEFAULT_DATABASE_ENV = "ATLAS_STF_DATABASE_URL"
DEFAULT_RATE_LIMIT_ENABLED_ENV = "ATLAS_STF_RATE_LIMIT_ENABLED"
DEFAULT_RATE_LIMIT_MAX_REQUESTS_ENV = "ATLAS_STF_RATE_LIMIT_MAX_REQUESTS"
DEFAULT_RATE_LIMIT_WINDOW_SECONDS_ENV = "ATLAS_STF_RATE_LIMIT_WINDOW_SECONDS"
DEFAULT_REQUEST_TIMEOUT_SECONDS_ENV = "ATLAS_STF_REQUEST_TIMEOUT_SECONDS"
DEFAULT_TRUST_PROXY_HEADERS_ENV = "ATLAS_STF_TRUST_PROXY_HEADERS"
DEFAULT_REVIEW_API_KEY_ENV = "ATLAS_STF_REVIEW_API_KEY"


_RATE_LIMITER_MAX_KEYS = 100_000


class _InMemoryRateLimiter:
    def __init__(self, *, max_requests: int, window_seconds: int) -> None:
        self._max_requests = max_requests
        self._window_seconds = window_seconds
        self._hits: dict[str, deque[float]] = defaultdict(deque)
        self._lock = Lock()

    def _evict_expired(self, now: float) -> None:
        cutoff = now - self._window_seconds
        expired = [k for k, v in self._hits.items() if not v or v[-1] <= cutoff]
        for k in expired:
            del self._hits[k]

    def check(self, client_id: str) -> tuple[bool, int, int]:
        now = time.monotonic()
        cutoff = now - self._window_seconds
        with self._lock:
            if len(self._hits) >= _RATE_LIMITER_MAX_KEYS and client_id not in self._hits:
                self._evict_expired(now)
            hits = self._hits[client_id]
            while hits and hits[0] <= cutoff:
                hits.popleft()
            if len(hits) >= self._max_requests:
                retry_after = max(1, math.ceil(hits[0] + self._window_seconds - now))
                return False, retry_after, 0
            hits.append(now)
            remaining = max(self._max_requests - len(hits), 0)
            return True, 0, remaining


def _get_rate_limit_settings() -> tuple[bool, int, int]:
    enabled_raw = os.getenv(DEFAULT_RATE_LIMIT_ENABLED_ENV, "true").strip().lower()
    enabled = enabled_raw not in {"0", "false", "no", "off"}
    try:
        max_requests = max(int(os.getenv(DEFAULT_RATE_LIMIT_MAX_REQUESTS_ENV, "120")), 1)
    except ValueError:
        max_requests = 120
    try:
        window_seconds = max(int(os.getenv(DEFAULT_RATE_LIMIT_WINDOW_SECONDS_ENV, "60")), 1)
    except ValueError:
        window_seconds = 60
    return enabled, max_requests, window_seconds


def _get_request_timeout_seconds() -> float | None:
    raw = os.getenv(DEFAULT_REQUEST_TIMEOUT_SECONDS_ENV, "30").strip()
    if raw.lower() in {"0", "false", "no", "off"}:
        return None
    try:
        timeout_seconds = float(raw)
    except ValueError:
        timeout_seconds = 30.0
    return timeout_seconds if timeout_seconds > 0 else None


def _get_trust_proxy_headers() -> bool:
    raw = os.getenv(DEFAULT_TRUST_PROXY_HEADERS_ENV, "false").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _make_client_identifier(trust_proxy: bool):
    if trust_proxy:
        logger.warning(
            "ATLAS_STF_TRUST_PROXY_HEADERS is enabled — rate limiter uses "
            "last X-Forwarded-For IP. Only enable behind a trusted reverse proxy."
        )

    def _get_client_identifier(request: Request) -> str:
        if trust_proxy:
            forwarded_for = request.headers.get("x-forwarded-for")
            if forwarded_for:
                parts = [p.strip() for p in forwarded_for.split(",")]
                return parts[-1] or "anonymous"
            real_ip = request.headers.get("x-real-ip")
            if real_ip:
                return real_ip.strip() or "anonymous"
        client = request.client
        if client and client.host:
            return client.host
        return "anonymous"

    return _get_client_identifier


def _get_database_url(explicit_database_url: str | None = None) -> str:
    database_url = explicit_database_url or os.getenv(DEFAULT_DATABASE_ENV)
    if not database_url:
        raise RuntimeError(f"Database URL missing. Set {DEFAULT_DATABASE_ENV} or pass database_url explicitly.")
    return database_url


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    yield
    engine = getattr(app.state, "engine", None)
    if engine is not None:
        engine.dispose()


def create_app(*, database_url: str | None = None) -> FastAPI:
    resolved_database_url = _get_database_url(database_url)
    engine_kwargs: dict[str, object] = {}
    if resolved_database_url.startswith("sqlite"):
        engine_kwargs["connect_args"] = {"check_same_thread": False}
    engine = create_engine(resolved_database_url, **engine_kwargs)

    @event.listens_for(engine, "connect")
    def _register_py_lower(dbapi_conn, _connection_record):
        dbapi_conn.create_function("py_lower", 1, lambda v: v.lower() if isinstance(v, str) else v)

    factory = sessionmaker(engine)

    app = FastAPI(title="Atlas STF API", version="1.0.0", lifespan=_lifespan)
    app.state.database_url = resolved_database_url
    app.state.engine = engine
    app.state.session_factory = factory

    @app.exception_handler(Exception)
    async def _unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("Unhandled error on %s %s", request.method, request.url.path)
        return JSONResponse(status_code=500, content={"detail": "internal_server_error"})

    @app.middleware("http")
    async def security_headers_middleware(request: Request, call_next):
        response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        return response

    rate_limit_enabled, rate_limit_max_requests, rate_limit_window_seconds = _get_rate_limit_settings()
    request_timeout_seconds = _get_request_timeout_seconds()
    get_client_id = _make_client_identifier(_get_trust_proxy_headers())
    if rate_limit_enabled:
        limiter = _InMemoryRateLimiter(
            max_requests=rate_limit_max_requests,
            window_seconds=rate_limit_window_seconds,
        )
        exempt_paths = {"/health"}

        @app.middleware("http")
        async def rate_limit_middleware(request: Request, call_next: ...):
            if request.method == "OPTIONS" or request.url.path in exempt_paths:
                return await call_next(request)

            allowed, retry_after, remaining = limiter.check(get_client_id(request))
            if not allowed:
                return JSONResponse(
                    status_code=429,
                    content={"detail": "rate_limited"},
                    headers={
                        "Retry-After": str(retry_after),
                        "X-RateLimit-Limit": str(rate_limit_max_requests),
                        "X-RateLimit-Remaining": "0",
                        "X-RateLimit-Window": str(rate_limit_window_seconds),
                    },
                )

            response = await call_next(request)
            response.headers.setdefault("X-RateLimit-Limit", str(rate_limit_max_requests))
            response.headers.setdefault("X-RateLimit-Remaining", str(remaining))
            response.headers.setdefault("X-RateLimit-Window", str(rate_limit_window_seconds))
            return response

    if request_timeout_seconds is not None:

        @app.middleware("http")
        async def request_timeout_middleware(request: Request, call_next: ...):
            try:
                async with asyncio_timeout(request_timeout_seconds):
                    return await call_next(request)
            except TimeoutError:
                return JSONResponse(
                    status_code=504,
                    content={"detail": "request_timeout"},
                )

    cors_origins_raw = os.getenv("ATLAS_STF_CORS_ORIGINS")
    cors_origins = [o.strip() for o in (cors_origins_raw or "").split(",") if o.strip()]
    if cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_methods=["GET"],
            allow_headers=["Accept", "Content-Type"],
        )

    def build_filters(
        *,
        minister: str | None,
        period: str | None,
        collegiate: str,
        judging_body: str | None,
        process_class: str | None,
    ) -> QueryFilters:
        return QueryFilters(
            minister=minister,
            period=period,
            collegiate=collegiate,  # type: ignore[arg-type]
            judging_body=judging_body,
            process_class=process_class,
        )

    async def get_base_filters(
        period: str | None = Query(default=None, pattern=r"^(\d{4}-\d{2}|__all__)$"),
        collegiate: str = Query(default="all", pattern="^(all|colegiado|monocratico)$"),
        judging_body: str | None = Query(default=None),
        process_class: str | None = Query(default=None),
    ) -> QueryFilters:
        return build_filters(
            minister=None,
            period=period,
            collegiate=collegiate,
            judging_body=judging_body,
            process_class=process_class,
        )

    # Register all route groups
    from ._routes_agenda import register_agenda_routes
    from ._routes_alerts_cases import register_alerts_cases_routes
    from ._routes_analytics import register_analytics_routes
    from ._routes_core import register_core_routes
    from ._routes_entities import register_entities_routes
    from ._routes_graph import register_graph_routes
    from ._routes_representation import register_representation_routes
    from ._routes_risk import register_risk_routes
    from ._routes_timeline import register_timeline_routes

    register_core_routes(app, factory, build_filters, get_base_filters)
    register_alerts_cases_routes(app, factory, build_filters, get_base_filters)
    register_entities_routes(app, factory, build_filters, get_base_filters)
    register_analytics_routes(app, factory, build_filters, get_base_filters)
    register_risk_routes(app, factory, build_filters, get_base_filters)
    register_timeline_routes(app, factory, build_filters, get_base_filters)
    register_representation_routes(app, factory, build_filters, get_base_filters)
    register_agenda_routes(app, factory, build_filters, get_base_filters)
    register_graph_routes(app, factory, build_filters, get_base_filters)

    review_key = os.getenv("ATLAS_STF_REVIEW_API_KEY", "")
    if not review_key:
        logger.warning(
            "ATLAS_STF_REVIEW_API_KEY not set — POST /review/decision will return 503"
        )
    elif review_key == "__dev__":
        logger.warning(
            "ATLAS_STF_REVIEW_API_KEY=__dev__ — POST /review/decision open without authentication (dev mode)"
        )

    return app
