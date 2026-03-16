"""Tests that API source_files paths never expose absolute paths."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
import pytest

from atlas_stf.api import create_app


@asynccontextmanager
async def _get_client(serving_db: str) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(database_url=serving_db)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            yield client


@pytest.mark.anyio
async def test_dashboard_source_files_no_absolute_paths(serving_db: str):
    async with _get_client(serving_db) as client:
        response = await client.get("/dashboard")
    assert response.status_code == 200
    payload = response.json()
    source_files = payload.get("source_files", [])
    for sf in source_files:
        path = sf.get("path", "")
        assert not path.startswith("/"), f"Absolute path leaked in API: {path}"
        assert ".." not in path, f"Traversal path leaked in API: {path}"
