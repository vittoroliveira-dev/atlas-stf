"""Elasticsearch query builders and response extractors."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ._config import ScrapeTarget
    from ._session import ApiSession

SEARCH_URL = "/api/search/search"


def build_search_body(
    target: ScrapeTarget,
    *,
    date_gte: str | None = None,
    date_lte: str | None = None,
    search_after: list[Any] | None = None,
) -> dict:
    """Build an Elasticsearch query body for the search endpoint."""
    filters: list[dict] = [{"term": {"base": target.base.value}}]

    if date_gte or date_lte:
        date_range: dict[str, str] = {}
        if date_gte:
            date_range["gte"] = date_gte
        if date_lte:
            date_range["lte"] = date_lte
        filters.append({"range": {"publicacao_data": date_range}})

    body: dict[str, Any] = {
        "query": {"bool": {"filter": filters}},
        "sort": [
            {target.sort_fields[0]: {"order": "asc"}},
            {target.sort_fields[1]: {"order": "asc"}},
        ],
        "size": target.page_size,
        "_source": list(target.fields_to_extract),
    }

    if search_after is not None:
        body["search_after"] = search_after

    return body


def search(session: ApiSession, body: dict) -> dict:
    """Execute a search request."""
    return session.post_json(SEARCH_URL, body)


def extract_hits(response: dict) -> list[dict]:
    """Extract source docs from API response, adding ``_id``."""
    hits = response.get("result", response).get("hits", {}).get("hits", [])
    docs = []
    for hit in hits:
        doc = dict(hit.get("_source", {}))
        doc["_id"] = hit.get("_id", "")
        docs.append(doc)
    return docs


def extract_total(response: dict) -> int:
    """Extract total hit count from API response."""
    total = response.get("result", response).get("hits", {}).get("total", {})
    if isinstance(total, dict):
        return int(total.get("value", 0))
    return int(total)


def extract_search_after(response: dict) -> list[Any] | None:
    """Extract the sort values from the last hit for ``search_after`` pagination."""
    hits = response.get("result", response).get("hits", {}).get("hits", [])
    if not hits:
        return None
    return hits[-1].get("sort")
