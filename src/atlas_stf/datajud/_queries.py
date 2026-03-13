"""Elasticsearch query builders for DataJud API."""

from __future__ import annotations


def build_total_query() -> dict:
    """Count total documents in an index."""
    return {"size": 0, "track_total_hits": True}


def build_assunto_aggregation(*, size: int = 20) -> dict:
    """Top assuntos (subjects) aggregation."""
    return {
        "size": 0,
        "track_total_hits": True,
        "aggs": {
            "top_assuntos": {
                "terms": {
                    "field": "assuntos.nome.keyword",
                    "size": size,
                }
            }
        },
    }


def build_orgao_julgador_aggregation(*, size: int = 10) -> dict:
    """Top orgaos julgadores aggregation."""
    return {
        "size": 0,
        "track_total_hits": True,
        "aggs": {
            "top_orgaos": {
                "terms": {
                    "field": "orgaoJulgador.nome.keyword",
                    "size": size,
                }
            }
        },
    }


def build_class_aggregation(*, size: int = 20) -> dict:
    """Class distribution aggregation."""
    return {
        "size": 0,
        "track_total_hits": True,
        "aggs": {
            "classes": {
                "terms": {
                    "field": "classe.nome.keyword",
                    "size": size,
                }
            }
        },
    }


def extract_aggregation_buckets(response: dict, agg_name: str) -> list[dict]:
    """Extract bucket list from an aggregation response."""
    aggs = response.get("aggregations", {})
    agg = aggs.get(agg_name, {})
    return [{"nome": bucket["key"], "count": bucket["doc_count"]} for bucket in agg.get("buckets", [])]


def extract_total(response: dict) -> int:
    """Extract total hits from a response."""
    hits = response.get("hits", {})
    total = hits.get("total", {})
    if isinstance(total, dict):
        return int(total.get("value", 0))
    return int(total) if total else 0
