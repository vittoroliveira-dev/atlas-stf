"""Tests for datajud/_queries.py."""

from __future__ import annotations

from atlas_stf.datajud._queries import (
    build_assunto_aggregation,
    build_class_aggregation,
    build_orgao_julgador_aggregation,
    build_total_query,
    extract_aggregation_buckets,
    extract_total,
)


class TestQueryBuilders:
    def test_total_query(self) -> None:
        q = build_total_query()
        assert q["size"] == 0
        assert q["track_total_hits"] is True

    def test_assunto_aggregation(self) -> None:
        q = build_assunto_aggregation(size=5)
        assert "aggs" in q
        assert q["aggs"]["top_assuntos"]["terms"]["size"] == 5

    def test_orgao_julgador_aggregation(self) -> None:
        q = build_orgao_julgador_aggregation(size=3)
        assert q["aggs"]["top_orgaos"]["terms"]["size"] == 3

    def test_class_aggregation(self) -> None:
        q = build_class_aggregation(size=10)
        assert q["aggs"]["classes"]["terms"]["size"] == 10


class TestExtractors:
    def test_extract_total_dict(self) -> None:
        resp = {"hits": {"total": {"value": 42, "relation": "eq"}}}
        assert extract_total(resp) == 42

    def test_extract_total_int(self) -> None:
        resp = {"hits": {"total": 99}}
        assert extract_total(resp) == 99

    def test_extract_total_empty(self) -> None:
        assert extract_total({}) == 0

    def test_extract_buckets(self) -> None:
        resp = {
            "aggregations": {
                "top_assuntos": {
                    "buckets": [
                        {"key": "Direito Civil", "doc_count": 500},
                        {"key": "Direito Penal", "doc_count": 300},
                    ]
                }
            }
        }
        result = extract_aggregation_buckets(resp, "top_assuntos")
        assert len(result) == 2
        assert result[0] == {"nome": "Direito Civil", "count": 500}

    def test_extract_buckets_missing(self) -> None:
        assert extract_aggregation_buckets({}, "nonexistent") == []
