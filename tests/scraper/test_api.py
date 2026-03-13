"""Tests for _api: query builders and response extractors."""

from atlas_stf.scraper._api import build_search_body, extract_hits, extract_search_after, extract_total
from atlas_stf.scraper._config import DECISOES_TARGET


class TestBuildSearchBody:
    def test_basic(self) -> None:
        body = build_search_body(DECISOES_TARGET)
        assert body["size"] == 250
        assert body["query"]["bool"]["filter"][0] == {"term": {"base": "decisoes"}}
        assert "_source" in body
        assert "search_after" not in body

    def test_with_date_range(self) -> None:
        body = build_search_body(DECISOES_TARGET, date_gte="2024-01-01", date_lte="2024-01-31")
        filters = body["query"]["bool"]["filter"]
        assert len(filters) == 2
        date_filter = filters[1]
        assert date_filter["range"]["publicacao_data"]["gte"] == "2024-01-01"
        assert date_filter["range"]["publicacao_data"]["lte"] == "2024-01-31"

    def test_with_search_after(self) -> None:
        body = build_search_body(DECISOES_TARGET, search_after=["2024-01-15", 12345])
        assert body["search_after"] == ["2024-01-15", 12345]

    def test_sort_order(self) -> None:
        body = build_search_body(DECISOES_TARGET)
        sorts = body["sort"]
        assert sorts[0] == {"publicacao_data": {"order": "asc"}}
        assert sorts[1] == {"processo_numero": {"order": "asc"}}


class TestExtractHits:
    def test_normal_response(self) -> None:
        response = {
            "result": {
                "hits": {
                    "hits": [
                        {"_id": "a1", "_source": {"processo_numero": "123"}},
                        {"_id": "b2", "_source": {"processo_numero": "456"}},
                    ]
                }
            }
        }
        docs = extract_hits(response)
        assert len(docs) == 2
        assert docs[0]["_id"] == "a1"
        assert docs[0]["processo_numero"] == "123"

    def test_empty(self) -> None:
        response = {"result": {"hits": {"hits": []}}}
        assert extract_hits(response) == []

    def test_no_result_wrapper(self) -> None:
        response = {"hits": {"hits": [{"_id": "x", "_source": {"a": 1}}]}}
        docs = extract_hits(response)
        assert len(docs) == 1


class TestExtractTotal:
    def test_dict_total(self) -> None:
        response = {"result": {"hits": {"total": {"value": 716511, "relation": "eq"}}}}
        assert extract_total(response) == 716511

    def test_int_total(self) -> None:
        response = {"result": {"hits": {"total": 500}}}
        assert extract_total(response) == 500


class TestExtractSearchAfter:
    def test_normal(self) -> None:
        response = {
            "result": {
                "hits": {
                    "hits": [
                        {"_id": "a", "sort": ["2024-01-01", 100]},
                        {"_id": "b", "sort": ["2024-01-02", 200]},
                    ]
                }
            }
        }
        assert extract_search_after(response) == ["2024-01-02", 200]

    def test_empty(self) -> None:
        response = {"result": {"hits": {"hits": []}}}
        assert extract_search_after(response) is None
