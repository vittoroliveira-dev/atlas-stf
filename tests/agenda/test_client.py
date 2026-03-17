from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from atlas_stf.agenda._client import AgendaClient, AgendaWafChallengeError
from atlas_stf.agenda._config import AgendaFetchConfig


def _cfg(**kw):
    d = {"rate_limit_seconds": 0.0, "max_retries": 3, "retry_delay_seconds": 0.0, "timeout_seconds": 5.0}
    d.update(kw)
    return AgendaFetchConfig(**d)


def _browser_response(status: int, body: str, headers: dict[str, str] | None = None) -> dict:
    """Simulate the dict returned by page.evaluate(fetch(...))."""
    return {
        "status": status,
        "headers": headers or {"content-type": "application/json"},
        "body": body,
    }


class TestAgendaClient:
    def test_get_success(self):
        data = {"data": {"agendaMinistrosPorDiaCategoria": [{"data": "02/03/2024"}]}}

        client = AgendaClient(_cfg())
        client._page = MagicMock()
        client._page.evaluate.return_value = _browser_response(200, json.dumps(data))

        d, meta = client.fetch_month(2024, 3)
        assert meta["fetch_method"] == "GET"
        assert meta["contract_version_detected"] is True
        assert d["data"]["agendaMinistrosPorDiaCategoria"][0]["data"] == "02/03/2024"

    def test_contract_unknown(self):
        data = {"data": {"other": []}}

        client = AgendaClient(_cfg())
        client._page = MagicMock()
        client._page.evaluate.return_value = _browser_response(200, json.dumps(data))

        _, meta = client.fetch_month(2024, 3)
        assert meta["contract_version_detected"] is False

    def test_waf_challenge_raises(self):
        client = AgendaClient(_cfg(max_retries=1))
        client._page = MagicMock()
        client._page.evaluate.return_value = _browser_response(
            202,
            "",
            headers={"x-amzn-waf-action": "challenge", "content-type": "text/html"},
        )

        with pytest.raises(AgendaWafChallengeError, match="WAF challenge"):
            client.fetch_month(2024, 3)

    def test_waf_challenge_no_retry(self):
        """WAF challenge must fail fast — no retries."""
        client = AgendaClient(_cfg(max_retries=3))
        client._page = MagicMock()
        client._page.evaluate.return_value = _browser_response(
            202,
            "",
            headers={"x-amzn-waf-action": "challenge", "content-type": "text/html"},
        )

        with pytest.raises(AgendaWafChallengeError):
            client.fetch_month(2024, 3)

        # Should have been called exactly once (no retries)
        assert client._page.evaluate.call_count == 1

    def test_http_error_retries(self):
        """Network errors should be retried."""
        data = {"data": {"agendaMinistrosPorDiaCategoria": []}}

        client = AgendaClient(_cfg(max_retries=3))
        client._page = MagicMock()
        client._page.evaluate.side_effect = [
            _browser_response(500, "Internal Server Error"),
            _browser_response(200, json.dumps(data)),
        ]

        _, meta = client.fetch_month(2024, 3)
        assert meta["contract_version_detected"] is True
        assert client._page.evaluate.call_count == 2

    def test_empty_body_retries(self):
        """Empty body (non-WAF) should be retried."""
        data = {"data": {"agendaMinistrosPorDiaCategoria": []}}

        client = AgendaClient(_cfg(max_retries=3))
        client._page = MagicMock()
        client._page.evaluate.side_effect = [
            _browser_response(200, ""),
            _browser_response(200, json.dumps(data)),
        ]

        _, meta = client.fetch_month(2024, 3)
        assert client._page.evaluate.call_count == 2
