from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from atlas_stf.agenda._client import AgendaClient
from atlas_stf.agenda._config import AgendaFetchConfig


def _cfg(**kw):
    d = {"rate_limit_seconds": 0.0, "max_retries": 3, "retry_delay_seconds": 0.0, "timeout_seconds": 5.0}
    d.update(kw)
    return AgendaFetchConfig(**d)


class TestAgendaClient:
    def test_get_success(self):
        data = {"data": {"agendaMinistrosPorDiaCategoria": [{"data": "02/03/2024"}]}}
        r = MagicMock(status_code=200, text=json.dumps(data), headers={"Content-Type": "application/json"})
        r.json.return_value = data
        r.raise_for_status = MagicMock()
        with patch("atlas_stf.agenda._client.httpx.Client") as C:
            C.return_value = (m := MagicMock())
            m.get.return_value = r
            with AgendaClient(_cfg()) as c:
                d, meta = c.fetch_month(2024, 3)
            assert meta["fetch_method"] == "GET"
            assert meta["contract_version_detected"] is True

    def test_fallback_post(self):
        data = {"data": {"agendaMinistrosPorDiaCategoria": []}}
        r403 = MagicMock(status_code=403)
        r200 = MagicMock(status_code=200, text=json.dumps(data), headers={})
        r200.json.return_value = data
        r200.raise_for_status = MagicMock()
        with patch("atlas_stf.agenda._client.httpx.Client") as C:
            C.return_value = (m := MagicMock())
            m.get.return_value = r403
            m.post.return_value = r200
            with AgendaClient(_cfg(max_retries=1)) as c:
                _, meta = c.fetch_month(2024, 3)
            assert meta["fetch_method"] == "POST"

    def test_contract_unknown(self):
        data = {"data": {"other": []}}
        r = MagicMock(status_code=200, text=json.dumps(data), headers={})
        r.json.return_value = data
        r.raise_for_status = MagicMock()
        with patch("atlas_stf.agenda._client.httpx.Client") as C:
            C.return_value = (m := MagicMock())
            m.get.return_value = r
            with AgendaClient(_cfg()) as c:
                _, meta = c.fetch_month(2024, 3)
            assert meta["contract_version_detected"] is False
