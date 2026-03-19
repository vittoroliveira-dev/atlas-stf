"""Tests for OAB/SP HTTP client (OabSpClient)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from atlas_stf.oab_sp._client import OabSpClient
from atlas_stf.oab_sp._config import (
    OABSP_DETAIL_URL,
    OABSP_INSCRITOS_URL,
    OABSP_SEARCH_URL,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_response(status_code: int = 200, text: str = "<html/>") -> MagicMock:
    """Build a minimal httpx.Response mock."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.text = text
    resp.request = MagicMock(spec=httpx.Request)
    resp.raise_for_status = MagicMock()
    return resp


def _make_ok_response(text: str = "<html/>") -> MagicMock:
    return _make_response(200, text)


# ---------------------------------------------------------------------------
# Context manager protocol
# ---------------------------------------------------------------------------


class TestContextManager:
    def test_enter_returns_self(self):
        client = OabSpClient(rate_limit=0.0, retry_delay=0.0)
        with patch("atlas_stf.oab_sp._client.httpx.Client"):
            result = client.__enter__()
        assert result is client

    def test_exit_closes_and_clears_client(self):
        with patch("atlas_stf.oab_sp._client.httpx.Client") as mock_cls:
            mock_http = mock_cls.return_value
            with OabSpClient(rate_limit=0.0, retry_delay=0.0) as client:
                assert client._client is mock_http
            # After __exit__, _client is None and close() was called
            mock_http.close.assert_called_once()
            assert client._client is None

    def test_exit_without_exception_returns_none(self):
        with patch("atlas_stf.oab_sp._client.httpx.Client"):
            client = OabSpClient(rate_limit=0.0, retry_delay=0.0)
            client.__enter__()
            result = client.__exit__(None, None, None)
            assert result is None

    def test_exit_idempotent_when_client_none(self):
        """__exit__ should not crash when _client is already None."""
        client = OabSpClient(rate_limit=0.0, retry_delay=0.0)
        assert client._client is None
        # Should not raise
        client.__exit__(None, None, None)

    def test_httpx_client_created_with_correct_headers(self):
        with patch("atlas_stf.oab_sp._client.httpx.Client") as mock_cls:
            with OabSpClient(timeout=15, rate_limit=0.0, retry_delay=0.0):
                pass
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs["timeout"] == 15
        assert call_kwargs["follow_redirects"] is True
        headers = call_kwargs["headers"]
        assert "User-Agent" in headers
        assert "AtlasSTF" in headers["User-Agent"]
        assert headers["Accept"] == "text/html,application/xhtml+xml"

    def test_init_stores_parameters(self):
        client = OabSpClient(timeout=10, rate_limit=2.0, max_retries=5, retry_delay=3.0)
        assert client._timeout == 10
        assert client._rate_limit == 2.0
        assert client._max_retries == 5
        assert client._retry_delay == 3.0
        assert client._last_request_time == 0.0
        assert client._client is None


# ---------------------------------------------------------------------------
# _request_with_retry — guard: outside context manager
# ---------------------------------------------------------------------------


class TestRequestWithRetryGuard:
    def test_raises_if_not_in_context_manager(self):
        client = OabSpClient(rate_limit=0.0, retry_delay=0.0)
        with pytest.raises(RuntimeError, match="context manager"):
            client._request_with_retry("GET", "https://example.com")


# ---------------------------------------------------------------------------
# _enforce_rate_limit
# ---------------------------------------------------------------------------


class TestEnforceRateLimit:
    def test_sleeps_when_not_enough_time_elapsed(self):
        with patch("atlas_stf.oab_sp._client.httpx.Client"):
            with OabSpClient(rate_limit=2.0, retry_delay=0.0) as client:
                # Simulate a recent request
                import time
                client._last_request_time = time.monotonic()  # just now
                with patch("atlas_stf.oab_sp._client.time.sleep") as mock_sleep:
                    client._enforce_rate_limit()
                # sleep should have been called with a positive value close to 2.0
                mock_sleep.assert_called_once()
                sleep_val = mock_sleep.call_args[0][0]
                assert 0 < sleep_val <= 2.0

    def test_no_sleep_when_enough_time_elapsed(self):
        with patch("atlas_stf.oab_sp._client.httpx.Client"):
            with OabSpClient(rate_limit=0.001, retry_delay=0.0) as client:
                # _last_request_time stays at 0.0 (epoch), so enough time has elapsed
                with patch("atlas_stf.oab_sp._client.time.sleep") as mock_sleep:
                    client._enforce_rate_limit()
                mock_sleep.assert_not_called()


# ---------------------------------------------------------------------------
# _request_with_retry — success path
# ---------------------------------------------------------------------------


class TestRequestWithRetrySuccess:
    def test_returns_response_on_200(self):
        resp = _make_ok_response("<html>OK</html>")
        with patch("atlas_stf.oab_sp._client.httpx.Client") as mock_cls:
            mock_cls.return_value.request.return_value = resp
            with patch("atlas_stf.oab_sp._client.time.sleep"):
                with OabSpClient(rate_limit=0.0, retry_delay=0.0) as client:
                    result = client._request_with_retry("GET", "https://example.com")
        assert result is resp
        resp.raise_for_status.assert_called_once()

    def test_passes_extra_kwargs_to_request(self):
        resp = _make_ok_response()
        with patch("atlas_stf.oab_sp._client.httpx.Client") as mock_cls:
            mock_http = mock_cls.return_value
            mock_http.request.return_value = resp
            with patch("atlas_stf.oab_sp._client.time.sleep"):
                with OabSpClient(rate_limit=0.0, retry_delay=0.0) as client:
                    client._request_with_retry("POST", "https://example.com", data={"k": "v"})
        mock_http.request.assert_called_once_with("POST", "https://example.com", data={"k": "v"})


# ---------------------------------------------------------------------------
# _request_with_retry — retry on transient HTTP status codes
# ---------------------------------------------------------------------------


class TestRequestWithRetryOnStatusCodes:
    @pytest.mark.parametrize("status_code", [429, 500, 502, 503, 504])
    def test_retries_on_transient_status_codes(self, status_code: int):
        """Transient error codes should trigger retry and eventually raise RuntimeError."""
        bad_resp = _make_response(status_code)
        with patch("atlas_stf.oab_sp._client.httpx.Client") as mock_cls:
            mock_http = mock_cls.return_value
            mock_http.request.return_value = bad_resp
            with patch("atlas_stf.oab_sp._client.time.sleep"):
                with OabSpClient(rate_limit=0.0, max_retries=2, retry_delay=0.0) as client:
                    with pytest.raises(RuntimeError, match="failed after 2 attempts"):
                        client._request_with_retry("GET", "https://example.com")
        # Should have been called exactly max_retries times
        assert mock_http.request.call_count == 2

    def test_retries_and_succeeds_after_transient_failure(self):
        """First call returns 503, second returns 200 → success."""
        bad_resp = _make_response(503)
        ok_resp = _make_ok_response("<html>retry worked</html>")
        with patch("atlas_stf.oab_sp._client.httpx.Client") as mock_cls:
            mock_http = mock_cls.return_value
            mock_http.request.side_effect = [bad_resp, ok_resp]
            with patch("atlas_stf.oab_sp._client.time.sleep"):
                with OabSpClient(rate_limit=0.0, max_retries=3, retry_delay=0.0) as client:
                    result = client._request_with_retry("GET", "https://example.com")
        assert result is ok_resp
        assert mock_http.request.call_count == 2

    def test_exponential_backoff_on_status_errors(self):
        """Sleep duration grows exponentially: delay * 2^attempt."""
        bad_resp = _make_response(500)
        with patch("atlas_stf.oab_sp._client.httpx.Client") as mock_cls:
            mock_http = mock_cls.return_value
            mock_http.request.return_value = bad_resp
            sleep_calls: list[float] = []
            with patch("atlas_stf.oab_sp._client.time.sleep", side_effect=lambda s: sleep_calls.append(s)):
                with OabSpClient(rate_limit=0.0, max_retries=3, retry_delay=1.0) as client:
                    with pytest.raises(RuntimeError):
                        client._request_with_retry("GET", "https://example.com")
        # Expect sleep calls for each retry attempt
        assert len(sleep_calls) == 3
        # Values should be 1*2^0=1, 1*2^1=2, 1*2^2=4
        assert sleep_calls[0] == pytest.approx(1.0)
        assert sleep_calls[1] == pytest.approx(2.0)
        assert sleep_calls[2] == pytest.approx(4.0)


# ---------------------------------------------------------------------------
# _request_with_retry — retry on RequestError
# ---------------------------------------------------------------------------


class TestRequestWithRetryOnRequestError:
    def test_retries_on_request_error(self):
        """httpx.RequestError causes retry and eventually raises RuntimeError."""
        exc = httpx.ConnectError("connection refused")
        with patch("atlas_stf.oab_sp._client.httpx.Client") as mock_cls:
            mock_http = mock_cls.return_value
            mock_http.request.side_effect = exc
            with patch("atlas_stf.oab_sp._client.time.sleep"):
                with OabSpClient(rate_limit=0.0, max_retries=2, retry_delay=0.0) as client:
                    with pytest.raises(RuntimeError, match="failed after 2 attempts"):
                        client._request_with_retry("GET", "https://example.com")
        assert mock_http.request.call_count == 2

    def test_recovers_after_request_error(self):
        """First call raises ConnectError, second succeeds."""
        exc = httpx.ConnectError("temporary failure")
        ok_resp = _make_ok_response("<html>recovered</html>")
        with patch("atlas_stf.oab_sp._client.httpx.Client") as mock_cls:
            mock_http = mock_cls.return_value
            mock_http.request.side_effect = [exc, ok_resp]
            with patch("atlas_stf.oab_sp._client.time.sleep"):
                with OabSpClient(rate_limit=0.0, max_retries=3, retry_delay=0.0) as client:
                    result = client._request_with_retry("GET", "https://example.com")
        assert result is ok_resp

    def test_exponential_backoff_on_request_errors(self):
        exc = httpx.TimeoutException("timed out")
        sleep_calls: list[float] = []
        with patch("atlas_stf.oab_sp._client.httpx.Client") as mock_cls:
            mock_http = mock_cls.return_value
            mock_http.request.side_effect = exc
            with patch("atlas_stf.oab_sp._client.time.sleep", side_effect=lambda s: sleep_calls.append(s)):
                with OabSpClient(rate_limit=0.0, max_retries=3, retry_delay=2.0) as client:
                    with pytest.raises(RuntimeError):
                        client._request_with_retry("GET", "https://example.com")
        assert len(sleep_calls) == 3
        assert sleep_calls[0] == pytest.approx(2.0)
        assert sleep_calls[1] == pytest.approx(4.0)
        assert sleep_calls[2] == pytest.approx(8.0)

    def test_last_error_is_chained_as_cause(self):
        exc = httpx.ConnectError("fail")
        with patch("atlas_stf.oab_sp._client.httpx.Client") as mock_cls:
            mock_http = mock_cls.return_value
            mock_http.request.side_effect = exc
            with patch("atlas_stf.oab_sp._client.time.sleep"):
                with OabSpClient(rate_limit=0.0, max_retries=1, retry_delay=0.0) as client:
                    with pytest.raises(RuntimeError) as exc_info:
                        client._request_with_retry("GET", "https://example.com")
        assert exc_info.value.__cause__ is exc


# ---------------------------------------------------------------------------
# search_by_registration
# ---------------------------------------------------------------------------


class TestSearchByRegistration:
    def test_returns_html_text(self):
        html = "<html>result</html>"
        resp = _make_ok_response(html)
        with patch("atlas_stf.oab_sp._client.httpx.Client") as mock_cls:
            mock_http = mock_cls.return_value
            mock_http.request.return_value = resp
            with patch("atlas_stf.oab_sp._client.time.sleep"):
                with OabSpClient(rate_limit=0.0, retry_delay=0.0) as client:
                    result = client.search_by_registration("12345")
        assert result == html

    def test_posts_to_correct_url(self):
        resp = _make_ok_response()
        with patch("atlas_stf.oab_sp._client.httpx.Client") as mock_cls:
            mock_http = mock_cls.return_value
            mock_http.request.return_value = resp
            with patch("atlas_stf.oab_sp._client.time.sleep"):
                with OabSpClient(rate_limit=0.0, retry_delay=0.0) as client:
                    client.search_by_registration("99999")
        args, kwargs = mock_http.request.call_args
        assert args[0] == "POST"
        assert args[1] == OABSP_SEARCH_URL

    def test_sends_correct_form_data(self):
        resp = _make_ok_response()
        with patch("atlas_stf.oab_sp._client.httpx.Client") as mock_cls:
            mock_http = mock_cls.return_value
            mock_http.request.return_value = resp
            with patch("atlas_stf.oab_sp._client.time.sleep"):
                with OabSpClient(rate_limit=0.0, retry_delay=0.0) as client:
                    client.search_by_registration("55555")
        _, kwargs = mock_http.request.call_args
        data = kwargs["data"]
        assert data["tipoConsulta"] == "1"
        assert data["nr_RegistroSociedade"] == "55555"
        assert data["nm_RazaoSocial"] == ""
        assert data["tipoSociedade"] == "1"
        assert data["id_Municipio"] == "0"

    def test_sends_correct_headers(self):
        resp = _make_ok_response()
        with patch("atlas_stf.oab_sp._client.httpx.Client") as mock_cls:
            mock_http = mock_cls.return_value
            mock_http.request.return_value = resp
            with patch("atlas_stf.oab_sp._client.time.sleep"):
                with OabSpClient(rate_limit=0.0, retry_delay=0.0) as client:
                    client.search_by_registration("11111")
        _, kwargs = mock_http.request.call_args
        headers = kwargs["headers"]
        assert headers["Content-Type"] == "application/x-www-form-urlencoded"
        assert "Referer" in headers


# ---------------------------------------------------------------------------
# fetch_detail
# ---------------------------------------------------------------------------


class TestFetchDetail:
    def test_returns_html_text(self):
        html = "<html>detail</html>"
        resp = _make_ok_response(html)
        with patch("atlas_stf.oab_sp._client.httpx.Client") as mock_cls:
            mock_http = mock_cls.return_value
            mock_http.request.return_value = resp
            with patch("atlas_stf.oab_sp._client.time.sleep"):
                with OabSpClient(rate_limit=0.0, retry_delay=0.0) as client:
                    result = client.fetch_detail("abc123")
        assert result == html

    def test_gets_correct_url_with_param(self):
        resp = _make_ok_response()
        with patch("atlas_stf.oab_sp._client.httpx.Client") as mock_cls:
            mock_http = mock_cls.return_value
            mock_http.request.return_value = resp
            with patch("atlas_stf.oab_sp._client.time.sleep"):
                with OabSpClient(rate_limit=0.0, retry_delay=0.0) as client:
                    client.fetch_detail("xyz789")
        args, _ = mock_http.request.call_args
        assert args[0] == "GET"
        assert args[1] == f"{OABSP_DETAIL_URL}?param=xyz789"


# ---------------------------------------------------------------------------
# search_inscrito
# ---------------------------------------------------------------------------


class TestSearchInscrito:
    def test_search_by_registration_number(self):
        html = "<html>inscrito by reg</html>"
        resp = _make_ok_response(html)
        with patch("atlas_stf.oab_sp._client.httpx.Client") as mock_cls:
            mock_http = mock_cls.return_value
            mock_http.request.return_value = resp
            with patch("atlas_stf.oab_sp._client.time.sleep"):
                with OabSpClient(rate_limit=0.0, retry_delay=0.0) as client:
                    result = client.search_inscrito(registration_number="123456")
        assert result == html
        _, kwargs = mock_http.request.call_args
        data = kwargs["data"]
        assert data["tipo_consulta"] == "1"
        assert data["nr_inscricao"] == "123456"
        assert data["nome_advogado"] == ""
        assert data["nr_cpf"] == ""

    def test_search_by_name(self):
        html = "<html>inscrito by name</html>"
        resp = _make_ok_response(html)
        with patch("atlas_stf.oab_sp._client.httpx.Client") as mock_cls:
            mock_http = mock_cls.return_value
            mock_http.request.return_value = resp
            with patch("atlas_stf.oab_sp._client.time.sleep"):
                with OabSpClient(rate_limit=0.0, retry_delay=0.0) as client:
                    result = client.search_inscrito(name="JOAO SILVA")
        assert result == html
        _, kwargs = mock_http.request.call_args
        data = kwargs["data"]
        assert data["tipo_consulta"] == "2"
        assert data["nome_advogado"] == "JOAO SILVA"
        assert data["nr_inscricao"] == ""

    def test_search_by_name_with_city_id(self):
        resp = _make_ok_response()
        with patch("atlas_stf.oab_sp._client.httpx.Client") as mock_cls:
            mock_http = mock_cls.return_value
            mock_http.request.return_value = resp
            with patch("atlas_stf.oab_sp._client.time.sleep"):
                with OabSpClient(rate_limit=0.0, retry_delay=0.0) as client:
                    client.search_inscrito(name="MARIA", city_id="617")
        _, kwargs = mock_http.request.call_args
        data = kwargs["data"]
        assert data["idCidade"] == "617"

    def test_search_by_name_default_city_id_is_zero(self):
        resp = _make_ok_response()
        with patch("atlas_stf.oab_sp._client.httpx.Client") as mock_cls:
            mock_http = mock_cls.return_value
            mock_http.request.return_value = resp
            with patch("atlas_stf.oab_sp._client.time.sleep"):
                with OabSpClient(rate_limit=0.0, retry_delay=0.0) as client:
                    client.search_inscrito(name="CARLOS")
        _, kwargs = mock_http.request.call_args
        data = kwargs["data"]
        assert data["idCidade"] == "0"

    def test_posts_to_inscritos_url(self):
        resp = _make_ok_response()
        with patch("atlas_stf.oab_sp._client.httpx.Client") as mock_cls:
            mock_http = mock_cls.return_value
            mock_http.request.return_value = resp
            with patch("atlas_stf.oab_sp._client.time.sleep"):
                with OabSpClient(rate_limit=0.0, retry_delay=0.0) as client:
                    client.search_inscrito(registration_number="999")
        args, kwargs = mock_http.request.call_args
        assert args[0] == "POST"
        assert args[1] == OABSP_INSCRITOS_URL
        assert kwargs["headers"]["Referer"] == OABSP_INSCRITOS_URL

    def test_raises_value_error_when_no_args(self):
        with patch("atlas_stf.oab_sp._client.httpx.Client"):
            with OabSpClient(rate_limit=0.0, retry_delay=0.0) as client:
                with pytest.raises(ValueError, match="registration_number or name"):
                    client.search_inscrito()

    def test_registration_takes_priority_over_name(self):
        """When both registration_number and name are provided, registration wins."""
        resp = _make_ok_response()
        with patch("atlas_stf.oab_sp._client.httpx.Client") as mock_cls:
            mock_http = mock_cls.return_value
            mock_http.request.return_value = resp
            with patch("atlas_stf.oab_sp._client.time.sleep"):
                with OabSpClient(rate_limit=0.0, retry_delay=0.0) as client:
                    client.search_inscrito(registration_number="111", name="JOAO")
        _, kwargs = mock_http.request.call_args
        data = kwargs["data"]
        assert data["tipo_consulta"] == "1"
        assert data["nr_inscricao"] == "111"
