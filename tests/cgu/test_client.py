"""Tests for cgu/_client.py."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from atlas_stf.cgu._client import CguClient


class TestCguClient:
    def test_must_use_context_manager(self) -> None:
        client = CguClient("test-key")
        with pytest.raises(RuntimeError, match="context manager"):
            client.search_ceis({})

    @patch("atlas_stf.cgu._client.httpx.Client")
    def test_search_ceis_success(self, mock_client_cls: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.json.return_value = [{"id": 1, "sancionado": {"nome": "TEST"}}]
        mock_response.raise_for_status = MagicMock()
        mock_client_cls.return_value.get.return_value = mock_response

        with CguClient("test-key", rate_limit=0.0) as client:
            result = client.search_ceis({"nomeFantasia": "TEST"})

        assert len(result) == 1
        assert result[0]["id"] == 1
        mock_client_cls.return_value.get.assert_called_once()

    @patch("atlas_stf.cgu._client.httpx.Client")
    def test_search_cnep_success(self, mock_client_cls: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.json.return_value = [{"id": 2}]
        mock_response.raise_for_status = MagicMock()
        mock_client_cls.return_value.get.return_value = mock_response

        with CguClient("test-key", rate_limit=0.0) as client:
            result = client.search_cnep({"nomeFantasia": "ABC"})

        assert len(result) == 1

    @patch("atlas_stf.cgu._client.httpx.Client")
    def test_auth_header(self, mock_client_cls: MagicMock) -> None:
        CguClient("my-secret-key").__enter__()
        call_kwargs = mock_client_cls.call_args
        headers = call_kwargs.kwargs.get("headers", {})
        assert headers["chave-api-dados"] == "my-secret-key"

    @patch("atlas_stf.cgu._client.time.sleep")
    @patch("atlas_stf.cgu._client.httpx.Client")
    def test_retry_on_failure(self, mock_client_cls: MagicMock, mock_sleep: MagicMock) -> None:
        import httpx

        mock_client_cls.return_value.get.side_effect = [
            httpx.TransportError("connection failed"),
            MagicMock(
                json=MagicMock(return_value=[{"id": 1}]),
                raise_for_status=MagicMock(),
            ),
        ]

        with CguClient("key", rate_limit=0.0, max_retries=3) as client:
            result = client.search_ceis({})

        assert len(result) == 1
        assert mock_client_cls.return_value.get.call_count == 2

    @patch("atlas_stf.cgu._client.time.sleep")
    @patch("atlas_stf.cgu._client.httpx.Client")
    def test_all_retries_exhausted(self, mock_client_cls: MagicMock, mock_sleep: MagicMock) -> None:
        import httpx

        mock_client_cls.return_value.get.side_effect = httpx.TransportError("down")

        with CguClient("key", rate_limit=0.0, max_retries=2) as client:
            with pytest.raises(RuntimeError, match="failed after 2 attempts"):
                client.search_ceis({})

    @patch("atlas_stf.cgu._client.httpx.Client")
    def test_empty_response_returns_empty_list(self, mock_client_cls: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.json.return_value = {}
        mock_response.raise_for_status = MagicMock()
        mock_client_cls.return_value.get.return_value = mock_response

        with CguClient("key", rate_limit=0.0) as client:
            result = client.search_ceis({})

        assert result == []

    @patch("atlas_stf.cgu._client.httpx.Client")
    def test_context_manager_closes(self, mock_client_cls: MagicMock) -> None:
        with CguClient("key"):
            pass
        mock_client_cls.return_value.close.assert_called_once()
