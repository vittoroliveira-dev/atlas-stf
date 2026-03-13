"""Tests for datajud/_client.py."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from atlas_stf.datajud._client import DatajudClient


class TestDatajudClient:
    def test_must_use_context_manager(self) -> None:
        client = DatajudClient("test-key")
        with pytest.raises(RuntimeError, match="context manager"):
            client.search("idx", {})

    def test_search_rejects_invalid_index(self) -> None:
        with DatajudClient("test-key", rate_limit=0.0) as client:
            with pytest.raises(ValueError, match="Invalid DataJud index"):
                client.search("../_all", {})

    @patch("atlas_stf.datajud._client.httpx.Client")
    def test_search_success(self, mock_client_cls: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.json.return_value = {"hits": {"total": {"value": 10}}}
        mock_response.raise_for_status = MagicMock()
        mock_client_cls.return_value.post.return_value = mock_response

        with DatajudClient("test-key", rate_limit=0.0) as client:
            result = client.search("api_publica_tjsp", {"size": 0})

        assert result["hits"]["total"]["value"] == 10
        mock_client_cls.return_value.post.assert_called_once()

    @patch("atlas_stf.datajud._client.httpx.Client")
    def test_auth_header(self, mock_client_cls: MagicMock) -> None:
        DatajudClient("my-secret-key").__enter__()
        call_kwargs = mock_client_cls.call_args
        headers = call_kwargs.kwargs.get("headers", {})
        assert headers["Authorization"] == "APIKey my-secret-key"

    @patch("atlas_stf.datajud._client.time.sleep")
    @patch("atlas_stf.datajud._client.httpx.Client")
    def test_retry_on_failure(self, mock_client_cls: MagicMock, mock_sleep: MagicMock) -> None:
        import httpx

        mock_client_cls.return_value.post.side_effect = [
            httpx.TransportError("connection failed"),
            MagicMock(
                json=MagicMock(return_value={"hits": {"total": {"value": 5}}}),
                raise_for_status=MagicMock(),
            ),
        ]

        with DatajudClient("key", rate_limit=0.0, max_retries=3) as client:
            result = client.search("api_publica_tjsp", {})

        assert result["hits"]["total"]["value"] == 5
        assert mock_client_cls.return_value.post.call_count == 2

    @patch("atlas_stf.datajud._client.time.sleep")
    @patch("atlas_stf.datajud._client.httpx.Client")
    def test_all_retries_exhausted(self, mock_client_cls: MagicMock, mock_sleep: MagicMock) -> None:
        import httpx

        mock_client_cls.return_value.post.side_effect = httpx.TransportError("down")

        with DatajudClient("key", rate_limit=0.0, max_retries=2) as client:
            with pytest.raises(RuntimeError, match="failed after 2 attempts"):
                client.search("api_publica_tjsp", {})
