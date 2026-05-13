"""Tests for DEOAB runner helpers."""

from __future__ import annotations

from unittest.mock import MagicMock

import httpx

from atlas_stf.deoab._runner import _probe_pdf


def _make_response(status_code: int, headers: dict[str, str] | None = None) -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.headers = headers or {}
    return response


def test_probe_pdf_returns_zero_for_invalid_content_length() -> None:
    client = MagicMock()
    client.head.return_value = _make_response(200, {"content-length": "not-a-number"})

    assert _probe_pdf(client, "https://example.test/deoab.pdf") == 0


def test_probe_pdf_returns_zero_when_content_length_missing() -> None:
    client = MagicMock()
    client.head.return_value = _make_response(200)

    assert _probe_pdf(client, "https://example.test/deoab.pdf") == 0


def test_probe_pdf_returns_zero_when_content_length_below_threshold() -> None:
    client = MagicMock()
    client.head.return_value = _make_response(200, {"content-length": "5000"})

    assert _probe_pdf(client, "https://example.test/deoab.pdf") == 0


def test_probe_pdf_returns_length_when_content_length_above_threshold() -> None:
    client = MagicMock()
    client.head.return_value = _make_response(200, {"content-length": "5001"})

    assert _probe_pdf(client, "https://example.test/deoab.pdf") == 5001


def test_probe_pdf_returns_zero_for_non_200_status() -> None:
    client = MagicMock()
    client.head.return_value = _make_response(404, {"content-length": "99999"})

    assert _probe_pdf(client, "https://example.test/deoab.pdf") == 0


def test_probe_pdf_returns_zero_on_http_error() -> None:
    client = MagicMock()
    client.head.side_effect = httpx.TransportError("network down")

    assert _probe_pdf(client, "https://example.test/deoab.pdf") == 0
