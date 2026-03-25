"""Tests for fetch/_remote_probe.py — HEAD/Range probing logic."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx

from atlas_stf.fetch._manifest_model import FetchUnit, RefreshPolicy, RemoteState
from atlas_stf.fetch._remote_probe import _probe_head, _probe_range, probe_remote_state

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_unit(
    unit_id: str = "tse:donations:2022",
    source: str = "tse_donations",
    remote_url: str = "https://example.com/file.zip",
) -> FetchUnit:
    return FetchUnit(
        unit_id=unit_id,
        source=source,
        label="Test unit",
        remote_url=remote_url,
        remote_state=RemoteState(url=remote_url),
    )


def _make_policy(
    source: str = "tse_donations",
    comparators: tuple[str, ...] = ("etag", "size"),
    force_refresh_supported: bool = True,
) -> RefreshPolicy:
    return RefreshPolicy(
        source=source,
        comparators=comparators,  # type: ignore[arg-type]
        freshness_window="monthly",
        force_refresh_supported=force_refresh_supported,
    )


def _mock_head_response(
    status_code: int = 200,
    etag: str = '"abc123"',
    content_length: str = "1024",
    last_modified: str = "Wed, 01 Jan 2025 00:00:00 GMT",
) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.headers = {
        "etag": etag,
        "content-length": content_length,
        "last-modified": last_modified,
    }
    resp.raise_for_status = MagicMock()
    return resp


def _mock_range_response(
    status_code: int = 206,
    content_range: str = "bytes 0-0/98765",
    last_modified: str = "Tue, 10 Jan 2023 12:00:00 GMT",
    content_length_fallback: str = "0",
) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    headers: dict[str, str] = {"last-modified": last_modified}
    if status_code == 206:
        headers["content-range"] = content_range
    else:
        headers["content-length"] = content_length_fallback
    resp.headers = headers
    return resp


# ---------------------------------------------------------------------------
# probe_remote_state — DataJud branch
# ---------------------------------------------------------------------------


class TestProbeRemoteStateDatajud:
    def test_datajud_returns_empty_state_without_http_call(self) -> None:
        unit = _make_unit(source="datajud", remote_url="https://datajud.example.com/api")
        policy = RefreshPolicy(
            source="datajud",
            comparators=("version_string",),
            freshness_window="weekly",
            force_refresh_supported=False,
        )
        with patch("atlas_stf.fetch._remote_probe.httpx.head") as mock_head:
            result = probe_remote_state("datajud", unit, policy)
        mock_head.assert_not_called()
        assert result.url == unit.remote_url
        assert result.etag == ""
        assert result.content_length == 0
        assert result.last_modified == ""

    def test_datajud_probed_at_is_set(self) -> None:
        unit = _make_unit(source="datajud", remote_url="https://datajud.example.com/api")
        policy = RefreshPolicy(
            source="datajud",
            comparators=("version_string",),
            freshness_window="weekly",
            force_refresh_supported=False,
        )
        result = probe_remote_state("datajud", unit, policy)
        assert result.probed_at != ""
        # ISO 8601 format check
        assert "T" in result.probed_at


# ---------------------------------------------------------------------------
# probe_remote_state — empty URL branch
# ---------------------------------------------------------------------------


class TestProbeRemoteStateEmptyUrl:
    def test_empty_url_returns_empty_state_without_http_call(self) -> None:
        unit = _make_unit(remote_url="")
        policy = _make_policy()
        with patch("atlas_stf.fetch._remote_probe.httpx.head") as mock_head:
            result = probe_remote_state("tse_donations", unit, policy)
        mock_head.assert_not_called()
        assert result.url == ""
        assert result.content_length == 0


# ---------------------------------------------------------------------------
# probe_remote_state — HEAD branch (etag / size comparators)
# ---------------------------------------------------------------------------


class TestProbeRemoteStateHead:
    def test_head_request_returns_etag_and_content_length(self) -> None:
        unit = _make_unit()
        policy = _make_policy(comparators=("etag", "size"))
        mock_resp = _mock_head_response(etag='"v42"', content_length="2048")

        with patch("atlas_stf.fetch._remote_probe.httpx.head", return_value=mock_resp):
            result = probe_remote_state("tse_donations", unit, policy)

        assert result.url == unit.remote_url
        assert result.etag == '"v42"'
        assert result.content_length == 2048
        assert result.last_modified == "Wed, 01 Jan 2025 00:00:00 GMT"

    def test_head_request_missing_etag_returns_empty_string(self) -> None:
        unit = _make_unit()
        policy = _make_policy(comparators=("size",))
        mock_resp = MagicMock(spec=httpx.Response)
        mock_resp.status_code = 200
        mock_resp.headers = {"content-length": "512"}
        mock_resp.raise_for_status = MagicMock()

        with patch("atlas_stf.fetch._remote_probe.httpx.head", return_value=mock_resp):
            result = probe_remote_state("rfb", unit, policy)

        assert result.etag == ""
        assert result.content_length == 512
        assert result.last_modified == ""

    def test_head_missing_content_length_defaults_to_zero(self) -> None:
        unit = _make_unit()
        policy = _make_policy(comparators=("etag",))
        mock_resp = MagicMock(spec=httpx.Response)
        mock_resp.status_code = 200
        mock_resp.headers = {"etag": '"xyz"'}
        mock_resp.raise_for_status = MagicMock()

        with patch("atlas_stf.fetch._remote_probe.httpx.head", return_value=mock_resp):
            result = probe_remote_state("tse_donations", unit, policy)

        assert result.content_length == 0

    def test_probed_at_is_set_on_successful_head(self) -> None:
        unit = _make_unit()
        policy = _make_policy()
        mock_resp = _mock_head_response()

        with patch("atlas_stf.fetch._remote_probe.httpx.head", return_value=mock_resp):
            result = probe_remote_state("tse_donations", unit, policy)

        assert result.probed_at != ""


# ---------------------------------------------------------------------------
# probe_remote_state — Range branch (content_length_date comparator)
# ---------------------------------------------------------------------------


class TestProbeRemoteStateRange:
    def test_range_request_used_when_comparator_is_content_length_date(self) -> None:
        unit = _make_unit()
        policy = _make_policy(comparators=("content_length_date",))
        mock_resp = _mock_range_response(status_code=206, content_range="bytes 0-0/55000")

        with (
            patch("atlas_stf.fetch._remote_probe.httpx.get", return_value=mock_resp) as mock_get,
            patch("atlas_stf.fetch._remote_probe.httpx.head") as mock_head,
        ):
            result = probe_remote_state("cgu", unit, policy)

        mock_get.assert_called_once()
        mock_head.assert_not_called()
        assert result.content_length == 55000

    def test_range_206_parses_content_range_header(self) -> None:
        unit = _make_unit()
        policy = _make_policy(comparators=("content_length_date",))
        mock_resp = _mock_range_response(
            status_code=206,
            content_range="bytes 0-0/123456",
            last_modified="Mon, 01 Jan 2024 00:00:00 GMT",
        )

        with patch("atlas_stf.fetch._remote_probe.httpx.get", return_value=mock_resp):
            result = probe_remote_state("cgu", unit, policy)

        assert result.content_length == 123456
        assert result.last_modified == "Mon, 01 Jan 2024 00:00:00 GMT"

    def test_range_200_fallback_uses_content_length_header(self) -> None:
        unit = _make_unit()
        policy = _make_policy(comparators=("content_length_date",))
        mock_resp = _mock_range_response(
            status_code=200,
            content_length_fallback="77777",
        )

        with patch("atlas_stf.fetch._remote_probe.httpx.get", return_value=mock_resp):
            result = probe_remote_state("cgu", unit, policy)

        assert result.content_length == 77777

    def test_range_206_malformed_content_range_defaults_to_zero(self) -> None:
        unit = _make_unit()
        policy = _make_policy(comparators=("content_length_date",))
        mock_resp = MagicMock(spec=httpx.Response)
        mock_resp.status_code = 206
        mock_resp.headers = {"content-range": "bytes 0-0/BADVALUE", "last-modified": ""}

        with patch("atlas_stf.fetch._remote_probe.httpx.get", return_value=mock_resp):
            result = probe_remote_state("cgu", unit, policy)

        assert result.content_length == 0

    def test_range_206_no_slash_in_content_range_defaults_to_zero(self) -> None:
        unit = _make_unit()
        policy = _make_policy(comparators=("content_length_date",))
        mock_resp = MagicMock(spec=httpx.Response)
        mock_resp.status_code = 206
        mock_resp.headers = {"content-range": "bytes 0-0", "last-modified": ""}

        with patch("atlas_stf.fetch._remote_probe.httpx.get", return_value=mock_resp):
            result = probe_remote_state("cgu", unit, policy)

        assert result.content_length == 0


# ---------------------------------------------------------------------------
# probe_remote_state — network failure handling
# ---------------------------------------------------------------------------


class TestProbeRemoteStateNetworkError:
    def test_connect_error_returns_empty_remote_state(self) -> None:
        unit = _make_unit()
        policy = _make_policy()

        with patch(
            "atlas_stf.fetch._remote_probe.httpx.head",
            side_effect=httpx.ConnectError("connection refused"),
        ):
            result = probe_remote_state("tse_donations", unit, policy)

        assert result.url == unit.remote_url
        assert result.etag == ""
        assert result.content_length == 0
        assert result.probed_at != ""

    def test_timeout_error_returns_empty_remote_state(self) -> None:
        unit = _make_unit()
        policy = _make_policy()

        with patch(
            "atlas_stf.fetch._remote_probe.httpx.head",
            side_effect=httpx.TimeoutException("timeout"),
        ):
            result = probe_remote_state("tse_donations", unit, policy)

        assert result.url == unit.remote_url
        assert result.etag == ""

    def test_range_network_error_returns_empty_state(self) -> None:
        unit = _make_unit()
        policy = _make_policy(comparators=("content_length_date",))

        with patch(
            "atlas_stf.fetch._remote_probe.httpx.get",
            side_effect=httpx.ConnectError("refused"),
        ):
            result = probe_remote_state("cgu", unit, policy)

        assert result.content_length == 0
        assert result.url == unit.remote_url

    def test_network_error_does_not_raise(self) -> None:
        unit = _make_unit()
        policy = _make_policy()

        with patch(
            "atlas_stf.fetch._remote_probe.httpx.head",
            side_effect=httpx.NetworkError("net err"),
        ):
            # must not raise
            result = probe_remote_state("tse_donations", unit, policy)
        assert isinstance(result, RemoteState)


# ---------------------------------------------------------------------------
# _probe_head — direct unit tests
# ---------------------------------------------------------------------------


class TestProbeHead:
    def test_all_headers_present(self) -> None:
        mock_resp = _mock_head_response(
            etag='"etag-value"',
            content_length="4096",
            last_modified="Fri, 01 Mar 2024 00:00:00 GMT",
        )
        with patch("atlas_stf.fetch._remote_probe.httpx.head", return_value=mock_resp):
            result = _probe_head(
                "https://example.com/file.zip",
                timeout=30,
                probed_at="2026-01-01T00:00:00+00:00",
            )

        assert result.etag == '"etag-value"'
        assert result.content_length == 4096
        assert result.last_modified == "Fri, 01 Mar 2024 00:00:00 GMT"
        assert result.probed_at == "2026-01-01T00:00:00+00:00"
        assert result.url == "https://example.com/file.zip"


# ---------------------------------------------------------------------------
# _probe_range — direct unit tests
# ---------------------------------------------------------------------------


class TestProbeRange:
    def test_206_response_extracted_correctly(self) -> None:
        mock_resp = _mock_range_response(
            status_code=206,
            content_range="bytes 0-0/999999",
            last_modified="Thu, 01 Feb 2024 00:00:00 GMT",
        )
        with patch("atlas_stf.fetch._remote_probe.httpx.get", return_value=mock_resp):
            result = _probe_range(
                "https://cgu.example.com/big.zip",
                timeout=30,
                probed_at="2026-01-01T00:00:00+00:00",
            )

        assert result.content_length == 999999
        assert result.last_modified == "Thu, 01 Feb 2024 00:00:00 GMT"
        assert result.url == "https://cgu.example.com/big.zip"
        assert result.probed_at == "2026-01-01T00:00:00+00:00"
        # Range probe never sets etag
        assert result.etag == ""
