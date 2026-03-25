"""Tests for core/fetch_result.py — structured fetch result logging."""

from __future__ import annotations

import json
import logging
import time

import pytest

from atlas_stf.core.fetch_result import FetchResult, FetchTimer


class TestFetchResult:
    def test_success_exit_code(self) -> None:
        r = FetchResult(source="tse", status="success", records_written=100, duration_seconds=5.0, exit_code=0)
        assert r.exit_code == 0

    def test_failed_exit_code(self) -> None:
        r = FetchResult(source="tse", status="failed", records_written=0, duration_seconds=1.0, exit_code=2)
        assert r.exit_code == 2

    def test_log_emits_json(self, caplog) -> None:
        with caplog.at_level(logging.INFO):
            r = FetchResult(source="cgu", status="success", records_written=42, duration_seconds=3.5, exit_code=0)
            r.log()
        assert "FETCH_RESULT" in caplog.text
        for record in caplog.records:
            if "FETCH_RESULT" in record.message:
                json_str = record.message.split("FETCH_RESULT ", 1)[1]
                data = json.loads(json_str)
                assert data["source"] == "cgu"
                assert data["records_written"] == 42
                break
        else:
            raise AssertionError("No FETCH_RESULT log found")  # pragma: no cover


class TestFetchTimer:
    def test_measures_elapsed(self) -> None:
        timer = FetchTimer("test")
        with timer:
            time.sleep(0.05)
        assert timer.elapsed >= 0.04

    def test_result_creates_fetch_result(self) -> None:
        timer = FetchTimer("cvm")
        with timer:
            pass
        result = timer.result(status="success", records_written=10)
        assert result.source == "cvm"
        assert result.status == "success"
        assert result.exit_code == 0
        assert result.records_written == 10
        assert result.duration_seconds >= 0.0

    def test_result_failed(self) -> None:
        timer = FetchTimer("rfb")
        with timer:
            pass
        result = timer.result(status="failed", detail="connection timeout")
        assert result.exit_code == 2
        assert result.detail == "connection timeout"

    def test_result_partial(self) -> None:
        timer = FetchTimer("tse")
        with timer:
            pass
        result = timer.result(status="partial", records_written=5)
        assert result.exit_code == 1

    def test_log_success_captures_elapsed(self) -> None:
        timer = FetchTimer("test")
        timer.start()
        time.sleep(0.02)
        timer.log_success(records_written=10)
        assert timer.elapsed >= 0.01
        assert timer._result_logged is True

    def test_log_failure_captures_elapsed(self) -> None:
        timer = FetchTimer("test")
        timer.start()
        time.sleep(0.02)
        timer.log_failure(ValueError("boom"))
        assert timer.elapsed >= 0.01
        assert timer._result_logged is True

    def test_log_failure_idempotent(self, caplog) -> None:
        """Calling log_failure twice should only emit one FETCH_RESULT."""
        timer = FetchTimer("test")
        timer.start()
        with caplog.at_level(logging.INFO):
            timer.log_failure(ValueError("first"))
            timer.log_failure(ValueError("second"))
        count = caplog.text.count("FETCH_RESULT")
        assert count == 1

    def test_context_manager_auto_logs_failure(self, caplog) -> None:
        """__exit__ should auto-log failure when exception is raised and no log_success called."""
        with caplog.at_level(logging.INFO):
            with pytest.raises(ValueError, match="kaboom"):
                with FetchTimer("auto") as timer:
                    raise ValueError("kaboom")
        assert timer._result_logged is True
        assert "FETCH_RESULT" in caplog.text
        assert '"status": "failed"' in caplog.text
        assert "kaboom" in caplog.text

    def test_context_manager_no_double_log_on_success(self, caplog) -> None:
        """If log_success is called, __exit__ should not log again."""
        with caplog.at_level(logging.INFO):
            with FetchTimer("nodouble") as timer:
                timer.log_success(records_written=5)
        count = caplog.text.count("FETCH_RESULT")
        assert count == 1

    def test_start_log_failure_pattern(self, caplog) -> None:
        """The explicit start() + try/except + log_failure() pattern used by runners."""
        timer = FetchTimer("explicit")
        timer.start()
        with caplog.at_level(logging.INFO):
            try:
                raise RuntimeError("network error")
            except Exception as exc:
                timer.log_failure(exc)
        assert "FETCH_RESULT" in caplog.text
        assert '"status": "failed"' in caplog.text
        assert "RuntimeError: network error" in caplog.text

    def test_context_manager_keyboard_interrupt_not_logged_as_failure(self, caplog) -> None:
        """KeyboardInterrupt is a cancellation, not a fetch failure — __exit__ must not log it."""
        with caplog.at_level(logging.INFO):
            with pytest.raises(KeyboardInterrupt):
                with FetchTimer("cancel") as timer:
                    raise KeyboardInterrupt
        assert timer._result_logged is False
        assert "FETCH_RESULT" not in caplog.text

    def test_context_manager_system_exit_not_logged_as_failure(self, caplog) -> None:
        """SystemExit is a cancellation, not a fetch failure — __exit__ must not log it."""
        with caplog.at_level(logging.INFO):
            with pytest.raises(SystemExit):
                with FetchTimer("exit") as timer:
                    raise SystemExit(1)
        assert timer._result_logged is False
        assert "FETCH_RESULT" not in caplog.text
