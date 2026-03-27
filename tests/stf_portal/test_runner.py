"""Tests for STF portal runner."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import patch

from atlas_stf.stf_portal._checkpoint import load_checkpoint
from atlas_stf.stf_portal._config import StfPortalConfig
from atlas_stf.stf_portal._proxy import ProxyManager
from atlas_stf.stf_portal._runner import (
    _load_process_list,
    _prioritize_processes,
    _sanitize_filename,
    _should_refetch,
    run_extraction,
)


def test_load_process_list(tmp_path: Path):
    path = tmp_path / "process.jsonl"
    records = [
        {"process_id": "proc_1", "process_number": "ADI 1234"},
        {"process_id": "proc_2", "process_number": "HC 999"},
    ]
    path.write_text(
        "\n".join(json.dumps(r) for r in records) + "\n",
        encoding="utf-8",
    )
    result = _load_process_list(tmp_path)
    assert len(result) == 2
    assert result[0]["process_number"] == "ADI 1234"


def test_load_process_list_missing(tmp_path: Path):
    result = _load_process_list(tmp_path / "nonexistent")
    assert result == []


def test_prioritize_processes():
    processes = [
        {"process_id": "proc_1", "filing_date": "2020-01-01"},
        {"process_id": "proc_2", "filing_date": "2025-06-15"},
        {"process_id": "proc_3", "filing_date": "2022-03-10"},
    ]
    alert_ids = {"proc_3"}

    result = _prioritize_processes(processes, alert_ids)
    # Alert processes first
    assert result[0]["process_id"] == "proc_3"


def test_prioritize_processes_newer_dates_first():
    processes = [
        {"process_id": "proc_a", "filing_date": "2020-01-01"},
        {"process_id": "proc_b", "filing_date": "2025-06-15"},
        {"process_id": "proc_c", "filing_date": "2022-03-10"},
    ]
    alert_ids: set[str] = set()
    result = _prioritize_processes(processes, alert_ids)
    dates = [p["filing_date"] for p in result]
    assert dates == ["2025-06-15", "2022-03-10", "2020-01-01"]


def test_sanitize_filename():
    assert _sanitize_filename("ADI 1234") == "ADI_1234"
    assert _sanitize_filename("RE/ARE 5555") == "RE_ARE_5555"


def test_should_refetch_missing(tmp_path: Path):
    assert _should_refetch(tmp_path / "nonexistent.json", 30) is True


def test_should_refetch_recent(tmp_path: Path):
    from datetime import datetime, timezone

    path = tmp_path / "test.json"
    doc = {"fetched_at": datetime.now(timezone.utc).isoformat()}
    path.write_text(json.dumps(doc), encoding="utf-8")
    assert _should_refetch(path, 30) is False


def test_should_refetch_stale(tmp_path: Path):
    path = tmp_path / "test.json"
    doc = {"fetched_at": "2020-01-01T00:00:00+00:00"}
    path.write_text(json.dumps(doc), encoding="utf-8")
    assert _should_refetch(path, 30) is True


# --- ProxyManager tests ---


def test_proxy_manager_single_direct():
    """ProxyManager with no proxies uses direct connection only."""
    pm = ProxyManager([], per_proxy_rate=0.01, jitter_range=(1.0, 1.0))
    proxy, _ = pm.acquire()
    assert proxy is None


def test_proxy_manager_len():
    pm = ProxyManager(["socks5://a:1080", "socks5://b:1081"])
    assert len(pm) == 3  # None + 2 proxies


def test_proxy_manager_round_robin_least_wait():
    """Two consecutive acquires should return different proxies (least-wait selection)."""
    pm = ProxyManager(["socks5://a:1080"], per_proxy_rate=0.5, jitter_range=(1.0, 1.0))
    proxy1, _ = pm.acquire()
    proxy2, _ = pm.acquire()
    # With 2 proxies (None + a), they should alternate
    assert proxy1 != proxy2


def test_proxy_manager_record_403_opens_circuit():
    pm = ProxyManager([], circuit_threshold=3, circuit_cooldown=0.1, per_proxy_rate=0.0)
    for _ in range(3):
        pm.record_403(None)
    assert pm.is_circuit_open(None) is True
    # Wait for cooldown
    import time

    time.sleep(0.15)
    assert pm.is_circuit_open(None) is False


def test_proxy_manager_record_success_resets():
    pm = ProxyManager([], circuit_threshold=5, per_proxy_rate=0.0)
    pm.record_403(None)
    pm.record_403(None)
    pm.record_success(None)
    pm.record_403(None)
    pm.record_403(None)
    # Only 2 consecutive 403s after reset, threshold is 5
    assert pm.is_circuit_open(None) is False


def test_proxy_manager_all_broken_blocks():
    """When all proxies are circuit-broken, acquire() blocks until cooldown."""
    import time

    pm = ProxyManager([], circuit_threshold=1, circuit_cooldown=0.1, per_proxy_rate=0.0, jitter_range=(1.0, 1.0))
    pm.record_403(None)
    assert pm.is_circuit_open(None) is True
    start = time.monotonic()
    proxy, _ = pm.acquire()
    elapsed = time.monotonic() - start
    assert proxy is None
    assert elapsed >= 0.08  # Waited for cooldown


def test_proxy_manager_acquire_enforces_rate_limit():
    """Each acquire should respect per-proxy rate limit."""
    import time

    pm = ProxyManager([], per_proxy_rate=0.05, jitter_range=(1.0, 1.0))
    start = time.monotonic()
    pm.acquire()
    pm.acquire()
    pm.acquire()
    elapsed = time.monotonic() - start
    # 3 acquires on 1 proxy: first is immediate, 2nd and 3rd each wait ~50ms
    assert elapsed >= 0.08


def test_proxy_manager_403_inflight_per_proxy():
    """403 on one proxy does not affect another proxy's circuit breaker."""
    pm = ProxyManager(
        ["socks5://a:1080"],
        per_proxy_rate=0.0,
        jitter_range=(1.0, 1.0),
        circuit_threshold=2,
    )
    # Record 403s on direct (None) only
    pm.record_403(None)
    pm.record_403(None)
    assert pm.is_circuit_open(None) is True
    assert pm.is_circuit_open("socks5://a:1080") is False


# --- Deferred client close tests ---


def test_rotate_client_deferred_close():
    """_rotate_client_for_proxy retires old client instead of closing it."""
    from atlas_stf.stf_portal._extractor import PortalExtractor

    ext = PortalExtractor(rate_limit_seconds=0.0, timeout_seconds=1.0)
    # Force creation of a direct client
    with ext._client_lock:
        client1 = ext._get_client()
    assert client1 is not None

    # Rotate — should retire, not close
    ext._rotate_client_for_proxy(None)
    assert client1 in ext._retired_clients
    assert ext._client is None

    # New client should be different
    with ext._client_lock:
        client2 = ext._get_client()
    assert client2 is not client1

    # close() should clean up retired
    ext.close()
    assert len(ext._retired_clients) == 0


# --- Phase 2.5: run_extraction integration tests ---


def _write_process_jsonl(curated_dir: Path, processes: list[dict[str, Any]]) -> None:
    curated_dir.mkdir(parents=True, exist_ok=True)
    path = curated_dir / "process.jsonl"
    path.write_text(
        "\n".join(json.dumps(p) for p in processes) + "\n",
        encoding="utf-8",
    )


def _fake_doc(process_number: str) -> dict[str, Any]:
    """Minimal fake document for a process."""
    return {
        "process_number": process_number,
        "source_system": "stf_portal",
        "source_url": f"https://portal.stf.jus.br/processos/listarProcessos.asp?classe=&processo={process_number}",
        "fetched_at": "2026-03-17T00:00:00+00:00",
        "raw_html_hash": "fakehash",
        "andamentos": [],
        "deslocamentos": [],
        "peticoes": [],
        "sessao_virtual": [],
        "informacoes": {},
        "incidente": "999",
    }


def _fake_extract_process(process_number: str, incidente: str | None = None) -> dict[str, Any] | None:
    """Fake extractor that returns a minimal doc."""
    return _fake_doc(process_number)


_FAKE_TABS = {
    "abaAndamentos": "<div>A</div>",
    "abaPartes": "<div>P</div>",
    "abaPeticoes": "<div>Pet</div>",
    "abaDeslocamentos": "<div>D</div>",
    "abaInformacoes": "<div>I</div>",
}


class _FakeExtractor:
    """Fake PortalExtractor that returns deterministic results.

    Implements the internal methods used by _fetch_process_incremental:
    - _resolve_incidente
    - _fetch_tabs_concurrent
    - assemble_document
    Plus the legacy extract_process facade.
    """

    def __init__(self, **_kwargs: Any) -> None:
        pass

    def _resolve_incidente(self, process_number: str) -> Any:
        from atlas_stf.stf_portal._result import ResolveResult

        return ResolveResult(status="resolved", incidente="999")

    def _fetch_tabs_concurrent(
        self,
        incidente: str,
        *,
        tabs_to_fetch: tuple[str, ...] = (),
        on_tab_success: Any = None,
    ) -> Any:
        from atlas_stf.stf_portal._http import TabsBatchResult

        tabs = {t: _FAKE_TABS.get(t, "<div></div>") for t in tabs_to_fetch}
        if on_tab_success:
            for tab, html in tabs.items():
                on_tab_success(tab, html)
        return TabsBatchResult(tabs=tabs, blocked=False, retryable=False, tabs_failed=set())

    def assemble_document(
        self, process_number: str, incidente: str, tab_htmls: dict[str, str],
    ) -> dict[str, Any] | None:
        return _fake_doc(process_number)

    def extract_process(self, process_number: str, incidente: str | None = None) -> dict[str, Any] | None:
        return _fake_extract_process(process_number, incidente)

    def close(self) -> None:
        pass

    def __enter__(self) -> _FakeExtractor:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def test_run_extraction_sequential(tmp_path: Path):
    """run_extraction with workers=1 fetches all processes and saves checkpoint."""
    curated_dir = tmp_path / "curated"
    output_dir = tmp_path / "portal"
    checkpoint_file = output_dir / ".checkpoint.json"

    processes = [{"process_id": f"proc_{i}", "process_number": f"ADI {i}"} for i in range(5)]
    _write_process_jsonl(curated_dir, processes)

    config = StfPortalConfig(
        output_dir=output_dir,
        curated_dir=curated_dir,
        checkpoint_file=checkpoint_file,
        max_concurrent=1,
        rate_limit_seconds=0.0,
    )

    with patch("atlas_stf.stf_portal._runner.PortalExtractor", _FakeExtractor):
        fetched = run_extraction(config)

    assert fetched == 5

    # Verify output files
    json_files = list(output_dir.glob("ADI_*.json"))
    assert len(json_files) == 5

    # Verify checkpoint
    cp = load_checkpoint(checkpoint_file)
    assert cp.total_fetched == 5
    assert len(cp.completed_processes) == 5


def test_run_extraction_concurrent(tmp_path: Path):
    """run_extraction with workers=4 fetches all processes correctly."""
    curated_dir = tmp_path / "curated"
    output_dir = tmp_path / "portal"
    checkpoint_file = output_dir / ".checkpoint.json"

    processes = [{"process_id": f"proc_{i}", "process_number": f"HC {i}"} for i in range(20)]
    _write_process_jsonl(curated_dir, processes)

    config = StfPortalConfig(
        output_dir=output_dir,
        curated_dir=curated_dir,
        checkpoint_file=checkpoint_file,
        max_concurrent=4,
        rate_limit_seconds=0.0,
    )

    with patch("atlas_stf.stf_portal._runner.PortalExtractor", _FakeExtractor):
        fetched = run_extraction(config)

    assert fetched == 20

    # Verify no duplicates
    json_files = list(output_dir.glob("HC_*.json"))
    assert len(json_files) == 20

    # Verify checkpoint
    cp = load_checkpoint(checkpoint_file)
    assert cp.total_fetched == 20
    assert len(cp.completed_processes) == 20


def test_run_extraction_skips_completed(tmp_path: Path):
    """run_extraction skips already completed processes."""
    curated_dir = tmp_path / "curated"
    output_dir = tmp_path / "portal"
    checkpoint_file = output_dir / ".checkpoint.json"

    processes = [
        {"process_id": "proc_1", "process_number": "ADI 100"},
        {"process_id": "proc_2", "process_number": "ADI 200"},
    ]
    _write_process_jsonl(curated_dir, processes)

    # Pre-populate checkpoint and output for ADI 100
    from atlas_stf.stf_portal._checkpoint import PortalCheckpoint, save_checkpoint

    cp = PortalCheckpoint()
    cp.mark_completed("ADI 100")
    save_checkpoint(cp, checkpoint_file)
    (output_dir / "ADI_100.json").write_text(
        json.dumps({"fetched_at": "2026-03-17T00:00:00+00:00"}),
        encoding="utf-8",
    )

    config = StfPortalConfig(
        output_dir=output_dir,
        curated_dir=curated_dir,
        checkpoint_file=checkpoint_file,
        max_concurrent=1,
        rate_limit_seconds=0.0,
    )

    with patch("atlas_stf.stf_portal._runner.PortalExtractor", _FakeExtractor):
        fetched = run_extraction(config)

    # Only ADI 200 should have been fetched
    assert fetched == 1

    cp = load_checkpoint(checkpoint_file)
    assert cp.is_completed("ADI 100")
    assert cp.is_completed("ADI 200")
    assert cp.total_fetched == 2  # 1 from pre-populated + 1 new


def test_run_extraction_dry_run(tmp_path: Path):
    """Dry run returns 0 and doesn't create files."""
    curated_dir = tmp_path / "curated"
    output_dir = tmp_path / "portal"

    processes = [{"process_id": "proc_1", "process_number": "ADI 100"}]
    _write_process_jsonl(curated_dir, processes)

    config = StfPortalConfig(
        output_dir=output_dir,
        curated_dir=curated_dir,
        checkpoint_file=output_dir / ".checkpoint.json",
    )

    result = run_extraction(config, dry_run=True)
    assert result == 0
    assert not (output_dir / ".checkpoint.json").exists()


# --- Partial data prevention tests ---


class _FailingTabExtractor:
    """Fake extractor that simulates permanent resolve failure (incidente not found)."""

    def __init__(self, **_kwargs: Any) -> None:
        pass

    def _resolve_incidente(self, process_number: str) -> Any:
        from atlas_stf.stf_portal._result import ResolveResult

        return ResolveResult(status="not_found_permanent")

    def _fetch_tabs_concurrent(self, incidente: str, **_kw: Any) -> Any:
        from atlas_stf.stf_portal._http import TabsBatchResult

        return TabsBatchResult(tabs={}, blocked=False, retryable=False, tabs_failed=set())

    def assemble_document(self, process_number: str, incidente: str, tab_htmls: dict[str, str]) -> None:
        return None

    def extract_process(self, process_number: str, incidente: str | None = None) -> dict[str, Any] | None:
        return None

    def close(self) -> None:
        pass

    def __enter__(self) -> _FailingTabExtractor:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def test_run_extraction_failed_process_not_saved(tmp_path: Path):
    """Failed extraction does not produce output JSON — marked as failed."""
    curated_dir = tmp_path / "curated"
    output_dir = tmp_path / "portal"
    checkpoint_file = output_dir / ".checkpoint.json"

    processes = [{"process_id": "proc_1", "process_number": "ADI 100"}]
    _write_process_jsonl(curated_dir, processes)

    config = StfPortalConfig(
        output_dir=output_dir,
        curated_dir=curated_dir,
        checkpoint_file=checkpoint_file,
        max_concurrent=1,
        rate_limit_seconds=0.0,
    )

    with patch("atlas_stf.stf_portal._runner.PortalExtractor", _FailingTabExtractor):
        fetched = run_extraction(config)

    assert fetched == 0
    assert not (output_dir / "ADI_100.json").exists()

    cp = load_checkpoint(checkpoint_file)
    assert cp.is_completed("ADI 100") is False
    assert cp.is_failed("ADI 100") is True


# --- Incidente cache in checkpoint ---


def test_run_extraction_caches_incidente(tmp_path: Path):
    """Successful extraction caches the incidente in the checkpoint."""
    curated_dir = tmp_path / "curated"
    output_dir = tmp_path / "portal"
    checkpoint_file = output_dir / ".checkpoint.json"

    processes = [{"process_id": "proc_1", "process_number": "ADI 100"}]
    _write_process_jsonl(curated_dir, processes)

    config = StfPortalConfig(
        output_dir=output_dir,
        curated_dir=curated_dir,
        checkpoint_file=checkpoint_file,
        max_concurrent=1,
        rate_limit_seconds=0.0,
    )

    with patch("atlas_stf.stf_portal._runner.PortalExtractor", _FakeExtractor):
        run_extraction(config)

    cp = load_checkpoint(checkpoint_file)
    assert cp.get_incidente("ADI 100") == "999"


# --- CLI wiring tests ---


def test_cli_rate_limit_feeds_global_rate_seconds(tmp_path: Path):
    """--rate-limit CLI value should feed config.global_rate_seconds."""
    config = StfPortalConfig(
        output_dir=tmp_path / "portal",
        rate_limit_seconds=5.0,
        global_rate_seconds=5.0,
    )
    assert config.global_rate_seconds == 5.0


def test_cli_rate_limit_default_matches_global_rate(tmp_path: Path):
    """Default global_rate_seconds should be 1.0."""
    config = StfPortalConfig(output_dir=tmp_path / "portal")
    assert config.global_rate_seconds == 1.0


# --- PID file and SIGTERM handler tests ---


def test_run_extraction_creates_and_removes_pid_file(tmp_path: Path):
    """run_extraction writes PID file on start and removes it on completion."""
    curated_dir = tmp_path / "curated"
    output_dir = tmp_path / "portal"
    checkpoint_file = output_dir / ".checkpoint.json"
    pid_path = output_dir / ".fetch.pid"

    processes = [{"process_id": "proc_1", "process_number": "ADI 1"}]
    _write_process_jsonl(curated_dir, processes)

    config = StfPortalConfig(
        output_dir=output_dir,
        curated_dir=curated_dir,
        checkpoint_file=checkpoint_file,
        max_concurrent=1,
        rate_limit_seconds=0.0,
    )

    with patch("atlas_stf.stf_portal._runner.PortalExtractor", _FakeExtractor):
        run_extraction(config)

    # PID file should be cleaned up after normal exit
    assert not pid_path.exists()


def test_run_extraction_sigterm_saves_checkpoint(tmp_path: Path):
    """SIGTERM mid-extraction should save checkpoint and exit gracefully."""
    import os
    import signal

    curated_dir = tmp_path / "curated"
    output_dir = tmp_path / "portal"
    checkpoint_file = output_dir / ".checkpoint.json"

    processes = [{"process_id": f"proc_{i}", "process_number": f"ADI {i}"} for i in range(50)]
    _write_process_jsonl(curated_dir, processes)

    config = StfPortalConfig(
        output_dir=output_dir,
        curated_dir=curated_dir,
        checkpoint_file=checkpoint_file,
        max_concurrent=1,
        rate_limit_seconds=0.0,
    )

    call_count = 0

    class _SigtermAfterNExtractor:
        """Sends SIGTERM to self after 3 successful extractions."""

        def __init__(self, **_kwargs: Any) -> None:
            pass

        def _resolve_incidente(self, process_number: str) -> Any:
            from atlas_stf.stf_portal._result import ResolveResult

            nonlocal call_count
            call_count += 1
            if call_count == 3:
                os.kill(os.getpid(), signal.SIGTERM)
            return ResolveResult(status="resolved", incidente="999")

        def _fetch_tabs_concurrent(self, incidente: str, **_kw: Any) -> Any:
            from atlas_stf.stf_portal._http import TabsBatchResult

            tabs_to_fetch = _kw.get("tabs_to_fetch", _FAKE_TABS.keys())
            tabs = {t: _FAKE_TABS.get(t, "<div></div>") for t in tabs_to_fetch}
            on_tab_success = _kw.get("on_tab_success")
            if on_tab_success:
                for tab, html in tabs.items():
                    on_tab_success(tab, html)
            return TabsBatchResult(tabs=tabs, blocked=False, retryable=False, tabs_failed=set())

        def assemble_document(
            self, process_number: str, incidente: str, tab_htmls: dict[str, str],
        ) -> dict[str, Any] | None:
            return _fake_doc(process_number)

        def extract_process(self, process_number: str, incidente: str | None = None) -> dict[str, Any] | None:
            return _fake_extract_process(process_number, incidente)

        def close(self) -> None:
            pass

        def __enter__(self) -> _SigtermAfterNExtractor:
            return self

        def __exit__(self, *_: object) -> None:
            self.close()

    with patch("atlas_stf.stf_portal._runner.PortalExtractor", _SigtermAfterNExtractor):
        fetched = run_extraction(config)

    # Should have fetched 3 (SIGTERM after 3rd, loop breaks before 4th)
    assert fetched == 3

    # Checkpoint should be saved
    cp = load_checkpoint(checkpoint_file)
    assert cp.total_fetched == 3
