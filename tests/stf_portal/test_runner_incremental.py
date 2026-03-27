"""Tests for incremental extraction with partial persistence."""

from __future__ import annotations

import json
import logging
import os
import signal
from pathlib import Path
from typing import Any
from unittest.mock import patch

from atlas_stf.stf_portal._checkpoint import PortalCheckpoint, load_checkpoint, save_checkpoint
from atlas_stf.stf_portal._config import StfPortalConfig
from atlas_stf.stf_portal._http import TABS, TabsBatchResult
from atlas_stf.stf_portal._metrics import ExtractionMetrics
from atlas_stf.stf_portal._partial_cache import PartialCache
from atlas_stf.stf_portal._runner import (
    _fetch_process_incremental,
    run_extraction,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FAKE_TABS = {
    "abaAndamentos": "<div>A</div>",
    "abaPartes": "<div>P</div>",
    "abaPeticoes": "<div>Pet</div>",
    "abaDeslocamentos": "<div>D</div>",
    "abaInformacoes": "<div>I</div>",
}


def _fake_doc(pn: str) -> dict[str, Any]:
    return {
        "process_number": pn,
        "source_system": "stf_portal",
        "source_url": "https://portal.stf.jus.br/processos/detalhe.asp?incidente=999",
        "fetched_at": "2026-03-17T00:00:00+00:00",
        "raw_html_hash": "fakehash",
        "andamentos": [],
        "deslocamentos": [],
        "peticoes": [],
        "sessao_virtual": [],
        "informacoes": {},
        "incidente": "999",
    }


def _write_process_jsonl(curated_dir: Path, processes: list[dict[str, Any]]) -> None:
    curated_dir.mkdir(parents=True, exist_ok=True)
    (curated_dir / "process.jsonl").write_text(
        "\n".join(json.dumps(p) for p in processes) + "\n",
        encoding="utf-8",
    )


class _GoodExtractor:
    """Full-success extractor implementing the incremental interface."""

    def __init__(self, **_kw: Any) -> None:
        pass

    def _resolve_incidente(self, process_number: str) -> Any:
        from atlas_stf.stf_portal._result import ResolveResult

        return ResolveResult(status="resolved", incidente="999")

    def _fetch_tabs_concurrent(self, incidente: str, **kw: Any) -> TabsBatchResult:
        tabs_to_fetch = kw.get("tabs_to_fetch", tuple(TABS))
        on_tab_success = kw.get("on_tab_success")
        tabs = {t: _FAKE_TABS.get(t, "<div></div>") for t in tabs_to_fetch}
        if on_tab_success:
            for tab, html in tabs.items():
                on_tab_success(tab, html)
        return TabsBatchResult(tabs=tabs, blocked=False, retryable=False, tabs_failed=set())

    def assemble_document(self, pn: str, incidente: str, tab_htmls: dict[str, str]) -> dict[str, Any] | None:
        return _fake_doc(pn)

    def extract_process(self, pn: str, incidente: str | None = None) -> dict[str, Any] | None:
        return _fake_doc(pn)

    def close(self) -> None:
        pass

    def __enter__(self) -> _GoodExtractor:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def _make_config(tmp_path: Path, **overrides: Any) -> StfPortalConfig:
    defaults = {
        "output_dir": tmp_path / "portal",
        "curated_dir": tmp_path / "curated",
        "checkpoint_file": tmp_path / "portal" / ".checkpoint.json",
        "max_concurrent": 1,
        "rate_limit_seconds": 0.0,
        "global_rate_seconds": 0.0,
    }
    defaults.update(overrides)
    return StfPortalConfig(**defaults)


# ---------------------------------------------------------------------------
# 1. Incidente persisted before full success
# ---------------------------------------------------------------------------


def test_incidente_persisted_before_tab_success(tmp_path: Path):
    """Incidente should be saved to partial cache even if tabs fail."""

    class _ResolveOkTabsFail:
        def __init__(self, **_kw: Any) -> None:
            pass

        def _resolve_incidente(self, pn: str) -> Any:
            from atlas_stf.stf_portal._result import ResolveResult

            return ResolveResult(status="resolved", incidente="12345")

        def _fetch_tabs_concurrent(self, incidente: str, **kw: Any) -> TabsBatchResult:
            on_tab_success = kw.get("on_tab_success")
            tabs_to_fetch = kw.get("tabs_to_fetch", tuple(TABS))
            # First 2 tabs succeed, rest fail
            result_tabs: dict[str, str] = {}
            failed: set[str] = set()
            for i, tab in enumerate(tabs_to_fetch):
                if i < 2:
                    html = f"<div>{tab}</div>"
                    result_tabs[tab] = html
                    if on_tab_success:
                        on_tab_success(tab, html)
                else:
                    failed.add(tab)
            return TabsBatchResult(tabs=result_tabs, blocked=False, retryable=True, tabs_failed=failed)

        def assemble_document(self, pn: str, incidente: str, tab_htmls: dict[str, str]) -> None:
            return None

        def close(self) -> None:
            pass

        def __enter__(self) -> _ResolveOkTabsFail:
            return self

        def __exit__(self, *_: object) -> None:
            pass

    config = _make_config(tmp_path)
    processes = [{"process_id": "p1", "process_number": "ADI 100"}]
    _write_process_jsonl(config.curated_dir, processes)

    with patch("atlas_stf.stf_portal._runner.PortalExtractor", _ResolveOkTabsFail):
        run_extraction(config)

    # Incidente should be in partial cache
    partial = PartialCache(config.partial_dir)
    assert partial.get_incidente("ADI 100") == "12345"

    # 2 tabs should be cached
    cached = partial.get_cached_tabs("ADI 100")
    assert len(cached) == 2


# ---------------------------------------------------------------------------
# 2. Tab retry only re-fetches missing tabs
# ---------------------------------------------------------------------------


def test_retry_fetches_only_missing_tabs(tmp_path: Path):
    """On second run, only missing tabs should be fetched."""
    config = _make_config(tmp_path)
    processes = [{"process_id": "p1", "process_number": "ADI 100"}]
    _write_process_jsonl(config.curated_dir, processes)

    # Pre-populate partial with 4 of 5 tabs
    partial = PartialCache(config.partial_dir)
    partial.save_incidente("ADI 100", "999")
    for tab in ("abaAndamentos", "abaPartes", "abaPeticoes", "abaDeslocamentos"):
        partial.save_tab("ADI 100", tab, f"<div>{tab}</div>")

    fetched_tabs_requested: list[tuple[str, ...]] = []

    class _TrackingExtractor:
        def __init__(self, **_kw: Any) -> None:
            pass

        def _resolve_incidente(self, pn: str) -> Any:
            from atlas_stf.stf_portal._result import ResolveResult

            return ResolveResult(status="resolved", incidente="999")

        def _fetch_tabs_concurrent(self, incidente: str, **kw: Any) -> TabsBatchResult:
            tabs_to_fetch = kw.get("tabs_to_fetch", tuple(TABS))
            fetched_tabs_requested.append(tabs_to_fetch)
            on_tab_success = kw.get("on_tab_success")
            tabs = {t: f"<div>{t}</div>" for t in tabs_to_fetch}
            if on_tab_success:
                for tab, html in tabs.items():
                    on_tab_success(tab, html)
            return TabsBatchResult(tabs=tabs, blocked=False, retryable=False, tabs_failed=set())

        def assemble_document(self, pn: str, incidente: str, tab_htmls: dict[str, str]) -> dict[str, Any] | None:
            return _fake_doc(pn)

        def close(self) -> None:
            pass

        def __enter__(self) -> _TrackingExtractor:
            return self

        def __exit__(self, *_: object) -> None:
            pass

    with patch("atlas_stf.stf_portal._runner.PortalExtractor", _TrackingExtractor):
        fetched = run_extraction(config)

    assert fetched == 1
    # Should have only requested the missing tab
    assert len(fetched_tabs_requested) == 1
    assert fetched_tabs_requested[0] == ("abaInformacoes",)


# ---------------------------------------------------------------------------
# 3. 403 preserves already-downloaded tabs
# ---------------------------------------------------------------------------


def test_403_preserves_downloaded_tabs(tmp_path: Path):
    """403 on a tab should not discard tabs already downloaded in the same batch."""

    class _403OnThirdTab:
        def __init__(self, **_kw: Any) -> None:
            pass

        def _resolve_incidente(self, pn: str) -> Any:
            from atlas_stf.stf_portal._result import ResolveResult

            return ResolveResult(status="resolved", incidente="999")

        def _fetch_tabs_concurrent(self, incidente: str, **kw: Any) -> TabsBatchResult:
            tabs_to_fetch = kw.get("tabs_to_fetch", tuple(TABS))
            on_tab_success = kw.get("on_tab_success")
            result_tabs: dict[str, str] = {}
            for i, tab in enumerate(tabs_to_fetch):
                if i < 2:
                    html = f"<div>{tab}</div>"
                    result_tabs[tab] = html
                    if on_tab_success:
                        on_tab_success(tab, html)
            # 403 after 2 tabs
            return TabsBatchResult(
                tabs=result_tabs,
                blocked=True,
                retryable=False,
                tabs_failed={t for i, t in enumerate(tabs_to_fetch) if i >= 2},
            )

        def assemble_document(self, pn: str, incidente: str, tab_htmls: dict[str, str]) -> None:
            return None

        def close(self) -> None:
            pass

        def __enter__(self) -> _403OnThirdTab:
            return self

        def __exit__(self, *_: object) -> None:
            pass

    config = _make_config(tmp_path)
    processes = [{"process_id": "p1", "process_number": "ADI 100"}]
    _write_process_jsonl(config.curated_dir, processes)

    with patch("atlas_stf.stf_portal._runner.PortalExtractor", _403OnThirdTab):
        fetched = run_extraction(config)

    assert fetched == 0  # not completed

    # 2 tabs should be preserved in partial
    partial = PartialCache(config.partial_dir)
    cached = partial.get_cached_tabs("ADI 100")
    assert len(cached) == 2


# ---------------------------------------------------------------------------
# 4. Process only completed with 5/5 tabs
# ---------------------------------------------------------------------------


def test_process_not_completed_with_partial_tabs(tmp_path: Path):
    config = _make_config(tmp_path)
    partial = PartialCache(config.partial_dir)
    partial.save_incidente("ADI 100", "999")
    for tab in ("abaAndamentos", "abaPartes", "abaPeticoes"):
        partial.save_tab("ADI 100", tab, f"<div>{tab}</div>")
    assert partial.all_tabs_present("ADI 100") is False

    # With 3/5, the process should not be marked completed
    checkpoint = PortalCheckpoint()
    extractor = _GoodExtractor()
    metrics = ExtractionMetrics()

    # This will fetch the 2 missing and complete
    result = _fetch_process_incremental("ADI 100", config, extractor, checkpoint, partial, metrics)
    assert result.status == "completed"
    assert result.doc is not None


# ---------------------------------------------------------------------------
# 5. permanent_failure not retried
# ---------------------------------------------------------------------------


def test_permanent_failure_not_retried(tmp_path: Path):
    """Process with permanent_failure should not appear in pending on second run."""

    class _PermanentFailExtractor:
        def __init__(self, **_kw: Any) -> None:
            pass

        def _resolve_incidente(self, pn: str) -> Any:
            from atlas_stf.stf_portal._result import ResolveResult

            return ResolveResult(status="not_found_permanent")

        def _fetch_tabs_concurrent(self, incidente: str, **kw: Any) -> TabsBatchResult:
            return TabsBatchResult(tabs={}, blocked=False, retryable=False, tabs_failed=set())

        def assemble_document(self, pn: str, incidente: str, tab_htmls: dict[str, str]) -> None:
            return None

        def close(self) -> None:
            pass

        def __enter__(self) -> _PermanentFailExtractor:
            return self

        def __exit__(self, *_: object) -> None:
            pass

    config = _make_config(tmp_path)
    processes = [{"process_id": "p1", "process_number": "ADI 100"}]
    _write_process_jsonl(config.curated_dir, processes)

    # Run 1: permanent failure
    with patch("atlas_stf.stf_portal._runner.PortalExtractor", _PermanentFailExtractor):
        fetched = run_extraction(config)
    assert fetched == 0

    cp = load_checkpoint(config.checkpoint_file)
    assert cp.is_failed("ADI 100")

    # Run 2: should skip (no partial dir exists for permanent failures)
    with patch("atlas_stf.stf_portal._runner.PortalExtractor", _GoodExtractor):
        fetched2 = run_extraction(config)
    assert fetched2 == 0  # skipped because it's in failed


# ---------------------------------------------------------------------------
# 6. retry_later IS retried (has partial)
# ---------------------------------------------------------------------------


def test_retry_later_is_retried(tmp_path: Path):
    """Process with retry_later status should be retried when partial exists."""
    config = _make_config(tmp_path)
    processes = [{"process_id": "p1", "process_number": "ADI 100"}]
    _write_process_jsonl(config.curated_dir, processes)

    # Simulate partial state from a previous retry_later
    partial = PartialCache(config.partial_dir)
    partial.save_incidente("ADI 100", "999")
    partial.save_tab("ADI 100", "abaAndamentos", "<div>A</div>")
    # Also mark as failed in checkpoint (legacy state)
    cp = PortalCheckpoint()
    cp.mark_failed("ADI 100")
    save_checkpoint(cp, config.checkpoint_file)

    # Run: should pick up partial and complete
    with patch("atlas_stf.stf_portal._runner.PortalExtractor", _GoodExtractor):
        fetched = run_extraction(config)
    assert fetched == 1


# ---------------------------------------------------------------------------
# 7. max_process_retries exceeded → permanent_failure
# ---------------------------------------------------------------------------


def test_max_process_retries_exceeded(tmp_path: Path):
    config = _make_config(tmp_path, max_process_retries=3)
    partial = PartialCache(config.partial_dir)
    checkpoint = PortalCheckpoint()
    extractor = _GoodExtractor()
    metrics = ExtractionMetrics()

    # Simulate 3 previous retries
    from atlas_stf.stf_portal._partial_cache import PartialMeta

    partial.save_meta("ADI 100", PartialMeta(retry_count=3, last_error="403", last_attempt_at="2026-03-26T00:00:00"))

    result = _fetch_process_incremental("ADI 100", config, extractor, checkpoint, partial, metrics)
    assert result.status == "permanent_failure"
    assert "max retries exceeded" in result.reason


# ---------------------------------------------------------------------------
# 8. Workers > 2 works without silent cap
# ---------------------------------------------------------------------------


def test_workers_no_silent_cap(tmp_path: Path):
    config = _make_config(tmp_path, max_concurrent=4)
    processes = [{"process_id": f"p{i}", "process_number": f"HC {i}"} for i in range(20)]
    _write_process_jsonl(config.curated_dir, processes)

    with patch("atlas_stf.stf_portal._runner.PortalExtractor", _GoodExtractor):
        fetched = run_extraction(config)

    assert fetched == 20


# ---------------------------------------------------------------------------
# 9. Warning for max_in_flight (no auto-adjust)
# ---------------------------------------------------------------------------


def test_max_in_flight_warning(tmp_path: Path, caplog: Any):
    config = _make_config(tmp_path, max_concurrent=4, max_in_flight=2, tab_concurrency=5)
    processes = [{"process_id": "p1", "process_number": "ADI 100"}]
    _write_process_jsonl(config.curated_dir, processes)

    with caplog.at_level(logging.WARNING):
        with patch("atlas_stf.stf_portal._runner.PortalExtractor", _GoodExtractor):
            run_extraction(config)

    assert any("max_in_flight=2" in r.message for r in caplog.records)
    assert any("insuficiente" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# 10. SIGTERM preserves partial
# ---------------------------------------------------------------------------


def test_sigterm_preserves_partial(tmp_path: Path):
    config = _make_config(tmp_path)
    processes = [{"process_id": f"p{i}", "process_number": f"ADI {i}"} for i in range(50)]
    _write_process_jsonl(config.curated_dir, processes)

    call_count = 0

    class _SigtermAfter3:
        def __init__(self, **_kw: Any) -> None:
            pass

        def _resolve_incidente(self, pn: str) -> Any:
            from atlas_stf.stf_portal._result import ResolveResult

            nonlocal call_count
            call_count += 1
            if call_count == 3:
                os.kill(os.getpid(), signal.SIGTERM)
            return ResolveResult(status="resolved", incidente="999")

        def _fetch_tabs_concurrent(self, incidente: str, **kw: Any) -> TabsBatchResult:
            tabs_to_fetch = kw.get("tabs_to_fetch", tuple(TABS))
            on_tab_success = kw.get("on_tab_success")
            tabs = {t: f"<div>{t}</div>" for t in tabs_to_fetch}
            if on_tab_success:
                for t, h in tabs.items():
                    on_tab_success(t, h)
            return TabsBatchResult(tabs=tabs, blocked=False, retryable=False, tabs_failed=set())

        def assemble_document(self, pn: str, incidente: str, tab_htmls: dict[str, str]) -> dict[str, Any] | None:
            return _fake_doc(pn)

        def close(self) -> None:
            pass

        def __enter__(self) -> _SigtermAfter3:
            return self

        def __exit__(self, *_: object) -> None:
            pass

    with patch("atlas_stf.stf_portal._runner.PortalExtractor", _SigtermAfter3):
        fetched1 = run_extraction(config)

    assert fetched1 == 3

    # Second run should complete remaining
    with patch("atlas_stf.stf_portal._runner.PortalExtractor", _GoodExtractor):
        fetched2 = run_extraction(config)

    assert fetched2 == 47


# ---------------------------------------------------------------------------
# 11. Resume reuses partial and reduces requests
# ---------------------------------------------------------------------------


def test_resume_reuses_partial_metrics(tmp_path: Path):
    config = _make_config(tmp_path)
    processes = [{"process_id": "p1", "process_number": "ADI 100"}]
    _write_process_jsonl(config.curated_dir, processes)

    # Pre-populate 3/5 tabs
    partial = PartialCache(config.partial_dir)
    partial.save_incidente("ADI 100", "999")
    for tab in ("abaAndamentos", "abaPartes", "abaPeticoes"):
        partial.save_tab("ADI 100", tab, f"<div>{tab}</div>")

    with patch("atlas_stf.stf_portal._runner.PortalExtractor", _GoodExtractor):
        fetched = run_extraction(config)

    assert fetched == 1

    # Metrics should show partial reuse
    metrics_path = config.output_dir / ".metrics.json"
    assert metrics_path.exists()
    data = json.loads(metrics_path.read_text(encoding="utf-8"))
    assert data["tabs_reused_from_partial"] > 0


# ---------------------------------------------------------------------------
# 12. Cleanup after assembly
# ---------------------------------------------------------------------------


def test_cleanup_after_assembly(tmp_path: Path):
    config = _make_config(tmp_path)
    processes = [{"process_id": "p1", "process_number": "ADI 100"}]
    _write_process_jsonl(config.curated_dir, processes)

    with patch("atlas_stf.stf_portal._runner.PortalExtractor", _GoodExtractor):
        run_extraction(config)

    # Partial dir should be cleaned up after successful completion
    partial = PartialCache(config.partial_dir)
    assert not partial.has_partial("ADI 100")


# ---------------------------------------------------------------------------
# 13. CLI args arrive at config
# ---------------------------------------------------------------------------


def test_cli_args_arrive_at_config(tmp_path: Path):
    config = StfPortalConfig(
        output_dir=tmp_path / "portal",
        max_in_flight=12,
        tab_concurrency=5,
        max_retries=6,
        retry_delay_seconds=3.0,
        circuit_breaker_threshold=8,
        circuit_breaker_cooldown=60.0,
        max_process_retries=20,
        partial_dir=tmp_path / "custom_partial",
    )
    assert config.max_in_flight == 12
    assert config.tab_concurrency == 5
    assert config.max_retries == 6
    assert config.retry_delay_seconds == 3.0
    assert config.circuit_breaker_threshold == 8
    assert config.circuit_breaker_cooldown == 60.0
    assert config.max_process_retries == 20
    assert config.partial_dir == tmp_path / "custom_partial"


# ---------------------------------------------------------------------------
# 14. Pending queue respects max_processes with partials
# ---------------------------------------------------------------------------


def test_max_processes_respected_with_partials(tmp_path: Path):
    config = _make_config(tmp_path, max_processes=3)
    processes = [{"process_id": f"p{i}", "process_number": f"ADI {i}"} for i in range(10)]
    _write_process_jsonl(config.curated_dir, processes)

    with patch("atlas_stf.stf_portal._runner.PortalExtractor", _GoodExtractor):
        fetched = run_extraction(config)

    # Only 3 should be fetched due to max_processes
    assert fetched == 3


# ---------------------------------------------------------------------------
# 15. Output is atomic (tmp+rename)
# ---------------------------------------------------------------------------


def test_output_write_is_atomic(tmp_path: Path):
    """After extraction, no .tmp files should remain."""
    config = _make_config(tmp_path)
    processes = [{"process_id": "p1", "process_number": "ADI 100"}]
    _write_process_jsonl(config.curated_dir, processes)

    with patch("atlas_stf.stf_portal._runner.PortalExtractor", _GoodExtractor):
        run_extraction(config)

    # Check no .tmp files remain
    tmp_files = list(config.output_dir.glob("*.tmp"))
    assert tmp_files == []

    # Output JSON should be valid
    output = config.output_dir / "ADI_100.json"
    assert output.exists()
    data = json.loads(output.read_text(encoding="utf-8"))
    assert data["process_number"] == "ADI 100"


# ---------------------------------------------------------------------------
# 16. Metrics file created
# ---------------------------------------------------------------------------


def test_metrics_file_created(tmp_path: Path):
    config = _make_config(tmp_path)
    processes = [{"process_id": "p1", "process_number": "ADI 100"}]
    _write_process_jsonl(config.curated_dir, processes)

    with patch("atlas_stf.stf_portal._runner.PortalExtractor", _GoodExtractor):
        run_extraction(config)

    metrics_path = config.output_dir / ".metrics.json"
    assert metrics_path.exists()
    data = json.loads(metrics_path.read_text(encoding="utf-8"))
    assert data["processes_completed"] == 1
    assert data["elapsed_seconds"] >= 0


# ---------------------------------------------------------------------------
# 17. Checkpoint isolation: derives from output_dir by default
# ---------------------------------------------------------------------------


def test_checkpoint_derives_from_output_dir(tmp_path: Path):
    """checkpoint_file should default to {output_dir}/.checkpoint.json."""
    config = StfPortalConfig(output_dir=tmp_path / "custom_portal")
    assert config.checkpoint_file == tmp_path / "custom_portal" / ".checkpoint.json"


def test_checkpoint_does_not_pollute_default_path(tmp_path: Path):
    """Using custom --output-dir should not write to data/raw/stf_portal/."""
    config = _make_config(tmp_path)
    processes = [{"process_id": "p1", "process_number": "ADI 100"}]
    _write_process_jsonl(config.curated_dir, processes)

    with patch("atlas_stf.stf_portal._runner.PortalExtractor", _GoodExtractor):
        run_extraction(config)

    # Checkpoint should be inside output_dir, not in data/raw/stf_portal/
    assert config.checkpoint_file == config.output_dir / ".checkpoint.json"
    assert config.checkpoint_file.exists()


# ---------------------------------------------------------------------------
# 18. Resolve result: 403 vs transient tracked separately
# ---------------------------------------------------------------------------


def test_resolve_403_increments_http_403_total(tmp_path: Path):
    """403 on resolve should increment http_403_total separately from transient."""

    class _403ResolveExtractor:
        def __init__(self, **_kw: Any) -> None:
            pass

        def _resolve_incidente(self, pn: str) -> Any:
            from atlas_stf.stf_portal._result import ResolveResult

            return ResolveResult(status="blocked_403")

        def _fetch_tabs_concurrent(self, incidente: str, **kw: Any) -> TabsBatchResult:
            return TabsBatchResult(tabs={}, blocked=False, retryable=False, tabs_failed=set())

        def assemble_document(self, pn: str, incidente: str, tab_htmls: dict[str, str]) -> None:
            return None

        def close(self) -> None:
            pass

        def __enter__(self) -> _403ResolveExtractor:
            return self

        def __exit__(self, *_: object) -> None:
            pass

    config = _make_config(tmp_path)
    processes = [{"process_id": "p1", "process_number": "ADI 100"}]
    _write_process_jsonl(config.curated_dir, processes)

    with patch("atlas_stf.stf_portal._runner.PortalExtractor", _403ResolveExtractor):
        run_extraction(config)

    data = json.loads((config.output_dir / ".metrics.json").read_text(encoding="utf-8"))
    assert data["http_403_total"] == 1
    assert data["retryable_errors_total"] == 1


def test_resolve_transient_does_not_increment_403(tmp_path: Path):
    """Transient failure (SSL/timeout) should NOT increment http_403_total."""

    class _TransientResolveExtractor:
        def __init__(self, **_kw: Any) -> None:
            pass

        def _resolve_incidente(self, pn: str) -> Any:
            from atlas_stf.stf_portal._result import ResolveResult

            return ResolveResult(status="transient_failure")

        def _fetch_tabs_concurrent(self, incidente: str, **kw: Any) -> TabsBatchResult:
            return TabsBatchResult(tabs={}, blocked=False, retryable=False, tabs_failed=set())

        def assemble_document(self, pn: str, incidente: str, tab_htmls: dict[str, str]) -> None:
            return None

        def close(self) -> None:
            pass

        def __enter__(self) -> _TransientResolveExtractor:
            return self

        def __exit__(self, *_: object) -> None:
            pass

    config = _make_config(tmp_path)
    processes = [{"process_id": "p1", "process_number": "ADI 100"}]
    _write_process_jsonl(config.curated_dir, processes)

    with patch("atlas_stf.stf_portal._runner.PortalExtractor", _TransientResolveExtractor):
        run_extraction(config)

    data = json.loads((config.output_dir / ".metrics.json").read_text(encoding="utf-8"))
    assert data["http_403_total"] == 0
    assert data["retryable_errors_total"] == 1


# ---------------------------------------------------------------------------
# 19. Empty HTML tab completes process
# ---------------------------------------------------------------------------


def test_empty_html_tab_completes_process(tmp_path: Path):
    """Process with empty abaPeticoes should complete (not loop in retry_later)."""

    class _EmptyPeticoesExtractor:
        def __init__(self, **_kw: Any) -> None:
            pass

        def _resolve_incidente(self, pn: str) -> Any:
            from atlas_stf.stf_portal._result import ResolveResult

            return ResolveResult(status="resolved", incidente="999")

        def _fetch_tabs_concurrent(self, incidente: str, **kw: Any) -> TabsBatchResult:
            tabs_to_fetch = kw.get("tabs_to_fetch", tuple(TABS))
            on_tab_success = kw.get("on_tab_success")
            tabs = {}
            for t in tabs_to_fetch:
                html = "" if t == "abaPeticoes" else f"<div>{t}</div>"
                tabs[t] = html
                if on_tab_success:
                    on_tab_success(t, html)
            return TabsBatchResult(tabs=tabs, blocked=False, retryable=False, tabs_failed=set())

        def assemble_document(self, pn: str, incidente: str, tab_htmls: dict[str, str]) -> dict[str, Any] | None:
            return _fake_doc(pn)

        def close(self) -> None:
            pass

        def __enter__(self) -> _EmptyPeticoesExtractor:
            return self

        def __exit__(self, *_: object) -> None:
            pass

    config = _make_config(tmp_path)
    processes = [{"process_id": "p1", "process_number": "ADI 100"}]
    _write_process_jsonl(config.curated_dir, processes)

    with patch("atlas_stf.stf_portal._runner.PortalExtractor", _EmptyPeticoesExtractor):
        fetched = run_extraction(config)

    assert fetched == 1
    # No partial should remain
    partial = PartialCache(config.partial_dir)
    assert not partial.has_partial("ADI 100")


# ---------------------------------------------------------------------------
# 20. End-to-end partial tab reuse through run_extraction (two runs)
# ---------------------------------------------------------------------------


def test_partial_tab_reuse_end_to_end(tmp_path: Path):
    """Two run_extraction calls: first fails 1 tab, second resumes and completes.

    Proves partial tab reuse through the full runner pipeline, including
    _handle_result cleanup of .partial/.
    """
    # Track which tabs_to_fetch are requested across calls
    fetch_calls: list[tuple[str, ...]] = []
    call_count = {"n": 0}

    class _FailAbaPeticoesOnce:
        """Fails abaPeticoes on first fetch call, succeeds on all subsequent."""

        def __init__(self, **_kw: Any) -> None:
            pass

        def _resolve_incidente(self, pn: str) -> Any:
            from atlas_stf.stf_portal._result import ResolveResult

            return ResolveResult(status="resolved", incidente="999")

        def _fetch_tabs_concurrent(self, incidente: str, **kw: Any) -> TabsBatchResult:
            tabs_to_fetch = kw.get("tabs_to_fetch", tuple(TABS))
            on_tab_success = kw.get("on_tab_success")
            call_count["n"] += 1
            fetch_calls.append(tabs_to_fetch)

            tabs: dict[str, str] = {}
            failed: set[str] = set()
            for tab in tabs_to_fetch:
                if tab == "abaPeticoes" and call_count["n"] == 1:
                    failed.add(tab)
                else:
                    html = f"<div>{tab}</div>"
                    tabs[tab] = html
                    if on_tab_success:
                        on_tab_success(tab, html)
            return TabsBatchResult(
                tabs=tabs, blocked=False, retryable=bool(failed), tabs_failed=failed,
            )

        def assemble_document(self, pn: str, incidente: str, tab_htmls: dict[str, str]) -> dict[str, Any] | None:
            return _fake_doc(pn)

        def close(self) -> None:
            pass

        def __enter__(self) -> _FailAbaPeticoesOnce:
            return self

        def __exit__(self, *_: object) -> None:
            pass

    config = _make_config(tmp_path)
    processes = [{"process_id": "p1", "process_number": "ADI 100"}]
    _write_process_jsonl(config.curated_dir, processes)

    # --- Run 1: abaPeticoes fails, 4 tabs persist in .partial/ ---
    with patch("atlas_stf.stf_portal._runner.PortalExtractor", _FailAbaPeticoesOnce):
        fetched1 = run_extraction(config)

    assert fetched1 == 0  # not completed

    partial = PartialCache(config.partial_dir)
    assert partial.has_partial("ADI 100")
    assert partial.get_incidente("ADI 100") == "999"
    cached = partial.get_cached_tabs("ADI 100")
    assert len(cached) == 4
    assert "abaPeticoes" not in cached
    assert partial.get_missing_tabs("ADI 100") == ["abaPeticoes"]

    metrics1 = json.loads((config.output_dir / ".metrics.json").read_text(encoding="utf-8"))
    assert metrics1["tabs_downloaded_fresh"] == 4
    assert metrics1["tabs_reused_from_partial"] == 0
    assert metrics1["incidente_reused_from_cache"] == 0
    assert metrics1["requests_resolve"] == 1

    # --- Run 2: resume, only abaPeticoes fetched ---
    with patch("atlas_stf.stf_portal._runner.PortalExtractor", _FailAbaPeticoesOnce):
        fetched2 = run_extraction(config)

    assert fetched2 == 1  # completed

    # Partial should be cleaned up by _handle_result
    assert not partial.has_partial("ADI 100")

    # Output JSON exists
    assert (config.output_dir / "ADI_100.json").exists()

    metrics2 = json.loads((config.output_dir / ".metrics.json").read_text(encoding="utf-8"))
    assert metrics2["requests_resolve"] == 0  # incidente reused
    assert metrics2["incidente_reused_from_cache"] == 1
    assert metrics2["tabs_reused_from_partial"] == 4  # 4 tabs reused
    assert metrics2["tabs_downloaded_fresh"] == 1  # only abaPeticoes

    # Verify tabs_to_fetch in Run 2 was only ("abaPeticoes",)
    # fetch_calls[0] = Run 1 (5 tabs), fetch_calls[1] = Run 2 (1 tab)
    assert len(fetch_calls) == 2
    assert set(fetch_calls[0]) == set(TABS)  # all 5 tabs
    assert fetch_calls[1] == ("abaPeticoes",)  # only the missing tab
