"""Tests for STF portal partial cache."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from atlas_stf.stf_portal._partial_cache import (
    PartialCache,
    PartialMeta,
    _sanitize_dir_name,
)

# --- Sanitization ---


def test_sanitize_dir_name_basic():
    assert _sanitize_dir_name("ADI 1234") == "ADI_1234"
    assert _sanitize_dir_name("RE/ARE 5555") == "RE_ARE_5555"


def test_sanitize_dir_name_rejects_path_traversal():
    with pytest.raises(ValueError, match="path traversal"):
        _sanitize_dir_name("../etc/passwd")


def test_sanitize_dir_name_rejects_absolute_path():
    with pytest.raises(ValueError, match="path traversal"):
        _sanitize_dir_name("/etc/passwd")


def test_sanitize_dir_name_rejects_empty():
    with pytest.raises(ValueError, match="empty"):
        _sanitize_dir_name("")


def test_sanitize_dir_name_rejects_special_chars():
    with pytest.raises(ValueError, match="invalid"):
        _sanitize_dir_name("ADI;1234")


# --- Incidente ---


def test_get_incidente_missing(tmp_path: Path):
    cache = PartialCache(tmp_path / ".partial")
    assert cache.get_incidente("ADI 1234") is None


def test_save_and_get_incidente(tmp_path: Path):
    cache = PartialCache(tmp_path / ".partial")
    cache.save_incidente("ADI 1234", "99999")
    assert cache.get_incidente("ADI 1234") == "99999"


def test_save_incidente_is_atomic(tmp_path: Path):
    cache = PartialCache(tmp_path / ".partial")
    cache.save_incidente("ADI 1234", "11111")
    # No .tmp files should remain
    process_dir = tmp_path / ".partial" / "ADI_1234"
    tmp_files = list(process_dir.glob("*.tmp"))
    assert tmp_files == []


def test_save_incidente_creates_directory(tmp_path: Path):
    cache = PartialCache(tmp_path / ".partial")
    cache.save_incidente("HC 999", "55555")
    assert (tmp_path / ".partial" / "HC_999" / "incidente.json").exists()


# --- Tabs ---


def test_get_tab_missing(tmp_path: Path):
    cache = PartialCache(tmp_path / ".partial")
    assert cache.get_tab("ADI 1234", "abaAndamentos") is None


def test_save_and_get_tab(tmp_path: Path):
    cache = PartialCache(tmp_path / ".partial")
    html = "<div>Andamento 1</div>"
    cache.save_tab("ADI 1234", "abaAndamentos", html)
    assert cache.get_tab("ADI 1234", "abaAndamentos") == html


def test_save_tab_accepts_empty_html(tmp_path: Path):
    """Empty HTML is a valid state (e.g. abaPeticoes for processes with no petitions)."""
    cache = PartialCache(tmp_path / ".partial")
    cache.save_tab("ADI 1234", "abaAndamentos", "")
    assert cache.get_tab("ADI 1234", "abaAndamentos") == ""


def test_save_tab_accepts_whitespace_html(tmp_path: Path):
    """Whitespace-only HTML is preserved as-is."""
    cache = PartialCache(tmp_path / ".partial")
    cache.save_tab("ADI 1234", "abaAndamentos", "   \n  ")
    assert cache.get_tab("ADI 1234", "abaAndamentos") == "   \n  "


def test_empty_tab_counts_as_cached(tmp_path: Path):
    """Empty HTML tab should count as present in get_cached_tabs."""
    cache = PartialCache(tmp_path / ".partial")
    cache.save_tab("ADI 1234", "abaAndamentos", "")
    cached = cache.get_cached_tabs("ADI 1234")
    assert "abaAndamentos" in cached
    missing = cache.get_missing_tabs("ADI 1234")
    assert "abaAndamentos" not in missing


def test_save_tab_atomic(tmp_path: Path):
    cache = PartialCache(tmp_path / ".partial")
    cache.save_tab("ADI 1234", "abaPartes", "<div>Partes</div>")
    process_dir = tmp_path / ".partial" / "ADI_1234"
    tmp_files = list(process_dir.glob("*.tmp"))
    assert tmp_files == []


# --- get_cached_tabs / get_missing_tabs / all_tabs_present ---


def test_get_cached_tabs_empty(tmp_path: Path):
    cache = PartialCache(tmp_path / ".partial")
    assert cache.get_cached_tabs("ADI 1234") == {}


def test_get_cached_tabs_partial(tmp_path: Path):
    cache = PartialCache(tmp_path / ".partial")
    cache.save_tab("ADI 1234", "abaAndamentos", "<div>A</div>")
    cache.save_tab("ADI 1234", "abaPartes", "<div>P</div>")
    cached = cache.get_cached_tabs("ADI 1234")
    assert set(cached.keys()) == {"abaAndamentos", "abaPartes"}


def test_get_missing_tabs_all_missing(tmp_path: Path):
    cache = PartialCache(tmp_path / ".partial")
    missing = cache.get_missing_tabs("ADI 1234")
    assert len(missing) == 5
    assert "abaAndamentos" in missing


def test_get_missing_tabs_some_cached(tmp_path: Path):
    cache = PartialCache(tmp_path / ".partial")
    cache.save_tab("ADI 1234", "abaAndamentos", "<div>A</div>")
    cache.save_tab("ADI 1234", "abaPartes", "<div>P</div>")
    cache.save_tab("ADI 1234", "abaPeticoes", "<div>Pet</div>")
    missing = cache.get_missing_tabs("ADI 1234")
    assert len(missing) == 2
    assert set(missing) == {"abaDeslocamentos", "abaInformacoes"}


def test_get_missing_tabs_all_present(tmp_path: Path):
    cache = PartialCache(tmp_path / ".partial")
    for tab in ("abaAndamentos", "abaPartes", "abaPeticoes", "abaDeslocamentos", "abaInformacoes"):
        cache.save_tab("ADI 1234", tab, f"<div>{tab}</div>")
    assert cache.get_missing_tabs("ADI 1234") == []


def test_all_tabs_present_true(tmp_path: Path):
    cache = PartialCache(tmp_path / ".partial")
    for tab in ("abaAndamentos", "abaPartes", "abaPeticoes", "abaDeslocamentos", "abaInformacoes"):
        cache.save_tab("ADI 1234", tab, f"<div>{tab}</div>")
    assert cache.all_tabs_present("ADI 1234") is True


def test_all_tabs_present_false(tmp_path: Path):
    cache = PartialCache(tmp_path / ".partial")
    cache.save_tab("ADI 1234", "abaAndamentos", "<div>A</div>")
    assert cache.all_tabs_present("ADI 1234") is False


# --- Meta ---


def test_get_meta_missing(tmp_path: Path):
    cache = PartialCache(tmp_path / ".partial")
    assert cache.get_meta("ADI 1234") is None


def test_save_and_get_meta(tmp_path: Path):
    cache = PartialCache(tmp_path / ".partial")
    meta = PartialMeta(retry_count=3, last_error="403 on abaPartes", last_attempt_at="2026-03-26T10:00:00+00:00")
    cache.save_meta("ADI 1234", meta)
    loaded = cache.get_meta("ADI 1234")
    assert loaded is not None
    assert loaded.retry_count == 3
    assert loaded.last_error == "403 on abaPartes"


def test_increment_retry(tmp_path: Path):
    cache = PartialCache(tmp_path / ".partial")
    m1 = cache.increment_retry("ADI 1234", "403 on abaPartes")
    assert m1.retry_count == 1

    m2 = cache.increment_retry("ADI 1234", "timeout on abaAndamentos")
    assert m2.retry_count == 2
    assert m2.last_error == "timeout on abaAndamentos"


# --- Cleanup ---


def test_cleanup_removes_directory(tmp_path: Path):
    cache = PartialCache(tmp_path / ".partial")
    cache.save_incidente("ADI 1234", "99999")
    cache.save_tab("ADI 1234", "abaAndamentos", "<div>A</div>")
    assert (tmp_path / ".partial" / "ADI_1234").exists()

    cache.cleanup("ADI 1234")
    assert not (tmp_path / ".partial" / "ADI_1234").exists()


def test_cleanup_nonexistent_no_error(tmp_path: Path):
    cache = PartialCache(tmp_path / ".partial")
    cache.cleanup("NONEXISTENT 999")  # should not raise


# --- List / Count / Has ---


def test_list_partial_processes(tmp_path: Path):
    cache = PartialCache(tmp_path / ".partial")
    cache.save_incidente("ADI 1234", "11111")
    cache.save_incidente("HC 999", "22222")
    names = cache.list_partial_processes()
    assert set(names) == {"ADI_1234", "HC_999"}


def test_list_partial_processes_empty(tmp_path: Path):
    cache = PartialCache(tmp_path / ".partial")
    assert cache.list_partial_processes() == []


def test_partial_count(tmp_path: Path):
    cache = PartialCache(tmp_path / ".partial")
    cache.save_incidente("ADI 1", "1")
    cache.save_incidente("ADI 2", "2")
    cache.save_incidente("ADI 3", "3")
    assert cache.partial_count() == 3


def test_has_partial(tmp_path: Path):
    cache = PartialCache(tmp_path / ".partial")
    assert cache.has_partial("ADI 1234") is False
    cache.save_incidente("ADI 1234", "99999")
    assert cache.has_partial("ADI 1234") is True


# --- Corrupted data resilience ---


def test_corrupted_incidente_returns_none(tmp_path: Path):
    cache = PartialCache(tmp_path / ".partial")
    d = tmp_path / ".partial" / "ADI_1234"
    d.mkdir(parents=True)
    (d / "incidente.json").write_text("not json", encoding="utf-8")
    assert cache.get_incidente("ADI 1234") is None


def test_corrupted_meta_returns_none(tmp_path: Path):
    cache = PartialCache(tmp_path / ".partial")
    d = tmp_path / ".partial" / "ADI_1234"
    d.mkdir(parents=True)
    (d / "_meta.json").write_text("{broken", encoding="utf-8")
    assert cache.get_meta("ADI 1234") is None


# --- JSON content verification ---


def test_incidente_json_content(tmp_path: Path):
    cache = PartialCache(tmp_path / ".partial")
    cache.save_incidente("ADI 1234", "99999")
    data = json.loads((tmp_path / ".partial" / "ADI_1234" / "incidente.json").read_text(encoding="utf-8"))
    assert data["incidente"] == "99999"
    assert "resolved_at" in data
