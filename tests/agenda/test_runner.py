from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import atlas_stf.agenda._runner as runner_module
from atlas_stf.agenda._config import AgendaFetchConfig
from atlas_stf.agenda._parser import RAW_EVENT_SCHEMA_VERSION
from atlas_stf.agenda._runner import _is_valid_cache, _write_text_atomic, run_agenda_fetch


def _cfg(tmp_path: Path, **kw: Any) -> AgendaFetchConfig:
    data: dict[str, Any] = {
        "output_dir": tmp_path / "agenda",
        "rate_limit_seconds": 0.0,
        "max_retries": 1,
        "retry_delay_seconds": 0.0,
        "timeout_seconds": 5.0,
    }
    data.update(kw)
    return AgendaFetchConfig(**data)


class _FakeAgendaClient:
    def __init__(self, config: AgendaFetchConfig, payload: dict[str, Any], meta: dict[str, Any]) -> None:
        self.config = config
        self.payload = payload
        self.meta = meta

    def __enter__(self) -> _FakeAgendaClient:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def fetch_month(self, year: int, month: int) -> tuple[dict[str, Any], dict[str, Any]]:
        assert (year, month) == (2024, 1)
        return self.payload, self.meta


def test_invalid_jsonl_cache_is_rejected_and_cleaned(tmp_path: Path):
    output_path = tmp_path / "2024-01.jsonl"
    raw_path = tmp_path / "_raw" / "2024-01.json"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text('{"broken":\n', encoding="utf-8")
    raw_path.write_text(
        json.dumps({"response": {"data": {"agendaMinistrosPorDiaCategoria": []}}, "metadata": {}}),
        encoding="utf-8",
    )

    assert _is_valid_cache(output_path, raw_path) is False
    assert not output_path.exists()
    assert not raw_path.exists()


def test_outdated_normalization_version_is_rejected_and_cleaned(tmp_path: Path):
    output_path = tmp_path / "2024-01.jsonl"
    raw_path = tmp_path / "_raw" / "2024-01.json"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps({"event_id": "agd_old", "normalization_version": "agenda-raw-v1"}) + "\n",
        encoding="utf-8",
    )
    raw_path.write_text(
        json.dumps({"response": {"data": {"agendaMinistrosPorDiaCategoria": []}}, "metadata": {}}),
        encoding="utf-8",
    )

    assert _is_valid_cache(output_path, raw_path) is False
    assert not output_path.exists()
    assert not raw_path.exists()


def test_write_text_atomic_replaces_content_without_leaving_tempfiles(tmp_path: Path):
    path = tmp_path / "sample.jsonl"
    path.write_text("old\n", encoding="utf-8")

    _write_text_atomic(path, "new\n")

    assert path.read_text(encoding="utf-8") == "new\n"
    assert list(tmp_path.glob(".sample.jsonl.*.tmp")) == []


def test_run_agenda_fetch_writes_via_atomic_helper(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    payload = {
        "data": {
            "agendaMinistrosPorDiaCategoria": [
                {
                    "data": "02/01/2024",
                    "descricaoData": "terça-feira",
                    "ministro": [
                        {
                            "nomeMinistro": "MIN. EDSON FACHIN",
                            "eventos": [{"titulo": "Audiência", "hora": "13h00"}],
                        }
                    ],
                }
            ]
        }
    }
    meta = {"fetched_at": "2026-04-01T00:00:00+00:00"}
    writes: list[tuple[Path, str]] = []

    def fake_write(path: Path, content: str) -> None:
        writes.append((path, content))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    monkeypatch.setattr(runner_module, "_write_text_atomic", fake_write)
    monkeypatch.setattr(
        runner_module,
        "AgendaClient",
        lambda config: _FakeAgendaClient(config, payload=payload, meta=meta),
    )

    fetched = run_agenda_fetch(_cfg(tmp_path, start_year=2024, start_month=1, end_year=2024, end_month=1))

    assert fetched == 1
    assert [path.name for path, _ in writes] == ["2024-01.json", "2024-01.jsonl"]
    assert writes[0][0].parent.name == "_raw"
    assert '"fetched_at": "2026-04-01T00:00:00+00:00"' in writes[0][1]
    assert '"event_id":' in writes[1][1]
    assert f'"normalization_version": "{RAW_EVENT_SCHEMA_VERSION}"' in writes[1][1]
