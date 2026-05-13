"""Tests for scraper runner helpers."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from atlas_stf.scraper._runner import _date_extremes


def test_date_extremes_returns_first_and_last_dates(tmp_path: Path) -> None:
    path = tmp_path / "sample.jsonl"
    path.write_text(
        '{"publicacao_data":"2024-01-01"}\n'
        '{"publicacao_data":"2024-01-15"}\n'
        '{"publicacao_data":"2024-01-31"}\n',
        encoding="utf-8",
    )

    assert _date_extremes(path) == ("2024-01-01", "2024-01-31")


def test_date_extremes_skips_malformed_jsonl_with_context(tmp_path: Path, caplog) -> None:
    path = tmp_path / "sample.jsonl"
    path.write_text('{"publicacao_data":"2024-01-01"}\nNOT JSON\n', encoding="utf-8")

    with caplog.at_level(logging.WARNING, logger="atlas_stf.scraper"):
        assert _date_extremes(path) == ("2024-01-01", "2024-01-01")

    assert f"{path}:2" in caplog.text
    assert "malformed" in caplog.text.lower()


def test_date_extremes_skips_non_object_jsonl_with_context(tmp_path: Path, caplog) -> None:
    path = tmp_path / "sample.jsonl"
    path.write_text('{"publicacao_data":"2024-01-01"}\n[]\n', encoding="utf-8")

    with caplog.at_level(logging.WARNING, logger="atlas_stf.scraper"):
        assert _date_extremes(path) == ("2024-01-01", "2024-01-01")

    assert f"{path}:2" in caplog.text
    assert "non-object" in caplog.text.lower()


@pytest.mark.parametrize(
    "payload",
    [
        '{"publicacao_data":123}\n',
        '{"publicacao_data":true}\n',
        '{"publicacao_data":["2024-01-01"]}\n',
        '{"publicacao_data":{"date":"2024-01-01"}}\n',
        '{"publicacao_data":0}\n',
        '{"publicacao_data":false}\n',
        '{"publicacao_data":[]}\n',
        '{"publicacao_data":{}}\n',
    ],
)
def test_date_extremes_skips_non_string_publicacao_data(tmp_path: Path, caplog, payload: str) -> None:
    path = tmp_path / "sample.jsonl"
    path.write_text('{"publicacao_data":"2024-01-01"}\n' + payload, encoding="utf-8")

    with caplog.at_level(logging.WARNING, logger="atlas_stf.scraper"):
        assert _date_extremes(path) == ("2024-01-01", "2024-01-01")

    assert f"{path}:2" in caplog.text
    assert "non-string publicacao_data" in caplog.text.lower()
