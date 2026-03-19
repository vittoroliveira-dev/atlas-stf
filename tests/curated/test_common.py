from __future__ import annotations

import json

import pytest

from atlas_stf.curated.common import read_jsonl_records, write_jsonl


def test_read_jsonl_records_raises_value_error_for_malformed_line(tmp_path):
    path = tmp_path / "broken.jsonl"
    path.write_text('{"ok": 1}\n{"broken": \n', encoding="utf-8")

    with pytest.raises(ValueError, match=r'Invalid JSONL record .* content=\'{"broken":\''):
        read_jsonl_records(path)


def test_write_jsonl_atomic_no_tmp_residue(tmp_path):
    """write_jsonl must use tmp+rename: final file exists, no .tmp residue."""
    output = tmp_path / "out.jsonl"
    records = [{"a": 1}, {"b": 2}]

    result = write_jsonl(records, output)

    assert result == output
    assert output.exists()
    assert not output.with_suffix(".jsonl.tmp").exists()
    loaded = read_jsonl_records(output)
    assert loaded == records


def test_write_jsonl_atomic_overwrites_existing(tmp_path):
    """write_jsonl must atomically overwrite a pre-existing file."""
    output = tmp_path / "out.jsonl"
    output.write_text(json.dumps({"old": True}) + "\n", encoding="utf-8")

    write_jsonl([{"new": True}], output)

    loaded = read_jsonl_records(output)
    assert loaded == [{"new": True}]


def test_write_jsonl_creates_parent_dirs(tmp_path):
    """write_jsonl must create parent directories if missing."""
    output = tmp_path / "nested" / "deep" / "out.jsonl"

    write_jsonl([{"x": 1}], output)

    assert output.exists()
    assert read_jsonl_records(output) == [{"x": 1}]
