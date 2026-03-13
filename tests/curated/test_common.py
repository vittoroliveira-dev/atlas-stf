from __future__ import annotations

import pytest

from atlas_stf.curated.common import read_jsonl_records


def test_read_jsonl_records_raises_value_error_for_malformed_line(tmp_path):
    path = tmp_path / "broken.jsonl"
    path.write_text('{"ok": 1}\n{"broken": \n', encoding="utf-8")

    with pytest.raises(ValueError, match=r'Invalid JSONL record .* content=\'{"broken":\''):
        read_jsonl_records(path)
