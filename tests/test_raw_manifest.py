from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.raw_manifest import build_raw_manifest


def test_build_raw_manifest(tmp_path: Path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    (raw_dir / "acervo.csv").write_text("col1,col2\n1,2\n3,4\n", encoding="utf-8")

    output = raw_dir / "_manifest.jsonl"
    records = build_raw_manifest(raw_dir, output_path=output)

    assert len(records) == 1
    payload = json.loads(output.read_text(encoding="utf-8").strip())
    assert payload["filename"] == "acervo.csv"
    assert payload["row_count"] == 2
    assert payload["column_count"] == 2
    assert payload["origin_url"].endswith("/acervo/acervo.html")
