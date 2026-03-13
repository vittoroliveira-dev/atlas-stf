from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.curated.build_subject import build_subject_jsonl, build_subject_records


def test_build_subject_records_deduplicates_by_normalized_value(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    process_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "process_id": "proc_1",
                        "subjects_raw": ["A | B", "C"],
                        "subjects_normalized": ["A | B", "C"],
                        "branch_of_law": "DIREITO X",
                    }
                ),
                json.dumps(
                    {
                        "process_id": "proc_2",
                        "subjects_raw": ["C"],
                        "subjects_normalized": ["C"],
                        "branch_of_law": "DIREITO Y",
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    records = build_subject_records(process_path=process_path)

    assert len(records) == 2
    subject_c = next(record for record in records if record["subject_normalized"] == "C")
    assert subject_c["branch_of_law"] == "DIREITO X"


def test_build_subject_jsonl_writes_file(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    process_path.write_text(
        json.dumps(
            {
                "process_id": "proc_1",
                "subjects_raw": ["A"],
                "subjects_normalized": ["A"],
                "branch_of_law": "DIREITO X",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    output = tmp_path / "subject.jsonl"
    build_subject_jsonl(process_path=process_path, output_path=output)

    payload = json.loads(output.read_text(encoding="utf-8").strip())
    assert payload["subject_raw"] == "A"
