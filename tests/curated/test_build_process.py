from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.curated.build_process import (
    build_process_jsonl,
    build_process_records,
)


def test_build_process_records_merges_sources(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    (staging_dir / "acervo.csv").write_text(
        "processo,classe,data_autuacao,data_baixa,orgao_origem,ramo_do_direito,assuntos,meio_processo,situacao_processual\n"
        "AC 1,AC,2026-03-01,2026-03-10,STF,DIREITO X,A | B,ELETRONICO,Baixado\n",
        encoding="utf-8",
    )
    (staging_dir / "reclamacoes.csv").write_text(
        "processo,numero_unico,data_autuacao,procedencia,ramo_direito,em_tramitacao\n"
        "Rcl 2,0002,2026-03-05,ACRE,DIREITO Y,Sim\n",
        encoding="utf-8",
    )

    records = build_process_records(staging_dir=staging_dir)

    assert len(records) == 2
    ac1 = next(record for record in records if record["process_number"] == "AC 1")
    assert ac1["process_class"] == "AC"
    assert ac1["filing_date"] == "2026-03-01"
    assert ac1["subjects_raw"] == ["A", "B"]
    assert ac1["source_record_hash"] is not None


def test_build_process_jsonl_writes_file(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    (staging_dir / "acervo.csv").write_text(
        "processo,classe,data_autuacao\nAC 1,AC,2026-03-01\n",
        encoding="utf-8",
    )

    output = tmp_path / "process.jsonl"
    build_process_jsonl(staging_dir=staging_dir, output_path=output)

    payload = json.loads(output.read_text(encoding="utf-8").strip())
    assert payload["process_number"] == "AC 1"
