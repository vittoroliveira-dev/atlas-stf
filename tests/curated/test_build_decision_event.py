from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.curated.build_decision_event import (
    build_decision_event_jsonl,
    build_decision_event_records,
)


def test_build_decision_event_records(tmp_path: Path):
    staging_file = tmp_path / "decisoes.csv"
    staging_file.write_text(
        "idfatodecisao,processo,relator_atual,origem_decisao,indicador_colegiado,ano_da_decisao,data_da_decisao,tipo_decisao,andamento_decisao,observacao_do_andamento,orgao_julgador\n"
        "1,AC 1,MIN X,MONOCRATICA,MONOCRATICA,2026,2026-03-06,Decisao Final,DECISAO,Obs,MONOCRATICA\n",
        encoding="utf-8",
    )

    records = build_decision_event_records(staging_file=staging_file)

    assert len(records) == 1
    record = records[0]
    assert record["source_row_id"] == "1"
    assert record["process_id"].startswith("proc_")
    assert record["is_collegiate"] is False
    assert record["time_bucket"] == "2026-03"


def test_build_decision_event_jsonl_writes_file(tmp_path: Path):
    staging_file = tmp_path / "decisoes.csv"
    staging_file.write_text(
        "idfatodecisao,processo,ano_da_decisao,data_da_decisao\n1,AC 1,2026,2026-03-06\n",
        encoding="utf-8",
    )

    output = tmp_path / "decision_event.jsonl"
    build_decision_event_jsonl(staging_file=staging_file, output_path=output)

    payload = json.loads(output.read_text(encoding="utf-8").strip())
    assert payload["source_row_id"] == "1"


def test_plenario_virtual_auto_detected(tmp_path: Path):
    """Supplementary plenario_virtual.csv is auto-detected and merged."""
    staging_file = tmp_path / "decisoes.csv"
    staging_file.write_text(
        "idfatodecisao,processo,ano_da_decisao,data_da_decisao\n1,AC 1,2026,2026-03-06\n",
        encoding="utf-8",
    )
    pv_file = tmp_path / "plenario_virtual.csv"
    pv_file.write_text(
        "processo,orgao_julgador,relator_da_decisao,relator_atual,data_autuacao,data_decisao,"
        "data_baixa,grupo_de_origem,tipo_de_classe,classe,ramos_do_direito,assunto,assunto_completo,"
        "seq_objeto_incidente,link_processo,cod_andamento,subgrupo_andamento,descricao_andamento,"
        "observacao_andamento,tipo_decisao,preferencia_covid19,preferencia_criminal,"
        "sigla_ultimo_recurso,recurso_interno_pendente,em_tramitacao,decisoes_virtual\n"
        "RE 999999,1a Turma,MIN Y,MIN Y,2025-01-01,2025-06-15,,SP,,RE,DIREITO CIVIL,,"
        "Dano moral,,link,6.230,Julgamento,Provido,Obs virtual,COLEGIADA,,,,,\n",
        encoding="utf-8",
    )

    records = build_decision_event_records(staging_file=staging_file)

    assert len(records) == 2
    decisoes_recs = [r for r in records if r["raw_fields"]["source_file"] == "decisoes.csv"]
    pv_recs = [r for r in records if r["raw_fields"]["source_file"] == "plenario_virtual.csv"]
    assert len(decisoes_recs) == 1
    assert len(pv_recs) == 1

    pv = pv_recs[0]
    assert pv["decision_date"] == "2025-06-15"
    assert pv["decision_year"] == 2025
    assert pv["current_rapporteur"] == "MIN Y"
    assert pv["decision_type"] == "COLEGIADA"
    assert pv["decision_progress"] == "Provido"
    assert pv["decision_note"] == "Obs virtual"
    assert pv["is_collegiate"] is True
    assert pv["judging_body"] == "1a Turma"
    assert pv["source_row_id"] == "6.230"
    assert pv["time_bucket"] == "2025-06"


def test_decisoes_covid_auto_detected(tmp_path: Path):
    """Supplementary decisoes_covid.csv is auto-detected and merged."""
    staging_file = tmp_path / "decisoes.csv"
    staging_file.write_text(
        "idfatodecisao,processo,ano_da_decisao,data_da_decisao\n1,AC 1,2026,2026-03-06\n",
        encoding="utf-8",
    )
    covid_file = tmp_path / "decisoes_covid.csv"
    covid_file.write_text(
        "processo,relator,data_autuacao,data_preferencia_covid,data_decisao,tipo_decisao,"
        "observacao_decisao,grupo_classe,tipo_classe,classe,ramo_do_direito,assunto,"
        "em_tramitacao,link_processo\n"
        "ADI 5555,MIN Z,,2020-04-01,2020-05-10,Prejudicado,Decisao COVID urgente,,,ADI,,,Sim,\n",
        encoding="utf-8",
    )

    records = build_decision_event_records(staging_file=staging_file)

    assert len(records) == 2
    covid_recs = [r for r in records if r["raw_fields"]["source_file"] == "decisoes_covid.csv"]
    assert len(covid_recs) == 1

    cr = covid_recs[0]
    assert cr["decision_date"] == "2020-05-10"
    assert cr["decision_year"] == 2020
    assert cr["current_rapporteur"] == "MIN Z"
    assert cr["decision_type"] == "Prejudicado"
    assert cr["decision_note"] == "Decisao COVID urgente"
    assert cr["decision_progress"] == "Prejudicado"
    assert cr["time_bucket"] == "2020-05"


def test_supplementary_skipped_when_absent(tmp_path: Path):
    """When supplementary CSVs don't exist, only primary is processed."""
    staging_file = tmp_path / "decisoes.csv"
    staging_file.write_text(
        "idfatodecisao,processo,ano_da_decisao,data_da_decisao\n1,AC 1,2026,2026-03-06\n",
        encoding="utf-8",
    )

    records = build_decision_event_records(staging_file=staging_file)
    assert len(records) == 1
    assert records[0]["raw_fields"]["source_file"] == "decisoes.csv"


def test_primary_seed_includes_filename_when_source_row_id_is_empty(tmp_path: Path):
    file_a = tmp_path / "decisoes.csv"
    file_b = tmp_path / "decisoes_extra.csv"
    payload = "idfatodecisao,processo,ano_da_decisao,data_da_decisao\n,AC 1,2026,2026-03-06\n"
    file_a.write_text(payload, encoding="utf-8")
    file_b.write_text(payload, encoding="utf-8")

    records_a = build_decision_event_records(staging_file=file_a)
    records_b = build_decision_event_records(staging_file=file_b)

    assert len(records_a) == 1
    assert len(records_b) == 1
    assert records_a[0]["source_row_id"] is None
    assert records_b[0]["source_row_id"] is None
    assert records_a[0]["decision_event_id"] != records_b[0]["decision_event_id"]
