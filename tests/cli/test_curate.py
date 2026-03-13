from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.cli import main


def test_cli_curate_process(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    (staging_dir / "acervo.csv").write_text(
        "processo,classe,data_autuacao\nAC 1,AC,2026-03-06\n",
        encoding="utf-8",
    )
    output = tmp_path / "process.jsonl"

    code = main(["curate", "process", "--staging-dir", str(staging_dir), "--output", str(output)])

    assert code == 0
    payload = json.loads(output.read_text(encoding="utf-8").strip())
    assert payload["process_number"] == "AC 1"


def test_cli_curate_process_with_juris_enrichment(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    (staging_dir / "acervo.csv").write_text(
        "processo,classe,data_autuacao\nAC 1,AC,2026-03-06\n",
        encoding="utf-8",
    )

    juris_dir = tmp_path / "jurisprudencia"
    decisoes_dir = juris_dir / "decisoes"
    decisoes_dir.mkdir(parents=True)
    (decisoes_dir / "2026-03.jsonl").write_text(
        json.dumps(
            {
                "processo_codigo_completo": "AC 1",
                "inteiro_teor_url": "https://example.com/ac1.pdf",
                "partes_lista_texto": "PARTE A vs PARTE B",
                "documental_legislacao_citada_texto": "CF art 5",
                "procedencia_geografica_completo": "SAO PAULO - SP",
                "processo_classe_processual_unificada_extenso": "ACAO CAUTELAR",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    output = tmp_path / "process.jsonl"

    code = main(
        [
            "curate",
            "process",
            "--staging-dir",
            str(staging_dir),
            "--juris-dir",
            str(juris_dir),
            "--output",
            str(output),
        ]
    )

    assert code == 0
    payload = json.loads(output.read_text(encoding="utf-8").strip())
    assert payload["juris_inteiro_teor_url"] == "https://example.com/ac1.pdf"


def test_cli_curate_subject(tmp_path: Path):
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

    code = main(["curate", "subject", "--process-path", str(process_path), "--output", str(output)])

    assert code == 0
    payload = json.loads(output.read_text(encoding="utf-8").strip())
    assert payload["subject_raw"] == "A"


def test_cli_curate_all_builds_extended_outputs(tmp_path: Path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    (staging_dir / "acervo.csv").write_text(
        "processo,classe,data_autuacao,assuntos\nAC 1,AC,2026-03-06,A | B\n",
        encoding="utf-8",
    )
    (staging_dir / "decisoes.csv").write_text(
        (
            "idfatodecisao,processo,ano_da_decisao,data_da_decisao,tipo_decisao,"
            "andamento_decisao,indicador_colegiado,orgao_julgador\n"
            "1,AC 1,2026,2026-03-06,Decisão Final,DECISÃO,MONOCRÁTICA,MONOCRÁTICA\n"
        ),
        encoding="utf-8",
    )

    juris_dir = tmp_path / "jurisprudencia"
    decisoes_dir = juris_dir / "decisoes"
    decisoes_dir.mkdir(parents=True)
    (decisoes_dir / "2026-03.jsonl").write_text(
        json.dumps(
            {
                "processo_codigo_completo": "AC 1",
                "partes_lista_texto": "PARTE A vs PARTE B",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    output_dir = tmp_path / "curated"
    code = main(
        [
            "curate",
            "all",
            "--staging-dir",
            str(staging_dir),
            "--juris-dir",
            str(juris_dir),
            "--output-dir",
            str(output_dir),
        ]
    )

    assert code == 0
    assert (output_dir / "subject.jsonl").exists()
    assert (output_dir / "party.jsonl").exists()
    assert (output_dir / "counsel.jsonl").exists()
    assert (output_dir / "process_party_link.jsonl").exists()
    assert (output_dir / "process_counsel_link.jsonl").exists()
    assert (output_dir / "entity_identifier.jsonl").exists()
    assert (output_dir / "entity_identifier_reconciliation.jsonl").exists()


def test_cli_curate_entity_identifier(tmp_path: Path):
    process_path = tmp_path / "process.jsonl"
    process_path.write_text(
        json.dumps({"process_id": "proc_1", "process_number": "AC 1"}) + "\n",
        encoding="utf-8",
    )

    juris_dir = tmp_path / "jurisprudencia"
    decisoes_dir = juris_dir / "decisoes"
    decisoes_dir.mkdir(parents=True)
    (decisoes_dir / "2026-03.jsonl").write_text(
        json.dumps(
            {
                "_id": "doc-1",
                "processo_codigo_completo": "AC 1",
                "decisao_texto": "CPF: 529.982.247-25.",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    output = tmp_path / "entity_identifier.jsonl"

    code = main(
        [
            "curate",
            "entity-identifier",
            "--process-path",
            str(process_path),
            "--juris-dir",
            str(juris_dir),
            "--output",
            str(output),
        ]
    )

    assert code == 0
    payload = json.loads(output.read_text(encoding="utf-8").strip())
    assert payload["identifier_value_normalized"] == "52998224725"


def test_cli_curate_entity_reconciliation(tmp_path: Path):
    curated_dir = tmp_path / "curated"
    curated_dir.mkdir()
    (curated_dir / "entity_identifier.jsonl").write_text(
        json.dumps(
            {
                "identifier_occurrence_id": "eid_1",
                "process_id": "proc_1",
                "process_number": "AC 1",
                "identifier_kind": "cpf",
                "identifier_value_raw": "529.982.247-25",
                "identifier_value_normalized": "52998224725",
                "context_snippet": "JOAO DA SILVA CPF: 529.982.247-25",
                "entity_name_hint": "JOAO DA SILVA",
                "source_doc_type": "decisoes",
                "source_file": "2026-03.jsonl",
                "source_field": "decisao_texto",
                "source_url": None,
                "juris_doc_id": "doc-1",
                "extraction_confidence": 0.95,
                "extraction_method": "regex_labeled_tax_id",
                "uncertainty_note": None,
                "created_at": "2026-03-09T00:00:00+00:00",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (curated_dir / "party.jsonl").write_text(
        json.dumps(
            {
                "party_id": "party_1",
                "party_name_raw": "Joao da Silva",
                "party_name_normalized": "JOAO DA SILVA",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (curated_dir / "counsel.jsonl").write_text("", encoding="utf-8")
    (curated_dir / "process_party_link.jsonl").write_text(
        json.dumps(
            {
                "link_id": "ppl_1",
                "process_id": "proc_1",
                "party_id": "party_1",
                "role_in_case": None,
                "source_id": "juris",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (curated_dir / "process_counsel_link.jsonl").write_text("", encoding="utf-8")
    output = curated_dir / "entity_identifier_reconciliation.jsonl"

    code = main(
        [
            "curate",
            "entity-reconciliation",
            "--entity-identifier-path",
            str(curated_dir / "entity_identifier.jsonl"),
            "--party-path",
            str(curated_dir / "party.jsonl"),
            "--counsel-path",
            str(curated_dir / "counsel.jsonl"),
            "--process-party-link-path",
            str(curated_dir / "process_party_link.jsonl"),
            "--process-counsel-link-path",
            str(curated_dir / "process_counsel_link.jsonl"),
            "--output",
            str(output),
        ]
    )

    assert code == 0
    payload = json.loads(output.read_text(encoding="utf-8").strip())
    assert payload["entity_id"] == "party_1"
