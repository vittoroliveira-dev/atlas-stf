from __future__ import annotations

import pandas as pd

from atlas_stf.staging._config import CONFIGS
from atlas_stf.staging._validators import validate_cross_file_reconciliation, validate_dataframe


def test_validate_dataframe_reports_required_and_date_warnings():
    df = pd.DataFrame(
        [
            {
                "classe": "AC",
                "no_do_processo": "1",
                "data_da_autuacao": "2026/03/06",
                "data_do_andamento": "2026-03-06",
                "tipo_de_andamento": "Distribuído aos Ministros",
            },
            {
                "classe": pd.NA,
                "no_do_processo": "2",
                "data_da_autuacao": "2026-03-07",
                "data_do_andamento": "2026-03-07",
                "tipo_de_andamento": "Distribuído aos Ministros",
            },
        ]
    )

    warnings = validate_dataframe(df, CONFIGS["distribuidos.csv"])

    assert any("required_fields:distribuidos.csv" in warning for warning in warnings)
    assert any("date_format:distribuidos.csv" in warning for warning in warnings)


def test_validate_dataframe_reports_duplicate_primary_key_and_process_format():
    df = pd.DataFrame(
        [
            {
                "classe": "123",
                "no_do_processo": "1",
                "data_da_autuacao": "2026-03-06",
                "data_do_andamento": "2026-03-06",
                "tipo_de_andamento": "Distribuído aos Ministros",
            },
            {
                "classe": "123",
                "no_do_processo": "1",
                "data_da_autuacao": "2026-03-07",
                "data_do_andamento": "2026-03-06",
                "tipo_de_andamento": "Distribuído aos Ministros",
            },
        ]
    )

    warnings = validate_dataframe(df, CONFIGS["distribuidos.csv"])

    assert any("duplicate_primary_key:distribuidos.csv" in warning for warning in warnings)
    assert any("process_number_format:distribuidos.csv" in warning for warning in warnings)


def test_validate_cross_file_reconciliation_reports_orphan_decision_processes():
    frames_by_file = {
        "acervo.csv": pd.DataFrame([{"processo": "AC 1", "numero_unico": "0001"}]),
        "decisoes.csv": pd.DataFrame(
            [
                {
                    "idfatodecisao": "1",
                    "processo": "AC 1",
                    "data_da_decisao": "2026-03-06",
                    "tipo_decisao": "Negado seguimento",
                    "andamento_decisao": "Baixa",
                },
                {
                    "idfatodecisao": "2",
                    "processo": "AC 99",
                    "data_da_decisao": "2026-03-07",
                    "tipo_decisao": "Negado seguimento",
                    "andamento_decisao": "Baixa",
                },
            ]
        ),
    }

    warnings_by_file = validate_cross_file_reconciliation(frames_by_file, CONFIGS)

    assert "acervo.csv" not in warnings_by_file
    assert "decisoes.csv" in warnings_by_file
    assert any("cross_file_reconciliation:decisoes.csv" in warning for warning in warnings_by_file["decisoes.csv"])
    assert any("AC 99" in warning for warning in warnings_by_file["decisoes.csv"])
