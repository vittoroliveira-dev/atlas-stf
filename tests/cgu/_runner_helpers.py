"""Shared helpers for CGU runner tests."""

from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path


class _FakeUrlopenResponse:
    def __init__(self, body: bytes | None = None, *, chunks: list[bytes] | None = None) -> None:
        if chunks is not None:
            self._chunks = list(chunks)
        else:
            self._chunks = [body or b""]

    def __enter__(self) -> _FakeUrlopenResponse:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def read(self, _size: int = -1) -> bytes:
        if not self._chunks:
            return b""
        return self._chunks.pop(0)


def _write_party_jsonl(path: Path, parties: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        for p in parties:
            fh.write(json.dumps(p) + "\n")


def _make_ceis_csv(records: list[list[str]]) -> str:
    """Build a CEIS CSV string with real portal headers + rows.

    Rows may have fewer than 24 columns; missing trailing columns are padded
    with empty strings so the data offsets match _CEIS_COL indices.
    """
    header = ";".join(
        [
            '"CADASTRO"',
            '"CÓDIGO DA SANÇÃO"',
            '"TIPO DE PESSOA"',
            '"CPF OU CNPJ DO SANCIONADO"',
            '"NOME DO SANCIONADO"',
            '"NOME INFORMADO PELO ÓRGÃO SANCIONADOR"',
            '"RAZÃO SOCIAL - CADASTRO RECEITA"',
            '"NOME FANTASIA - CADASTRO RECEITA"',
            '"NÚMERO DO PROCESSO"',
            '"CATEGORIA DA SANÇÃO"',
            '"DATA INÍCIO SANÇÃO"',
            '"DATA FINAL SANÇÃO"',
            '"DATA PUBLICAÇÃO"',
            '"PUBLICAÇÃO"',
            '"DETALHAMENTO DO MEIO DE PUBLICAÇÃO"',
            '"DATA DO TRÂNSITO EM JULGADO"',
            '"ABRAGÊNCIA DA SANÇÃO"',
            '"ÓRGÃO SANCIONADOR"',
            '"UF ÓRGÃO SANCIONADOR"',
            '"ESFERA ÓRGÃO SANCIONADOR"',
            '"FUNDAMENTAÇÃO LEGAL"',
            '"DATA ORIGEM INFORMAÇÃO"',
            '"ORIGEM INFORMAÇÕES"',
            '"OBSERVAÇÕES"',
        ]
    )
    lines = [header]
    for row in records:
        # Pad to 24 columns so header/data alignment is always valid
        padded = list(row) + [""] * (24 - len(row))
        lines.append(";".join(f'"{v}"' for v in padded[:24]))
    return "\n".join(lines)


def _make_csv_zip(csv_content: str, csv_name: str = "20260306_CEIS.csv") -> bytes:
    """Create a ZIP containing one CSV file."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(csv_name, csv_content.encode("latin-1"))
    return buf.getvalue()


def _make_cnep_csv(records: list[list[str]]) -> str:
    """Build a CNEP CSV string with real portal headers + rows.

    Identical to CEIS except it has "VALOR DA MULTA" at position 10, making it
    25 columns total. Rows may have fewer than 25 columns; missing trailing
    columns are padded with empty strings.
    """
    header = ";".join(
        [
            '"CADASTRO"',
            '"CÓDIGO DA SANÇÃO"',
            '"TIPO DE PESSOA"',
            '"CPF OU CNPJ DO SANCIONADO"',
            '"NOME DO SANCIONADO"',
            '"NOME INFORMADO PELO ÓRGÃO SANCIONADOR"',
            '"RAZÃO SOCIAL - CADASTRO RECEITA"',
            '"NOME FANTASIA - CADASTRO RECEITA"',
            '"NÚMERO DO PROCESSO"',
            '"CATEGORIA DA SANÇÃO"',
            '"VALOR DA MULTA"',
            '"DATA INÍCIO SANÇÃO"',
            '"DATA FINAL SANÇÃO"',
            '"DATA PUBLICAÇÃO"',
            '"PUBLICAÇÃO"',
            '"DETALHAMENTO DO MEIO DE PUBLICAÇÃO"',
            '"DATA DO TRÂNSITO EM JULGADO"',
            '"ABRAGÊNCIA DA SANÇÃO"',
            '"ÓRGÃO SANCIONADOR"',
            '"UF ÓRGÃO SANCIONADOR"',
            '"ESFERA ÓRGÃO SANCIONADOR"',
            '"FUNDAMENTAÇÃO LEGAL"',
            '"DATA ORIGEM INFORMAÇÃO"',
            '"ORIGEM INFORMAÇÕES"',
            '"OBSERVAÇÕES"',
        ]
    )
    lines = [header]
    for row in records:
        padded = list(row) + [""] * (25 - len(row))
        lines.append(";".join(f'"{v}"' for v in padded[:25]))
    return "\n".join(lines)


def _make_leniencia_csv(records: list[list[str]]) -> str:
    """Build a Leniência CSV string with real portal headers + rows."""
    header = ";".join(
        [
            '"ID DO ACORDO"',
            '"CNPJ DO SANCIONADO"',
            '"RAZÃO SOCIAL  CADASTRO RECEITA"',
            '"NOME FANTASIA  CADASTRO RECEITA"',
            '"DATA DE INÍCIO DO ACORDO"',
            '"DATA DE FIM DO ACORDO"',
            '"SITUAÇÃO DO ACORDO DE LENIÊNCIA"',
            '"DATA DA INFORMAÇÃO"',
            '"NÚMERO DO PROCESSO"',
            '"TERMOS DO ACORDO"',
            '"ÓRGÃO SANCIONADOR"',
        ]
    )
    lines = [header]
    for row in records:
        lines.append(";".join(f'"{v}"' for v in row))
    return "\n".join(lines)
