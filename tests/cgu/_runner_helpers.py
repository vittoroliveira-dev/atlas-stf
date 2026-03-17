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
    """Build a CEIS CSV string with header + rows."""
    header = ";".join(
        [
            '"CADASTRO"',
            '"CÓDIGO"',
            '"TIPO"',
            '"CPF/CNPJ"',
            '"NOME"',
            '"NOME ORG"',
            '"RAZAO"',
            '"FANTASIA"',
            '"PROCESSO"',
            '"CATEGORIA"',
            '"DATA INÍCIO"',
            '"DATA FIM"',
            '"DATA PUB"',
            '"PUBLICAÇÃO"',
            '"DETALHE"',
            '"TRANSITO"',
            '"ABRANGENCIA"',
            '"ÓRGÃO"',
        ]
    )
    lines = [header]
    for row in records:
        lines.append(";".join(f'"{v}"' for v in row))
    return "\n".join(lines)


def _make_csv_zip(csv_content: str, csv_name: str = "20260306_CEIS.csv") -> bytes:
    """Create a ZIP containing one CSV file."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(csv_name, csv_content.encode("latin-1"))
    return buf.getvalue()


def _make_leniencia_csv(records: list[list[str]]) -> str:
    """Build a Leniência CSV string with header + rows (real CSV structure)."""
    header = ";".join(
        [
            '"ID DO ACORDO"',
            '"CNPJ DO SANCIONADO"',
            '"RAZAO SOCIAL - CADASTRO RECEITA"',
            '"NOME FANTASIA - CADASTRO RECEITA"',
            '"DATA DE INICIO DO ACORDO"',
            '"DATA DE FIM DO ACORDO"',
            '"SITUACAO DO ACORDO DE LENIENCIA"',
            '"DATA DA INFORMACAO"',
            '"NUMERO DO PROCESSO"',
            '"TERMOS DO ACORDO"',
            '"ORGAO SANCIONADOR"',
        ]
    )
    lines = [header]
    for row in records:
        lines.append(";".join(f'"{v}"' for v in row))
    return "\n".join(lines)
