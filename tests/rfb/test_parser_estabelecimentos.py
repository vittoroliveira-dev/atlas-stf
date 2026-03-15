"""Tests for RFB Estabelecimentos CSV parser."""

from __future__ import annotations

import io

from atlas_stf.rfb._parser_estabelecimentos import parse_estabelecimentos_csv_filtered_text


class TestParseEstabelecimentosCsvFilteredText:
    def _make_row(self, cols: list[str]) -> str:
        """Build a single CSV row with semicolon delimiter."""
        return ";".join(cols)

    def _make_stream(self, rows: list[str]) -> io.StringIO:
        """Build a text stream from raw CSV lines."""
        return io.StringIO("\n".join(rows) + "\n")

    def _full_row(self, **overrides: str) -> str:
        """Build a 21-column establishment row with sensible defaults."""
        defaults = [
            "12345678",  # 0  cnpj_basico
            "0001",  # 1  cnpj_ordem
            "95",  # 2  cnpj_dv
            "1",  # 3  matriz_filial
            "NOME FANTASIA",  # 4 nome_fantasia
            "02",  # 5  situacao_cadastral
            "20200101",  # 6  data_situacao_cadastral
            "01",  # 7  motivo_situacao_cadastral
            "",  # 8  (unused)
            "",  # 9  (unused)
            "20100315",  # 10 data_inicio_atividade
            "6911701",  # 11 cnae_fiscal
            "6920601,7020400",  # 12 cnae_fiscal_secundaria
            "",  # 13 (unused)
            "RUA EXEMPLO",  # 14 logradouro
            "100",  # 15 numero
            "",  # 16 (unused)
            "CENTRO",  # 17 bairro
            "01000000",  # 18 cep
            "SP",  # 19 uf
            "7107",  # 20 municipio
        ]
        for key, value in overrides.items():
            idx = int(key.replace("col", ""))
            defaults[idx] = value
        return self._make_row(defaults)

    def test_delimiter_semicolon(self) -> None:
        row = self._full_row()
        stream = self._make_stream([row])
        records = parse_estabelecimentos_csv_filtered_text(stream, target_cnpjs={"12345678"})
        assert len(records) == 1
        assert records[0]["cnpj_basico"] == "12345678"

    def test_filter_by_cnpj(self) -> None:
        row_match = self._full_row()
        row_other = self._full_row(**{"col0": "99999999"})
        stream = self._make_stream([row_match, row_other])
        records = parse_estabelecimentos_csv_filtered_text(stream, target_cnpjs={"12345678"})
        assert len(records) == 1
        assert records[0]["cnpj_basico"] == "12345678"

    def test_cnpj_full_assembly(self) -> None:
        row = self._full_row()
        stream = self._make_stream([row])
        records = parse_estabelecimentos_csv_filtered_text(stream, target_cnpjs={"12345678"})
        assert len(records) == 1
        assert records[0]["cnpj_full"] == "12345678000195"
        assert len(records[0]["cnpj_full"]) == 14

    def test_cnae_fiscal_secundaria_split(self) -> None:
        row = self._full_row()
        stream = self._make_stream([row])
        records = parse_estabelecimentos_csv_filtered_text(stream, target_cnpjs={"12345678"})
        assert records[0]["cnae_fiscal_secundaria"] == ["6920601", "7020400"]

    def test_situacao_cadastral_extracted(self) -> None:
        row = self._full_row(**{"col5": "08"})
        stream = self._make_stream([row])
        records = parse_estabelecimentos_csv_filtered_text(stream, target_cnpjs={"12345678"})
        assert records[0]["situacao_cadastral"] == "08"

    def test_empty_nome_fantasia(self) -> None:
        row = self._full_row(**{"col4": ""})
        stream = self._make_stream([row])
        records = parse_estabelecimentos_csv_filtered_text(stream, target_cnpjs={"12345678"})
        assert records[0]["nome_fantasia"] == ""

    def test_short_row_skipped(self) -> None:
        """Rows with fewer than 21 columns are silently skipped."""
        short_row = self._make_row(["12345678", "0001", "95", "1", "NOME"] + [""] * 10)
        assert short_row.count(";") < 20  # fewer than 21 columns
        stream = self._make_stream([short_row])
        records = parse_estabelecimentos_csv_filtered_text(stream, target_cnpjs={"12345678"})
        assert len(records) == 0

    def test_empty_target_cnpjs_returns_empty(self) -> None:
        row = self._full_row()
        stream = self._make_stream([row])
        records = parse_estabelecimentos_csv_filtered_text(stream, target_cnpjs=set())
        assert records == []
