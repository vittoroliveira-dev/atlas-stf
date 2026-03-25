"""Parser field contracts for RFB Socios/Empresas/Estabelecimentos."""

from __future__ import annotations

import io

from atlas_stf.rfb._parser import (
    parse_empresas_csv_filtered_text,
    parse_socios_csv_filtered_text,
)
from atlas_stf.rfb._parser_estabelecimentos import (
    _ESTABELECIMENTOS_SCHEMA,
    _EXPECTED_FULL_COLUMNS,
    _EXPECTED_MIN_COLUMNS,
    parse_estabelecimentos_csv_filtered_text,
)


class TestSociosFieldContract:
    """RFB Socios: positional CSV (no header), semicolon-separated."""

    def test_happy_path_all_fields(self) -> None:
        # 10 columns: cnpj_basico;partner_type;partner_name;partner_cpf_cnpj;
        # qualification_code;entry_date;col6;representative_cpf_cnpj;
        # representative_name;representative_qualification
        line = "12345678;2;JOAO DA SILVA;12345678901;22;20200101;0;98765432100;MARIA DA SILVA;05\n"
        stream = io.StringIO(line)
        records, cnpjs = parse_socios_csv_filtered_text(
            stream,
            target_names={"JOAO DA SILVA"},
            target_cnpjs=set(),
        )
        assert len(records) == 1
        rec = records[0]
        assert rec["cnpj_basico"] == "12345678"
        assert rec["partner_type"] == "2"
        assert rec["partner_name"] == "JOAO DA SILVA"
        assert rec["partner_cpf_cnpj"] == "12345678901"
        assert rec["qualification_code"] == "22"
        assert rec["entry_date"] == "20200101"
        assert rec["representative_cpf_cnpj"] == "98765432100"
        assert rec["representative_name"] == "MARIA DA SILVA"
        assert "12345678" in cnpjs

    def test_empty_representative(self) -> None:
        line = "12345678;2;JOAO DA SILVA;12345678901;22;20200101;0;;;\n"
        stream = io.StringIO(line)
        records, _ = parse_socios_csv_filtered_text(
            stream,
            target_names={"JOAO DA SILVA"},
            target_cnpjs=set(),
        )
        assert len(records) == 1
        assert records[0]["representative_cpf_cnpj"] == ""
        assert records[0]["representative_name"] == ""

    def test_short_row_skipped(self) -> None:
        """Rows with fewer than 6 columns should be skipped."""
        line = "12345678;2;NAME;CPF;CODE\n"  # only 5 cols
        stream = io.StringIO(line)
        records, _ = parse_socios_csv_filtered_text(
            stream,
            target_names={"NAME"},
            target_cnpjs=set(),
        )
        assert len(records) == 0

    def test_cnpj_match_includes_row(self) -> None:
        """Row matched via target_cnpjs should appear in results."""
        line = "12345678;2;OUTRO NOME;12345678901;22;20200101;0;;;\n"
        stream = io.StringIO(line)
        records, cnpjs = parse_socios_csv_filtered_text(
            stream,
            target_names=set(),
            target_cnpjs={"12345678"},
        )
        assert len(records) == 1
        # cnpj_match does NOT add to matched_cnpjs (only name/cpf/partner_cnpj matches do)
        assert "12345678" not in cnpjs


class TestEmpresasFieldContract:
    def test_happy_path(self) -> None:
        # 6 columns: cnpj_basico;razao_social;natureza_juridica;col3;capital_social;porte
        line = "12345678;EMPRESA TESTE LTDA;2062;0;100000,00;05\n"
        stream = io.StringIO(line)
        records = parse_empresas_csv_filtered_text(stream, {"12345678"})
        assert len(records) == 1
        rec = records[0]
        assert rec["cnpj_basico"] == "12345678"
        assert rec["razao_social"] == "EMPRESA TESTE LTDA"
        assert rec["natureza_juridica"] == "2062"
        assert rec["capital_social"] == 100000.0
        assert rec["porte_empresa"] == "05"

    def test_invalid_capital_is_none(self) -> None:
        line = "12345678;EMPRESA TESTE LTDA;2062;0;INVALID;05\n"
        stream = io.StringIO(line)
        records = parse_empresas_csv_filtered_text(stream, {"12345678"})
        assert len(records) == 1
        assert records[0]["capital_social"] is None

    def test_empty_capital_is_none(self) -> None:
        line = "12345678;EMPRESA TESTE LTDA;2062;0;;05\n"
        stream = io.StringIO(line)
        records = parse_empresas_csv_filtered_text(stream, {"12345678"})
        assert len(records) == 1
        assert records[0]["capital_social"] is None

    def test_unmatched_cnpj_skipped(self) -> None:
        line = "99999999;OUTRA EMPRESA;2062;0;5000,00;01\n"
        stream = io.StringIO(line)
        records = parse_empresas_csv_filtered_text(stream, {"12345678"})
        assert len(records) == 0


class TestEstabelecimentosFieldContract:
    def test_happy_path(self) -> None:
        cols = [
            "12345678",  # 0: cnpj_basico
            "0001",  # 1: cnpj_ordem
            "00",  # 2: cnpj_dv
            "1",  # 3: matriz_filial
            "TESTE FANTASIA",  # 4: nome_fantasia
            "02",  # 5: situacao_cadastral
            "20200101",  # 6: data_situacao
            "01",  # 7: motivo_situacao
            "",  # 8
            "",  # 9
            "20180601",  # 10: data_inicio_atividade
            "4712100",  # 11: cnae_fiscal
            "4511101,4520001",  # 12: cnae_secundaria
            "",  # 13
            "RUA TESTE",  # 14: logradouro
            "100",  # 15: numero
            "",  # 16
            "CENTRO",  # 17: bairro
            "01001000",  # 18: cep
            "SP",  # 19: uf
            "7107",  # 20: municipio
        ]
        line = ";".join(cols) + "\n"
        stream = io.StringIO(line)
        records = parse_estabelecimentos_csv_filtered_text(stream, {"12345678"})
        assert len(records) == 1
        rec = records[0]
        # cnpj_full = cnpj_basico + cnpj_ordem + cnpj_dv
        assert rec["cnpj_full"] == "12345678000100"
        assert rec["cnpj_basico"] == "12345678"
        assert rec["cnpj_ordem"] == "0001"
        assert rec["cnpj_dv"] == "00"
        assert rec["nome_fantasia"] == "TESTE FANTASIA"
        assert rec["cnae_fiscal"] == "4712100"
        assert rec["cnae_fiscal_secundaria"] == ["4511101", "4520001"]
        assert rec["uf"] == "SP"
        assert rec["bairro"] == "CENTRO"

    def test_short_row_skipped(self) -> None:
        """Rows with fewer than 21 columns should be skipped."""
        cols = ["12345678"] + ["x"] * 10  # only 11 cols
        line = ";".join(cols) + "\n"
        stream = io.StringIO(line)
        records = parse_estabelecimentos_csv_filtered_text(stream, {"12345678"})
        assert len(records) == 0

    def test_empty_cnae_secundaria(self) -> None:
        cols = [
            "12345678",
            "0001",
            "00",
            "1",
            "",
            "02",
            "20200101",
            "01",
            "",
            "",
            "20180601",
            "4712100",
            "",  # 12: cnae_secundaria vazia
            "",
            "RUA X",
            "1",
            "",
            "BAIRRO",
            "01001000",
            "RJ",
            "1234",
        ]
        line = ";".join(cols) + "\n"
        stream = io.StringIO(line)
        records = parse_estabelecimentos_csv_filtered_text(stream, {"12345678"})
        assert len(records) == 1
        assert records[0]["cnae_fiscal_secundaria"] == []

    def test_full_30_columns_with_email(self) -> None:
        """Full 30-column row: correio_eletronico at index 27 is extracted."""
        cols = [
            "12345678",  # 0: cnpj_basico
            "0001",  # 1: cnpj_ordem
            "30",  # 2: cnpj_dv
            "1",  # 3: matriz_filial
            "CLINICA VIDA",  # 4: nome_fantasia
            "02",  # 5: situacao_cadastral (ativa)
            "20200601",  # 6: data_situacao_cadastral
            "00",  # 7: motivo_situacao_cadastral
            "",  # 8: nome_cidade_exterior
            "",  # 9: cod_pais
            "20150301",  # 10: data_inicio_atividade
            "8630502",  # 11: cnae_fiscal
            "",  # 12: cnae_fiscal_secundaria
            "RUA",  # 13: tipo_logradouro
            "DR ARNALDO",  # 14: logradouro
            "100",  # 15: numero
            "SALA 3",  # 16: complemento
            "CONSOLACAO",  # 17: bairro
            "01246903",  # 18: cep
            "SP",  # 19: uf
            "7107",  # 20: cod_municipio
            "11",  # 21: ddd1
            "31234567",  # 22: telefone1
            "",  # 23: ddd2
            "",  # 24: telefone2
            "",  # 25: ddd_fax
            "",  # 26: fax
            "contato@clinicavida.com.br",  # 27: correio_eletronico
            "",  # 28: situacao_especial
            "",  # 29: data_situacao_especial
        ]
        assert len(cols) == 30
        line = ";".join(cols) + "\n"
        stream = io.StringIO(line)
        records = parse_estabelecimentos_csv_filtered_text(stream, {"12345678"})
        assert len(records) == 1
        rec = records[0]
        assert rec["correio_eletronico"] == "contato@clinicavida.com.br"
        assert rec["cnae_fiscal"] == "8630502"
        assert rec["nome_fantasia"] == "CLINICA VIDA"

    def test_email_empty_when_21_columns(self) -> None:
        """21-column row (legacy short): correio_eletronico defaults to empty."""
        cols = ["12345678", "0001", "00", "1", "", "02", "20200101", "01", "", "", "20180601",
                "4712100", "", "", "RUA X", "1", "", "BAIRRO", "01001000", "RJ", "1234"]
        assert len(cols) == 21
        line = ";".join(cols) + "\n"
        stream = io.StringIO(line)
        records = parse_estabelecimentos_csv_filtered_text(stream, {"12345678"})
        assert len(records) == 1
        assert records[0]["correio_eletronico"] == ""


class TestEstabelecimentosSchemaContract:
    """Verify schema constants match real RFB layout assumptions."""

    def test_schema_covers_30_columns(self) -> None:
        assert _EXPECTED_FULL_COLUMNS == 30

    def test_min_columns_is_21(self) -> None:
        assert _EXPECTED_MIN_COLUMNS == 21

    def test_email_at_index_27(self) -> None:
        assert _ESTABELECIMENTOS_SCHEMA[27] == "correio_eletronico"

    def test_cnpj_basico_at_index_0(self) -> None:
        assert _ESTABELECIMENTOS_SCHEMA[0] == "cnpj_basico"

    def test_cnae_fiscal_at_index_11(self) -> None:
        assert _ESTABELECIMENTOS_SCHEMA[11] == "cnae_fiscal"

    def test_uf_at_index_19(self) -> None:
        assert _ESTABELECIMENTOS_SCHEMA[19] == "uf"

    def test_schema_contiguous(self) -> None:
        """All indices 0-29 must be present in schema."""
        for i in range(_EXPECTED_FULL_COLUMNS):
            assert i in _ESTABELECIMENTOS_SCHEMA, f"Missing index {i} in schema"


class TestManifestCapturingStream:
    """Verify single-pass manifest capture wrapper used by _parse_csv_from_zip_text."""

    def test_captures_first_n_lines(self) -> None:
        from atlas_stf.rfb._runner_http import _ManifestCapturingStream

        lines = [f"line{i}\n" for i in range(200)]
        inner = io.StringIO("".join(lines))
        wrapper = _ManifestCapturingStream(inner, max_lines=5)

        consumed = list(wrapper)
        assert len(consumed) == 200
        assert len(wrapper.captured_lines) == 5
        assert wrapper.captured_lines == [f"line{i}" for i in range(5)]

    def test_parser_sees_all_lines(self) -> None:
        """Parser receives every line, not just the captured sample."""
        from atlas_stf.rfb._runner_http import _ManifestCapturingStream

        data = "a;b;c\n1;2;3\n4;5;6\n"
        inner = io.StringIO(data)
        wrapper = _ManifestCapturingStream(inner, max_lines=1)

        import csv

        reader = csv.reader(wrapper, delimiter=";")
        rows = list(reader)
        assert len(rows) == 3
        assert rows[0] == ["a", "b", "c"]

    def test_readline_also_captures(self) -> None:
        from atlas_stf.rfb._runner_http import _ManifestCapturingStream

        inner = io.StringIO("first\nsecond\nthird\n")
        wrapper = _ManifestCapturingStream(inner, max_lines=2)

        assert wrapper.readline() == "first\n"
        assert wrapper.readline() == "second\n"
        assert wrapper.readline() == "third\n"
        assert len(wrapper.captured_lines) == 2
        assert wrapper.captured_lines == ["first", "second"]
