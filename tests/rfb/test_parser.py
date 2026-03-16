"""Tests for RFB CSV parser."""

from __future__ import annotations

from atlas_stf.rfb._parser import (
    detect_encoding,
    parse_empresas_csv_filtered,
    parse_socios_csv_filtered,
)


class TestDetectEncoding:
    def test_utf8(self) -> None:
        assert detect_encoding("hello world".encode("utf-8")) == "utf-8"

    def test_iso_8859_1(self) -> None:
        raw = "José da Silva".encode("iso-8859-1")
        assert detect_encoding(raw) == "iso-8859-1"

    def test_empty(self) -> None:
        assert detect_encoding(b"") == "utf-8"

    def test_invalid_utf8_after_ascii_prefix_falls_back_to_iso_8859_1(self) -> None:
        raw = (b"A" * 5000) + b"\xff"
        assert detect_encoding(raw) == "iso-8859-1"


class TestParseSociosCsvFiltered:
    def _make_csv(self, rows: list[list[str]], encoding: str = "utf-8") -> bytes:
        lines = [";".join(cols) for cols in rows]
        return "\n".join(lines).encode(encoding)

    def test_filter_by_name(self) -> None:
        csv_bytes = self._make_csv(
            [
                ["12345678", "2", "JOSE DA SILVA", "12345678901", "49", "20200101", "", "", "", "", ""],
                ["87654321", "2", "MARIA SOUZA", "98765432100", "22", "20190501", "", "", "", "", ""],
            ]
        )
        records, matched_cnpjs = parse_socios_csv_filtered(
            csv_bytes, target_names={"JOSE DA SILVA"}, target_cnpjs=set()
        )
        assert len(records) == 1
        assert records[0]["cnpj_basico"] == "12345678"
        assert records[0]["partner_name_normalized"] == "JOSE DA SILVA"
        assert "12345678" in matched_cnpjs
        assert "87654321" not in matched_cnpjs

    def test_filter_by_cnpj(self) -> None:
        csv_bytes = self._make_csv(
            [
                ["12345678", "2", "JOSE DA SILVA", "12345678901", "49", "20200101", "", "", "", "", ""],
                ["87654321", "2", "MARIA SOUZA", "98765432100", "22", "20190501", "", "", "", "", ""],
            ]
        )
        records, matched_cnpjs = parse_socios_csv_filtered(csv_bytes, target_names=set(), target_cnpjs={"87654321"})
        assert len(records) == 1
        assert records[0]["cnpj_basico"] == "87654321"
        assert len(matched_cnpjs) == 0  # Only name matches populate matched_cnpjs

    def test_no_match(self) -> None:
        csv_bytes = self._make_csv(
            [
                ["12345678", "2", "JOSE DA SILVA", "12345678901", "49", "20200101", "", "", "", "", ""],
            ]
        )
        records, matched_cnpjs = parse_socios_csv_filtered(csv_bytes, target_names={"OUTRO NOME"}, target_cnpjs=set())
        assert len(records) == 0
        assert len(matched_cnpjs) == 0

    def test_short_row_skipped(self) -> None:
        csv_bytes = self._make_csv(
            [
                ["12345678", "2", "JOSE"],  # only 3 cols
            ]
        )
        records, _ = parse_socios_csv_filtered(csv_bytes, target_names={"JOSE"}, target_cnpjs=set())
        assert len(records) == 0

    def test_iso_encoding(self) -> None:
        csv_bytes = self._make_csv(
            [["12345678", "2", "JOSÉ DA SILVA", "12345678901", "49", "20200101", "", "", "", "", ""]],
            encoding="iso-8859-1",
        )
        records, _ = parse_socios_csv_filtered(csv_bytes, target_names={"JOSÉ DA SILVA"}, target_cnpjs=set())
        assert len(records) == 1

    def test_invalid_utf8_suffix_is_not_silently_replaced(self) -> None:
        prefix = ("12345678;2;JOSE DA SILVA;12345678901;49;20200101;;;;;\n").encode("utf-8")
        csv_bytes = prefix + b"\xff"
        records, _ = parse_socios_csv_filtered(csv_bytes, target_names={"JOSE DA SILVA"}, target_cnpjs=set())
        assert len(records) == 1

    def test_both_name_and_cnpj(self) -> None:
        csv_bytes = self._make_csv(
            [
                ["12345678", "2", "JOSE DA SILVA", "12345678901", "49", "20200101", "", "", "", "", ""],
            ]
        )
        records, matched_cnpjs = parse_socios_csv_filtered(
            csv_bytes, target_names={"JOSE DA SILVA"}, target_cnpjs={"12345678"}
        )
        assert len(records) == 1
        assert "12345678" in matched_cnpjs

    def test_socios_extracts_representative_fields(self) -> None:
        """CSV with cols 7-9 filled -> record includes representative fields."""
        csv_bytes = self._make_csv(
            [
                [
                    "12345678",
                    "2",
                    "JOSE DA SILVA",
                    "12345678901",
                    "49",
                    "20200101",
                    "0",
                    "99988877766",
                    "REPRESENTANTE LEGAL",
                    "05",
                ],
            ]
        )
        records, _ = parse_socios_csv_filtered(csv_bytes, target_names={"JOSE DA SILVA"}, target_cnpjs=set())
        assert len(records) == 1
        assert records[0]["representative_name"] == "REPRESENTANTE LEGAL"
        assert records[0]["representative_cpf_cnpj"] == "99988877766"
        assert records[0]["representative_qualification"] == "05"

    def test_socios_matches_by_representative_name(self) -> None:
        """partner_name NOT in target, but representative_name IS -> record included."""
        csv_bytes = self._make_csv(
            [
                [
                    "12345678",
                    "1",
                    "PJ QUALQUER LTDA",
                    "00111222000133",
                    "49",
                    "20200101",
                    "0",
                    "12345678901",
                    "MINISTRO ALVO",
                    "05",
                ],
            ]
        )
        records, matched_cnpjs = parse_socios_csv_filtered(
            csv_bytes, target_names={"MINISTRO ALVO"}, target_cnpjs=set()
        )
        assert len(records) == 1
        assert records[0]["representative_name"] == "MINISTRO ALVO"
        assert "12345678" in matched_cnpjs

    def test_socios_representative_empty(self) -> None:
        """Cols 7-9 empty -> representative fields as empty string."""
        csv_bytes = self._make_csv(
            [
                ["12345678", "2", "JOSE DA SILVA", "12345678901", "49", "20200101", "", "", "", "", ""],
            ]
        )
        records, _ = parse_socios_csv_filtered(csv_bytes, target_names={"JOSE DA SILVA"}, target_cnpjs=set())
        assert len(records) == 1
        assert records[0]["representative_name"] == ""
        assert records[0]["representative_cpf_cnpj"] == ""


    def test_filter_by_cpf(self) -> None:
        """partner_cpf_cnpj matches target_cpfs -> record included + cnpj_basico in matched."""
        csv_bytes = self._make_csv(
            [
                ["44556677", "2", "QUALQUER NOME", "52998224725", "49", "20200101", "", "", "", "", ""],
                ["88776655", "2", "OUTRO NOME", "99988877766", "22", "20190501", "", "", "", "", ""],
            ]
        )
        records, matched_cnpjs = parse_socios_csv_filtered(
            csv_bytes, target_names=set(), target_cnpjs=set(), target_cpfs={"52998224725"}
        )
        assert len(records) == 1
        assert records[0]["cnpj_basico"] == "44556677"
        assert "44556677" in matched_cnpjs

    def test_filter_by_partner_cnpj(self) -> None:
        """CNPJ normalizado in target_partner_cnpjs -> record included + cnpj_basico in matched."""
        csv_bytes = self._make_csv(
            [
                ["99887766", "1", "PJ SOCIA", "11222333000181", "22", "20200101", "", "", "", "", ""],
            ]
        )
        records, matched_cnpjs = parse_socios_csv_filtered(
            csv_bytes, target_names=set(), target_cnpjs=set(), target_partner_cnpjs={"11222333000181"}
        )
        assert len(records) == 1
        assert records[0]["cnpj_basico"] == "99887766"
        assert "99887766" in matched_cnpjs

    def test_cpf_no_match(self) -> None:
        """CPF not in target_cpfs -> row ignored."""
        csv_bytes = self._make_csv(
            [
                ["44556677", "2", "QUALQUER NOME", "99988877766", "49", "20200101", "", "", "", "", ""],
            ]
        )
        records, _ = parse_socios_csv_filtered(
            csv_bytes, target_names=set(), target_cnpjs=set(), target_cpfs={"52998224725"}
        )
        assert len(records) == 0

    def test_cpf_formatted_differently_still_matches(self) -> None:
        """CPF with dots/dash in raw RFB -> normalize_tax_id removes formatting -> match works."""
        csv_bytes = self._make_csv(
            [
                ["44556677", "2", "ALGUEM", "529.982.247-25", "49", "20200101", "", "", "", "", ""],
            ]
        )
        records, matched_cnpjs = parse_socios_csv_filtered(
            csv_bytes, target_names=set(), target_cnpjs=set(), target_cpfs={"52998224725"}
        )
        assert len(records) == 1
        assert "44556677" in matched_cnpjs

    def test_cpf_and_name_combined(self) -> None:
        """Both CPF and name filters active simultaneously."""
        csv_bytes = self._make_csv(
            [
                ["11111111", "2", "ALVO POR NOME", "00000000000", "49", "20200101", "", "", "", "", ""],
                ["22222222", "2", "NAO ALVO", "52998224725", "49", "20200101", "", "", "", "", ""],
            ]
        )
        records, matched_cnpjs = parse_socios_csv_filtered(
            csv_bytes, target_names={"ALVO POR NOME"}, target_cnpjs=set(), target_cpfs={"52998224725"}
        )
        assert len(records) == 2
        assert "11111111" in matched_cnpjs  # from name match
        assert "22222222" in matched_cnpjs  # from CPF match


class TestParseEmpresasCsvFiltered:
    def _make_csv(self, rows: list[list[str]]) -> bytes:
        lines = [";".join(cols) for cols in rows]
        return "\n".join(lines).encode("utf-8")

    def test_filter_by_cnpj(self) -> None:
        csv_bytes = self._make_csv(
            [
                ["12345678", "EMPRESA XYZ LTDA", "2062", "", "100000,50", "03"],
                ["87654321", "OUTRA EMPRESA SA", "2011", "", "500000,00", "05"],
            ]
        )
        records = parse_empresas_csv_filtered(csv_bytes, target_cnpjs={"12345678"})
        assert len(records) == 1
        assert records[0]["cnpj_basico"] == "12345678"
        assert records[0]["razao_social"] == "EMPRESA XYZ LTDA"
        assert records[0]["capital_social"] == 100000.50

    def test_no_match(self) -> None:
        csv_bytes = self._make_csv(
            [
                ["12345678", "EMPRESA XYZ LTDA", "2062", "", "100000,50", "03"],
            ]
        )
        records = parse_empresas_csv_filtered(csv_bytes, target_cnpjs={"99999999"})
        assert len(records) == 0

    def test_invalid_capital(self) -> None:
        csv_bytes = self._make_csv(
            [
                ["12345678", "EMPRESA", "2062", "", "INVALID", "03"],
            ]
        )
        records = parse_empresas_csv_filtered(csv_bytes, target_cnpjs={"12345678"})
        assert len(records) == 1
        assert records[0]["capital_social"] == 0.0
