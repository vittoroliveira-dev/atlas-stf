"""Tests for tse/_parser_expenses.py — all 6 CSV schema generations."""

from __future__ import annotations

from pathlib import Path

from atlas_stf.tse._parser_expenses import parse_despesas_csv

# ---------------------------------------------------------------------------
# CSV fixture factories — headers match real TSE files exactly
# ---------------------------------------------------------------------------


def _make_gen1_csv() -> str:
    """2002 format: 12 columns."""
    header = ";".join(
        [
            '"SEQUENCIAL_CANDIDATO"',
            '"SG_UF"',
            '"SG_PART"',
            '"DS_CARGO"',
            '"NO_CAND"',
            '"NR_CAND"',
            '"DT_DOC_DESP"',
            '"CD_CPF_CGC"',
            '"SG_UF_FORNECEDOR"',
            '"NO_FOR"',
            '"VR_DESPESA"',
            '"DS_TITULO"',
        ]
    )
    row = ";".join(
        [
            '"426"',
            '"AC"',
            '"PL"',
            '"Deputado Estadual"',
            '"JOAO DA SILVA"',
            '"22234"',
            '"14/08/2002"',
            '"04116398000187"',
            '"AC"',
            '"ACME COMERCIAL LTDA"',
            '"160,00"',
            '"Publicidade"',
        ]
    )
    return f"{header}\n{row}\n"


def _make_gen2_csv() -> str:
    """2004 format: includes SQL artifact column RTRIM(LTRIM(DR.DS_TITULO))."""
    header = ";".join(
        [
            '"NO_CAND"',
            '"DS_CARGO"',
            '"NR_CAND"',
            '"SG_UE"',
            '"SG_PART"',
            '"VR_DESPESA"',
            '"DT_DOC_DESP"',
            '"RTRIM(LTRIM(DR.DS_TITULO))"',
            '"NR_DOC_DESP"',
            '"DS_TIPO_DOCUMENTO"',
            '"NO_FOR"',
            '"CD_CPF_CGC"',
        ]
    )
    row = ";".join(
        [
            '"MARIA SOUZA"',
            '"Vereador"',
            '"25155"',
            '"76830"',
            '"PFL"',
            '"250,50"',
            '"15/06/2004"',
            '"Material de Escritorio"',
            '"DOC001"',
            '"Nota Fiscal"',
            '"PAPELARIA CENTRAL"',
            '"12345678000199"',
        ]
    )
    return f"{header}\n{row}\n"


def _make_gen3_csv() -> str:
    """2006 format: verbose Portuguese naming."""
    header = ";".join(
        [
            '"NOME_CANDIDATO"',
            '"DESCRICAO_CARGO"',
            '"NUMERO_CANDIDATO"',
            '"UNIDADE_ELEITORAL_CANDIDATO"',
            '"SIGLA_PARTIDO"',
            '"VALOR_DESPESA"',
            '"DATA_DESPESA"',
            '"TIPO_DESPESA"',
            '"NUMERO_DOCUMENTO"',
            '"TIPO_DOCUMENTO"',
            '"NOME_FORNECEDOR"',
            '"NUMERO_CPF_CGC_FORNECEDOR"',
            '"UNIDADE_ELEITORAL_FORNECEDOR"',
        ]
    )
    row = ";".join(
        [
            '"PEDRO OLIVEIRA"',
            '"Presidente"',
            '"27"',
            '"BR"',
            '"PSDC"',
            '"2,40"',
            '"01/09/2006"',
            '"Publicidade por adesivos"',
            '"NF123"',
            '"Nota Fiscal"',
            '"GRAFICA MODELO SA"',
            '"33445566000188"',
            '"SP"',
        ]
    )
    return f"{header}\n{row}\n"


def _make_gen4_csv() -> str:
    """2008 format: NM_* prefix, 29 cols (essential subset)."""
    header = ";".join(
        [
            '"NM_CANDIDATO"',
            '"DS_CARGO"',
            '"NR_CANDIDATO"',
            '"SG_PARTIDO"',
            '"SG_UE"',
            '"VR_DESPESA"',
            '"DT_DESPESA"',
            '"DS_TITULO"',
            '"DS_NR_DOCUMENTO"',
            '"DS_TIPO_DOCUMENTO"',
            '"NM_FORNECEDOR"',
            '"CD_CPF_CNPJ_FORNECEDOR"',
        ]
    )
    row = ";".join(
        [
            '"ANA COSTA"',
            '"Prefeito"',
            '"15"',
            '"PMDB"',
            '"01120"',
            '"5.000,00"',
            '"20/09/2008"',
            '"Combustivel e lubrificantes"',
            '"NF-456"',
            '"Nota Fiscal"',
            '"POSTO SOL LTDA"',
            '"11222333000144"',
        ]
    )
    return f"{header}\n{row}\n"


def _make_gen5_csv() -> str:
    """2010 format: Portuguese headers with accents, per-UF TXT."""
    header = ";".join(
        [
            '"UF"',
            '"Sigla Partido"',
            '"Número candidato"',
            '"Cargo"',
            '"Nome candidato"',
            '"CPF do candidato"',
            '"Tipo do documento"',
            '"Número do documento"',
            '"CPF/CNPJ do fornecedor"',
            '"Nome do fornecedor"',
            '"Data da despesa"',
            '"Valor despesa"',
            '"Tipo despesa"',
            '"Descriçao da despesa"',
        ]
    )
    row = ";".join(
        [
            '"PE"',
            '"PT"',
            '"13"',
            '"Governador"',
            '"CARLOS LIMA"',
            '"98765432100"',
            '"Recibo"',
            '"REC-789"',
            '"55667788000122"',
            '"RADIO FM LOCAL"',
            '"05/10/2010"',
            '"8.500,00"',
            '"Publicidade por insercoes"',
            '"Vinheta eleitoral radio"',
        ]
    )
    return f"{header}\n{row}\n"


def _make_gen6_csv(rows: list[dict[str, str]] | None = None) -> str:
    """2022+ despesas_contratadas format (essential columns only)."""
    header = ";".join(
        [
            '"AA_ELEICAO"',
            '"SG_UF"',
            '"DS_CARGO"',
            '"NM_CANDIDATO"',
            '"NR_CPF_CANDIDATO"',
            '"NR_CANDIDATO"',
            '"SG_PARTIDO"',
            '"NM_PARTIDO"',
            '"NR_CPF_CNPJ_FORNECEDOR"',
            '"NM_FORNECEDOR"',
            '"NM_FORNECEDOR_RFB"',
            '"CD_CNAE_FORNECEDOR"',
            '"DS_CNAE_FORNECEDOR"',
            '"SG_UF_FORNECEDOR"',
            '"VR_DESPESA_CONTRATADA"',
            '"DT_DESPESA"',
            '"DS_DESPESA"',
            '"DS_TIPO_DOCUMENTO"',
            '"NR_DOCUMENTO"',
            '"DS_ORIGEM_DESPESA"',
        ]
    )
    if rows is None:
        rows = [{}]
    lines = [header]
    for row in rows:
        vals = [
            row.get("year", "2022"),
            row.get("state", "SP"),
            row.get("position", "PREFEITO"),
            row.get("candidate", "FULANO DE TAL"),
            row.get("candidate_cpf", "12345678901"),
            row.get("candidate_number", "45"),
            row.get("party_abbrev", "PT"),
            row.get("party_name", "PARTIDO DOS TRABALHADORES"),
            row.get("supplier_tax_id", "98765432000111"),
            row.get("supplier_name", "GRAFICA ABC LTDA"),
            row.get("supplier_name_rfb", "GRAFICA ABC LTDA ME"),
            row.get("cnae_code", "1813001"),
            row.get("cnae_desc", "Impressao de material para uso publicitario"),
            row.get("supplier_state", "SP"),
            row.get("amount", "15000,00"),
            row.get("date", "10/08/2022"),
            row.get("description", "Impressao de santinhos"),
            row.get("doc_type", "Nota Fiscal"),
            row.get("doc_number", "NF-001"),
            row.get("origin", "Recurso de outros candidatos"),
        ]
        lines.append(";".join(f'"{v}"' for v in vals))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Tests by generation
# ---------------------------------------------------------------------------


class TestGen1Parsing:
    """2002 format: 12 columns, minimal. No candidate_cpf, no party_name."""

    def test_parse_gen1(self, tmp_path: Path) -> None:
        path = tmp_path / "despesas.csv"
        path.write_text(_make_gen1_csv(), encoding="latin-1")

        records = parse_despesas_csv(path)
        assert len(records) == 1
        r = records[0]
        assert r["candidate_name"] == "JOAO DA SILVA"
        assert r["candidate_cpf"] is None  # absent in Gen1
        assert r["party_abbrev"] == "PL"
        assert r["party_name"] is None  # absent in Gen1
        assert r["supplier_name"] == "ACME COMERCIAL LTDA"
        assert r["supplier_tax_id"] == "04116398000187"
        assert r["expense_amount_raw"] == "160,00"
        assert r["expense_date_raw"] == "14/08/2002"
        assert r["expense_description"] == "Publicidade"
        assert r["state"] == "AC"
        assert r["supplier_state"] == "AC"
        # Gen6-only fields absent
        assert r["supplier_name_rfb"] is None
        assert r["supplier_cnae_code"] is None
        assert r["origin_description"] is None


class TestGen2Parsing:
    """2004 format: SQL artifact column names (RTRIM, DECODE)."""

    def test_parse_gen2_sql_artifact(self, tmp_path: Path) -> None:
        path = tmp_path / "despesas.csv"
        path.write_text(_make_gen2_csv(), encoding="latin-1")

        records = parse_despesas_csv(path)
        assert len(records) == 1
        r = records[0]
        assert r["candidate_name"] == "MARIA SOUZA"
        assert r["candidate_cpf"] is None  # absent in Gen2
        assert r["party_abbrev"] == "PFL"
        assert r["expense_description"] == "Material de Escritorio"  # from RTRIM(LTRIM(...))
        assert r["expense_document_type"] == "Nota Fiscal"
        assert r["expense_document_number"] == "DOC001"
        assert r["supplier_name"] == "PAPELARIA CENTRAL"
        assert r["supplier_tax_id"] == "12345678000199"


class TestGen3Parsing:
    """2006 format: verbose Portuguese naming."""

    def test_parse_gen3(self, tmp_path: Path) -> None:
        path = tmp_path / "despesas.csv"
        path.write_text(_make_gen3_csv(), encoding="latin-1")

        records = parse_despesas_csv(path)
        assert len(records) == 1
        r = records[0]
        assert r["candidate_name"] == "PEDRO OLIVEIRA"
        assert r["candidate_cpf"] is None  # absent in Gen3
        assert r["party_abbrev"] == "PSDC"
        assert r["position"] == "Presidente"
        assert r["expense_description"] == "Publicidade por adesivos"  # from TIPO_DESPESA
        assert r["supplier_name"] == "GRAFICA MODELO SA"
        assert r["supplier_tax_id"] == "33445566000188"
        assert r["supplier_state"] == "SP"  # from UNIDADE_ELEITORAL_FORNECEDOR
        assert r["state"] == "BR"  # from UNIDADE_ELEITORAL_CANDIDATO


class TestGen4Parsing:
    """2008 format: NM_* prefix."""

    def test_parse_gen4(self, tmp_path: Path) -> None:
        path = tmp_path / "despesas.csv"
        path.write_text(_make_gen4_csv(), encoding="latin-1")

        records = parse_despesas_csv(path)
        assert len(records) == 1
        r = records[0]
        assert r["candidate_name"] == "ANA COSTA"
        assert r["candidate_cpf"] is None  # absent in Gen4
        assert r["party_abbrev"] == "PMDB"
        assert r["expense_amount_raw"] == "5.000,00"
        assert r["expense_description"] == "Combustivel e lubrificantes"  # from DS_TITULO
        assert r["supplier_name"] == "POSTO SOL LTDA"


class TestGen5Parsing:
    """2010 format: Portuguese headers with accents, separate type + description."""

    def test_parse_gen5(self, tmp_path: Path) -> None:
        path = tmp_path / "despesas.csv"
        path.write_text(_make_gen5_csv(), encoding="latin-1")

        records = parse_despesas_csv(path)
        assert len(records) == 1
        r = records[0]
        assert r["candidate_name"] == "CARLOS LIMA"
        assert r["candidate_cpf"] == "98765432100"  # present in Gen5
        assert r["party_abbrev"] == "PT"
        assert r["position"] == "Governador"
        assert r["expense_description"] == "Vinheta eleitoral radio"  # from Descriçao da despesa
        assert r["expense_type"] == "Publicidade por insercoes"  # from Tipo despesa (separate)
        assert r["supplier_name"] == "RADIO FM LOCAL"
        assert r["supplier_tax_id"] == "55667788000122"
        assert r["state"] == "PE"


class TestGen6Parsing:
    """2022+ format: full candidate info, supplier CNAE/RFB."""

    def test_parse_gen6(self, tmp_path: Path) -> None:
        path = tmp_path / "despesas.csv"
        path.write_text(_make_gen6_csv(), encoding="utf-8")

        records = parse_despesas_csv(path)
        assert len(records) == 1
        r = records[0]
        assert r["candidate_name"] == "FULANO DE TAL"
        assert r["candidate_cpf"] == "12345678901"
        assert r["party_abbrev"] == "PT"
        assert r["party_name"] == "PARTIDO DOS TRABALHADORES"
        assert r["supplier_name"] == "GRAFICA ABC LTDA"
        assert r["supplier_name_rfb"] == "GRAFICA ABC LTDA ME"
        assert r["supplier_cnae_code"] == "1813001"
        assert r["supplier_cnae_desc"] == "Impressao de material para uso publicitario"
        assert r["origin_description"] == "Recurso de outros candidatos"
        assert r["expense_type"] is None  # Tipo despesa only in Gen5
