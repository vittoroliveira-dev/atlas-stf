"""Tests for OAB/SP inscrito lookup parser (classify + row parse)."""

from __future__ import annotations

from atlas_stf.oab_sp._parser_inscritos import _parse_inscrito_row, classify_inscritos_response

# ---------------------------------------------------------------------------
# HTML fixtures
# ---------------------------------------------------------------------------

_FIRM_LINK = (
    '<a href="../consultaSociedades/consultaSociedades03.asp?param=3031">R. L. SCHWARTZ SOCIEDADE DE ADVOGADOS</a>'
)

_SINGLE_WITH_FIRM = f"""
<html><body>
<p>Resultado da pesquisa</p>
<table><tbody>
<tr>
<td><iframe></iframe></td>
<td>
<strong>RENATO LAINER SCHWARTZ</strong>
<strong>OAB SP nº:</strong> 100000 - Definitivo
<strong>Data Inscrição:</strong> 21/08/1989
<strong>Subseção:</strong> São Paulo
<strong>Situação:</strong> Ativo - Normal
<strong>Sócio de:</strong> {_FIRM_LINK}
</td>
</tr>
</tbody></table>
</body></html>
"""

_SINGLE_WITHOUT_FIRM = """
<html><body>
<p>Resultado da pesquisa</p>
<table><tbody>
<tr>
<td><iframe></iframe></td>
<td>
<strong>MARIA SANTOS OLIVEIRA</strong>
<strong>OAB SP nº:</strong> 200000 - Definitivo
<strong>Data Inscrição:</strong> 15/03/2010
<strong>Subseção:</strong> Campinas
<strong>Situação:</strong> Ativo - Normal
</td>
</tr>
</tbody></table>
</body></html>
"""

_MULTI_MATCH = """
<html><body>
<p>Resultado da pesquisa</p>
<table><tbody>
<tr>
<td><iframe></iframe></td>
<td>
<strong>JOSE SILVA SANTOS</strong>
<strong>OAB SP nº:</strong> 111111 - Definitivo
<strong>Data Inscrição:</strong> 10/01/2000
<strong>Subseção:</strong> São Paulo
<strong>Situação:</strong> Ativo - Normal
</td>
</tr>
<tr>
<td><iframe></iframe></td>
<td>
<strong>JOSE SILVA SANTOS JUNIOR</strong>
<strong>OAB SP nº:</strong> 222222 - Definitivo
<strong>Data Inscrição:</strong> 05/06/2005
<strong>Subseção:</strong> São Paulo
<strong>Situação:</strong> Ativo - Normal
</td>
</tr>
</tbody></table>
</body></html>
"""

_NOT_FOUND = """
<html><body>
<p>Resultado da pesquisa</p>
<div>Não há resultados que satisfaçam a busca.</div>
</body></html>
"""

_UNEXPECTED = """
<html><body>
<p>Página inicial do sistema</p>
<div>Bem-vindo ao portal OAB/SP.</div>
</body></html>
"""

_EMPTY_TABLE = """
<html><body>
<p>Resultado da pesquisa</p>
<table><tbody>
</tbody></table>
</body></html>
"""

_UNICODE_NAME = """
<html><body>
<p>Resultado da pesquisa</p>
<table><tbody>
<tr>
<td><iframe></iframe></td>
<td>
<strong>JOSÉ ANTÔNIO MÜLLER</strong>
<strong>OAB SP nº:</strong> 300000 - Definitivo
<strong>Data Inscrição:</strong> 07/09/1995
<strong>Subseção:</strong> São Paulo
<strong>Situação:</strong> Ativo - Normal
</td>
</tr>
</tbody></table>
</body></html>
"""


# ---------------------------------------------------------------------------
# classify_inscritos_response
# ---------------------------------------------------------------------------


def test_classify_single_match_with_firm() -> None:
    status, records = classify_inscritos_response(_SINGLE_WITH_FIRM)
    assert status == "single_match"
    assert len(records) == 1
    assert records[0]["firm_param"] == "3031"


def test_classify_single_match_without_firm() -> None:
    status, records = classify_inscritos_response(_SINGLE_WITHOUT_FIRM)
    assert status == "single_match"
    assert len(records) == 1
    assert records[0]["firm_param"] is None


def test_classify_not_found() -> None:
    status, records = classify_inscritos_response(_NOT_FOUND)
    assert status == "not_found"
    assert records == []


def test_classify_multi_match() -> None:
    status, records = classify_inscritos_response(_MULTI_MATCH)
    assert status == "multi_match"
    assert len(records) == 2


def test_classify_unexpected() -> None:
    status, records = classify_inscritos_response(_UNEXPECTED)
    assert status == "unexpected"
    assert records == []


def test_classify_empty_table() -> None:
    """Table present but no valid rows → unexpected."""
    status, records = classify_inscritos_response(_EMPTY_TABLE)
    assert status == "unexpected"
    assert records == []


# ---------------------------------------------------------------------------
# _parse_inscrito_row — field extraction
# ---------------------------------------------------------------------------


def test_parse_name() -> None:
    record = _parse_inscrito_row(_SINGLE_WITH_FIRM)
    assert record is not None
    assert record["lawyer_name"] == "RENATO LAINER SCHWARTZ"


def test_parse_oab_number() -> None:
    record = _parse_inscrito_row(_SINGLE_WITH_FIRM)
    assert record is not None
    assert record["oab_number"] == "100000"
    assert record["oab_type"] == "Definitivo"


def test_parse_inscription_date() -> None:
    record = _parse_inscrito_row(_SINGLE_WITH_FIRM)
    assert record is not None
    assert record["inscription_date"] == "21/08/1989"


def test_parse_subsection() -> None:
    record = _parse_inscrito_row(_SINGLE_WITH_FIRM)
    assert record is not None
    assert record["subsection"] == "São Paulo"


def test_parse_situation() -> None:
    record = _parse_inscrito_row(_SINGLE_WITH_FIRM)
    assert record is not None
    assert record["situation"] == "Ativo - Normal"


def test_parse_firm_param() -> None:
    record = _parse_inscrito_row(_SINGLE_WITH_FIRM)
    assert record is not None
    assert record["firm_param"] == "3031"


def test_parse_firm_name() -> None:
    record = _parse_inscrito_row(_SINGLE_WITH_FIRM)
    assert record is not None
    assert record["firm_name"] == "R. L. SCHWARTZ SOCIEDADE DE ADVOGADOS"


def test_parse_no_firm_produces_none() -> None:
    """Row without 'Sócio de' → firm_param and firm_name are None."""
    record = _parse_inscrito_row(_SINGLE_WITHOUT_FIRM)
    assert record is not None
    assert record["firm_param"] is None
    assert record["firm_name"] is None


def test_parse_unicode_name() -> None:
    """Names with accented characters are preserved."""
    record = _parse_inscrito_row(_UNICODE_NAME)
    assert record is not None
    assert "JOSÉ" in record["lawyer_name"]
    assert "ANTÔNIO" in record["lawyer_name"]
    assert "MÜLLER" in record["lawyer_name"]
