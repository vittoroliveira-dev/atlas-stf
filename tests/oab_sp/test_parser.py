"""Tests for OAB/SP society detail parser (regex-based, no external deps)."""

from __future__ import annotations

from atlas_stf.oab_sp._parser import extract_param_from_search, parse_society_detail

# ---------------------------------------------------------------------------
# HTML fixtures
# ---------------------------------------------------------------------------

_SEARCH_WITH_RESULT = """
<html><body>
<table><tr><td>
<a href="consultaSociedades03.asp?param=19640">FERREIRA E ASSOCIADOS SOCIEDADE DE ADVOGADOS</a>
</td></tr></table>
</body></html>
"""

_SEARCH_NOT_FOUND = """
<html><body>
<div>Não há resultados para a consulta informada.</div>
</body></html>
"""

_SEARCH_EMPTY = ""

_SEARCH_NO_SIGNAL = """
<html><body>
<div>Consulta realizada com sucesso.</div>
</body></html>
"""

_DETAIL_FULL = """
<html><body>
<div class="boxCampo"><b>FERREIRA E ASSOCIADOS SOCIEDADE DE ADVOGADOS</b></div>
<table>
<tr><td><label>Nº de Registro:</label></td><td><label>18554</label></td></tr>
<tr><td><label>Endereço:</label></td><td><label>Rua Augusta 1234, Sala 56</label></td></tr>
<tr><td><label>Bairro:</label></td><td><label>Consolação</label></td></tr>
<tr><td><label>CEP:</label></td><td><label>01305-100</label></td></tr>
<tr><td><label>Cidade / Estado:</label></td><td><label>São Paulo / SP</label></td></tr>
<tr><td><label>Email:</label></td><td><label>contato@ferreira.adv.br</label></td></tr>
<tr><td><label>Telefone:</label></td><td><label>(11) 3333-4444</label></td></tr>
<tr><td colspan="2"><label><b>Sociedade de Advogados</b></label></td></tr>
</table>
</body></html>
"""

_DETAIL_INDIVIDUAL = """
<html><body>
<div class="boxCampo"><b>SILVA ADVOCACIA INDIVIDUAL</b></div>
<table>
<tr><td><label>Nº de Registro:</label></td><td><label>99001</label></td></tr>
<tr><td><label>Endereço:</label></td><td><label>Av. Paulista 100</label></td></tr>
<tr><td><label>Bairro:</label></td><td><label>Bela Vista</label></td></tr>
<tr><td><label>CEP:</label></td><td><label>01310-100</label></td></tr>
<tr><td><label>Cidade / Estado:</label></td><td><label>São Paulo / SP</label></td></tr>
<tr><td><label>Email:</label></td><td><label>silva@individual.adv.br</label></td></tr>
<tr><td><label>Telefone:</label></td><td><label>(11) 9999-0000</label></td></tr>
<tr><td colspan="2"><label><b>Sociedade Individual de Advocacia</b></label></td></tr>
</table>
</body></html>
"""

_DETAIL_NO_EMAIL = """
<html><body>
<div class="boxCampo"><b>COSTA ADVOGADOS</b></div>
<table>
<tr><td><label>Nº de Registro:</label></td><td><label>22001</label></td></tr>
<tr><td><label>Endereço:</label></td><td><label>Rua das Flores 5</label></td></tr>
<tr><td><label>Bairro:</label></td><td><label>Centro</label></td></tr>
<tr><td><label>CEP:</label></td><td><label>01001-000</label></td></tr>
<tr><td><label>Cidade / Estado:</label></td><td><label>Campinas / SP</label></td></tr>
<tr><td><label>Telefone:</label></td><td><label>(19) 1111-2222</label></td></tr>
<tr><td colspan="2"><label><b>Sociedade de Advogados</b></label></td></tr>
</table>
</body></html>
"""

_DETAIL_NO_PHONE = """
<html><body>
<div class="boxCampo"><b>LIMA E SANTOS ADVOGADOS</b></div>
<table>
<tr><td><label>Nº de Registro:</label></td><td><label>33500</label></td></tr>
<tr><td><label>Endereço:</label></td><td><label>Alameda dos Anjos 7</label></td></tr>
<tr><td><label>Bairro:</label></td><td><label>Jardim América</label></td></tr>
<tr><td><label>CEP:</label></td><td><label>01432-000</label></td></tr>
<tr><td><label>Cidade / Estado:</label></td><td><label>São Paulo / SP</label></td></tr>
<tr><td><label>Email:</label></td><td><label>lima@santos.adv.br</label></td></tr>
<tr><td colspan="2"><label><b>Sociedade de Advogados</b></label></td></tr>
</table>
</body></html>
"""

_DETAIL_UNICODE = """
<html><body>
<div class="boxCampo"><b>ÁVILA, BARÇANTE &amp; GONÇALVES ADVOGADOS</b></div>
<table>
<tr><td><label>Nº de Registro:</label></td><td><label>44200</label></td></tr>
<tr><td><label>Endereço:</label></td><td><label>Rua Açaí 33</label></td></tr>
<tr><td><label>Bairro:</label></td><td><label>Pinheiros</label></td></tr>
<tr><td><label>CEP:</label></td><td><label>05422-030</label></td></tr>
<tr><td><label>Cidade / Estado:</label></td><td><label>São Paulo / SP</label></td></tr>
<tr><td><label>Email:</label></td><td><label>contato@avila.adv.br</label></td></tr>
<tr><td><label>Telefone:</label></td><td><label>(11) 4567-8900</label></td></tr>
<tr><td colspan="2"><label><b>Sociedade de Advogados</b></label></td></tr>
</table>
</body></html>
"""

_DETAIL_EMPTY = "<html><body><div>Nenhum resultado.</div></body></html>"

_DETAIL_MULTILINE_ADDRESS = """
<html><body>
<div class="boxCampo"><b>MULTILINE ADVOGADOS</b></div>
<table>
<tr><td><label>Nº de Registro:</label></td><td><label>55100</label></td></tr>
<tr><td><label>Endereço:</label></td><td><label>Rua Longa 999<br/>Andar 10</label></td></tr>
<tr><td><label>Bairro:</label></td><td><label>Itaim Bibi</label></td></tr>
<tr><td><label>CEP:</label></td><td><label>04534-001</label></td></tr>
<tr><td><label>Cidade / Estado:</label></td><td><label>São Paulo / SP</label></td></tr>
<tr><td><label>Email:</label></td><td><label>ml@ml.adv.br</label></td></tr>
<tr><td><label>Telefone:</label></td><td><label>(11) 1234-5678</label></td></tr>
<tr><td colspan="2"><label><b>Sociedade de Advogados</b></label></td></tr>
</table>
</body></html>
"""


# ---------------------------------------------------------------------------
# extract_param_from_search
# ---------------------------------------------------------------------------


def test_extract_param_from_search_found():
    param, status = extract_param_from_search(_SEARCH_WITH_RESULT)
    assert param == "19640"
    assert status == "found"


def test_extract_param_from_search_not_found():
    param, status = extract_param_from_search(_SEARCH_NOT_FOUND)
    assert param is None
    assert status == "not_found"


def test_extract_param_from_search_empty():
    param, status = extract_param_from_search(_SEARCH_EMPTY)
    assert param is None
    assert status == "unexpected"


def test_extract_param_from_search_unexpected():
    """Valid HTML without the expected link or 'Não há resultados' → unexpected."""
    param, status = extract_param_from_search(_SEARCH_NO_SIGNAL)
    assert param is None
    assert status == "unexpected"


# ---------------------------------------------------------------------------
# parse_society_detail
# ---------------------------------------------------------------------------


def test_parse_detail_full():
    result = parse_society_detail(_DETAIL_FULL, "18554")
    assert result is not None
    assert result["firm_name"] == "FERREIRA E ASSOCIADOS SOCIEDADE DE ADVOGADOS"
    assert result["registration_number"] == "18554"
    assert "Rua Augusta" in result["address"]
    assert result["neighborhood"] == "Consolação"
    assert result["city"] == "São Paulo"
    assert result["state"] == "SP"
    assert result["email"] == "contato@ferreira.adv.br"
    assert result["phone"] == "(11) 3333-4444"
    assert result["society_type"] == "sociedade_advogados"


def test_parse_detail_individual():
    result = parse_society_detail(_DETAIL_INDIVIDUAL, "99001")
    assert result is not None
    assert result["society_type"] == "individual"


def test_parse_detail_plural():
    result = parse_society_detail(_DETAIL_FULL, "18554")
    assert result is not None
    assert result["society_type"] == "sociedade_advogados"


def test_parse_detail_missing_email():
    result = parse_society_detail(_DETAIL_NO_EMAIL, "22001")
    assert result is not None
    assert result["email"] is None


def test_parse_detail_missing_phone():
    result = parse_society_detail(_DETAIL_NO_PHONE, "33500")
    assert result is not None
    assert result["phone"] is None


def test_parse_detail_zip_normalization():
    result = parse_society_detail(_DETAIL_FULL, "18554")
    assert result is not None
    # CEP "01305-100" → "01305100"
    assert result["zip_code"] == "01305100"


def test_parse_detail_unicode():
    result = parse_society_detail(_DETAIL_UNICODE, "44200")
    assert result is not None
    name = result["firm_name"]
    assert "VILA" in name  # Ávila or ÁVILA
    assert "ALVES" in name  # Gonçalves or GONÇALVES


def test_parse_detail_empty_page():
    result = parse_society_detail(_DETAIL_EMPTY, "00000")
    assert result is None


def test_parse_detail_multiline_address():
    result = parse_society_detail(_DETAIL_MULTILINE_ADDRESS, "55100")
    assert result is not None
    address = result["address"]
    # No raw HTML tags in the output
    assert "<br" not in address
    assert "Rua Longa 999" in address
