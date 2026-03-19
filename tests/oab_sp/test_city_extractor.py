"""Tests for OAB/SP city options extractor."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from atlas_stf.oab_sp._city_extractor import extract_city_options, fetch_and_save_cities, load_cities

# ---------------------------------------------------------------------------
# HTML fixture
# ---------------------------------------------------------------------------

_CITY_SELECT_HTML = """
<html><body>
<form>
<select id="idCidade">
<option value="0">----- Todas ------</option>
<option value="617">SÃO PAULO</option>
<option value="48">CAMPINAS</option>
<option value="580">SANTOS</option>
</select>
</form>
</body></html>
"""

_CITY_SELECT_ACCENTS = """
<html><body>
<select id="idCidade">
<option value="0">----- Todas ------</option>
<option value="617">SÃO PAULO</option>
<option value="200">RIBEIRÃO PRETO</option>
<option value="300">ARARAQUARA</option>
</select>
</body></html>
"""


# ---------------------------------------------------------------------------
# extract_city_options — returns {name: id}
# ---------------------------------------------------------------------------


def test_extract_city_options() -> None:
    """Parses select element and returns name → id mapping."""
    result = extract_city_options(_CITY_SELECT_HTML)
    assert result == {"SÃO PAULO": "617", "CAMPINAS": "48", "SANTOS": "580"}


def test_extract_excludes_todas() -> None:
    """Option with value=0 (Todas) is excluded from the result."""
    result = extract_city_options(_CITY_SELECT_HTML)
    assert "----- Todas ------" not in result
    assert len(result) == 3


def test_save_and_load_roundtrip(tmp_path: Path) -> None:
    """Save then load returns the same mapping."""
    cities = {"SÃO PAULO": "617", "CAMPINAS": "48"}
    path = tmp_path / "cidades_oabsp.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump(cities, f, ensure_ascii=False)
    loaded = load_cities(tmp_path)
    assert loaded == cities


def test_load_missing_file(tmp_path: Path) -> None:
    """load_cities on a directory without the file returns an empty dict."""
    result = load_cities(tmp_path)
    assert result == {}


def test_extract_preserves_accents() -> None:
    """City names with accented characters are preserved exactly."""
    result = extract_city_options(_CITY_SELECT_ACCENTS)
    assert result.get("SÃO PAULO") == "617"
    assert result.get("RIBEIRÃO PRETO") == "200"


def test_extract_empty_html() -> None:
    """Empty string returns an empty dict."""
    assert extract_city_options("") == {}


def test_extract_no_options() -> None:
    """HTML with a select but no option elements returns an empty dict."""
    html = "<html><body><select id='idCidade'></select></body></html>"
    assert extract_city_options(html) == {}


def test_extract_only_todas() -> None:
    """HTML with only the value=0 option returns an empty dict (Todas excluded)."""
    html = "<select><option value='0'>Todas</option></select>"
    assert extract_city_options(html) == {}


def test_extract_strips_whitespace_from_names() -> None:
    """Names with surrounding whitespace are stripped."""
    html = "<select><option value='10'>  GUARULHOS  </option></select>"
    result = extract_city_options(html)
    assert "GUARULHOS" in result
    assert result["GUARULHOS"] == "10"


def test_extract_unquoted_value() -> None:
    """Regex also matches option values without surrounding quotes."""
    html = "<select><option value=42>BAURU</option></select>"
    result = extract_city_options(html)
    assert result.get("BAURU") == "42"


def test_extract_single_quotes_value() -> None:
    """Regex matches option values wrapped in single quotes."""
    html = "<select><option value='99'>SOROCABA</option></select>"
    result = extract_city_options(html)
    assert result.get("SOROCABA") == "99"


# ---------------------------------------------------------------------------
# fetch_and_save_cities — HTTP fetch + JSON persistence
# ---------------------------------------------------------------------------


def _make_mock_client(html: str, status_code: int = 200) -> MagicMock:
    """Build a mock httpx.Client context manager that returns the given HTML."""
    response = MagicMock()
    response.text = html
    response.raise_for_status = MagicMock()
    if status_code >= 400:
        import httpx

        response.raise_for_status.side_effect = httpx.HTTPStatusError(
            message=f"HTTP {status_code}",
            request=MagicMock(),
            response=MagicMock(status_code=status_code),
        )

    client = MagicMock()
    client.get.return_value = response

    cm = MagicMock()
    cm.__enter__.return_value = client
    cm.__exit__.return_value = False
    return cm


def test_fetch_and_save_cities_creates_json(tmp_path: Path) -> None:
    """Successful fetch saves cidades_oabsp.json and returns the mapping."""
    mock_cm = _make_mock_client(_CITY_SELECT_HTML)

    with patch("atlas_stf.oab_sp._city_extractor.httpx.Client", return_value=mock_cm):
        result = fetch_and_save_cities(tmp_path)

    assert result == {"SÃO PAULO": "617", "CAMPINAS": "48", "SANTOS": "580"}
    saved_path = tmp_path / "cidades_oabsp.json"
    assert saved_path.exists()
    loaded = json.loads(saved_path.read_text(encoding="utf-8"))
    assert loaded == result


def test_fetch_and_save_cities_roundtrip(tmp_path: Path) -> None:
    """Data written by fetch_and_save_cities can be re-read by load_cities."""
    mock_cm = _make_mock_client(_CITY_SELECT_HTML)

    with patch("atlas_stf.oab_sp._city_extractor.httpx.Client", return_value=mock_cm):
        fetch_and_save_cities(tmp_path)

    loaded = load_cities(tmp_path)
    assert loaded == {"SÃO PAULO": "617", "CAMPINAS": "48", "SANTOS": "580"}


def test_fetch_and_save_cities_creates_parent_dir(tmp_path: Path) -> None:
    """Output directory is created automatically if it does not exist."""
    output_dir = tmp_path / "nested" / "dir"
    assert not output_dir.exists()

    mock_cm = _make_mock_client(_CITY_SELECT_HTML)

    with patch("atlas_stf.oab_sp._city_extractor.httpx.Client", return_value=mock_cm):
        fetch_and_save_cities(output_dir)

    assert (output_dir / "cidades_oabsp.json").exists()


def test_fetch_and_save_cities_json_sorted_keys(tmp_path: Path) -> None:
    """Saved JSON file uses sort_keys so it is deterministic."""
    mock_cm = _make_mock_client(_CITY_SELECT_HTML)

    with patch("atlas_stf.oab_sp._city_extractor.httpx.Client", return_value=mock_cm):
        fetch_and_save_cities(tmp_path)

    raw = (tmp_path / "cidades_oabsp.json").read_text(encoding="utf-8")
    # Key "CAMPINAS" < "SANTOS" < "SÃO PAULO" in JSON file order
    pos_campinas = raw.index("CAMPINAS")
    pos_santos = raw.index("SANTOS")
    assert pos_campinas < pos_santos


def test_fetch_and_save_cities_http_error_propagates(tmp_path: Path) -> None:
    """HTTP error from raise_for_status propagates to the caller."""
    import httpx

    mock_cm = _make_mock_client(_CITY_SELECT_HTML, status_code=503)

    with patch("atlas_stf.oab_sp._city_extractor.httpx.Client", return_value=mock_cm):
        with pytest.raises(httpx.HTTPStatusError):
            fetch_and_save_cities(tmp_path)
