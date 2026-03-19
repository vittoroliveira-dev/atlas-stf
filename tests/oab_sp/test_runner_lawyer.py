"""Tests for OAB/SP lawyer lookup runner."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from atlas_stf.oab_sp._checkpoint import OabSpCheckpoint, load_checkpoint, save_checkpoint
from atlas_stf.oab_sp._config import OabSpLawyerLookupConfig
from atlas_stf.oab_sp._runner_lawyer import run_lawyer_lookup

# ---------------------------------------------------------------------------
# HTML fixtures
# ---------------------------------------------------------------------------

_SINGLE_MATCH_WITH_FIRM_TPL = """
<html><body>
<p>Resultado da pesquisa</p>
<table><tbody>
<tr>
<td><iframe></iframe></td>
<td>
<strong>{name}</strong>
<strong>OAB SP nº:</strong> {oab_number} - Definitivo
<strong>Data Inscrição:</strong> 01/01/2000
<strong>Subseção:</strong> São Paulo
<strong>Situação:</strong> Ativo - Normal
<strong>Sócio de:</strong> \
<a href="../consultaSociedades/consultaSociedades03.asp?param={firm_param}">{firm_name}</a>
</td>
</tr>
</tbody></table>
</body></html>
"""

_SINGLE_MATCH_NO_FIRM_TPL = """
<html><body>
<p>Resultado da pesquisa</p>
<table><tbody>
<tr>
<td><iframe></iframe></td>
<td>
<strong>{name}</strong>
<strong>OAB SP nº:</strong> {oab_number} - Definitivo
<strong>Data Inscrição:</strong> 01/01/2000
<strong>Subseção:</strong> São Paulo
<strong>Situação:</strong> Ativo - Normal
</td>
</tr>
</tbody></table>
</body></html>
"""

_NOT_FOUND_HTML = """
<html><body>
<p>Resultado da pesquisa</p>
<div>Não há resultados que satisfaçam a busca.</div>
</body></html>
"""

_MULTI_MATCH_HTML = """
<html><body>
<p>Resultado da pesquisa</p>
<table><tbody>
<tr>
<td><iframe></iframe></td>
<td>
<strong>JOSE SILVA</strong>
<strong>OAB SP nº:</strong> 111111 - Definitivo
<strong>Data Inscrição:</strong> 10/01/2000
<strong>Subseção:</strong> São Paulo
<strong>Situação:</strong> Ativo - Normal
</td>
</tr>
<tr>
<td><iframe></iframe></td>
<td>
<strong>JOSE SILVA JUNIOR</strong>
<strong>OAB SP nº:</strong> 222222 - Definitivo
<strong>Data Inscrição:</strong> 05/06/2005
<strong>Subseção:</strong> São Paulo
<strong>Situação:</strong> Ativo - Normal
</td>
</tr>
</tbody></table>
</body></html>
"""


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _make_config(tmp_path: Path, **kwargs: object) -> OabSpLawyerLookupConfig:
    output_dir = tmp_path / "oab_sp"
    curated_dir = tmp_path / "curated"
    deoab_dir = tmp_path / "deoab"
    output_dir.mkdir(parents=True, exist_ok=True)
    curated_dir.mkdir(parents=True, exist_ok=True)
    deoab_dir.mkdir(parents=True, exist_ok=True)
    return OabSpLawyerLookupConfig(
        output_dir=output_dir,
        checkpoint_file=output_dir / ".checkpoint_lawyer.json",
        deoab_dir=deoab_dir,
        curated_dir=curated_dir,
        rate_limit_seconds=0.0,
        retry_delay_seconds=0.0,
        **kwargs,
    )


def _write_sociedade_detalhe(output_dir: Path, records: list[dict]) -> None:
    """Write sociedade_detalhe.jsonl to oab_sp output dir (where runner reads it)."""
    path = output_dir / "sociedade_detalhe.jsonl"
    with path.open("w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")


def _write_cities(output_dir: Path) -> None:
    """Write minimal cities file to avoid HTTP fetch."""
    path = output_dir / "cidades_oabsp.json"
    with path.open("w", encoding="utf-8") as fh:
        json.dump({"SÃO PAULO": "617"}, fh)


def _individual_firm_record(reg: str, firm_name: str) -> dict:
    return {
        "registration_number": reg,
        "firm_name": firm_name,
        "society_type": "individual",
        "city": "São Paulo",
        "oab_sp_param": "9999",
    }


def _mock_client(html_response: str) -> MagicMock:
    """Build a mock OabSpClient context manager."""
    client = MagicMock()
    client.search_inscrito.return_value = html_response
    cm = MagicMock()
    cm.__enter__.return_value = client
    cm.__exit__.return_value = False
    return cm


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_lookup_by_name_success(tmp_path: Path) -> None:
    """Candidate from individual firm → single match → completed."""
    config = _make_config(tmp_path)
    _write_cities(config.output_dir)
    _write_sociedade_detalhe(
        config.output_dir,
        [_individual_firm_record("12345", "ANA PAULA FERREIRA SOCIEDADE INDIVIDUAL DE ADVOCACIA")],
    )

    html = _SINGLE_MATCH_NO_FIRM_TPL.format(name="ANA PAULA FERREIRA", oab_number="100000")
    mock_cm = _mock_client(html)

    with patch("atlas_stf.oab_sp._runner_lawyer.OabSpClient", return_value=mock_cm):
        count = run_lawyer_lookup(config)

    assert count == 1
    checkpoint = load_checkpoint(config.checkpoint_file)
    assert checkpoint.stats["completed"] == 1


def test_lookup_not_found(tmp_path: Path) -> None:
    """Search returns 'Não há resultados' → not_found."""
    config = _make_config(tmp_path)
    _write_cities(config.output_dir)
    _write_sociedade_detalhe(
        config.output_dir,
        [_individual_firm_record("99999", "JOAO INEXISTENTE SOCIEDADE INDIVIDUAL DE ADVOCACIA")],
    )

    mock_cm = _mock_client(_NOT_FOUND_HTML)
    with patch("atlas_stf.oab_sp._runner_lawyer.OabSpClient", return_value=mock_cm):
        count = run_lawyer_lookup(config)

    assert count == 0
    checkpoint = load_checkpoint(config.checkpoint_file)
    assert checkpoint.stats["not_found"] >= 1
    assert checkpoint.stats["completed"] == 0


def test_lookup_multi_match_rejected(tmp_path: Path) -> None:
    """Multiple results → not_found (ambiguous, not failed)."""
    config = _make_config(tmp_path)
    _write_cities(config.output_dir)
    _write_sociedade_detalhe(
        config.output_dir,
        [_individual_firm_record("55555", "JOSE SILVA SOCIEDADE INDIVIDUAL DE ADVOCACIA")],
    )

    mock_cm = _mock_client(_MULTI_MATCH_HTML)
    with patch("atlas_stf.oab_sp._runner_lawyer.OabSpClient", return_value=mock_cm):
        count = run_lawyer_lookup(config)

    assert count == 0
    checkpoint = load_checkpoint(config.checkpoint_file)
    assert checkpoint.stats["not_found"] >= 1
    assert checkpoint.stats["failed"] == 0


def test_dry_run(tmp_path: Path) -> None:
    """Dry run makes no HTTP requests and returns 0."""
    config = _make_config(tmp_path, dry_run=True)
    _write_cities(config.output_dir)
    _write_sociedade_detalhe(
        config.output_dir,
        [_individual_firm_record("77777", "DRY FIRMA SOCIEDADE INDIVIDUAL DE ADVOCACIA")],
    )

    mock_cm = _mock_client("")
    with patch("atlas_stf.oab_sp._runner_lawyer.OabSpClient", return_value=mock_cm):
        count = run_lawyer_lookup(config)

    assert count == 0
    # Client context manager should not have been entered
    mock_cm.__enter__.assert_not_called()


def test_resume_from_checkpoint(tmp_path: Path) -> None:
    """Pre-populated checkpoint skips resolved entries."""
    config = _make_config(tmp_path)
    _write_cities(config.output_dir)
    _write_sociedade_detalhe(
        config.output_dir,
        [
            _individual_firm_record("10001", "FIRMA A SOCIEDADE INDIVIDUAL DE ADVOCACIA"),
            _individual_firm_record("10002", "FIRMA B SOCIEDADE INDIVIDUAL DE ADVOCACIA"),
            _individual_firm_record("10003", "FIRMA C SOCIEDADE INDIVIDUAL DE ADVOCACIA"),
        ],
    )

    # Pre-populate checkpoint for first two
    cp = OabSpCheckpoint()
    cp.mark_completed("name_10001_FIRMA A")
    cp.mark_not_found("name_10002_FIRMA B")
    save_checkpoint(cp, config.checkpoint_file)

    html = _SINGLE_MATCH_NO_FIRM_TPL.format(name="FIRMA C", oab_number="300000")
    mock_cm = _mock_client(html)

    with patch("atlas_stf.oab_sp._runner_lawyer.OabSpClient", return_value=mock_cm):
        count = run_lawyer_lookup(config)

    assert count == 1
    checkpoint = load_checkpoint(config.checkpoint_file)
    # 1 from pre-populated + 1 from this run
    assert checkpoint.stats["completed"] >= 2
    assert checkpoint.stats["not_found"] >= 1
