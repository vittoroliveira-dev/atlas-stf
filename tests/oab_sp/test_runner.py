"""Tests for OAB/SP society fetch runner."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from atlas_stf.oab_sp._checkpoint import OabSpCheckpoint, load_checkpoint, save_checkpoint
from atlas_stf.oab_sp._config import OabSpFetchConfig
from atlas_stf.oab_sp._runner import _load_pending_registrations, run_society_fetch

# ---------------------------------------------------------------------------
# DEOAB JSONL fixture helpers
# ---------------------------------------------------------------------------

_SEARCH_WITH_RESULT_TPL = (
    "<html><body><table><tr><td>"
    '<a href="consultaSociedades03.asp?param={param}">{name}</a>'
    "</td></tr></table></body></html>"
)

_DETAIL_TPL = """
<html><body>
<div class="boxCampo"><b>{name}</b></div>
<table>
<tr><td><label>Nº de Registro:</label></td><td><label>{reg}</label></td></tr>
<tr><td><label>Endereço:</label></td><td><label>Rua Teste 1</label></td></tr>
<tr><td><label>Bairro:</label></td><td><label>Centro</label></td></tr>
<tr><td><label>CEP:</label></td><td><label>01001-000</label></td></tr>
<tr><td><label>Cidade / Estado:</label></td><td><label>São Paulo / SP</label></td></tr>
<tr><td><label>Email:</label></td><td><label>x@x.adv.br</label></td></tr>
<tr><td><label>Telefone:</label></td><td><label>(11) 0000-0000</label></td></tr>
<tr><td colspan="2"><label><b>Sociedade de Advogados</b></label></td></tr>
</table>
</body></html>
"""

_SEARCH_NOT_FOUND = "<html><body><div>Não há resultados para a consulta informada.</div></body></html>"


def _write_deoab_jsonl(deoab_dir: Path, records: list[dict]) -> None:
    """Write minimal DEOAB JSONL with SP registrations."""
    deoab_dir.mkdir(parents=True, exist_ok=True)
    path = deoab_dir / "oab_sociedade_vinculo.jsonl"
    with path.open("w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")


def _sp_record(reg: str, name: str = "FIRMA TESTE SOCIEDADE DE ADVOGADOS") -> dict:
    """Minimal DEOAB record for an SP registration."""
    return {
        "seccional": "SP",
        "sociedade_registro": reg,
        "sociedade_nome": name,
        "oab_number": None,
        "advogado_nome": None,
        "sociedade_tipo": "plural",
        "cidade": "São Paulo",
        "tipo_ato": "registro",
        "data_publicacao": "2026-01-01",
        "fonte": "DEOAB",
        "fonte_url": "https://example.com/test.pdf",
        "confidence": 0.85,
    }


def _make_config(tmp_path: Path, **kwargs) -> OabSpFetchConfig:
    output_dir = tmp_path / "output"
    deoab_dir = tmp_path / "deoab"
    output_dir.mkdir(parents=True, exist_ok=True)
    deoab_dir.mkdir(parents=True, exist_ok=True)
    cfg = OabSpFetchConfig(
        output_dir=output_dir,
        checkpoint_file=output_dir / ".checkpoint.json",
        deoab_dir=deoab_dir,
        rate_limit_seconds=0.0,
        retry_delay_seconds=0.0,
        **kwargs,
    )
    return cfg


def _mock_client_cm(search_html: str, detail_html: str) -> MagicMock:
    """Build a mock OabSpClient context manager."""
    client = MagicMock()
    client.search_by_registration.return_value = search_html
    client.fetch_detail.return_value = detail_html
    cm = MagicMock()
    cm.__enter__.return_value = client
    cm.__exit__.return_value = False
    return cm


# ---------------------------------------------------------------------------
# _load_pending_registrations
# ---------------------------------------------------------------------------


def test_dedup_registrations(tmp_path: Path):
    """Same registration number in multiple DEOAB records → loaded only once."""
    deoab_dir = tmp_path / "deoab"
    _write_deoab_jsonl(
        deoab_dir,
        [
            _sp_record("11111"),
            _sp_record("11111"),  # duplicate
            _sp_record("22222"),
        ],
    )
    result = _load_pending_registrations(deoab_dir)
    assert result.count("11111") == 1
    assert "22222" in result
    assert len(result) == 2


def test_load_pending_filters_non_sp(tmp_path: Path):
    """Only SP records are loaded; other seccionais are skipped."""
    deoab_dir = tmp_path / "deoab"
    _write_deoab_jsonl(
        deoab_dir,
        [
            _sp_record("10001"),
            {**_sp_record("20001"), "seccional": "AM"},
            {**_sp_record("30001"), "seccional": ""},
        ],
    )
    result = _load_pending_registrations(deoab_dir)
    assert result == ["10001"]


def test_load_pending_missing_file(tmp_path: Path):
    """Missing DEOAB file → empty list, no exception."""
    deoab_dir = tmp_path / "deoab"
    deoab_dir.mkdir()
    result = _load_pending_registrations(deoab_dir)
    assert result == []


# ---------------------------------------------------------------------------
# run_society_fetch — happy paths and error paths
# ---------------------------------------------------------------------------


def test_society_fetch_success(tmp_path: Path):
    """Valid search + matching detail → registration marked completed."""
    reg = "18554"
    search_html = _SEARCH_WITH_RESULT_TPL.format(param="19640", name="FERREIRA")
    detail_html = _DETAIL_TPL.format(name="FERREIRA SOCIEDADE DE ADVOGADOS", reg=reg)

    config = _make_config(tmp_path)
    _write_deoab_jsonl(config.deoab_dir, [_sp_record(reg)])

    mock_cm = _mock_client_cm(search_html, detail_html)
    with patch("atlas_stf.oab_sp._runner.OabSpClient", return_value=mock_cm):
        count = run_society_fetch(config)

    assert count == 1
    checkpoint = load_checkpoint(config.checkpoint_file)
    assert checkpoint.stats["completed"] == 1
    assert checkpoint.stats["failed"] == 0

    # Output JSONL must exist and contain the record
    output = config.output_dir / "sociedade_detalhe.jsonl"
    assert output.exists()
    records = [json.loads(line) for line in output.read_text().strip().splitlines()]
    assert len(records) == 1
    assert records[0]["registration_number"] == reg


def test_society_not_found(tmp_path: Path):
    """Search returns 'Não há resultados' → not_found state, not failed."""
    reg = "99999"
    config = _make_config(tmp_path)
    _write_deoab_jsonl(config.deoab_dir, [_sp_record(reg)])

    mock_cm = _mock_client_cm(_SEARCH_NOT_FOUND, "")
    with patch("atlas_stf.oab_sp._runner.OabSpClient", return_value=mock_cm):
        count = run_society_fetch(config)

    assert count == 0
    checkpoint = load_checkpoint(config.checkpoint_file)
    assert checkpoint.stats["not_found"] == 1
    assert checkpoint.stats["failed"] == 0


def test_registration_mismatch(tmp_path: Path):
    """Detail page returns a different registration number → mark_failed."""
    reg = "11111"
    search_html = _SEARCH_WITH_RESULT_TPL.format(param="99000", name="FIRMA X")
    # Detail page claims reg=22222, not 11111
    detail_html = _DETAIL_TPL.format(name="FIRMA X ADVOGADOS", reg="22222")

    config = _make_config(tmp_path)
    _write_deoab_jsonl(config.deoab_dir, [_sp_record(reg)])

    mock_cm = _mock_client_cm(search_html, detail_html)
    with patch("atlas_stf.oab_sp._runner.OabSpClient", return_value=mock_cm):
        count = run_society_fetch(config)

    assert count == 0
    checkpoint = load_checkpoint(config.checkpoint_file)
    assert checkpoint.stats["failed"] == 1
    assert checkpoint.stats["completed"] == 0


def test_retry_on_5xx(tmp_path: Path):
    """RuntimeError from client.search_by_registration → mark_failed."""
    reg = "33333"
    config = _make_config(tmp_path)
    _write_deoab_jsonl(config.deoab_dir, [_sp_record(reg)])

    client = MagicMock()
    client.search_by_registration.side_effect = RuntimeError("HTTP 500 after retries")
    mock_cm = MagicMock()
    mock_cm.__enter__.return_value = client
    mock_cm.__exit__.return_value = False

    with patch("atlas_stf.oab_sp._runner.OabSpClient", return_value=mock_cm):
        count = run_society_fetch(config)

    assert count == 0
    checkpoint = load_checkpoint(config.checkpoint_file)
    assert checkpoint.stats["failed"] == 1


def test_resume_from_checkpoint(tmp_path: Path):
    """Pre-resolved registrations are skipped; only pending ones are processed."""
    reg_completed = "10001"
    reg_not_found = "10002"
    reg_exhausted = "10003"
    reg_pending = "10004"

    config = _make_config(tmp_path)
    _write_deoab_jsonl(
        config.deoab_dir,
        [
            _sp_record(reg_completed),
            _sp_record(reg_not_found),
            _sp_record(reg_exhausted),
            _sp_record(reg_pending),
        ],
    )

    # Pre-populate checkpoint with terminal states
    cp = OabSpCheckpoint()
    cp.mark_completed(reg_completed)
    cp.mark_not_found(reg_not_found)
    cp.mark_failed(reg_exhausted)
    cp.promote_exhausted(max_retries=0)
    save_checkpoint(cp, config.checkpoint_file)

    search_html = _SEARCH_WITH_RESULT_TPL.format(param="55000", name="FIRMA PENDENTE")
    detail_html = _DETAIL_TPL.format(name="FIRMA PENDENTE ADVOGADOS", reg=reg_pending)
    mock_cm = _mock_client_cm(search_html, detail_html)

    with patch("atlas_stf.oab_sp._runner.OabSpClient", return_value=mock_cm) as mock_cls:
        count = run_society_fetch(config)

    assert count == 1
    # Only the pending registration was processed
    inner_client = mock_cls.return_value.__enter__.return_value
    assert inner_client.search_by_registration.call_count == 1
    call_args = inner_client.search_by_registration.call_args[0][0]
    assert call_args == reg_pending


def test_dry_run(tmp_path: Path):
    """Dry run makes no HTTP requests and returns 0."""
    config = _make_config(tmp_path, dry_run=True)
    _write_deoab_jsonl(config.deoab_dir, [_sp_record("77777")])

    mock_cm = _mock_client_cm("", "")
    with patch("atlas_stf.oab_sp._runner.OabSpClient", return_value=mock_cm) as mock_cls:
        count = run_society_fetch(config)

    assert count == 0
    # The client context manager must not have been entered
    mock_cls.return_value.__enter__.assert_not_called()
