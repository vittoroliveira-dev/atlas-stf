"""Unit discovery — enumerate fetchable artifacts per source.

Each source function yields ``FetchUnit`` objects representing the current
set of artifacts the source *could* provide.  No I/O beyond config reads
and, for CGU, a lightweight date-discovery HEAD request.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from pathlib import Path

from ._manifest_model import FetchUnit, RemoteState, build_unit_id

logger = logging.getLogger(__name__)


def discover_units(source: str, *, output_dir: Path, **kwargs: object) -> Iterator[FetchUnit]:
    """Dispatch discovery to the appropriate source handler.

    Extra ``kwargs`` are forwarded to the per-source function (e.g. years,
    process_path, etc.).
    """
    dispatch = {
        "tse_donations": _discover_tse_donations,
        "tse_expenses": _discover_tse_expenses,
        "tse_party_org": _discover_tse_party_org,
        "cgu": _discover_cgu,
        "cvm": _discover_cvm,
        "rfb": _discover_rfb,
        "datajud": _discover_datajud,
    }
    handler = dispatch.get(source)
    if handler is None:
        msg = f"Unknown source: {source!r}"
        raise ValueError(msg)
    yield from handler(output_dir=output_dir, **kwargs)


# ---------------------------------------------------------------------------
# TSE — Donations
# ---------------------------------------------------------------------------

_TSE_CDN_BASE = "https://cdn.tse.jus.br/estatistica/sead/odsele/prestacao_contas"
_TSE_ELECTION_YEARS = (2002, 2004, 2006, 2008, 2010, 2012, 2014, 2016, 2018, 2020, 2022, 2024)


def _tse_zip_urls(year: int) -> list[str]:
    return [
        f"{_TSE_CDN_BASE}/prestacao_de_contas_eleitorais_candidatos_{year}.zip",
        f"{_TSE_CDN_BASE}/prestacao_contas_{year}.zip",
        f"{_TSE_CDN_BASE}/prestacao_final_{year}.zip",
    ]


def _discover_tse_donations(*, output_dir: Path, **kwargs: object) -> Iterator[FetchUnit]:
    years: tuple[int, ...] = kwargs.get("years", _TSE_ELECTION_YEARS)  # type: ignore[assignment]
    for year in years:
        uid = build_unit_id("tse_donations", str(year))
        urls = _tse_zip_urls(year)
        yield FetchUnit(
            unit_id=uid,
            source="tse_donations",
            label=f"TSE donations {year}",
            remote_url=urls[0],
            remote_state=RemoteState(url=urls[0]),
            local_path=str(output_dir / f"tse_{year}.zip"),
            metadata={"candidate_urls": urls},
        )


# ---------------------------------------------------------------------------
# TSE — Expenses
# ---------------------------------------------------------------------------

_TSE_EXPENSE_YEARS = (2002, 2004, 2006, 2008, 2010, 2022, 2024)


def _discover_tse_expenses(*, output_dir: Path, **kwargs: object) -> Iterator[FetchUnit]:
    years: tuple[int, ...] = kwargs.get("years", _TSE_EXPENSE_YEARS)  # type: ignore[assignment]
    for year in years:
        uid = build_unit_id("tse_expenses", str(year))
        urls = _tse_zip_urls(year)
        yield FetchUnit(
            unit_id=uid,
            source="tse_expenses",
            label=f"TSE expenses {year}",
            remote_url=urls[0],
            remote_state=RemoteState(url=urls[0]),
            local_path=str(output_dir / f"tse_expenses_{year}.zip"),
            metadata={"candidate_urls": urls},
        )


# ---------------------------------------------------------------------------
# TSE — Party organisation
# ---------------------------------------------------------------------------

_TSE_PARTY_ORG_YEARS = (2018, 2020, 2022, 2024)


def _discover_tse_party_org(*, output_dir: Path, **kwargs: object) -> Iterator[FetchUnit]:
    years: tuple[int, ...] = kwargs.get("years", _TSE_PARTY_ORG_YEARS)  # type: ignore[assignment]
    for year in years:
        uid = build_unit_id("tse_party_org", str(year))
        urls = _tse_zip_urls(year)
        yield FetchUnit(
            unit_id=uid,
            source="tse_party_org",
            label=f"TSE party org {year}",
            remote_url=urls[0],
            remote_state=RemoteState(url=urls[0]),
            local_path=str(output_dir / f"tse_party_org_{year}.zip"),
            metadata={"candidate_urls": urls},
        )


# ---------------------------------------------------------------------------
# CGU
# ---------------------------------------------------------------------------

_CGU_DOWNLOAD_URL = "https://portaldatransparencia.gov.br/download-de-dados"
_CGU_DATASETS = ("ceis", "cnep", "acordos-leniencia")


def _discover_cgu(*, output_dir: Path, **kwargs: object) -> Iterator[FetchUnit]:
    date_str: str = kwargs.get("date_str", "")  # type: ignore[assignment]
    for dataset in _CGU_DATASETS:
        uid = (
            build_unit_id("cgu", dataset.replace("-", "_"), date_str)
            if date_str
            else build_unit_id("cgu", dataset.replace("-", "_"))
        )
        url = f"{_CGU_DOWNLOAD_URL}/{dataset}/{date_str}" if date_str else f"{_CGU_DOWNLOAD_URL}/{dataset}"
        yield FetchUnit(
            unit_id=uid,
            source="cgu",
            label=f"CGU {dataset}",
            remote_url=url,
            remote_state=RemoteState(url=url),
            local_path=str(output_dir / f"{dataset}.csv"),
        )


# ---------------------------------------------------------------------------
# CVM
# ---------------------------------------------------------------------------

_CVM_DATA_URL = "https://dados.cvm.gov.br/dados/PROCESSO/SANCIONADOR/DADOS/processo_sancionador.zip"


def _discover_cvm(*, output_dir: Path, **kwargs: object) -> Iterator[FetchUnit]:
    uid = build_unit_id("cvm", "sanctions")
    yield FetchUnit(
        unit_id=uid,
        source="cvm",
        label="CVM sanctions",
        remote_url=_CVM_DATA_URL,
        remote_state=RemoteState(url=_CVM_DATA_URL),
        local_path=str(output_dir / "processo_sancionador.zip"),
    )


# ---------------------------------------------------------------------------
# RFB
# ---------------------------------------------------------------------------

_RFB_PASSES = (
    ("socios_pass1", 10),
    ("socios_pass2", 10),
    ("empresas", 10),
    ("estabelecimentos", 10),
)


def _discover_rfb(*, output_dir: Path, **kwargs: object) -> Iterator[FetchUnit]:
    base_url: str = kwargs.get("base_url", "")  # type: ignore[assignment]
    for pass_name, count in _RFB_PASSES:
        for i in range(count):
            discriminator = f"{pass_name}_{i}" if count > 1 else pass_name
            uid = build_unit_id("rfb", discriminator)
            file_type = pass_name.split("_")[0].capitalize()
            url = f"{base_url}/{file_type}{i}.zip" if base_url else ""
            yield FetchUnit(
                unit_id=uid,
                source="rfb",
                label=f"RFB {pass_name} #{i}",
                remote_url=url,
                remote_state=RemoteState(url=url),
                local_path=str(output_dir / f"{file_type}{i}.zip"),
                metadata={"pass_name": pass_name, "file_index": i},
            )


# ---------------------------------------------------------------------------
# DataJud
# ---------------------------------------------------------------------------


def _discover_datajud(*, output_dir: Path, **kwargs: object) -> Iterator[FetchUnit]:
    process_path: Path | None = kwargs.get("process_path")  # type: ignore[assignment]
    if process_path is None or not Path(process_path).exists():
        logger.warning("DataJud discovery requires process_path; skipping")
        return

    indices = _read_datajud_indices(Path(process_path))
    for index in sorted(indices):
        uid = build_unit_id("datajud", index.lower().replace(".", "_"))
        yield FetchUnit(
            unit_id=uid,
            source="datajud",
            label=f"DataJud {index}",
            remote_url="",
            remote_state=RemoteState(url=""),
            local_path=str(output_dir / f"{index}.json"),
            metadata={"index": index},
        )


def _read_datajud_indices(process_path: Path) -> set[str]:
    """Extract unique DataJud index names from process.jsonl."""
    from ..core.origin_mapping import map_origin_to_datajud_indices

    indices: set[str] = set()
    with open(process_path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            court = rec.get("origin_court_or_body", "")
            state = rec.get("origin_description", "")
            if court:
                for idx in map_origin_to_datajud_indices(court, state):
                    indices.add(idx)
    return indices
