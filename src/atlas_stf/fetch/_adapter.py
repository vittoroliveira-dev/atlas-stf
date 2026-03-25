"""Source adapters — uniform interface over per-source discovery and probing.

Each adapter encapsulates the source-specific logic behind a ``Protocol``
so the planner and executor interact with a single abstraction.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Protocol, runtime_checkable

from ._manifest_model import (
    REFRESH_POLICIES,
    FetchUnit,
    RefreshPolicy,
    RemoteState,
    source_output_dir,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class FetchSourceAdapter(Protocol):
    """Public interface that every source adapter must satisfy."""

    @property
    def source_name(self) -> str: ...

    @property
    def policy(self) -> RefreshPolicy: ...

    def discover_units(self) -> Iterator[FetchUnit]: ...

    def probe_remote(self, unit: FetchUnit) -> RemoteState: ...


# ---------------------------------------------------------------------------
# Concrete adapters
# ---------------------------------------------------------------------------


class TseDonationsAdapter:
    """TSE campaign donation data (per-year ZIPs)."""

    def __init__(self, output_dir: Path, *, years: tuple[int, ...] | None = None, timeout: int = 120) -> None:
        self._output_dir = output_dir
        self._years = years
        self._timeout = timeout

    @property
    def source_name(self) -> str:
        return "tse_donations"

    @property
    def policy(self) -> RefreshPolicy:
        return REFRESH_POLICIES["tse_donations"]

    def discover_units(self) -> Iterator[FetchUnit]:
        from ._discovery import discover_units

        kwargs: dict[str, object] = {}
        if self._years is not None:
            kwargs["years"] = self._years
        yield from discover_units("tse_donations", output_dir=self._output_dir, **kwargs)

    def probe_remote(self, unit: FetchUnit) -> RemoteState:
        from ._remote_probe import probe_remote_state

        return probe_remote_state("tse_donations", unit, self.policy, timeout=self._timeout)


class TseExpensesAdapter:
    """TSE campaign expense data (per-year ZIPs)."""

    def __init__(self, output_dir: Path, *, years: tuple[int, ...] | None = None, timeout: int = 120) -> None:
        self._output_dir = output_dir
        self._years = years
        self._timeout = timeout

    @property
    def source_name(self) -> str:
        return "tse_expenses"

    @property
    def policy(self) -> RefreshPolicy:
        return REFRESH_POLICIES["tse_expenses"]

    def discover_units(self) -> Iterator[FetchUnit]:
        from ._discovery import discover_units

        kwargs: dict[str, object] = {}
        if self._years is not None:
            kwargs["years"] = self._years
        yield from discover_units("tse_expenses", output_dir=self._output_dir, **kwargs)

    def probe_remote(self, unit: FetchUnit) -> RemoteState:
        from ._remote_probe import probe_remote_state

        return probe_remote_state("tse_expenses", unit, self.policy, timeout=self._timeout)


class TsePartyOrgAdapter:
    """TSE party organ finance data (per-year ZIPs)."""

    def __init__(self, output_dir: Path, *, years: tuple[int, ...] | None = None, timeout: int = 120) -> None:
        self._output_dir = output_dir
        self._years = years
        self._timeout = timeout

    @property
    def source_name(self) -> str:
        return "tse_party_org"

    @property
    def policy(self) -> RefreshPolicy:
        return REFRESH_POLICIES["tse_party_org"]

    def discover_units(self) -> Iterator[FetchUnit]:
        from ._discovery import discover_units

        kwargs: dict[str, object] = {}
        if self._years is not None:
            kwargs["years"] = self._years
        yield from discover_units("tse_party_org", output_dir=self._output_dir, **kwargs)

    def probe_remote(self, unit: FetchUnit) -> RemoteState:
        from ._remote_probe import probe_remote_state

        return probe_remote_state("tse_party_org", unit, self.policy, timeout=self._timeout)


class CguAdapter:
    """CGU CEIS/CNEP/Leniência bulk CSV data."""

    def __init__(self, output_dir: Path, *, date_str: str = "", timeout: int = 30) -> None:
        self._output_dir = output_dir
        self._date_str = date_str
        self._timeout = timeout

    @property
    def source_name(self) -> str:
        return "cgu"

    @property
    def policy(self) -> RefreshPolicy:
        return REFRESH_POLICIES["cgu"]

    def discover_units(self) -> Iterator[FetchUnit]:
        from ._discovery import discover_units

        kwargs: dict[str, object] = {}
        if self._date_str:
            kwargs["date_str"] = self._date_str
        yield from discover_units("cgu", output_dir=self._output_dir, **kwargs)

    def probe_remote(self, unit: FetchUnit) -> RemoteState:
        from ._remote_probe import probe_remote_state

        return probe_remote_state("cgu", unit, self.policy, timeout=self._timeout)


class CvmAdapter:
    """CVM processo sancionador (single ZIP)."""

    def __init__(self, output_dir: Path, *, timeout: int = 120) -> None:
        self._output_dir = output_dir
        self._timeout = timeout

    @property
    def source_name(self) -> str:
        return "cvm"

    @property
    def policy(self) -> RefreshPolicy:
        return REFRESH_POLICIES["cvm"]

    def discover_units(self) -> Iterator[FetchUnit]:
        from ._discovery import discover_units

        yield from discover_units("cvm", output_dir=self._output_dir)

    def probe_remote(self, unit: FetchUnit) -> RemoteState:
        from ._remote_probe import probe_remote_state

        return probe_remote_state("cvm", unit, self.policy, timeout=self._timeout)


class RfbAdapter:
    """RFB CNPJ socios/empresas/estabelecimentos (WebDAV ZIPs)."""

    def __init__(self, output_dir: Path, *, base_url: str = "", timeout: int = 300) -> None:
        self._output_dir = output_dir
        self._base_url = base_url
        self._timeout = timeout

    @property
    def source_name(self) -> str:
        return "rfb"

    @property
    def policy(self) -> RefreshPolicy:
        return REFRESH_POLICIES["rfb"]

    def discover_units(self) -> Iterator[FetchUnit]:
        from ._discovery import discover_units

        kwargs: dict[str, object] = {}
        if self._base_url:
            kwargs["base_url"] = self._base_url
        yield from discover_units("rfb", output_dir=self._output_dir, **kwargs)

    def probe_remote(self, unit: FetchUnit) -> RemoteState:
        from ._remote_probe import probe_remote_state

        return probe_remote_state("rfb", unit, self.policy, timeout=self._timeout)


class DatajudAdapter:
    """DataJud CNJ API aggregation (no HTTP probe)."""

    def __init__(self, output_dir: Path, *, process_path: Path | None = None) -> None:
        self._output_dir = output_dir
        self._process_path = process_path

    @property
    def source_name(self) -> str:
        return "datajud"

    @property
    def policy(self) -> RefreshPolicy:
        return REFRESH_POLICIES["datajud"]

    def discover_units(self) -> Iterator[FetchUnit]:
        from ._discovery import discover_units

        kwargs: dict[str, object] = {}
        if self._process_path is not None:
            kwargs["process_path"] = self._process_path
        yield from discover_units("datajud", output_dir=self._output_dir, **kwargs)

    def probe_remote(self, unit: FetchUnit) -> RemoteState:
        """DataJud has no HTTP probe — return empty state."""
        from datetime import UTC, datetime

        return RemoteState(url="", probed_at=datetime.now(UTC).isoformat())


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

_ADAPTER_FACTORIES: dict[str, type] = {
    "tse_donations": TseDonationsAdapter,
    "tse_expenses": TseExpensesAdapter,
    "tse_party_org": TsePartyOrgAdapter,
    "cgu": CguAdapter,
    "cvm": CvmAdapter,
    "rfb": RfbAdapter,
    "datajud": DatajudAdapter,
}

def get_adapter(
    source: str,
    base_dir: Path,
    **kwargs: object,
) -> FetchSourceAdapter:
    """Instantiate the adapter for *source*.

    ``base_dir`` is the root data directory (e.g. ``data/raw``);
    the source subdirectory is resolved automatically.
    """
    cls = _ADAPTER_FACTORIES.get(source)
    if cls is None:
        msg = f"No adapter registered for source {source!r}"
        raise ValueError(msg)
    output_dir = source_output_dir(source, base_dir)
    return cls(output_dir, **kwargs)  # type: ignore[call-arg]


def list_sources() -> list[str]:
    """Return sorted list of registered source names."""
    return sorted(_ADAPTER_FACTORIES)
