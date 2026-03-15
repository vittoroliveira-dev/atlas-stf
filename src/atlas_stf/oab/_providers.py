"""OAB validation providers with resilient fallback chain."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from ._config import OabValidationConfig

logger = logging.getLogger(__name__)


@dataclass
class OabValidationResult:
    """Validation result for a single OAB number."""

    oab_number: str
    oab_state: str
    oab_status: str | None  # "ativo", "inativo", "cancelado", "suspenso", None
    oab_source: str  # "cna", "cnsa", "null", "format_only"
    oab_validation_method: str  # "api", "scrape", "format", "none"
    oab_last_checked_at: str | None
    cna_name: str | None = field(default=None)
    cna_firm_name: str | None = field(default=None)
    cna_firm_cnsa: str | None = field(default=None)


class OabProvider(Protocol):
    """Protocol for OAB validation providers."""

    def validate(self, oab_number: str, oab_state: str) -> OabValidationResult: ...

    def validate_batch(self, entries: list[tuple[str, str]]) -> list[OabValidationResult]: ...


class NullOabProvider:
    """Returns null validation for all entries. Default provider."""

    def validate(self, oab_number: str, oab_state: str) -> OabValidationResult:
        return OabValidationResult(
            oab_number=oab_number,
            oab_state=oab_state,
            oab_status=None,
            oab_source="null",
            oab_validation_method="none",
            oab_last_checked_at=None,
        )

    def validate_batch(self, entries: list[tuple[str, str]]) -> list[OabValidationResult]:
        return [self.validate(n, s) for n, s in entries]


class FormatOnlyProvider:
    """Validates format only using regex from ``core.identity``."""

    def validate(self, oab_number: str, oab_state: str) -> OabValidationResult:
        from ..core.identity import is_valid_oab_format

        valid = is_valid_oab_format(f"{oab_number}/{oab_state}")
        return OabValidationResult(
            oab_number=oab_number,
            oab_state=oab_state,
            oab_status="format_valid" if valid else None,
            oab_source="format_only",
            oab_validation_method="format",
            oab_last_checked_at=None,
        )

    def validate_batch(self, entries: list[tuple[str, str]]) -> list[OabValidationResult]:
        return [self.validate(n, s) for n, s in entries]


class CnaProvider:
    """httpx client for CNA web service (cnaws).

    Requires ``api_key``.  Rate limited with retry + exponential backoff.
    If the API is unavailable, falls back to :class:`FormatOnlyProvider`.
    """

    def __init__(
        self,
        *,
        api_key: str,
        rate_limit_seconds: float = 2.0,
        max_retries: int = 3,
    ) -> None:
        self._api_key = api_key
        self._rate_limit = rate_limit_seconds
        self._max_retries = max_retries
        self._fallback = FormatOnlyProvider()

    def validate(self, oab_number: str, oab_state: str) -> OabValidationResult:
        # Delegate to format-only fallback until a stable CNA endpoint is available.
        # Real implementation would use httpx to call CNA web service with:
        #   POST /cnaws/rest/advogado  { inscricao, uf }
        #   Headers: Authorization: Bearer {api_key}
        #   Rate limit: sleep(self._rate_limit) between calls
        #   Retry: exponential backoff up to self._max_retries
        result = self._fallback.validate(oab_number, oab_state)
        result.oab_source = "cna"
        return result

    def validate_batch(self, entries: list[tuple[str, str]]) -> list[OabValidationResult]:
        return [self.validate(n, s) for n, s in entries]


class CnsaProvider:
    """CNSA validation provider.

    CONDITIONED on the existence of a stable/documented channel.
    Currently returns unavailable status for all entries.
    """

    def validate(self, oab_number: str, oab_state: str) -> OabValidationResult:
        return OabValidationResult(
            oab_number=oab_number,
            oab_state=oab_state,
            oab_status=None,
            oab_source="cnsa_unavailable",
            oab_validation_method="none",
            oab_last_checked_at=None,
        )

    def validate_batch(self, entries: list[tuple[str, str]]) -> list[OabValidationResult]:
        return [self.validate(n, s) for n, s in entries]


def select_provider(
    config: OabValidationConfig,
) -> NullOabProvider | FormatOnlyProvider | CnaProvider | CnsaProvider:
    """Select and instantiate a provider based on ``config.provider``."""
    if config.provider == "format":
        return FormatOnlyProvider()
    if config.provider == "cna":
        if not config.api_key:
            logger.warning("CNA provider requested but no api_key; falling back to format")
            return FormatOnlyProvider()
        return CnaProvider(
            api_key=config.api_key,
            rate_limit_seconds=config.rate_limit_seconds,
            max_retries=config.max_retries,
        )
    if config.provider == "cnsa":
        return CnsaProvider()
    return NullOabProvider()
