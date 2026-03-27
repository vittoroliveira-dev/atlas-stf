"""Result types for STF portal extraction orchestration."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass
class ResolveResult:
    """Outcome of resolving the incidente ID for a process.

    Four possible statuses:
    - ``resolved``: incidente found successfully.
    - ``not_found_permanent``: HTTP 200 without incidente pattern,
      or HTTP 400/401/404.  Process does not exist.
    - ``transient_failure``: SSL, timeout, network error after max retries.
    - ``blocked_403``: WAF block after max retries.
    """

    status: Literal["resolved", "not_found_permanent", "transient_failure", "blocked_403"]
    incidente: str | None = None


@dataclass
class ProcessResult:
    """Outcome of attempting to extract a single process.

    Three possible statuses:
    - ``completed``: all tabs fetched and parsed, document assembled.
    - ``retry_later``: transient failure (403, timeout, server error).
      Partial progress preserved on disk for next run.
    - ``permanent_failure``: non-recoverable (404, parse error, max retries
      exceeded).  Marked as failed; will not be retried automatically.
    """

    status: Literal["completed", "retry_later", "permanent_failure"]
    doc: dict[str, Any] | None = None
    tabs_fetched: dict[str, str] = field(default_factory=dict)
    reason: str = ""
