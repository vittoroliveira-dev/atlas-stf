"""Orchestrator for ministerial agenda data extraction."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

from ._client import AgendaClient, AgendaWafChallengeError
from ._config import STF_PRESIDENTS, AgendaFetchConfig
from ._parser import normalize_raw_day

logger = logging.getLogger(__name__)


def _month_range(config: AgendaFetchConfig) -> list[tuple[int, int]]:
    now = datetime.now(timezone.utc)
    end_year = config.end_year or now.year
    end_month = config.end_month or now.month
    months: list[tuple[int, int]] = []
    year, month = config.start_year, config.start_month
    while (year, month) <= (end_year, end_month):
        months.append((year, month))
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    return months


def _is_valid_cache(output_path: Path, raw_path: Path) -> bool:
    """Return True only if the cached artefact is usable."""
    if not output_path.exists():
        return False

    # Empty JSONL = previous failed fetch wrote nothing useful
    if output_path.stat().st_size == 0:
        logger.info("Stale cache (empty): %s — will refetch", output_path.name)
        output_path.unlink()
        if raw_path.exists():
            raw_path.unlink()
        return False

    # Raw file with GraphQL errors but no data = bad fetch
    if raw_path.exists():
        try:
            with raw_path.open(encoding="utf-8") as f:
                raw = json.load(f)
            resp = raw.get("response", {})
            has_errors = bool(resp.get("errors"))
            has_data = bool((resp.get("data") or {}).get("agendaMinistrosPorDiaCategoria"))
            if has_errors and not has_data:
                logger.info("Stale cache (GraphQL errors): %s — will refetch", output_path.name)
                output_path.unlink()
                raw_path.unlink()
                return False
        except json.JSONDecodeError, OSError:
            pass

    return True


def run_agenda_fetch(
    config: AgendaFetchConfig,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> int:
    months = _month_range(config)
    if not months:
        logger.warning("No months to fetch")
        return 0

    logger.info(
        "Agenda fetch: %d months (%04d-%02d to %04d-%02d)",
        len(months),
        months[0][0],
        months[0][1],
        months[-1][0],
        months[-1][1],
    )

    if config.dry_run:
        for year, month in months[:20]:
            existing = config.output_dir / f"{year:04d}-{month:02d}.jsonl"
            status = "exists" if existing.exists() else "pending"
            logger.info("  [DRY] %04d-%02d (%s)", year, month, status)
        if len(months) > 20:
            logger.info("  ... and %d more", len(months) - 20)
        return 0

    fetched = 0
    raw_dir = config.output_dir / "_raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    total = len(months)
    with AgendaClient(config) as client:
        for i, (year, month) in enumerate(months):
            output_path = config.output_dir / f"{year:04d}-{month:02d}.jsonl"
            raw_path = raw_dir / f"{year:04d}-{month:02d}.json"

            if on_progress:
                on_progress(i, total, f"Agenda: {year:04d}-{month:02d}")

            if _is_valid_cache(output_path, raw_path):
                logger.debug("Skipping %04d-%02d (valid cache)", year, month)
                continue

            try:
                raw_data, meta = client.fetch_month(year, month)
            except AgendaWafChallengeError:
                logger.error(
                    "WAF challenge on %04d-%02d — the STF site requires a browser session "
                    "that could not be established. Aborting remaining months.",
                    year,
                    month,
                )
                break
            except Exception:
                logger.exception("Failed to fetch %04d-%02d", year, month)
                continue

            with raw_path.open("w", encoding="utf-8") as f:
                json.dump({"response": raw_data, "metadata": meta}, f, ensure_ascii=False, indent=2)
                f.write("\n")

            days = (raw_data.get("data") or {}).get("agendaMinistrosPorDiaCategoria") or []
            normalized: list[dict[str, object]] = []
            for day in days:
                day["fetched_at"] = meta.get("fetched_at", "")
                normalized.extend(normalize_raw_day(day, STF_PRESIDENTS))

            with output_path.open("w", encoding="utf-8") as f:
                for event in normalized:
                    f.write(json.dumps(event, ensure_ascii=False) + "\n")

            fetched += 1
            logger.info("Fetched %04d-%02d: %d events", year, month, len(normalized))

        if on_progress:
            on_progress(total, total, "Agenda: Concluído")

    logger.info("Agenda fetch complete: %d months", fetched)
    return fetched
