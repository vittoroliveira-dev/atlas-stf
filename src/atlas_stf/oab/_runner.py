"""OAB validation runner: reads counsel data, validates OAB numbers, writes results."""

from __future__ import annotations

import json
import logging
from dataclasses import asdict
from pathlib import Path

from ._config import OabValidationConfig
from ._providers import select_provider

logger = logging.getLogger(__name__)


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    """Read JSONL file, returning an empty list when the file does not exist."""
    if not path.exists():
        return []
    records: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def _write_jsonl(records: list[dict[str, object]], path: Path) -> None:
    """Write records to JSONL, creating parent directories as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")


def _extract_oab_entries(
    records: list[dict[str, object]],
) -> list[tuple[int, str, str]]:
    """Extract (index, oab_number, oab_state) for records that need validation.

    A record needs validation when it has ``oab_number`` and ``oab_state`` filled
    but ``oab_status`` is ``None`` (not yet validated).
    """
    entries: list[tuple[int, str, str]] = []
    for idx, record in enumerate(records):
        oab_number = record.get("oab_number")
        oab_state = record.get("oab_state")
        oab_status = record.get("oab_status")
        if oab_number and oab_state and oab_status is None:
            entries.append((idx, str(oab_number), str(oab_state)))
    return entries


def run_oab_validation(config: OabValidationConfig) -> int:
    """Run OAB validation pipeline.

    1. Read ``lawyer_entity.jsonl`` from ``config.curated_dir``
    2. Filter lawyers with ``oab_number`` filled and ``oab_status == None``
    3. Instantiate provider via :func:`select_provider`
    4. Validate in batch with rate limiting
    5. Update ``lawyer_entity.jsonl`` with results
    6. Return count of validated entries
    """
    input_path = config.curated_dir / "lawyer_entity.jsonl"
    records = _read_jsonl(input_path)

    if not records:
        logger.info("No lawyer_entity records found at %s", input_path)
        return 0

    entries = _extract_oab_entries(records)
    if not entries:
        logger.info("No records pending OAB validation")
        return 0

    provider = select_provider(config)
    logger.info(
        "Validating %d OAB entries with provider=%s",
        len(entries),
        config.provider,
    )

    validated_count = 0
    for batch_start in range(0, len(entries), config.batch_size):
        batch = entries[batch_start : batch_start + config.batch_size]
        batch_pairs = [(num, state) for _, num, state in batch]
        results = provider.validate_batch(batch_pairs)

        for (idx, _, _), result in zip(batch, results, strict=True):
            result_dict = asdict(result)
            for key, value in result_dict.items():
                records[idx][key] = value
            validated_count += 1

    output_path = config.output_dir / "lawyer_entity.jsonl"
    _write_jsonl(records, output_path)

    logger.info(
        "OAB validation complete: %d entries validated, written to %s",
        validated_count,
        output_path,
    )
    return validated_count
