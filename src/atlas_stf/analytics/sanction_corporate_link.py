"""Build sanction → corporate → STF indirect links via RFB bridge.

Public API re-exported from sub-modules:
  - build_sanction_corporate_links  (main entry point)
  - RED_FLAG_DELTA_THRESHOLD        (threshold constant used in tests)
  - _degree_decay                   (decay helper, accessed in tests)
  - _record_hash                    (hash helper, accessed in tests)
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path

from ._atomic_io import AtomicJsonlWriter
from ._match_helpers import degree_decay as _degree_decay
from ._run_context import RunContext
from ._scl_bridge import _load_sanctions, _record_hash
from ._scl_resolver import RED_FLAG_DELTA_THRESHOLD, resolve_and_write

logger = logging.getLogger(__name__)

DEFAULT_CGU_DIR = Path("data/raw/cgu")
DEFAULT_CVM_DIR = Path("data/raw/cvm")
DEFAULT_RFB_DIR = Path("data/raw/rfb")
DEFAULT_CURATED_DIR = Path("data/curated")
DEFAULT_ANALYTICS_DIR = Path("data/analytics")
DEFAULT_OUTPUT_DIR = Path("data/analytics")

__all__ = [
    "build_sanction_corporate_links",
    "RED_FLAG_DELTA_THRESHOLD",
    "_degree_decay",
    "_record_hash",
]


def build_sanction_corporate_links(
    *,
    cgu_dir: Path = DEFAULT_CGU_DIR,
    cvm_dir: Path = DEFAULT_CVM_DIR,
    rfb_dir: Path = DEFAULT_RFB_DIR,
    curated_dir: Path = DEFAULT_CURATED_DIR,
    analytics_dir: Path = DEFAULT_ANALYTICS_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> Path:
    """Build sanction → corporate → STF indirect links."""
    output_dir.mkdir(parents=True, exist_ok=True)
    total_steps = 11
    ctx = RunContext("cgu-corporate-links", output_dir, total_steps, on_progress=on_progress)

    try:
        # Step 1: Load sanctions
        ctx.start_step(1, "SCL: Carregando sanções...")
        sanctions = _load_sanctions(cgu_dir, cvm_dir)
        if not sanctions:
            logger.warning("No sanctions_raw.jsonl found — skipping sanction corporate links")
            ctx.finish(outputs=[])
            output_path = output_dir / "sanction_corporate_link.jsonl"
            with AtomicJsonlWriter(output_path) as _fh:
                pass
            return output_path
        ctx.log_memory(f"sanctions loaded: {len(sanctions)}")

        return resolve_and_write(
            sanctions=sanctions,
            rfb_dir=rfb_dir,
            curated_dir=curated_dir,
            analytics_dir=analytics_dir,
            output_dir=output_dir,
            ctx=ctx,
            on_progress=on_progress,
        )

    except BaseException:
        ctx.finish(outputs=[])
        raise
