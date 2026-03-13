"""Project-wide CLI entrypoint."""

from __future__ import annotations

import sys
from pathlib import Path

DEFAULT_RAW_DIR = Path("data/raw/transparencia")
DEFAULT_STAGING_DIR = Path("data/staging/transparencia")
DEFAULT_JURIS_DIR = Path("data/raw/jurisprudencia")
DEFAULT_CURATED_DIR = Path("data/curated")
DEFAULT_ANALYTICS_DIR = Path("data/analytics")
DEFAULT_DATABASE_ENV = "ATLAS_STF_DATABASE_URL"


def _has_juris_data(juris_dir: Path) -> bool:
    """Check if jurisprudencia directory exists and contains JSONL files."""
    for subdir in ("decisoes", "acordaos"):
        path = juris_dir / subdir
        if path.is_dir() and any(path.glob("*.jsonl")):
            return True
    return False


def _resolve_process_index(juris_dir: Path) -> dict | None:
    if not _has_juris_data(juris_dir):
        return None
    from ..curated.jurisprudencia_index import build_process_index

    return build_process_index(juris_dir)


def _resolve_decision_index(juris_dir: Path) -> dict | None:
    if not _has_juris_data(juris_dir):
        return None
    from ..curated.jurisprudencia_index import build_decision_index

    return build_decision_index(juris_dir)


def _should_use_default_juris_dir(*, requested_juris_dir: Path, primary_path: Path, default_primary_path: Path) -> bool:
    if requested_juris_dir != DEFAULT_JURIS_DIR:
        return True
    return primary_path == default_primary_path


def main(argv: list[str] | None = None) -> int:
    from ._handlers import dispatch
    from ._parsers import _build_parser

    parser = _build_parser()
    args = parser.parse_args(argv)
    return dispatch(parser, args)


if __name__ == "__main__":
    sys.exit(main())
