from __future__ import annotations

import argparse

from ._parsers_analytics import _add_analytics_parsers
from ._parsers_curate import _add_curate_parsers
from ._parsers_data_prep import _add_data_prep_parsers
from ._parsers_external import _add_external_parsers
from ._parsers_ops import _add_ops_parsers


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Atlas STF")
    subparsers = parser.add_subparsers(dest="command", required=True)
    _add_data_prep_parsers(subparsers)
    _add_curate_parsers(subparsers)
    _add_analytics_parsers(subparsers)
    _add_external_parsers(subparsers)
    _add_ops_parsers(subparsers)
    return parser
