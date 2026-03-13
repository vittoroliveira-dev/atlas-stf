"""Command dispatch logic for Atlas STF CLI."""

from __future__ import annotations

import argparse

from ._handlers_analytics import dispatch_analytics
from ._handlers_data import dispatch_data
from ._handlers_external import dispatch_external
from ._handlers_serving import dispatch_serving


def dispatch(parser: argparse.ArgumentParser, args: argparse.Namespace) -> int:
    for handler in (dispatch_data, dispatch_analytics, dispatch_external, dispatch_serving):
        result = handler(parser, args)
        if result is not None:
            return result
    parser.print_help()
    return 1
