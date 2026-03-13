"""Dispatch handlers for serving and API commands."""

from __future__ import annotations

import argparse
import os

from . import DEFAULT_DATABASE_ENV


def dispatch_serving(parser: argparse.ArgumentParser, args: argparse.Namespace) -> int | None:
    if args.command == "serving" and args.serving_target == "build":
        from ..serving.builder import build_serving_database
        from ._progress import cli_progress

        database_url = args.database_url or os.getenv(DEFAULT_DATABASE_ENV)
        if not database_url:
            parser.error(f"--database-url is required when {DEFAULT_DATABASE_ENV} is not set.")
        with cli_progress("Serving") as on_progress:
            build_serving_database(
                database_url=database_url,
                curated_dir=args.curated_dir,
                analytics_dir=args.analytics_dir,
                on_progress=on_progress,
            )
        return 0

    if args.command == "api" and args.api_target == "serve":
        import uvicorn

        database_url = args.database_url or os.getenv(DEFAULT_DATABASE_ENV)
        if not database_url:
            parser.error(f"--database-url is required when {DEFAULT_DATABASE_ENV} is not set.")
        os.environ[DEFAULT_DATABASE_ENV] = database_url
        uvicorn.run(
            "atlas_stf.api.app:create_app",
            factory=True,
            host=args.host,
            port=args.port,
            reload=args.reload,
        )
        return 0

    return None
