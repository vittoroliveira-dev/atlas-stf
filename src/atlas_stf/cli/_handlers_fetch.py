"""CLI handlers for unified fetch manifest commands."""

from __future__ import annotations

import argparse
import json
import logging
import sys

logger = logging.getLogger(__name__)


def dispatch_fetch(parser: argparse.ArgumentParser, args: argparse.Namespace) -> int | None:
    """Handle ``atlas-stf fetch {plan,status,migrate,run}``."""
    if args.command != "fetch":
        return None

    target: str = args.fetch_target

    if target == "plan":
        return _handle_plan(args)
    if target == "status":
        return _handle_status(args)
    if target == "migrate":
        return _handle_migrate(args)
    if target == "run":
        return _handle_run(args)

    parser.print_help()
    return 1


def _handle_plan(args: argparse.Namespace) -> int:
    from ..fetch._manifest_planner import generate_plan

    discovery_kwargs: dict[str, dict[str, object]] = {}
    process_path = getattr(args, "process_path", None)
    if process_path:
        discovery_kwargs["datajud"] = {"process_path": process_path}

    plan = generate_plan(
        sources=args.sources,
        base_dir=args.output_dir,
        force_refresh=args.force_refresh,
        discovery_kwargs=discovery_kwargs,
    )

    downloads = [i for i in plan.items if i.action != "skip"]
    skips = [i for i in plan.items if i.action == "skip"]

    if args.json_output:
        from ..fetch._manifest_model import serialize_plan

        sys.stdout.write(serialize_plan(plan))
        sys.stdout.write("\n")
    else:
        pid = plan.plan_id[:12]
        n_items, n_dl, n_skip = len(plan.items), len(downloads), len(skips)
        print(f"Plan {pid}  |  {n_items} items  |  {n_dl} actions  |  {n_skip} skipped")
        print()
        for item in sorted(plan.items, key=lambda i: (i.source, i.unit_id)):
            marker = "  " if item.action == "skip" else "→ "
            print(f"  {marker}{item.action:<12} {item.unit_id:<40} {item.reason}")

    return 0


def _handle_status(args: argparse.Namespace) -> int:
    from ..fetch._manifest_model import REFRESH_POLICIES
    from ..fetch._manifest_store import load_manifest

    source_dirs: dict[str, str] = {
        "tse_donations": "tse",
        "tse_expenses": "tse",
        "tse_party_org": "tse",
        "cgu": "cgu",
        "cvm": "cvm",
        "rfb": "rfb",
        "datajud": "datajud",
    }

    sources = args.sources or sorted(REFRESH_POLICIES)
    all_data: dict[str, object] = {}

    for source in sources:
        subdir = source_dirs.get(source, source)
        out_dir = args.output_dir / subdir
        manifest = load_manifest(source, out_dir)

        if manifest is None:
            if args.json_output:
                all_data[source] = {"status": "no_manifest"}
            else:
                print(f"{source}: no manifest")
            continue

        committed = sum(1 for u in manifest.units.values() if u.status == "committed")
        failed = sum(1 for u in manifest.units.values() if u.status == "failed")
        pending = sum(1 for u in manifest.units.values() if u.status == "pending")
        total_records = sum(u.published_record_count for u in manifest.units.values())

        if args.json_output:
            all_data[source] = {
                "units": len(manifest.units),
                "committed": committed,
                "failed": failed,
                "pending": pending,
                "total_records": total_records,
                "last_updated": manifest.last_updated,
            }
        else:
            parts = [f"{len(manifest.units)} units"]
            if committed:
                parts.append(f"{committed} committed")
            if failed:
                parts.append(f"{failed} failed")
            if pending:
                parts.append(f"{pending} pending")
            if total_records:
                parts.append(f"{total_records} records")
            print(f"{source}: {', '.join(parts)}")

    if args.json_output:
        sys.stdout.write(json.dumps(all_data, ensure_ascii=False, indent=2))
        sys.stdout.write("\n")

    return 0


def _handle_migrate(args: argparse.Namespace) -> int:
    from ..fetch._migration import MigrationError, migrate_all

    try:
        reports = migrate_all(args.output_dir, sources=args.sources, dry_run=args.dry_run)
    except MigrationError as exc:
        logger.error("Migration failed: %s", exc)
        return 1

    prefix = "[dry-run] " if args.dry_run else ""

    for report in reports:
        if report.units_inferred == 0 and report.legacy_path is None:
            continue

        print(f"{prefix}{report.source}: {report.units_inferred} units inferred, {report.units_committed} committed")

        if report.fields_absent:
            for note in report.fields_absent:
                print(f"  ⚠ absent: {note}")
        if report.fidelity_losses:
            for note in report.fidelity_losses:
                print(f"  ⚠ fidelity: {note}")
        if report.ambiguities:
            for note in report.ambiguities:
                print(f"  ⚠ ambiguity: {note}")

    return 0


def _handle_run(args: argparse.Namespace) -> int:
    import os

    from ..datajud._fetch_adapter import execute_datajud_item
    from ..fetch._executor import execute_plan, validate_plan_for_execution
    from ..fetch._manifest_model import REFRESH_POLICIES

    plan_path = getattr(args, "plan", None)

    if plan_path is not None:
        # Pre-generated plan: validate supports_deferred_run
        from ..fetch._manifest_model import FetchPlan

        plan_data = json.loads(plan_path.read_text(encoding="utf-8"))
        plan = FetchPlan.from_dict(plan_data)

        # Fail-closed: reject deferred plan for non-deferred sources
        for item in plan.items:
            if item.action == "skip":
                continue
            policy = REFRESH_POLICIES.get(item.source)
            if policy and not policy.supports_deferred_run:
                logger.error(
                    "Source %r does not support deferred runs (supports_deferred_run=False). "
                    "Use 'fetch run --sources %s' instead of --plan for inline execution.",
                    item.source,
                    item.source,
                )
                return 1
    else:
        # Inline plan generation + execution (default, safe for all sources)
        from ..fetch._manifest_planner import generate_plan

        discovery_kwargs: dict[str, dict[str, object]] = {}
        process_path = getattr(args, "process_path", None)
        if process_path:
            discovery_kwargs["datajud"] = {"process_path": process_path}

        plan = generate_plan(
            sources=args.sources,
            base_dir=args.output_dir,
            force_refresh=getattr(args, "force_refresh", False),
            discovery_kwargs=discovery_kwargs,
        )

    errors = validate_plan_for_execution(plan)
    if errors:
        for e in errors:
            logger.error("Plan validation: %s", e)
        return 1

    actionable = [i for i in plan.items if i.action != "skip"]
    if not actionable:
        print("Nothing to do — all items up to date.")
        return 0

    print(f"Executing {len(actionable)} items...")

    api_key = getattr(args, "api_key", "") or os.getenv("DATAJUD_API_KEY", "")
    process_path = getattr(args, "process_path", None)

    results = execute_plan(
        plan,
        base_dir=args.output_dir,
        datajud_api_key=api_key,
        datajud_process_path=process_path,
        source_executors={"datajud": execute_datajud_item},
    )

    ok = sum(1 for r in results if r.success)
    fail = sum(1 for r in results if not r.success)
    for r in results:
        marker = "✓" if r.success else "✗"
        detail = f"{r.records_written} records" if r.success else r.error
        print(f"  {marker} {r.unit_id}: {detail}")

    if fail:
        print(f"\n{ok} succeeded, {fail} failed")
        return 1
    print(f"\n{ok} items executed successfully")
    return 0
