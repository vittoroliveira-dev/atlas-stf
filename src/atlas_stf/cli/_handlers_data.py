"""Dispatch handlers for data preparation and curate commands."""

from __future__ import annotations

import argparse

from . import (
    DEFAULT_STAGING_DIR,
    _resolve_decision_index,
    _resolve_process_index,
    _should_use_default_juris_dir,
)


def dispatch_data(parser: argparse.ArgumentParser, args: argparse.Namespace) -> int | None:
    if args.command == "manifest" and args.manifest_target == "raw":
        from ..raw_manifest import build_raw_manifest

        build_raw_manifest(
            input_dir=args.dir,
            output_path=args.output,
            source_id=args.source_id,
            filter_description=args.filter_description,
            coverage_note=args.coverage_note,
        )
        return 0

    if args.command == "stage":
        from ..staging._runner import clean_all, clean_file, setup_logging

        args.staging_dir.mkdir(parents=True, exist_ok=True)
        setup_logging(args.staging_dir, verbose=args.verbose)
        if args.file:
            clean_file(args.file, raw_dir=args.raw_dir, staging_dir=args.staging_dir, dry_run=args.dry_run)
        elif args.dry_run:
            clean_all(raw_dir=args.raw_dir, staging_dir=args.staging_dir, dry_run=args.dry_run)
        else:
            from ._progress import cli_progress

            with cli_progress("Staging") as on_progress:
                clean_all(raw_dir=args.raw_dir, staging_dir=args.staging_dir, on_progress=on_progress)
        return 0

    if args.command == "profile" and args.profile_target == "staging":
        from ..profile import profile_staging

        profile_staging(input_dir=args.dir, output_dir=args.output_dir, filename=args.file)
        return 0

    if args.command == "validate" and args.validate_target == "staging":
        from ..validate import validate_staging

        validate_staging(input_dir=args.dir, output_path=args.output, filename=args.file)
        return 0

    if args.command == "audit" and args.audit_target == "stage":
        from ..audit_gates import audit_stage

        audit_stage(staging_dir=args.staging_dir, output_path=args.output)
        return 0

    if args.command == "audit" and args.audit_target == "curated":
        from ..audit_gates import audit_curated

        audit_curated(curated_dir=args.curated_dir, output_path=args.output)
        return 0

    if args.command == "audit" and args.audit_target == "analytics":
        from ..audit_gates import audit_analytics

        audit_analytics(
            comparison_group_path=args.comparison_group_path,
            link_path=args.link_path,
            baseline_path=args.baseline_path,
            alert_path=args.alert_path,
            decision_event_path=args.decision_event_path,
            process_path=args.process_path,
            evidence_dir=args.evidence_dir,
            output_path=args.output,
        )
        return 0

    if args.command == "curate" and args.curate_target == "process":
        from ..curated.build_process import build_process_jsonl

        juris_index = None
        if _should_use_default_juris_dir(
            requested_juris_dir=args.juris_dir,
            primary_path=args.staging_dir,
            default_primary_path=DEFAULT_STAGING_DIR,
        ):
            juris_index = _resolve_process_index(args.juris_dir)
        build_process_jsonl(staging_dir=args.staging_dir, output_path=args.output, juris_index=juris_index)
        return 0

    if args.command == "curate" and args.curate_target == "decision-event":
        from ..curated.build_decision_event import build_decision_event_jsonl

        decision_index = None
        if _should_use_default_juris_dir(
            requested_juris_dir=args.juris_dir,
            primary_path=args.staging_file,
            default_primary_path=DEFAULT_STAGING_DIR / "decisoes.csv",
        ):
            decision_index = _resolve_decision_index(args.juris_dir)
        build_decision_event_jsonl(
            staging_file=args.staging_file,
            output_path=args.output,
            decision_index=decision_index,
        )
        return 0

    if args.command == "curate" and args.curate_target == "all":
        from ..curated.build_counsel import build_counsel_jsonl
        from ..curated.build_decision_event import build_decision_event_jsonl
        from ..curated.build_entity_identifier import build_entity_identifier_jsonl
        from ..curated.build_entity_identifier_reconciliation import build_entity_identifier_reconciliation_jsonl
        from ..curated.build_links import build_process_links_jsonl
        from ..curated.build_party import build_party_jsonl
        from ..curated.build_process import build_process_jsonl
        from ..curated.build_subject import build_subject_jsonl
        from ._progress import cli_progress

        juris_index = None
        decision_index = None
        if _should_use_default_juris_dir(
            requested_juris_dir=args.juris_dir,
            primary_path=args.staging_dir,
            default_primary_path=DEFAULT_STAGING_DIR,
        ):
            juris_index = _resolve_process_index(args.juris_dir)
            decision_index = _resolve_decision_index(args.juris_dir)

        total = 8
        with cli_progress("Curate") as on_progress:
            process_path = args.output_dir / "process.jsonl"
            on_progress(0, total, "Curate: Construindo processos...")
            build_process_jsonl(
                staging_dir=args.staging_dir,
                output_path=process_path,
                juris_index=juris_index,
            )
            on_progress(1, total, "Curate: Construindo decisões...")
            build_decision_event_jsonl(
                staging_file=args.staging_dir / "decisoes.csv",
                output_path=args.output_dir / "decision_event.jsonl",
                decision_index=decision_index,
            )
            on_progress(2, total, "Curate: Construindo assuntos...")
            build_subject_jsonl(process_path=process_path, output_path=args.output_dir / "subject.jsonl")
            on_progress(3, total, "Curate: Construindo partes...")
            build_party_jsonl(process_path=process_path, output_path=args.output_dir / "party.jsonl")
            on_progress(4, total, "Curate: Construindo advogados...")
            build_counsel_jsonl(process_path=process_path, output_path=args.output_dir / "counsel.jsonl")
            on_progress(5, total, "Curate: Construindo vínculos...")
            build_process_links_jsonl(
                process_path=process_path,
                party_output_path=args.output_dir / "process_party_link.jsonl",
                counsel_output_path=args.output_dir / "process_counsel_link.jsonl",
            )
            on_progress(6, total, "Curate: Identificadores de entidade...")
            build_entity_identifier_jsonl(
                process_path=process_path,
                juris_dir=args.juris_dir,
                output_path=args.output_dir / "entity_identifier.jsonl",
            )
            on_progress(7, total, "Curate: Reconciliação de entidades...")
            build_entity_identifier_reconciliation_jsonl(
                entity_identifier_path=args.output_dir / "entity_identifier.jsonl",
                party_path=args.output_dir / "party.jsonl",
                counsel_path=args.output_dir / "counsel.jsonl",
                process_party_link_path=args.output_dir / "process_party_link.jsonl",
                process_counsel_link_path=args.output_dir / "process_counsel_link.jsonl",
                output_path=args.output_dir / "entity_identifier_reconciliation.jsonl",
            )
            on_progress(total, total, "Curate: Concluído")
        return 0

    if args.command == "curate" and args.curate_target == "subject":
        from ..curated.build_subject import build_subject_jsonl

        build_subject_jsonl(process_path=args.process_path, output_path=args.output)
        return 0

    if args.command == "curate" and args.curate_target == "party":
        from ..curated.build_party import build_party_jsonl

        build_party_jsonl(process_path=args.process_path, output_path=args.output)
        return 0

    if args.command == "curate" and args.curate_target == "counsel":
        from ..curated.build_counsel import build_counsel_jsonl

        build_counsel_jsonl(process_path=args.process_path, output_path=args.output)
        return 0

    if args.command == "curate" and args.curate_target == "entity-identifier":
        from ..curated.build_entity_identifier import build_entity_identifier_jsonl

        build_entity_identifier_jsonl(
            process_path=args.process_path,
            juris_dir=args.juris_dir,
            output_path=args.output,
        )
        return 0

    if args.command == "curate" and args.curate_target == "entity-reconciliation":
        from ..curated.build_entity_identifier_reconciliation import build_entity_identifier_reconciliation_jsonl

        build_entity_identifier_reconciliation_jsonl(
            entity_identifier_path=args.entity_identifier_path,
            party_path=args.party_path,
            counsel_path=args.counsel_path,
            process_party_link_path=args.process_party_link_path,
            process_counsel_link_path=args.process_counsel_link_path,
            output_path=args.output,
        )
        return 0

    if args.command == "curate" and args.curate_target == "links":
        from ..curated.build_links import build_process_links_jsonl

        build_process_links_jsonl(
            process_path=args.process_path,
            party_output_path=args.party_output,
            counsel_output_path=args.counsel_output,
        )
        return 0

    if args.command == "scrape":
        from ..scraper._config import TARGETS, ScrapeConfig
        from ..scraper._runner import scrape_target

        config = ScrapeConfig(
            target=TARGETS[args.target],
            output_dir=args.output_dir,
            start_date=args.start_date,
            end_date=args.end_date,
            rate_limit_seconds=args.rate_limit,
            headless=not args.no_headless,
            verbose=args.verbose,
            dry_run=args.dry_run,
        )
        try:
            scrape_target(config)
        except KeyboardInterrupt:
            pass
        return 0

    return None
