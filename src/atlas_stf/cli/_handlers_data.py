"""Dispatch handlers for data preparation and curate commands."""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from pathlib import Path

from . import (
    DEFAULT_STAGING_DIR,
    _resolve_decision_index,
    _resolve_process_index,
    _should_use_default_juris_dir,
)

logger = logging.getLogger(__name__)


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

    if args.command == "audit" and args.audit_target == "representation":
        from ..audit_gates import audit_representation

        audit_representation(
            curated_dir=args.curated_dir,
            analytics_dir=args.analytics_dir,
            output_path=args.output,
        )
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
        from concurrent.futures import ThreadPoolExecutor, as_completed

        from ._progress import cli_progress

        portal_dir = Path("data/raw/stf_portal")
        output_dir = args.output_dir
        staging_dir = args.staging_dir
        juris_dir = args.juris_dir
        process_path = output_dir / "process.jsonl"

        # Each builder runs as a subprocess for memory isolation.
        # Waves encode the dependency graph:
        #   Wave 1: process + decision-event + movement (all read raw, independent)
        #   Wave 2: subject + party + counsel + links + entity-id (all depend on process)
        #   Wave 3: entity-reconciliation + representation (depend on wave 2 outputs)
        #   Wave 4: session-event (depends on movement + decision-event)
        waves: list[list[tuple[str, list[str]]]] = [
            # Wave 1: independent raw readers
            [
                (
                    "Construindo processos",
                    [
                        "curate",
                        "process",
                        "--staging-dir",
                        str(staging_dir),
                        "--output",
                        str(process_path),
                        "--juris-dir",
                        str(juris_dir),
                    ],
                ),
                (
                    "Construindo decisões",
                    [
                        "curate",
                        "decision-event",
                        "--staging-file",
                        str(staging_dir / "decisoes.csv"),
                        "--output",
                        str(output_dir / "decision_event.jsonl"),
                        "--juris-dir",
                        str(juris_dir),
                    ],
                ),
            ],
            # Wave 2: depend on process.jsonl
            [
                (
                    "Construindo assuntos",
                    [
                        "curate",
                        "subject",
                        "--process-path",
                        str(process_path),
                        "--output",
                        str(output_dir / "subject.jsonl"),
                    ],
                ),
                (
                    "Construindo partes",
                    [
                        "curate",
                        "party",
                        "--process-path",
                        str(process_path),
                        "--output",
                        str(output_dir / "party.jsonl"),
                    ],
                ),
                (
                    "Construindo advogados",
                    [
                        "curate",
                        "counsel",
                        "--process-path",
                        str(process_path),
                        "--output",
                        str(output_dir / "counsel.jsonl"),
                    ],
                ),
                (
                    "Construindo vínculos",
                    [
                        "curate",
                        "links",
                        "--process-path",
                        str(process_path),
                        "--party-output",
                        str(output_dir / "process_party_link.jsonl"),
                        "--counsel-output",
                        str(output_dir / "process_counsel_link.jsonl"),
                    ],
                ),
                (
                    "Identificadores de entidade",
                    [
                        "curate",
                        "entity-identifier",
                        "--process-path",
                        str(process_path),
                        "--juris-dir",
                        str(juris_dir),
                        "--output",
                        str(output_dir / "entity_identifier.jsonl"),
                    ],
                ),
            ],
            # Wave 3: depend on wave 2 outputs
            [
                (
                    "Reconciliação de entidades",
                    [
                        "curate",
                        "entity-reconciliation",
                        "--entity-identifier-path",
                        str(output_dir / "entity_identifier.jsonl"),
                        "--party-path",
                        str(output_dir / "party.jsonl"),
                        "--counsel-path",
                        str(output_dir / "counsel.jsonl"),
                        "--process-party-link-path",
                        str(output_dir / "process_party_link.jsonl"),
                        "--process-counsel-link-path",
                        str(output_dir / "process_counsel_link.jsonl"),
                        "--output",
                        str(output_dir / "entity_identifier_reconciliation.jsonl"),
                    ],
                ),
                (
                    "Construindo rede de representação",
                    [
                        "curate",
                        "representation",
                        "--process-path",
                        str(process_path),
                        "--portal-dir",
                        str(portal_dir),
                        "--curated-dir",
                        str(output_dir),
                    ],
                ),
            ],
        ]

        # Wave 1 also includes movement (independent, reads portal only)
        if portal_dir.exists():
            waves[0].append(
                (
                    "Construindo movimentações",
                    [
                        "curate",
                        "movement",
                        "--portal-dir",
                        str(portal_dir),
                        "--output",
                        str(output_dir / "movement.jsonl"),
                    ],
                )
            )
            # Wave 4: session-event depends on movement + decision-event
            waves.append(
                [
                    (
                        "Construindo sessões",
                        [
                            "curate",
                            "session-event",
                            "--movement-path",
                            str(output_dir / "movement.jsonl"),
                            "--portal-dir",
                            str(portal_dir),
                            "--decision-event-path",
                            str(output_dir / "decision_event.jsonl"),
                            "--output",
                            str(output_dir / "session_event.jsonl"),
                        ],
                    ),
                ]
            )

        total_builders = sum(len(w) for w in waves)
        completed = 0

        def _run_builder(desc: str, cli_args: list[str]) -> tuple[str, int]:
            """Run a single builder subprocess. Returns (desc, returncode)."""
            cmd = [sys.executable, "-m", "atlas_stf"] + cli_args
            try:
                result = subprocess.run(cmd, check=False, timeout=1800)
            except subprocess.TimeoutExpired:
                logger.error("Builder '%s' excedeu timeout de 30 minutos", desc)
                return desc, 1
            return desc, result.returncode

        with cli_progress("Curate") as on_progress:
            for wave_idx, wave in enumerate(waves):
                wave_descs = ", ".join(d for d, _ in wave)
                on_progress(completed, total_builders, f"Curate: Onda {wave_idx + 1} ({wave_descs})...")

                if len(wave) == 1:
                    # Single builder — run directly (no pool overhead)
                    desc, cli_args = wave[0]
                    desc, rc = _run_builder(desc, cli_args)
                    if rc != 0:
                        logger.error("Builder '%s' falhou com exit code %d", desc, rc)
                        return rc
                    completed += 1
                else:
                    # Multiple builders — run in parallel via thread pool.
                    # Threads wait on subprocess.run(); on first failure we stop
                    # collecting results and return early.  Already-running
                    # subprocesses will finish in the background (not killed),
                    # but no new wave is started.
                    with ThreadPoolExecutor(max_workers=min(len(wave), 3)) as pool:
                        futures = {pool.submit(_run_builder, desc, cli_args): desc for desc, cli_args in wave}
                        for future in as_completed(futures):
                            desc, rc = future.result()
                            if rc != 0:
                                logger.error("Builder '%s' falhou com exit code %d", desc, rc)
                                return rc
                            completed += 1
                            on_progress(completed, total_builders, f"Curate: {desc} concluído")

            on_progress(total_builders, total_builders, "Curate: Concluído")
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

    if args.command == "curate" and args.curate_target == "representation":
        from ..curated.build_representation import build_representation_jsonl
        from ._progress import cli_progress

        with cli_progress("Representation") as on_progress:
            build_representation_jsonl(
                process_path=args.process_path,
                portal_dir=args.portal_dir,
                curated_dir=args.curated_dir,
                on_progress=on_progress,
            )
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

    if args.command == "curate" and args.curate_target == "movement":
        from ..curated.build_movement import build_movement_jsonl

        build_movement_jsonl(portal_dir=args.portal_dir, output_path=args.output)
        return 0

    if args.command == "curate" and args.curate_target == "session-event":
        from ..curated.build_session_event import build_session_event_jsonl

        build_session_event_jsonl(
            movement_path=args.movement_path,
            portal_dir=args.portal_dir,
            output_path=args.output,
            decision_event_path=args.decision_event_path,
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
            ignore_tls=getattr(args, "ignore_tls", False),
        )
        try:
            scrape_target(config)
        except KeyboardInterrupt:
            pass
        return 0

    return None
