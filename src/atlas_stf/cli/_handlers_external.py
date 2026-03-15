"""Dispatch handlers for external data source commands."""

from __future__ import annotations

import argparse
import os


def dispatch_external(parser: argparse.ArgumentParser, args: argparse.Namespace) -> int | None:
    if args.command == "evidence" and args.evidence_target == "build":
        from ..evidence.build_bundle import build_evidence_bundle

        build_evidence_bundle(
            args.alert_id,
            alert_path=args.alert_path,
            baseline_path=args.baseline_path,
            comparison_group_path=args.comparison_group_path,
            decision_event_path=args.decision_event_path,
            process_path=args.process_path,
            evidence_dir=args.evidence_dir,
            report_dir=args.report_dir,
        )
        return 0

    if args.command == "evidence" and args.evidence_target == "build-all":
        from ..evidence.build_bundle import build_all_evidence_bundles

        build_all_evidence_bundles(
            alert_path=args.alert_path,
            baseline_path=args.baseline_path,
            comparison_group_path=args.comparison_group_path,
            decision_event_path=args.decision_event_path,
            process_path=args.process_path,
            evidence_dir=args.evidence_dir,
            report_dir=args.report_dir,
        )
        return 0

    if args.command == "datajud" and args.datajud_target == "fetch":
        from ..datajud._config import DATAJUD_API_KEY_ENV, DatajudFetchConfig
        from ..datajud._runner import fetch_origin_data

        api_key = args.api_key or os.getenv(DATAJUD_API_KEY_ENV)
        if not api_key:
            parser.error(f"--api-key is required when {DATAJUD_API_KEY_ENV} is not set.")
        config = DatajudFetchConfig(
            api_key=api_key,
            process_path=args.process_path,
            output_dir=args.output_dir,
            dry_run=args.dry_run,
        )
        fetch_origin_data(config)
        return 0

    if args.command == "datajud" and args.datajud_target == "build-context":
        from ..analytics.origin_context import build_origin_context

        build_origin_context(
            datajud_dir=args.datajud_dir,
            process_path=args.process_path,
            output_dir=args.output_dir,
        )
        return 0

    if args.command == "cgu" and args.cgu_target == "fetch":
        from ..cgu._config import CGU_API_KEY_ENV, CguFetchConfig
        from ..cgu._runner import fetch_sanctions_data
        from ._progress import cli_progress

        api_key = args.api_key or os.getenv(CGU_API_KEY_ENV, "")
        config = CguFetchConfig(
            output_dir=args.output_dir,
            api_key=api_key,
            party_path=args.party_path,
            dry_run=args.dry_run,
        )
        if config.dry_run:
            fetch_sanctions_data(config)
        else:
            with cli_progress("CGU") as on_progress:
                fetch_sanctions_data(config, on_progress=on_progress)
        return 0

    if args.command == "cgu" and args.cgu_target == "build-matches":
        from ..analytics.sanction_match import build_sanction_matches

        build_sanction_matches(
            cgu_dir=args.cgu_dir,
            output_dir=args.output_dir,
        )
        return 0

    if args.command == "tse" and args.tse_target == "fetch":
        from ..tse._config import TSE_ELECTION_YEARS, TseFetchConfig
        from ..tse._runner import fetch_donation_data
        from ._progress import cli_progress

        years = tuple(args.years) if args.years else TSE_ELECTION_YEARS
        config = TseFetchConfig(
            output_dir=args.output_dir,
            years=years,
            dry_run=args.dry_run,
            force_refresh=getattr(args, "force_refresh", False),
        )
        if config.dry_run:
            fetch_donation_data(config)
        else:
            with cli_progress("TSE") as on_progress:
                fetch_donation_data(config, on_progress=on_progress)
        return 0

    if args.command == "tse" and args.tse_target == "build-matches":
        from ..analytics.donation_match import build_donation_matches

        build_donation_matches(tse_dir=args.tse_dir, output_dir=args.output_dir)
        return 0

    if args.command == "cvm" and args.cvm_target == "fetch":
        from ..cvm._config import CvmFetchConfig
        from ..cvm._runner import fetch_cvm_data
        from ._progress import cli_progress

        config = CvmFetchConfig(output_dir=args.output_dir, dry_run=args.dry_run)
        if config.dry_run:
            fetch_cvm_data(config)
        else:
            with cli_progress("CVM") as on_progress:
                fetch_cvm_data(config, on_progress=on_progress)
        return 0

    if args.command == "cvm" and args.cvm_target == "build-matches":
        from ..analytics.sanction_match import build_sanction_matches

        build_sanction_matches(cvm_dir=args.cvm_dir, output_dir=args.output_dir)
        return 0

    if args.command == "rfb" and args.rfb_target == "fetch":
        from ..rfb._config import RfbFetchConfig
        from ..rfb._runner import fetch_rfb_data
        from ._progress import cli_progress

        config = RfbFetchConfig(output_dir=args.output_dir, dry_run=args.dry_run)
        if config.dry_run:
            fetch_rfb_data(config)
        else:
            with cli_progress("RFB") as on_progress:
                fetch_rfb_data(config, on_progress=on_progress)
        return 0

    if args.command == "rfb" and args.rfb_target == "build-network":
        from ..analytics.corporate_network import build_corporate_network

        build_corporate_network(
            rfb_dir=args.rfb_dir,
            output_dir=args.output_dir,
            max_link_degree=args.max_degree,
        )
        return 0

    if args.command == "rfb" and args.rfb_target == "build-groups":
        from ..analytics.economic_group import build_economic_groups

        build_economic_groups(
            rfb_dir=args.rfb_dir,
            output_dir=args.output_dir,
        )
        return 0

    if args.command == "oab" and args.oab_target == "validate":
        from ..oab._config import OAB_API_KEY_ENV, OabValidationConfig
        from ..oab._runner import run_oab_validation

        api_key = args.api_key or os.getenv(OAB_API_KEY_ENV)
        config = OabValidationConfig(
            curated_dir=args.curated_dir,
            output_dir=args.curated_dir,
            provider=args.provider,
            api_key=api_key,
        )
        run_oab_validation(config)
        return 0

    if args.command == "doc-extract" and args.doc_extract_target == "run":
        from ..doc_extractor._config import DocExtractorConfig
        from ..doc_extractor._runner import run_doc_extraction

        config = DocExtractorConfig(
            curated_dir=args.curated_dir,
            min_confidence_gap=args.min_confidence,
            max_documents=args.max_documents,
        )
        run_doc_extraction(config)
        return 0

    if args.command == "stf-portal" and args.stf_portal_target == "fetch":
        from ..stf_portal._config import StfPortalConfig
        from ..stf_portal._runner import run_extraction

        config = StfPortalConfig(
            output_dir=args.output_dir,
            curated_dir=args.curated_dir,
            max_processes=args.max_processes,
            rate_limit_seconds=args.rate_limit,
        )
        run_extraction(config, dry_run=args.dry_run)
        return 0

    if args.command == "agenda" and args.agenda_target == "fetch":
        from ..agenda._config import AgendaFetchConfig
        from ..agenda._runner import run_agenda_fetch

        config = AgendaFetchConfig(
            output_dir=args.output_dir,
            start_year=args.start_year,
            end_year=args.end_year,
            rate_limit_seconds=args.rate_limit,
            dry_run=args.dry_run,
        )
        run_agenda_fetch(config)
        return 0

    if args.command == "agenda" and args.agenda_target == "build-events":
        from ..curated.build_agenda import build_agenda_events

        build_agenda_events(raw_dir=args.raw_dir, curated_dir=args.curated_dir)
        return 0

    return None
