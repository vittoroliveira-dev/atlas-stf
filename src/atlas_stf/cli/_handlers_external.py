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
        from ..datajud._config import DATAJUD_API_KEY_ENV, DATAJUD_DEFAULT_API_KEY, DatajudFetchConfig
        from ..datajud._runner import fetch_origin_data

        api_key = args.api_key or os.getenv(DATAJUD_API_KEY_ENV) or DATAJUD_DEFAULT_API_KEY
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
            force_refresh=getattr(args, "force_refresh", False),
        )
        if config.dry_run:
            fetch_sanctions_data(config)
        else:
            with cli_progress("CGU") as on_progress:
                fetch_sanctions_data(config, on_progress=on_progress)
        return 0

    if args.command == "cgu" and args.cgu_target == "build-matches":
        from ..analytics.sanction_match import build_sanction_matches
        from ._progress import cli_progress

        with cli_progress("CGU Matches") as on_progress:
            build_sanction_matches(
                cgu_dir=args.cgu_dir,
                output_dir=args.output_dir,
                on_progress=on_progress,
            )
        return 0

    if args.command == "cgu" and args.cgu_target == "build-corporate-links":
        from ..analytics.sanction_corporate_link import build_sanction_corporate_links
        from ._progress import cli_progress

        with cli_progress("CGU Corporate Links") as on_progress:
            build_sanction_corporate_links(
                cgu_dir=args.cgu_dir,
                cvm_dir=args.cvm_dir,
                rfb_dir=args.rfb_dir,
                output_dir=args.output_dir,
                on_progress=on_progress,
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

    if args.command == "tse" and args.tse_target == "fetch-expenses":
        from ..tse._config import TseExpenseFetchConfig
        from ..tse._runner_expenses import fetch_expense_data
        from ._progress import cli_progress

        years = tuple(args.years) if args.years else None
        config = TseExpenseFetchConfig(
            output_dir=args.output_dir,
            years=years,
            dry_run=args.dry_run,
            force_refresh=getattr(args, "force_refresh", False),
        )
        if config.dry_run:
            fetch_expense_data(config)
        else:
            with cli_progress("TSE Despesas") as on_progress:
                fetch_expense_data(config, on_progress=on_progress)
        return 0

    if args.command == "tse" and args.tse_target == "fetch-party-org":
        from ..tse._config import TSE_PARTY_ORG_YEARS, TsePartyOrgFetchConfig
        from ..tse._runner_party_org import fetch_party_org_data
        from ._progress import cli_progress

        if args.years:
            invalid = [y for y in args.years if y not in TSE_PARTY_ORG_YEARS]
            if invalid:
                parser.error(
                    f"Ano(s) {invalid} não suportado(s) para órgãos partidários. "
                    f"Anos disponíveis: {', '.join(str(y) for y in TSE_PARTY_ORG_YEARS)}."
                )
            years = tuple(args.years)
        else:
            years = TSE_PARTY_ORG_YEARS
        config = TsePartyOrgFetchConfig(
            output_dir=args.output_dir,
            years=years,
            dry_run=args.dry_run,
            force_refresh=getattr(args, "force_refresh", False),
        )
        if config.dry_run:
            fetch_party_org_data(config)
        else:
            with cli_progress("TSE Party Org") as on_progress:
                fetch_party_org_data(config, on_progress=on_progress)
        return 0

    if args.command == "tse" and args.tse_target == "build-matches":
        from ..analytics.donation_match import build_donation_matches
        from ._progress import cli_progress

        with cli_progress("TSE Matches") as on_progress:
            build_donation_matches(tse_dir=args.tse_dir, output_dir=args.output_dir, on_progress=on_progress)
        return 0

    if args.command == "tse" and args.tse_target == "build-counterparties":
        from ..analytics.payment_counterparty import build_payment_counterparties

        build_payment_counterparties(tse_dir=args.tse_dir, output_dir=args.output_dir)
        return 0

    if args.command == "tse" and args.tse_target == "build-donor-links":
        from ..analytics.donor_corporate_link import build_donor_corporate_links

        build_donor_corporate_links(tse_dir=args.tse_dir, rfb_dir=args.rfb_dir, output_dir=args.output_dir)
        return 0

    if args.command == "tse" and args.tse_target == "empirical-report":
        from ..analytics.donation_empirical import build_empirical_report

        build_empirical_report(tse_dir=args.tse_dir, analytics_dir=args.analytics_dir, output_dir=args.output_dir)
        return 0

    if args.command == "cvm" and args.cvm_target == "fetch":
        from ..cvm._config import CvmFetchConfig
        from ..cvm._runner import fetch_cvm_data
        from ._progress import cli_progress

        config = CvmFetchConfig(
            output_dir=args.output_dir,
            dry_run=args.dry_run,
            force_refresh=getattr(args, "force_refresh", False),
        )
        if config.dry_run:
            fetch_cvm_data(config)
        else:
            with cli_progress("CVM") as on_progress:
                fetch_cvm_data(config, on_progress=on_progress)
        return 0

    if args.command == "cvm" and args.cvm_target == "build-matches":
        from ..analytics.sanction_match import build_sanction_matches
        from ._progress import cli_progress

        with cli_progress("CVM Matches") as on_progress:
            build_sanction_matches(cvm_dir=args.cvm_dir, output_dir=args.output_dir, on_progress=on_progress)
        return 0

    if args.command == "rfb" and args.rfb_target == "fetch":
        from ..rfb._config import RfbFetchConfig
        from ..rfb._runner import fetch_rfb_data
        from ._progress import cli_progress

        config = RfbFetchConfig(
            output_dir=args.output_dir,
            dry_run=args.dry_run,
            force_refresh=getattr(args, "force_refresh", False),
        )
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
        from ._progress import cli_progress

        proxies = [p.strip() for p in args.proxies.split(",") if p.strip()] if getattr(args, "proxies", None) else []
        config = StfPortalConfig(
            output_dir=args.output_dir,
            curated_dir=args.curated_dir,
            max_processes=args.max_processes,
            rate_limit_seconds=args.rate_limit,
            global_rate_seconds=args.rate_limit,
            max_concurrent=getattr(args, "workers", 1),
            ignore_tls=args.ignore_tls,
            proxies=proxies,
        )
        if args.dry_run:
            run_extraction(config, dry_run=True)
        else:
            with cli_progress("STF Portal") as on_progress:
                run_extraction(config, on_progress=on_progress)
        return 0

    if args.command == "transparencia" and args.transparencia_target == "fetch":
        from ..transparencia._config import ALL_PAINEL_SLUGS, TransparenciaFetchConfig
        from ..transparencia._runner import fetch_transparencia_data
        from ._progress import cli_progress

        paineis = tuple(args.paineis) if args.paineis else ALL_PAINEL_SLUGS
        config = TransparenciaFetchConfig(
            output_dir=args.output_dir,
            paineis=paineis,
            headless=args.headless,
            ignore_tls=args.ignore_tls,
            dry_run=args.dry_run,
        )
        if config.dry_run:
            fetch_transparencia_data(config)
        else:
            with cli_progress("Transparência") as on_progress:
                fetch_transparencia_data(config, on_progress=on_progress)
        return 0

    if args.command == "agenda" and args.agenda_target == "fetch":
        from ..agenda._config import AgendaFetchConfig
        from ..agenda._runner import run_agenda_fetch
        from ._progress import cli_progress

        config = AgendaFetchConfig(
            output_dir=args.output_dir,
            start_year=args.start_year,
            end_year=args.end_year,
            rate_limit_seconds=args.rate_limit,
            dry_run=args.dry_run,
        )
        if config.dry_run:
            run_agenda_fetch(config)
        else:
            with cli_progress("Agenda") as on_progress:
                run_agenda_fetch(config, on_progress=on_progress)
        return 0

    if args.command == "agenda" and args.agenda_target == "build-events":
        from ..curated.build_agenda import build_agenda_events

        build_agenda_events(raw_dir=args.raw_dir, curated_dir=args.curated_dir)
        return 0

    if args.command == "oab-sp" and args.oab_sp_target == "fetch":
        from ..oab_sp._config import OabSpFetchConfig
        from ..oab_sp._runner import run_society_fetch
        from ._progress import cli_progress

        config = OabSpFetchConfig(
            output_dir=args.output_dir,
            checkpoint_file=args.output_dir / ".checkpoint.json",
            deoab_dir=args.deoab_dir,
            rate_limit_seconds=args.rate_limit,
            max_retries=args.max_retries,
            dry_run=args.dry_run,
        )
        if config.dry_run:
            run_society_fetch(config)
        else:
            with cli_progress("OAB/SP") as on_progress:
                run_society_fetch(config, on_progress=on_progress)
        return 0

    if args.command == "oab-sp" and args.oab_sp_target == "lookup":
        from ..oab_sp._config import OabSpLawyerLookupConfig
        from ..oab_sp._runner_lawyer import run_lawyer_lookup
        from ._progress import cli_progress

        config = OabSpLawyerLookupConfig(
            output_dir=args.output_dir,
            checkpoint_file=args.output_dir / ".checkpoint_lawyer.json",
            deoab_dir=args.deoab_dir,
            curated_dir=args.curated_dir,
            rate_limit_seconds=args.rate_limit,
            max_retries=args.max_retries,
            dry_run=args.dry_run,
        )
        if config.dry_run:
            run_lawyer_lookup(config)
        else:
            with cli_progress("OAB/SP Lawyer") as on_progress:
                run_lawyer_lookup(config, on_progress=on_progress)
        return 0

    if args.command == "deoab" and args.deoab_target == "fetch":
        from ..deoab._config import DeoabFetchConfig
        from ..deoab._runner import run_deoab_fetch
        from ._progress import cli_progress

        config = DeoabFetchConfig(
            output_dir=args.output_dir,
            start_year=args.start_year,
            end_year=args.end_year,
            dry_run=args.dry_run,
            force_reprocess=getattr(args, "force_reprocess", False),
        )
        if config.dry_run:
            run_deoab_fetch(config)
        else:
            with cli_progress("DEOAB") as on_progress:
                run_deoab_fetch(config, on_progress=on_progress)
        return 0

    return None
