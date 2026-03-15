"""Dispatch handlers for analytics commands."""

from __future__ import annotations

import argparse


def dispatch_analytics(parser: argparse.ArgumentParser, args: argparse.Namespace) -> int | None:
    if args.command == "analytics" and args.analytics_target == "build-groups":
        from ..analytics.build_groups import build_groups
        from ._progress import cli_progress

        with cli_progress("Groups") as on_progress:
            build_groups(
                process_path=args.process_path,
                decision_event_path=args.decision_event_path,
                output_dir=args.output_dir,
                on_progress=on_progress,
            )
        return 0

    if args.command == "analytics" and args.analytics_target == "build-baseline":
        from ..analytics.baseline import build_baseline
        from ._progress import cli_progress

        with cli_progress("Baseline") as on_progress:
            build_baseline(
                comparison_group_path=args.comparison_group_path,
                link_path=args.link_path,
                decision_event_path=args.decision_event_path,
                output_path=args.output,
                summary_path=args.summary_output,
                on_progress=on_progress,
            )
        return 0

    if args.command == "analytics" and args.analytics_target == "build-alerts":
        from ..analytics.build_alerts import build_alerts
        from ._progress import cli_progress

        with cli_progress("Alerts") as on_progress:
            build_alerts(
                baseline_path=args.baseline_path,
                link_path=args.link_path,
                decision_event_path=args.decision_event_path,
                output_path=args.output,
                summary_path=args.summary_output,
                on_progress=on_progress,
            )
        return 0

    if args.command == "analytics" and args.analytics_target == "rapporteur-profile":
        from ..analytics.rapporteur_profile import build_rapporteur_profiles
        from ._progress import cli_progress

        with cli_progress("Rapporteur") as on_progress:
            build_rapporteur_profiles(
                decision_event_path=args.decision_event_path,
                process_path=args.process_path,
                output_dir=args.output_dir,
                on_progress=on_progress,
            )
        return 0

    if args.command == "analytics" and args.analytics_target == "assignment-audit":
        from ..analytics.assignment_audit import build_assignment_audit
        from ._progress import cli_progress

        with cli_progress("Assignment") as on_progress:
            build_assignment_audit(
                decision_event_path=args.decision_event_path,
                output_dir=args.output_dir,
                on_progress=on_progress,
            )
        return 0

    if args.command == "analytics" and args.analytics_target == "sequential":
        from ..analytics.sequential import build_sequential_analysis
        from ._progress import cli_progress

        with cli_progress("Sequential") as on_progress:
            build_sequential_analysis(
                decision_event_path=args.decision_event_path,
                output_dir=args.output_dir,
                on_progress=on_progress,
            )
        return 0

    if args.command == "analytics" and args.analytics_target == "build-temporal-analysis":
        from ..analytics.temporal_analysis import build_temporal_analysis
        from ._progress import cli_progress

        with cli_progress("Temporal") as on_progress:
            build_temporal_analysis(
                decision_event_path=args.decision_event_path,
                process_path=args.process_path,
                minister_bio_path=args.minister_bio_path,
                party_path=args.party_path,
                counsel_path=args.counsel_path,
                process_party_link_path=args.process_party_link_path,
                process_counsel_link_path=args.process_counsel_link_path,
                external_events_dir=args.external_events_dir,
                rfb_dir=args.rfb_dir,
                output_dir=args.output_dir,
                on_progress=on_progress,
            )
        return 0

    if args.command == "analytics" and args.analytics_target == "ml-outlier":
        from ..analytics.ml_outlier import build_ml_outlier_scores
        from ._progress import cli_progress

        with cli_progress("ML Outlier") as on_progress:
            build_ml_outlier_scores(
                decision_event_path=args.decision_event_path,
                output_dir=args.output_dir,
                on_progress=on_progress,
            )
        return 0

    if args.command == "analytics" and args.analytics_target == "minister-flow":
        from ..analytics.minister_flow import build_minister_flow
        from ._progress import cli_progress

        collegiate_filter = "all"
        if args.collegiate_only:
            collegiate_filter = "colegiado"
        elif args.monocratic_only:
            collegiate_filter = "monocratico"

        with cli_progress("Minister Flow") as on_progress:
            build_minister_flow(
                minister=args.minister,
                year=args.year,
                month=args.month,
                collegiate_filter=collegiate_filter,
                decision_event_path=args.decision_event_path,
                process_path=args.process_path,
                alert_path=args.alert_path,
                output_path=args.output,
                on_progress=on_progress,
            )
        return 0

    if args.command == "analytics" and args.analytics_target == "decision-velocity":
        from ..analytics.decision_velocity import build_decision_velocity
        from ._progress import cli_progress

        with cli_progress("Velocity") as on_progress:
            build_decision_velocity(
                process_path=args.process_path,
                decision_event_path=args.decision_event_path,
                output_dir=args.output_dir,
                on_progress=on_progress,
            )
        return 0

    if args.command == "analytics" and args.analytics_target == "rapporteur-change":
        from ..analytics.rapporteur_change import build_rapporteur_changes
        from ._progress import cli_progress

        with cli_progress("Rapporteur Change") as on_progress:
            build_rapporteur_changes(
                decision_event_path=args.decision_event_path,
                process_path=args.process_path,
                output_dir=args.output_dir,
                on_progress=on_progress,
            )
        return 0

    if args.command == "analytics" and args.analytics_target == "counsel-network":
        from ..analytics.counsel_network import build_counsel_network
        from ._progress import cli_progress

        with cli_progress("Counsel Network") as on_progress:
            build_counsel_network(
                curated_dir=args.curated_dir,
                output_dir=args.output_dir,
                on_progress=on_progress,
            )
        return 0

    if args.command == "analytics" and args.analytics_target == "procedural-timeline":
        from ..analytics.procedural_timeline import build_procedural_timeline
        from ._progress import cli_progress

        with cli_progress("Timeline") as on_progress:
            build_procedural_timeline(
                curated_dir=args.curated_dir,
                output_dir=args.output_dir,
                on_progress=on_progress,
            )
        return 0

    if args.command == "analytics" and args.analytics_target == "pauta-anomaly":
        from ..analytics.pauta_anomaly import build_pauta_anomaly
        from ._progress import cli_progress

        with cli_progress("Pauta Anomaly") as on_progress:
            build_pauta_anomaly(
                session_event_path=args.session_event_path,
                output_dir=args.output_dir,
                on_progress=on_progress,
            )
        return 0

    if args.command == "analytics" and args.analytics_target == "counsel-affinity":
        from ..analytics.counsel_affinity import build_counsel_affinity
        from ._progress import cli_progress

        with cli_progress("Affinity") as on_progress:
            build_counsel_affinity(output_dir=args.output_dir, on_progress=on_progress)
        return 0

    if args.command == "analytics" and args.analytics_target == "compound-risk":
        from ..analytics.compound_risk import build_compound_risk
        from ._progress import cli_progress

        with cli_progress("Compound Risk") as on_progress:
            build_compound_risk(
                curated_dir=args.curated_dir,
                analytics_dir=args.analytics_dir,
                output_dir=args.output_dir,
                on_progress=on_progress,
            )
        return 0

    if args.command == "analytics" and args.analytics_target == "representation-graph":
        from ..analytics.representation_graph import build_representation_graph
        from ._progress import cli_progress

        with cli_progress("Rep Graph") as on_progress:
            build_representation_graph(
                curated_dir=args.curated_dir,
                output_dir=args.output_dir,
                on_progress=on_progress,
            )
        return 0

    if args.command == "analytics" and args.analytics_target == "representation-recurrence":
        from ..analytics.representation_recurrence import build_representation_recurrence
        from ._progress import cli_progress

        with cli_progress("Recurrence") as on_progress:
            build_representation_recurrence(
                curated_dir=args.curated_dir,
                output_dir=args.output_dir,
                on_progress=on_progress,
            )
        return 0

    if args.command == "analytics" and args.analytics_target == "representation-windows":
        from ..analytics.representation_windows import build_representation_windows
        from ._progress import cli_progress

        with cli_progress("Windows") as on_progress:
            build_representation_windows(
                curated_dir=args.curated_dir,
                output_dir=args.output_dir,
                on_progress=on_progress,
            )
        return 0

    if args.command == "analytics" and args.analytics_target == "amicus-network":
        from ..analytics.amicus_network import build_amicus_network
        from ._progress import cli_progress

        with cli_progress("Amicus") as on_progress:
            build_amicus_network(
                curated_dir=args.curated_dir,
                output_dir=args.output_dir,
                on_progress=on_progress,
            )
        return 0

    if args.command == "analytics" and args.analytics_target == "firm-cluster":
        from ..analytics.firm_cluster import build_firm_cluster
        from ._progress import cli_progress

        with cli_progress("Firm Cluster") as on_progress:
            build_firm_cluster(
                curated_dir=args.curated_dir,
                output_dir=args.output_dir,
                on_progress=on_progress,
            )
        return 0

    if args.command == "analytics" and args.analytics_target == "agenda-exposure":
        from ..analytics.agenda_exposure import build_agenda_exposure
        from ._progress import cli_progress

        with cli_progress("Agenda Exposure") as on_progress:
            build_agenda_exposure(
                curated_dir=args.curated_dir,
                analytics_dir=args.output_dir,
                on_progress=on_progress,
            )
        return 0

    return None
