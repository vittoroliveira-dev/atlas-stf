"""Analytics and scrape subparsers."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def _add_analytics_parsers(subparsers: Any) -> None:
    scrape = subparsers.add_parser("scrape", help="Scrape jurisprudência from STF API")
    scrape.add_argument("target", choices=["decisoes", "acordaos"], help="Target base to scrape")
    scrape.add_argument("--start-date", help="Start date yyyy-MM-dd (default: 2000-01-01)")
    scrape.add_argument("--end-date", help="End date yyyy-MM-dd (default: today)")
    scrape.add_argument("--output-dir", default="data/raw/jurisprudencia", help="Base output directory")
    scrape.add_argument("--rate-limit", type=float, default=0.5, help="Seconds between pages")
    scrape.add_argument("--no-headless", action="store_true", help="Show browser window")
    scrape.add_argument("--verbose", "-v", action="store_true", help="Debug-level logging")
    scrape.add_argument("--dry-run", action="store_true", help="List partitions without downloading")

    analytics = subparsers.add_parser("analytics", help="Build analytics-layer artifacts")
    analytics_sub = analytics.add_subparsers(dest="analytics_target", required=True)
    analytics_groups = analytics_sub.add_parser("build-groups", help="Build comparison groups")
    analytics_groups.add_argument(
        "--process-path",
        type=Path,
        default=Path("data/curated/process.jsonl"),
        help="Curated process JSONL path",
    )
    analytics_groups.add_argument(
        "--decision-event-path",
        type=Path,
        default=Path("data/curated/decision_event.jsonl"),
        help="Curated decision-event JSONL path",
    )
    analytics_groups.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/analytics"),
        help="Output directory for analytics artifacts",
    )
    analytics_baseline = analytics_sub.add_parser("build-baseline", help="Build baselines for valid groups")
    analytics_baseline.add_argument(
        "--comparison-group-path",
        type=Path,
        default=Path("data/analytics/comparison_group.jsonl"),
        help="Comparison group JSONL path",
    )
    analytics_baseline.add_argument(
        "--link-path",
        type=Path,
        default=Path("data/analytics/decision_event_group_link.jsonl"),
        help="Decision-event group link JSONL path",
    )
    analytics_baseline.add_argument(
        "--decision-event-path",
        type=Path,
        default=Path("data/curated/decision_event.jsonl"),
        help="Curated decision-event JSONL path",
    )
    analytics_baseline.add_argument(
        "--output",
        type=Path,
        default=Path("data/analytics/baseline.jsonl"),
        help="Output baseline JSONL path",
    )
    analytics_baseline.add_argument(
        "--summary-output",
        type=Path,
        default=Path("data/analytics/baseline_summary.json"),
        help="Output baseline summary JSON path",
    )
    analytics_alerts = analytics_sub.add_parser("build-alerts", help="Build outlier alerts from baselines")
    analytics_alerts.add_argument(
        "--baseline-path",
        type=Path,
        default=Path("data/analytics/baseline.jsonl"),
        help="Baseline JSONL path",
    )
    analytics_alerts.add_argument(
        "--link-path",
        type=Path,
        default=Path("data/analytics/decision_event_group_link.jsonl"),
        help="Decision-event group link JSONL path",
    )
    analytics_alerts.add_argument(
        "--decision-event-path",
        type=Path,
        default=Path("data/curated/decision_event.jsonl"),
        help="Curated decision-event JSONL path",
    )
    analytics_alerts.add_argument(
        "--output",
        type=Path,
        default=Path("data/analytics/outlier_alert.jsonl"),
        help="Output outlier alert JSONL path",
    )
    analytics_alerts.add_argument(
        "--summary-output",
        type=Path,
        default=Path("data/analytics/outlier_alert_summary.json"),
        help="Output alert summary JSON path",
    )
    analytics_rp = analytics_sub.add_parser("rapporteur-profile", help="Build rapporteur deviation profiles")
    analytics_rp.add_argument(
        "--decision-event-path",
        type=Path,
        default=Path("data/curated/decision_event.jsonl"),
        help="Curated decision-event JSONL path",
    )
    analytics_rp.add_argument(
        "--process-path",
        type=Path,
        default=Path("data/curated/process.jsonl"),
        help="Curated process JSONL path",
    )
    analytics_rp.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/analytics"),
        help="Output directory for analytics artifacts",
    )

    analytics_aa = analytics_sub.add_parser("assignment-audit", help="Audit rapporteur assignment uniformity")
    analytics_aa.add_argument(
        "--decision-event-path",
        type=Path,
        default=Path("data/curated/decision_event.jsonl"),
        help="Curated decision-event JSONL path",
    )
    analytics_aa.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/analytics"),
        help="Output directory for analytics artifacts",
    )

    analytics_seq = analytics_sub.add_parser("sequential", help="Build sequential decision analysis")
    analytics_seq.add_argument(
        "--decision-event-path",
        type=Path,
        default=Path("data/curated/decision_event.jsonl"),
        help="Curated decision-event JSONL path",
    )
    analytics_seq.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/analytics"),
        help="Output directory for analytics artifacts",
    )

    analytics_temporal = analytics_sub.add_parser("build-temporal-analysis", help="Build temporal analysis artifacts")
    analytics_temporal.add_argument(
        "--decision-event-path",
        type=Path,
        default=Path("data/curated/decision_event.jsonl"),
        help="Curated decision-event JSONL path",
    )
    analytics_temporal.add_argument(
        "--process-path",
        type=Path,
        default=Path("data/curated/process.jsonl"),
        help="Curated process JSONL path",
    )
    analytics_temporal.add_argument(
        "--minister-bio-path",
        type=Path,
        default=Path("data/curated/minister_bio.json"),
        help="Minister bio JSON path",
    )
    analytics_temporal.add_argument(
        "--party-path",
        type=Path,
        default=Path("data/curated/party.jsonl"),
        help="Curated party JSONL path",
    )
    analytics_temporal.add_argument(
        "--counsel-path",
        type=Path,
        default=Path("data/curated/counsel.jsonl"),
        help="Curated counsel JSONL path",
    )
    analytics_temporal.add_argument(
        "--process-party-link-path",
        type=Path,
        default=Path("data/curated/process_party_link.jsonl"),
        help="Process-party link JSONL path",
    )
    analytics_temporal.add_argument(
        "--process-counsel-link-path",
        type=Path,
        default=Path("data/curated/process_counsel_link.jsonl"),
        help="Process-counsel link JSONL path",
    )
    analytics_temporal.add_argument(
        "--external-events-dir",
        type=Path,
        default=Path("data/raw/external_events"),
        help="Directory with external event JSONL files",
    )
    analytics_temporal.add_argument(
        "--rfb-dir",
        type=Path,
        default=Path("data/raw/rfb"),
        help="Directory with RFB raw JSONL files",
    )
    analytics_temporal.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/analytics"),
        help="Output directory for analytics artifacts",
    )

    analytics_ml = analytics_sub.add_parser("ml-outlier", help="Build ML outlier scores (requires scikit-learn)")
    analytics_ml.add_argument(
        "--decision-event-path",
        type=Path,
        default=Path("data/curated/decision_event.jsonl"),
        help="Curated decision-event JSONL path",
    )
    analytics_ml.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/analytics"),
        help="Output directory for analytics artifacts",
    )

    analytics_velocity = analytics_sub.add_parser("decision-velocity", help="Build decision velocity analytics")
    analytics_velocity.add_argument(
        "--process-path",
        type=Path,
        default=Path("data/curated/process.jsonl"),
        help="Curated process JSONL path",
    )
    analytics_velocity.add_argument(
        "--decision-event-path",
        type=Path,
        default=Path("data/curated/decision_event.jsonl"),
        help="Curated decision-event JSONL path",
    )
    analytics_velocity.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/analytics"),
        help="Output directory for analytics artifacts",
    )

    analytics_rchange = analytics_sub.add_parser("rapporteur-change", help="Build rapporteur change analytics")
    analytics_rchange.add_argument(
        "--decision-event-path",
        type=Path,
        default=Path("data/curated/decision_event.jsonl"),
        help="Curated decision-event JSONL path",
    )
    analytics_rchange.add_argument(
        "--process-path",
        type=Path,
        default=Path("data/curated/process.jsonl"),
        help="Curated process JSONL path",
    )
    analytics_rchange.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/analytics"),
        help="Output directory for analytics artifacts",
    )

    analytics_cnet = analytics_sub.add_parser("counsel-network", help="Build counsel network clustering")
    analytics_cnet.add_argument(
        "--curated-dir",
        type=Path,
        default=Path("data/curated"),
        help="Curated JSONL directory",
    )
    analytics_cnet.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/analytics"),
        help="Output directory for analytics artifacts",
    )

    analytics_ptl = analytics_sub.add_parser("procedural-timeline", help="Build procedural timeline analytics")
    analytics_ptl.add_argument(
        "--curated-dir",
        type=Path,
        default=Path("data/curated"),
        help="Curated JSONL directory",
    )
    analytics_ptl.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/analytics"),
        help="Output directory for analytics artifacts",
    )

    analytics_pa = analytics_sub.add_parser("pauta-anomaly", help="Build pauta anomaly analytics")
    analytics_pa.add_argument(
        "--session-event-path",
        type=Path,
        default=Path("data/curated/session_event.jsonl"),
        help="Session event JSONL path",
    )
    analytics_pa.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/analytics"),
        help="Output directory for analytics artifacts",
    )

    analytics_affinity = analytics_sub.add_parser("counsel-affinity", help="Build counsel affinity analytics")
    analytics_affinity.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/analytics"),
        help="Output directory for analytics artifacts",
    )
    analytics_compound = analytics_sub.add_parser(
        "compound-risk",
        help="Build compound risk analytics from converging signals",
    )
    analytics_compound.add_argument(
        "--curated-dir",
        type=Path,
        default=Path("data/curated"),
        help="Curated JSONL directory",
    )
    analytics_compound.add_argument(
        "--analytics-dir",
        type=Path,
        default=Path("data/analytics"),
        help="Input analytics artifact directory",
    )
    analytics_compound.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/analytics"),
        help="Output directory for analytics artifacts",
    )

    analytics_repgraph = analytics_sub.add_parser("representation-graph", help="Build representation graph analytics")
    analytics_repgraph.add_argument(
        "--curated-dir",
        type=Path,
        default=Path("data/curated"),
        help="Curated JSONL directory",
    )
    analytics_repgraph.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/analytics"),
        help="Output directory for analytics artifacts",
    )

    analytics_recurrence = analytics_sub.add_parser(
        "representation-recurrence", help="Build representation recurrence analytics"
    )
    analytics_recurrence.add_argument(
        "--curated-dir",
        type=Path,
        default=Path("data/curated"),
        help="Curated JSONL directory",
    )
    analytics_recurrence.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/analytics"),
        help="Output directory for analytics artifacts",
    )

    analytics_windows = analytics_sub.add_parser(
        "representation-windows", help="Build representation windows analytics"
    )
    analytics_windows.add_argument(
        "--curated-dir",
        type=Path,
        default=Path("data/curated"),
        help="Curated JSONL directory",
    )
    analytics_windows.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/analytics"),
        help="Output directory for analytics artifacts",
    )

    analytics_amicus = analytics_sub.add_parser("amicus-network", help="Build amicus network analytics")
    analytics_amicus.add_argument(
        "--curated-dir",
        type=Path,
        default=Path("data/curated"),
        help="Curated JSONL directory",
    )
    analytics_amicus.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/analytics"),
        help="Output directory for analytics artifacts",
    )

    analytics_firmcluster = analytics_sub.add_parser("firm-cluster", help="Build firm cluster analytics")
    analytics_firmcluster.add_argument(
        "--curated-dir",
        type=Path,
        default=Path("data/curated"),
        help="Curated JSONL directory",
    )
    analytics_firmcluster.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/analytics"),
        help="Output directory for analytics artifacts",
    )

    analytics_agenda_exp = analytics_sub.add_parser("agenda-exposure", help="Build agenda exposure analytics")
    analytics_agenda_exp.add_argument(
        "--curated-dir",
        type=Path,
        default=Path("data/curated"),
        help="Curated JSONL directory",
    )
    analytics_agenda_exp.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/analytics"),
        help="Output directory for analytics artifacts",
    )

    analytics_flow = analytics_sub.add_parser(
        "minister-flow",
        help="Build monthly decision flow summary for a minister",
    )
    analytics_flow.add_argument("--minister", required=True, help="Minister name or partial string to match")
    analytics_flow.add_argument("--year", type=int, required=True, help="Year of the target period")
    analytics_flow.add_argument("--month", type=int, required=True, help="Month of the target period")
    flow_filter = analytics_flow.add_mutually_exclusive_group()
    flow_filter.add_argument(
        "--collegiate-only",
        action="store_true",
        help="Restrict flow to collegiate decision events",
    )
    flow_filter.add_argument(
        "--monocratic-only",
        action="store_true",
        help="Restrict flow to monocratic decision events",
    )
    analytics_flow.add_argument(
        "--decision-event-path",
        type=Path,
        default=Path("data/curated/decision_event.jsonl"),
        help="Curated decision-event JSONL path",
    )
    analytics_flow.add_argument(
        "--process-path",
        type=Path,
        default=Path("data/curated/process.jsonl"),
        help="Curated process JSONL path for class and thematic segmentation",
    )
    analytics_flow.add_argument(
        "--alert-path",
        type=Path,
        default=Path("data/analytics/outlier_alert.jsonl"),
        help="Outlier alert JSONL path for linked alert counts",
    )
    analytics_flow.add_argument(
        "--output",
        type=Path,
        help="Output minister flow JSON path",
    )
