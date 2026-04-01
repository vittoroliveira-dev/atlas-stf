from __future__ import annotations

import logging
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, insert
from sqlalchemy.orm import Session

from ._builder_flow import FLOW_SHAPES as FLOW_SHAPES  # re-export
from ._builder_flow import _materialize_minister_flows
from ._builder_graph import materialize_graph
from ._builder_loaders import (
    build_source_audits,
    load_alerts,
    load_assignment_audits,
    load_cases,
    load_compound_risks,
    load_corporate_conflicts,
    load_counsel_affinities,
    load_counsel_network_clusters,
    load_counsels,
    load_decision_velocities,
    load_donation_events,
    load_donation_matches,
    load_law_firm_entities,
    load_lawyer_entities,
    load_metrics,
    load_minister_bios,
    load_ml_outlier_scores,
    load_movements,
    load_origin_contexts,
    load_parties,
    load_payment_counterparties,
    load_process_lawyers,
    load_rapporteur_changes,
    load_rapporteur_profiles,
    load_representation_edges,
    load_representation_events,
    load_sanction_corporate_links,
    load_sanction_matches,
    load_sequential_analyses,
    load_session_events,
    load_temporal_analyses,
    stream_economic_group_rows,
)
from ._builder_loaders_agenda import load_agenda_coverage, load_agenda_events, load_agenda_exposures
from ._builder_schema import (
    SERVING_SCHEMA_SINGLETON_KEY,
    SERVING_SCHEMA_VERSION,
    _ensure_compatible_schema,
    _serving_schema_fingerprint,
)
from ._builder_scoring import compute_graph_scores
from ._builder_sources import DEFAULT_ANALYTICS_DIR, DEFAULT_CURATED_DIR, _collect_source_files
from ._builder_utils import (
    _EG_BATCH_SIZE,
    ServingBuildResult,
    _extract_db_path,
    _log_phase,
    _rss_mb,
    _validate_critical_tables,
    _validate_inputs,
)
from .models import ServingEconomicGroup, ServingSchemaMeta

logger = logging.getLogger(__name__)


def build_serving_database(
    *,
    database_url: str,
    curated_dir: Path = DEFAULT_CURATED_DIR,
    analytics_dir: Path = DEFAULT_ANALYTICS_DIR,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> ServingBuildResult:
    final_path = _extract_db_path(database_url)
    build_path = final_path.with_suffix(final_path.suffix + ".build")
    build_url = f"sqlite+pysqlite:///{build_path}"

    # Remove stale build artifact if present.
    build_path.unlink(missing_ok=True)

    source_files = _collect_source_files(curated_dir, analytics_dir)
    validation_report_build = build_path.parent / "validation_report.json.build"
    _validate_inputs(
        curated_dir,
        analytics_dir,
        report_path=validation_report_build,
    )

    engine = create_engine(build_url)
    try:
        _ensure_compatible_schema(engine)

        _total = 39  # 34 loaders + 5 DB phases
        _step = 0

        def _tick(desc: str) -> None:
            nonlocal _step
            if on_progress:
                on_progress(_step, _total, desc)
            _step += 1

        # ── Phase 1: Schema + cases + alerts ──────────────────────────
        rss0 = _rss_mb()
        t0 = time.monotonic()

        _tick("Serving: Carregando processos...")
        cases = load_cases(curated_dir)
        _tick("Serving: Carregando alertas...")
        alerts = load_alerts(analytics_dir)

        _tick("Serving: Inserindo processos e alertas...")
        with Session(engine) as session:
            with session.begin():
                session.add_all(cases)
                session.add_all(alerts)

        phase1_rows = len(cases) + len(alerts)
        case_count = len(cases)
        alert_count = len(alerts)
        del cases, alerts
        _log_phase(
            "1-cases-alerts",
            rss_before=rss0,
            rss_after=_rss_mb(),
            row_count=phase1_rows,
            elapsed=time.monotonic() - t0,
        )

        # ── Phase 2: Minister flows ──────────────────────────────────
        rss0 = _rss_mb()
        t0 = time.monotonic()

        _tick("Serving: Materializando fluxo ministros...")
        with Session(engine) as session:
            with session.begin():
                minister_flows = _materialize_minister_flows(session)
                session.add_all(minister_flows)

        phase2_rows = len(minister_flows)
        del minister_flows
        _log_phase(
            "2-minister-flows",
            rss_before=rss0,
            rss_after=_rss_mb(),
            row_count=phase2_rows,
            elapsed=time.monotonic() - t0,
        )

        # ── Phase 3: Entities ─────────────────────────────────────────
        rss0 = _rss_mb()
        t0 = time.monotonic()

        _tick("Serving: Carregando advogados...")
        counsels, process_counsels = load_counsels(curated_dir)
        _tick("Serving: Carregando partes...")
        parties, process_parties = load_parties(curated_dir)

        _tick("Serving: Inserindo entidades...")
        with Session(engine) as session:
            with session.begin():
                session.add_all(counsels)
                session.add_all(process_counsels)
                session.add_all(parties)
                session.add_all(process_parties)

        phase3_rows = len(counsels) + len(process_counsels) + len(parties) + len(process_parties)
        counsel_count = len(counsels)
        party_count = len(parties)
        del counsels, process_counsels, parties, process_parties
        _log_phase(
            "3-entities",
            rss_before=rss0,
            rss_after=_rss_mb(),
            row_count=phase3_rows,
            elapsed=time.monotonic() - t0,
        )

        # ── Phase 4: Analytics ────────────────────────────────────────
        rss0 = _rss_mb()
        t0 = time.monotonic()

        _tick("Serving: Carregando perfis relatores...")
        rapporteur_profiles = load_rapporteur_profiles(analytics_dir)
        _tick("Serving: Carregando análises sequenciais...")
        sequential_analyses = load_sequential_analyses(analytics_dir)
        _tick("Serving: Carregando análises temporais...")
        temporal_analyses = load_temporal_analyses(analytics_dir)
        _tick("Serving: Carregando auditoria distribuição...")
        assignment_audits = load_assignment_audits(analytics_dir)
        _tick("Serving: Carregando ML outliers...")
        ml_outlier_scores = load_ml_outlier_scores(analytics_dir)
        _tick("Serving: Carregando contexto origem...")
        origin_contexts = load_origin_contexts(analytics_dir)

        _tick("Serving: Inserindo analytics...")
        with Session(engine) as session:
            with session.begin():
                session.add_all(rapporteur_profiles)
                session.add_all(sequential_analyses)
                session.add_all(temporal_analyses)
                session.add_all(assignment_audits)
                session.add_all(ml_outlier_scores)
                session.add_all(origin_contexts)

        phase4_rows = (
            len(rapporteur_profiles)
            + len(sequential_analyses)
            + len(temporal_analyses)
            + len(assignment_audits)
            + len(ml_outlier_scores)
            + len(origin_contexts)
        )
        del rapporteur_profiles, sequential_analyses, temporal_analyses
        del assignment_audits, ml_outlier_scores, origin_contexts
        _log_phase(
            "4-analytics",
            rss_before=rss0,
            rss_after=_rss_mb(),
            row_count=phase4_rows,
            elapsed=time.monotonic() - t0,
        )

        # ── Phase 5: Risk ─────────────────────────────────────────────
        rss0 = _rss_mb()
        t0 = time.monotonic()

        _tick("Serving: Carregando sanções...")
        sanction_matches, counsel_sanction_profiles = load_sanction_matches(analytics_dir)
        _tick("Serving: Carregando vínculos corporativos sanção...")
        sanction_corporate_links = load_sanction_corporate_links(analytics_dir)
        _tick("Serving: Carregando doações...")
        donation_matches, counsel_donation_profiles = load_donation_matches(analytics_dir)
        donation_events = load_donation_events(analytics_dir)
        _tick("Serving: Carregando contrapartes pagamento...")
        payment_counterparties = load_payment_counterparties(analytics_dir)
        _tick("Serving: Carregando vínculos societários...")
        corporate_conflicts = load_corporate_conflicts(analytics_dir)
        _tick("Serving: Carregando afinidade advogado...")
        counsel_affinities = load_counsel_affinities(analytics_dir)
        _tick("Serving: Carregando risco composto...")
        compound_risks = load_compound_risks(analytics_dir)
        _tick("Serving: Carregando velocidade decisória...")
        decision_velocities = load_decision_velocities(analytics_dir)
        _tick("Serving: Carregando mudanças de relatoria...")
        rapporteur_changes = load_rapporteur_changes(analytics_dir)
        _tick("Serving: Carregando clusters de advogados...")
        counsel_network_clusters = load_counsel_network_clusters(analytics_dir)

        _tick("Serving: Inserindo risco...")
        with Session(engine) as session:
            with session.begin():
                session.add_all(sanction_matches)
                session.add_all(counsel_sanction_profiles)
                session.add_all(sanction_corporate_links)
                session.add_all(donation_matches)
                session.add_all(donation_events)
                session.add_all(counsel_donation_profiles)
                session.add_all(payment_counterparties)
                session.add_all(corporate_conflicts)
                session.add_all(counsel_affinities)
                session.add_all(compound_risks)
                session.add_all(decision_velocities)
                session.add_all(rapporteur_changes)
                session.add_all(counsel_network_clusters)

        phase5_rows = (
            len(sanction_matches)
            + len(counsel_sanction_profiles)
            + len(sanction_corporate_links)
            + len(donation_matches)
            + len(donation_events)
            + len(counsel_donation_profiles)
            + len(payment_counterparties)
            + len(corporate_conflicts)
            + len(counsel_affinities)
            + len(compound_risks)
            + len(decision_velocities)
            + len(rapporteur_changes)
            + len(counsel_network_clusters)
        )
        del sanction_matches, counsel_sanction_profiles, sanction_corporate_links
        del donation_matches, donation_events, counsel_donation_profiles
        del payment_counterparties, corporate_conflicts, counsel_affinities
        del compound_risks, decision_velocities, rapporteur_changes
        del counsel_network_clusters
        _log_phase("5-risk", rss_before=rss0, rss_after=_rss_mb(), row_count=phase5_rows, elapsed=time.monotonic() - t0)

        # ── Phase 6: Economic groups (Core bulk insert) ───────────────
        rss0 = _rss_mb()
        t0 = time.monotonic()

        _tick("Serving: Carregando grupos economicos...")
        eg_count = 0
        batch: list[dict[str, Any]] = []
        with engine.begin() as conn:
            for row in stream_economic_group_rows(analytics_dir):
                batch.append(row)
                if len(batch) >= _EG_BATCH_SIZE:
                    conn.execute(insert(ServingEconomicGroup), batch)
                    eg_count += len(batch)
                    batch = []
            if batch:
                conn.execute(insert(ServingEconomicGroup), batch)
                eg_count += len(batch)

        _log_phase(
            "6-economic-groups",
            rss_before=rss0,
            rss_after=_rss_mb(),
            row_count=eg_count,
            elapsed=time.monotonic() - t0,
        )

        # ── Phase 7: Remaining ────────────────────────────────────────
        rss0 = _rss_mb()
        t0 = time.monotonic()

        _tick("Serving: Carregando biografias ministros...")
        minister_bios = load_minister_bios(curated_dir)
        _tick("Serving: Carregando movimentações...")
        movements = load_movements(curated_dir)
        _tick("Serving: Carregando sessões...")
        session_events = load_session_events(curated_dir)
        _tick("Serving: Carregando entidades advogados...")
        lawyer_entities = load_lawyer_entities(curated_dir)
        _tick("Serving: Carregando escritorios...")
        law_firm_entities = load_law_firm_entities(curated_dir)
        _tick("Serving: Carregando vinculos processo-advogado...")
        process_lawyers = load_process_lawyers(curated_dir)
        _tick("Serving: Carregando arestas representacao...")
        representation_edges = load_representation_edges(curated_dir)
        _tick("Serving: Carregando eventos representacao...")
        representation_events = load_representation_events(curated_dir)
        _tick("Serving: Carregando eventos agenda...")
        agenda_events = load_agenda_events(curated_dir)
        _tick("Serving: Carregando cobertura agenda...")
        agenda_coverage = load_agenda_coverage(curated_dir)
        _tick("Serving: Carregando exposicoes agenda...")
        agenda_exposures = load_agenda_exposures(analytics_dir)

        _tick("Serving: Inserindo dados restantes...")
        with Session(engine) as session:
            with session.begin():
                session.add_all(minister_bios)
                session.add_all(movements)
                session.add_all(session_events)
                session.add_all(lawyer_entities)
                session.add_all(law_firm_entities)
                session.add_all(process_lawyers)
                session.add_all(representation_edges)
                session.add_all(representation_events)
                session.add_all(agenda_events)
                session.add_all(agenda_coverage)
                session.add_all(agenda_exposures)

        phase7_rows = (
            len(minister_bios)
            + len(movements)
            + len(session_events)
            + len(lawyer_entities)
            + len(law_firm_entities)
            + len(process_lawyers)
            + len(representation_edges)
            + len(representation_events)
            + len(agenda_events)
            + len(agenda_coverage)
            + len(agenda_exposures)
        )
        del minister_bios, movements, session_events
        del lawyer_entities, law_firm_entities, process_lawyers
        del representation_edges, representation_events
        del agenda_events, agenda_coverage, agenda_exposures
        _log_phase(
            "7-remaining",
            rss_before=rss0,
            rss_after=_rss_mb(),
            row_count=phase7_rows,
            elapsed=time.monotonic() - t0,
        )

        # ── Phase 8: Graph materialization ────────────────────────────
        rss0 = _rss_mb()
        t0 = time.monotonic()
        _tick("Serving: Materializando grafo...")
        with Session(engine) as session:
            with session.begin():
                graph_counts = materialize_graph(
                    session,
                    analytics_dir=analytics_dir,
                    curated_dir=curated_dir,
                )
        graph_total = sum(graph_counts.values())
        _log_phase(
            "8-graph",
            rss_before=rss0,
            rss_after=_rss_mb(),
            row_count=graph_total,
            elapsed=time.monotonic() - t0,
        )

        # ── Phase 8b: Graph scoring ───────────────────────────────────
        rss0 = _rss_mb()
        t0 = time.monotonic()
        _tick("Serving: Scoring de grafo...")
        with Session(engine) as session:
            with session.begin():
                score_counts = compute_graph_scores(session)
        _log_phase(
            "8b-scoring",
            rss_before=rss0,
            rss_after=_rss_mb(),
            row_count=score_counts.get("scores", 0),
            elapsed=time.monotonic() - t0,
        )

        # ── Phase 9: Metadata ─────────────────────────────────────────
        rss0 = _rss_mb()
        t0 = time.monotonic()

        _tick("Serving: Calculando auditorias...")
        audits = build_source_audits(source_files)
        _tick("Serving: Carregando métricas...")
        metrics = load_metrics(analytics_dir)

        _tick("Serving: Finalizando metadados...")
        with Session(engine) as session:
            with session.begin():
                session.add_all(metrics)
                session.add_all(audits)
                session.add(
                    ServingSchemaMeta(
                        singleton_key=SERVING_SCHEMA_SINGLETON_KEY,
                        schema_version=SERVING_SCHEMA_VERSION,
                        schema_fingerprint=_serving_schema_fingerprint(),
                        built_at=datetime.now().astimezone(),
                    )
                )

        source_count = len(audits)
        del metrics, audits
        _log_phase(
            "8-metadata",
            rss_before=rss0,
            rss_after=_rss_mb(),
            row_count=source_count,
            elapsed=time.monotonic() - t0,
        )

        # ── Validation before publish ─────────────────────────────────
        _validate_critical_tables(engine)

    except BaseException:
        # Build failed after engine creation — clean up the temporary
        # .build file so it doesn't consume disk until the next run.
        # The final published database is never touched here.
        engine.dispose()
        build_path.unlink(missing_ok=True)
        raise
    else:
        engine.dispose()

    # ── Atomic publication ────────────────────────────────────────────
    final_path.parent.mkdir(parents=True, exist_ok=True)
    build_path.rename(final_path)
    validation_report_final = final_path.parent / "validation_report.json"
    if validation_report_build.exists():
        validation_report_build.rename(validation_report_final)

    if on_progress:
        on_progress(_total, _total, "Serving: Concluido")

    return ServingBuildResult(
        database_url=database_url,
        case_count=case_count,
        alert_count=alert_count,
        counsel_count=counsel_count,
        party_count=party_count,
        source_count=source_count,
    )
