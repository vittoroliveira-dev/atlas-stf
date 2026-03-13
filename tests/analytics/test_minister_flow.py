from __future__ import annotations

import json
from pathlib import Path

from atlas_stf.analytics.minister_flow import build_minister_flow
from tests.analytics.conftest import make_decision_event, make_process, write_jsonl


def test_build_minister_flow_ok(tmp_path: Path):
    decision_event_path = tmp_path / "decision_event.jsonl"
    process_path = tmp_path / "process.jsonl"
    alert_path = tmp_path / "outlier_alert.jsonl"
    output = tmp_path / "flow.json"

    write_jsonl(
        decision_event_path,
        [
            make_decision_event(decision_event_id="de_0", process_id="proc_0", decision_date="2025-12-20"),
            make_decision_event(
                decision_event_id="de_1",
                process_id="proc_1",
                decision_date="2026-01-10",
                decision_type="Decisão Final",
                decision_progress="NEGOU PROVIMENTO",
                judging_body="TURMA",
                is_collegiate=True,
            ),
            make_decision_event(decision_event_id="de_2", process_id="proc_2", decision_date="2026-01-10"),
            make_decision_event(
                decision_event_id="de_3",
                process_id="proc_3",
                decision_date="2026-01-11",
                current_rapporteur="MIN. GILMAR MENDES",
            ),
        ],
    )
    alert_path.write_text(json.dumps({"decision_event_id": "de_1"}) + "\n", encoding="utf-8")
    write_jsonl(
        process_path,
        [
            make_process(process_id="proc_0"),
            make_process(process_id="proc_1"),
            make_process(process_id="proc_2", process_class="RCL", subjects_normalized=[], branch_of_law="DIREITO X"),
        ],
    )

    build_minister_flow(
        minister="TOFFOLI",
        year=2026,
        month=1,
        collegiate_filter="all",
        decision_event_path=decision_event_path,
        process_path=process_path,
        alert_path=alert_path,
        output_path=output,
    )

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["status"] == "ok"
    assert payload["collegiate_filter"] == "all"
    assert payload["event_count"] == 2
    assert payload["process_count"] == 2
    assert payload["linked_alert_count"] == 1
    assert payload["minister_reference"] == "MIN. DIAS TOFFOLI"
    assert payload["historical_event_count"] == 1
    assert payload["historical_active_day_count"] == 1
    assert payload["historical_average_events_per_active_day"] == 1.0
    assert payload["thematic_key_rule"] == "first_subject_normalized_else_branch_of_law"
    assert payload["thematic_source_distribution"] == {
        "branch_of_law_fallback": 1,
        "subjects_normalized_first": 1,
    }
    assert payload["historical_thematic_source_distribution"] == {"subjects_normalized_first": 1}
    assert payload["thematic_flow_interpretation_status"] == "inconclusivo"
    assert payload["thematic_flow_interpretation_reasons"] == [
        "event_count_lt_5",
        "active_day_count_lt_3",
        "historical_event_count_lt_20",
    ]
    assert payload["process_class_distribution"] == {"AC": 1, "RCL": 1}
    assert payload["thematic_distribution"] == {"DIREITO X": 1, "TEMA A": 1}
    assert payload["collegiate_distribution"] == {"colegiado": 1, "monocratico": 1}
    assert payload["daily_counts"] == [
        {
            "decision_date": "2026-01-10",
            "event_count": 2,
            "delta_vs_historical_average": 1.0,
            "ratio_vs_historical_average": 2.0,
        }
    ]
    assert payload["decision_type_flow"] == [
        {
            "segment_value": "Decisão Final",
            "event_count": 1,
            "process_count": 1,
            "active_day_count": 1,
            "historical_event_count": 0,
            "historical_active_day_count": 0,
            "historical_average_events_per_active_day": None,
            "daily_counts": [
                {
                    "decision_date": "2026-01-10",
                    "event_count": 1,
                    "delta_vs_historical_average": None,
                    "ratio_vs_historical_average": None,
                }
            ],
        },
        {
            "segment_value": "Despacho",
            "event_count": 1,
            "process_count": 1,
            "active_day_count": 1,
            "historical_event_count": 1,
            "historical_active_day_count": 1,
            "historical_average_events_per_active_day": 1.0,
            "daily_counts": [
                {
                    "decision_date": "2026-01-10",
                    "event_count": 1,
                    "delta_vs_historical_average": 0.0,
                    "ratio_vs_historical_average": 1.0,
                }
            ],
        },
    ]
    assert payload["decision_progress_flow"] == [
        {
            "segment_value": "DESPACHO",
            "event_count": 1,
            "process_count": 1,
            "active_day_count": 1,
            "historical_event_count": 1,
            "historical_active_day_count": 1,
            "historical_average_events_per_active_day": 1.0,
            "daily_counts": [
                {
                    "decision_date": "2026-01-10",
                    "event_count": 1,
                    "delta_vs_historical_average": 0.0,
                    "ratio_vs_historical_average": 1.0,
                }
            ],
        },
        {
            "segment_value": "NEGOU PROVIMENTO",
            "event_count": 1,
            "process_count": 1,
            "active_day_count": 1,
            "historical_event_count": 0,
            "historical_active_day_count": 0,
            "historical_average_events_per_active_day": None,
            "daily_counts": [
                {
                    "decision_date": "2026-01-10",
                    "event_count": 1,
                    "delta_vs_historical_average": None,
                    "ratio_vs_historical_average": None,
                }
            ],
        },
    ]
    assert payload["judging_body_flow"] == [
        {
            "segment_value": "MONOCRÁTICA",
            "event_count": 1,
            "process_count": 1,
            "active_day_count": 1,
            "historical_event_count": 1,
            "historical_active_day_count": 1,
            "historical_average_events_per_active_day": 1.0,
            "daily_counts": [
                {
                    "decision_date": "2026-01-10",
                    "event_count": 1,
                    "delta_vs_historical_average": 0.0,
                    "ratio_vs_historical_average": 1.0,
                }
            ],
        },
        {
            "segment_value": "TURMA",
            "event_count": 1,
            "process_count": 1,
            "active_day_count": 1,
            "historical_event_count": 0,
            "historical_active_day_count": 0,
            "historical_average_events_per_active_day": None,
            "daily_counts": [
                {
                    "decision_date": "2026-01-10",
                    "event_count": 1,
                    "delta_vs_historical_average": None,
                    "ratio_vs_historical_average": None,
                }
            ],
        },
    ]
    assert payload["process_class_flow"] == [
        {
            "segment_value": "AC",
            "event_count": 1,
            "process_count": 1,
            "active_day_count": 1,
            "historical_event_count": 1,
            "historical_active_day_count": 1,
            "historical_average_events_per_active_day": 1.0,
            "daily_counts": [
                {
                    "decision_date": "2026-01-10",
                    "event_count": 1,
                    "delta_vs_historical_average": 0.0,
                    "ratio_vs_historical_average": 1.0,
                }
            ],
        },
        {
            "segment_value": "RCL",
            "event_count": 1,
            "process_count": 1,
            "active_day_count": 1,
            "historical_event_count": 0,
            "historical_active_day_count": 0,
            "historical_average_events_per_active_day": None,
            "daily_counts": [
                {
                    "decision_date": "2026-01-10",
                    "event_count": 1,
                    "delta_vs_historical_average": None,
                    "ratio_vs_historical_average": None,
                }
            ],
        },
    ]
    assert payload["thematic_flow"] == [
        {
            "segment_value": "DIREITO X",
            "event_count": 1,
            "process_count": 1,
            "active_day_count": 1,
            "historical_event_count": 0,
            "historical_active_day_count": 0,
            "historical_average_events_per_active_day": None,
            "daily_counts": [
                {
                    "decision_date": "2026-01-10",
                    "event_count": 1,
                    "delta_vs_historical_average": None,
                    "ratio_vs_historical_average": None,
                }
            ],
        },
        {
            "segment_value": "TEMA A",
            "event_count": 1,
            "process_count": 1,
            "active_day_count": 1,
            "historical_event_count": 1,
            "historical_active_day_count": 1,
            "historical_average_events_per_active_day": 1.0,
            "daily_counts": [
                {
                    "decision_date": "2026-01-10",
                    "event_count": 1,
                    "delta_vs_historical_average": 0.0,
                    "ratio_vs_historical_average": 1.0,
                }
            ],
        },
    ]


def test_build_minister_flow_does_not_mutate_source_events(tmp_path: Path):
    decision_event_path = tmp_path / "decision_event.jsonl"
    process_path = tmp_path / "process.jsonl"

    source_events = [
        make_decision_event(decision_event_id="de_1", process_id="proc_1", decision_date="2026-01-10"),
        make_decision_event(decision_event_id="de_2", process_id="proc_1", decision_date="2025-12-10"),
    ]
    write_jsonl(decision_event_path, source_events)
    write_jsonl(
        process_path,
        [
            make_process(
                process_id="proc_1",
                process_class="RCL",
                branch_of_law="DIREITO X",
                subjects_normalized=[],
            )
        ],
    )

    build_minister_flow(
        minister="TOFFOLI",
        year=2026,
        month=1,
        collegiate_filter="all",
        decision_event_path=decision_event_path,
        process_path=process_path,
        output_path=tmp_path / "flow.json",
    )

    assert all("process_class" not in row for row in source_events)
    assert all("thematic_key" not in row for row in source_events)


def test_build_minister_flow_empty(tmp_path: Path):
    decision_event_path = tmp_path / "decision_event.jsonl"
    process_path = tmp_path / "process.jsonl"
    output = tmp_path / "flow.json"

    write_jsonl(
        decision_event_path,
        [
            make_decision_event(
                decision_event_id="de_1",
                process_id="proc_1",
                current_rapporteur="MIN. GILMAR MENDES",
            ),
        ],
    )
    write_jsonl(process_path, [make_process(process_id="proc_1")])

    build_minister_flow(
        minister="TOFFOLI",
        year=2026,
        month=1,
        collegiate_filter="all",
        decision_event_path=decision_event_path,
        process_path=process_path,
        alert_path=None,
        output_path=output,
    )

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["status"] == "empty"
    assert payload["collegiate_filter"] == "all"
    assert payload["event_count"] == 0
    assert payload["historical_event_count"] == 0
    assert payload["linked_alert_count"] == 0
    assert payload["thematic_key_rule"] == "first_subject_normalized_else_branch_of_law"
    assert payload["thematic_source_distribution"] == {}
    assert payload["historical_thematic_source_distribution"] == {}
    assert payload["thematic_flow_interpretation_status"] == "inconclusivo"
    assert payload["thematic_flow_interpretation_reasons"] == [
        "no_events_in_period",
        "event_count_lt_5",
        "active_day_count_lt_3",
        "historical_event_count_lt_20",
    ]
    assert payload["daily_counts"] == []
    assert payload["decision_type_flow"] == []
    assert payload["decision_progress_flow"] == []
    assert payload["judging_body_flow"] == []
    assert payload["process_class_flow"] == []
    assert payload["thematic_flow"] == []


def test_build_minister_flow_collegiate_filter(tmp_path: Path):
    decision_event_path = tmp_path / "decision_event.jsonl"
    process_path = tmp_path / "process.jsonl"
    output = tmp_path / "flow.json"

    write_jsonl(
        decision_event_path,
        [
            make_decision_event(decision_event_id="de_0", process_id="proc_0", decision_date="2025-12-20"),
            make_decision_event(
                decision_event_id="de_1",
                process_id="proc_1",
                decision_date="2026-01-10",
                decision_type="Decisão Final",
                decision_progress="NEGOU PROVIMENTO",
                judging_body="TURMA",
                is_collegiate=True,
            ),
            make_decision_event(decision_event_id="de_2", process_id="proc_2", decision_date="2026-01-10"),
        ],
    )
    write_jsonl(
        process_path,
        [
            make_process(process_id="proc_1"),
            make_process(process_id="proc_2", process_class="RCL", subjects_normalized=[], branch_of_law="DIREITO X"),
        ],
    )

    build_minister_flow(
        minister="TOFFOLI",
        year=2026,
        month=1,
        collegiate_filter="colegiado",
        decision_event_path=decision_event_path,
        process_path=process_path,
        alert_path=None,
        output_path=output,
    )

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["collegiate_filter"] == "colegiado"
    assert payload["event_count"] == 1
    assert payload["collegiate_distribution"] == {"colegiado": 1}
    assert payload["thematic_key_rule"] == "first_subject_normalized_else_branch_of_law"
    assert payload["thematic_source_distribution"] == {"subjects_normalized_first": 1}
    assert payload["historical_thematic_source_distribution"] == {}
    assert payload["thematic_flow_interpretation_status"] == "inconclusivo"
    assert payload["thematic_flow_interpretation_reasons"] == [
        "event_count_lt_5",
        "active_day_count_lt_3",
        "historical_event_count_lt_20",
    ]
    assert payload["process_class_distribution"] == {"AC": 1}
    assert payload["thematic_distribution"] == {"TEMA A": 1}
    assert payload["decision_progress_flow"] == [
        {
            "segment_value": "NEGOU PROVIMENTO",
            "event_count": 1,
            "process_count": 1,
            "active_day_count": 1,
            "historical_event_count": 0,
            "historical_active_day_count": 0,
            "historical_average_events_per_active_day": None,
            "daily_counts": [
                {
                    "decision_date": "2026-01-10",
                    "event_count": 1,
                    "delta_vs_historical_average": None,
                    "ratio_vs_historical_average": None,
                }
            ],
        }
    ]
    assert payload["process_class_flow"] == [
        {
            "segment_value": "AC",
            "event_count": 1,
            "process_count": 1,
            "active_day_count": 1,
            "historical_event_count": 0,
            "historical_active_day_count": 0,
            "historical_average_events_per_active_day": None,
            "daily_counts": [
                {
                    "decision_date": "2026-01-10",
                    "event_count": 1,
                    "delta_vs_historical_average": None,
                    "ratio_vs_historical_average": None,
                }
            ],
        }
    ]
    assert payload["thematic_flow"] == [
        {
            "segment_value": "TEMA A",
            "event_count": 1,
            "process_count": 1,
            "active_day_count": 1,
            "historical_event_count": 0,
            "historical_active_day_count": 0,
            "historical_average_events_per_active_day": None,
            "daily_counts": [
                {
                    "decision_date": "2026-01-10",
                    "event_count": 1,
                    "delta_vs_historical_average": None,
                    "ratio_vs_historical_average": None,
                }
            ],
        }
    ]
