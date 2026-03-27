"""Post-pipeline validation: checks that the 4 bug fixes are effective."""

import json
from pathlib import Path


def count_lines(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open() as f:
        return sum(1 for _ in f)


def check_rapporteur_coverage(path: Path) -> tuple[int, int]:
    total = with_rapporteur = 0
    if not path.exists():
        return 0, 0
    with path.open() as f:
        for line in f:
            total += 1
            rec = json.loads(line)
            if rec.get("rapporteur_at_event"):
                with_rapporteur += 1
    return with_rapporteur, total


def main() -> None:
    print("=" * 60)
    print("Validação pós-correção do pipeline")
    print("=" * 60)

    checks_passed = 0
    checks_total = 0

    # 1. Representation events (was: 1 event)
    event_path = Path("data/curated/representation_event.jsonl")
    n_events = count_lines(event_path)
    ok = n_events > 100
    checks_total += 1
    if ok:
        checks_passed += 1
    status = "OK" if ok else "FAIL"
    print(f"\n[{status}] representation_event: {n_events:,} eventos (antes: 1)")

    # 2. Rapporteur coverage in session_event (was: 0%)
    session_path = Path("data/curated/session_event.jsonl")
    with_rapp, total_se = check_rapporteur_coverage(session_path)
    pct = (with_rapp / total_se * 100) if total_se else 0
    ok = pct > 10
    checks_total += 1
    if ok:
        checks_passed += 1
    status = "OK" if ok else "FAIL"
    print(f"[{status}] session_event rapporteur: {with_rapp:,}/{total_se:,} ({pct:.1f}%) (antes: 0%)")

    # 3. Representation recurrence (was: 0 pairs)
    recurrence_path = Path("data/analytics/representation_recurrence.jsonl")
    n_recurrence = count_lines(recurrence_path)
    ok = n_recurrence > 0
    checks_total += 1
    if ok:
        checks_passed += 1
    status = "OK" if ok else "FAIL"
    print(f"[{status}] representation_recurrence: {n_recurrence:,} pares (antes: 0)")

    # 4. Pauta anomaly (was: 0 records — depends on rapporteur fix)
    pauta_path = Path("data/analytics/pauta_anomaly.jsonl")
    n_pauta = count_lines(pauta_path)
    ok = n_pauta > 0
    checks_total += 1
    if ok:
        checks_passed += 1
    status = "OK" if ok else "FAIL"
    print(f"[{status}] pauta_anomaly: {n_pauta:,} registros (antes: 0)")

    # 5. Temporal analysis (was: Error 143)
    temporal_path = Path("data/analytics/temporal_analysis.jsonl")
    n_temporal = count_lines(temporal_path)
    ok = n_temporal > 0
    checks_total += 1
    if ok:
        checks_passed += 1
    status = "OK" if ok else "FAIL"
    print(f"[{status}] temporal_analysis: {n_temporal:,} registros (antes: killed)")

    # 6. Serving database exists and is recent
    db_path = Path("data/serving/atlas_stf.db")
    ok = db_path.exists() and db_path.stat().st_size > 1_000_000
    checks_total += 1
    if ok:
        checks_passed += 1
    status = "OK" if ok else "FAIL"
    size_mb = db_path.stat().st_size / 1_048_576 if db_path.exists() else 0
    print(f"[{status}] serving DB: {size_mb:.1f} MB")

    print(f"\n{'=' * 60}")
    print(f"Resultado: {checks_passed}/{checks_total} checks passaram")
    if checks_passed == checks_total:
        print("Pipeline corrigido com sucesso.")
    else:
        print("Há checks falhando — investigar.")
    print("=" * 60)


if __name__ == "__main__":
    main()
