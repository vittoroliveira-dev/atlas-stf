from __future__ import annotations

from atlas_stf.analytics._match_helpers import (
    compute_favorable_rate,
    compute_favorable_rate_role_aware,
)


def test_compute_favorable_rate_excludes_neutral_outcomes() -> None:
    rate = compute_favorable_rate(["Provido", "Prejudicado", "Não provido"])
    assert rate == 0.5


def test_compute_favorable_rate_returns_none_when_only_neutral() -> None:
    assert compute_favorable_rate(["Prejudicado", "Extinto o processo"]) is None


def test_compute_favorable_rate_role_aware_excludes_neutral_outcomes() -> None:
    rate = compute_favorable_rate_role_aware(
        [
            ("Agravo regimental não provido", "RECDO.(A/S)"),
            ("Prejudicado", "RECDO.(A/S)"),
            ("Provido", "RECDO.(A/S)"),
        ]
    )
    assert rate == 0.5
