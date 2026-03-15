from __future__ import annotations

from atlas_stf.analytics._match_helpers import (
    compute_favorable_rate,
    compute_favorable_rate_role_aware,
    compute_favorable_rate_substantive,
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


def test_substantive_rate_filters_liminars() -> None:
    """Liminares should be excluded from substantive rate."""
    rate, n = compute_favorable_rate_substantive(
        [
            ("Provido", None),
            ("Liminar deferida", None),
            ("Não provido", None),
        ]
    )
    assert rate == 0.5
    assert n == 2


def test_substantive_rate_filters_procedural() -> None:
    """Desistências and inadmissibility should be excluded."""
    rate, n = compute_favorable_rate_substantive(
        [
            ("Provido", None),
            ("Homologada a desistência", None),
            ("Negado seguimento", None),
        ]
    )
    assert rate == 1.0
    assert n == 1


def test_substantive_rate_returns_none_when_no_substantive() -> None:
    """No substantive decisions → (None, 0)."""
    rate, n = compute_favorable_rate_substantive(
        [
            ("Liminar deferida", None),
            ("Prejudicado", None),
        ]
    )
    assert rate is None
    assert n == 0


def test_substantive_rate_returns_denominator() -> None:
    """n_substantive should count all substantive decisions."""
    rate, n = compute_favorable_rate_substantive(
        [
            ("Provido", None),
            ("Não provido", None),
            ("Desprovido", None),
        ]
    )
    assert n == 3
    assert rate is not None


def test_substantive_rate_excludes_unknown_embargos() -> None:
    """Embargos are unknown → excluded from substantive rate."""
    rate, n = compute_favorable_rate_substantive(
        [
            ("Provido", None),
            ("Embargos rejeitados", None),
            ("Embargos recebidos", None),
        ]
    )
    assert n == 1
    assert rate == 1.0
