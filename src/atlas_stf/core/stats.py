"""Pure statistical functions — no dependencies beyond math."""

from __future__ import annotations

import math

# Pre-computed chi-square critical values for p=0.05, df=1..30
_CHI2_CRITICAL_005: dict[int, float] = {
    1: 3.841,
    2: 5.991,
    3: 7.815,
    4: 9.488,
    5: 11.070,
    6: 12.592,
    7: 14.067,
    8: 15.507,
    9: 16.919,
    10: 18.307,
    11: 19.675,
    12: 21.026,
    13: 22.362,
    14: 23.685,
    15: 24.996,
    16: 26.296,
    17: 27.587,
    18: 28.869,
    19: 30.144,
    20: 31.410,
    21: 32.671,
    22: 33.924,
    23: 35.172,
    24: 36.415,
    25: 37.652,
    26: 38.885,
    27: 40.113,
    28: 41.337,
    29: 42.557,
    30: 43.773,
}

# Critical values for p=0.01
_CHI2_CRITICAL_01: dict[int, float] = {
    1: 6.635,
    2: 9.210,
    3: 11.345,
    4: 13.277,
    5: 15.086,
    6: 16.812,
    7: 18.475,
    8: 20.090,
    9: 21.666,
    10: 23.209,
    11: 24.725,
    12: 26.217,
    13: 27.688,
    14: 29.141,
    15: 30.578,
    16: 31.999,
    17: 33.409,
    18: 34.805,
    19: 36.191,
    20: 37.566,
    21: 38.932,
    22: 40.289,
    23: 41.638,
    24: 42.980,
    25: 44.314,
    26: 45.642,
    27: 46.963,
    28: 48.278,
    29: 49.588,
    30: 50.892,
}

# Critical values for p=0.001
_CHI2_CRITICAL_001: dict[int, float] = {
    1: 10.828,
    2: 13.816,
    3: 16.266,
    4: 18.467,
    5: 20.515,
    6: 22.458,
    7: 24.322,
    8: 26.125,
    9: 27.877,
    10: 29.588,
    11: 31.264,
    12: 32.910,
    13: 34.528,
    14: 36.123,
    15: 37.697,
    16: 39.252,
    17: 40.790,
    18: 42.312,
    19: 43.820,
    20: 45.315,
    21: 46.797,
    22: 48.268,
    23: 49.728,
    24: 51.179,
    25: 52.620,
    26: 54.052,
    27: 55.476,
    28: 56.892,
    29: 58.301,
    30: 59.703,
}


def chi_square_statistic(observed: list[float], expected: list[float]) -> float:
    """Compute Pearson's chi-square test statistic.

    Both lists must have the same length and expected values must be > 0.
    """
    if len(observed) != len(expected):
        raise ValueError("observed and expected must have the same length")
    if not observed:
        raise ValueError("observed must not be empty")
    chi2 = 0.0
    for obs, exp in zip(observed, expected):
        if exp <= 0:
            raise ValueError(f"expected value must be > 0, got {exp}")
        chi2 += (obs - exp) ** 2 / exp
    return round(chi2, 6)


def beta_binomial_posterior_mean(
    successes: int,
    trials: int,
    *,
    alpha: float = 1.0,
    beta: float = 1.0,
) -> float:
    """Return the posterior mean for a Beta-Binomial model.

    This is equivalent to Laplace smoothing when ``alpha=beta=1``.
    """
    if trials < 0:
        raise ValueError(f"trials must be >= 0, got {trials}")
    if successes < 0 or successes > trials:
        raise ValueError("successes must be between 0 and trials")
    if alpha <= 0 or beta <= 0:
        raise ValueError("alpha and beta must be > 0")
    posterior_successes = successes + alpha
    posterior_trials = trials + alpha + beta
    return round(posterior_successes / posterior_trials, 6)


def chi_square_p_value_approx(chi2: float, df: int) -> float:
    """Approximate p-value using lookup tables for df=1..30.

    Returns:
        A conservative bracket: 0.001, 0.01, 0.05, or 1.0.
    """
    if df < 1:
        raise ValueError(f"degrees of freedom must be >= 1, got {df}")

    if df <= 30:
        if chi2 >= _CHI2_CRITICAL_001[df]:
            return 0.001
        if chi2 >= _CHI2_CRITICAL_01[df]:
            return 0.01
        if chi2 >= _CHI2_CRITICAL_005[df]:
            return 0.05
        return 1.0
    # Wilson-Hilferty approximation for df > 30
    z = (chi2 / df) ** (1 / 3) - (1 - 2 / (9 * df))
    z /= math.sqrt(2 / (9 * df))
    if z >= 3.291:
        return 0.001
    if z >= 2.576:
        return 0.01
    if z >= 1.960:
        return 0.05
    return 1.0


def odds_ratio(a: int, b: int, c: int, d: int) -> float:
    """Compute odds ratio from a 2x2 contingency table.

    Layout:
        |          | outcome+ | outcome- |
        | group A  |    a     |    b     |
        | group B  |    c     |    d     |

    Returns math.inf if denominator is zero.
    """
    denom = b * c
    if denom == 0:
        return math.inf
    return round((a * d) / denom, 6)


def autocorrelation_lag1(series: list[int]) -> float:
    """Compute lag-1 autocorrelation of a binary (0/1) series.

    Returns 0.0 if the series has fewer than 3 elements or zero variance.
    """
    n = len(series)
    if n < 3:
        return 0.0
    mean = sum(series) / n
    variance = sum((x - mean) ** 2 for x in series) / n
    if variance == 0:
        return 0.0
    covariance = sum((series[i] - mean) * (series[i + 1] - mean) for i in range(n - 1)) / n
    return round(covariance / variance, 6)


def z_score(value: float, mean: float, std: float) -> float:
    """Compute z-score. Returns 0.0 if std is zero."""
    if std == 0:
        return 0.0
    return round((value - mean) / std, 6)


def red_flag_power(n: int, p0: float, delta: float = 0.15, alpha: float = 0.05) -> float:
    """Compute statistical power for a one-sided z-test on proportions.

    Returns the probability of detecting an effect of size ``delta`` given
    sample size ``n`` and null proportion ``p0``.  Result is clamped to [0, 1].
    """
    if n <= 0 or p0 <= 0.0 or p0 >= 1.0:
        return 0.0
    p1 = min(p0 + delta, 0.999)
    se0 = math.sqrt(p0 * (1 - p0) / n)
    if se0 == 0:
        return 0.0
    z_alpha = 1.6448536269514729  # norm.ppf(1 - 0.05)
    threshold = p0 + z_alpha * se0
    se1 = math.sqrt(p1 * (1 - p1) / n)
    if se1 == 0:
        return 0.0
    z_power = (threshold - p1) / se1
    power = 0.5 * math.erfc(z_power / math.sqrt(2))
    return max(0.0, min(1.0, power))


def red_flag_confidence_label(power: float | None) -> str | None:
    """Classify power into a confidence label."""
    if power is None:
        return None
    if power >= 0.80:
        return "high"
    if power >= 0.50:
        return "moderate"
    return "low"
