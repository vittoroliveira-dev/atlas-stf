"""Tests for core statistical functions."""

import math

import pytest

from atlas_stf.core.stats import (
    autocorrelation_lag1,
    beta_binomial_posterior_mean,
    chi_square_p_value_approx,
    chi_square_statistic,
    odds_ratio,
    red_flag_confidence_label,
    red_flag_power,
    z_score,
)


class TestChiSquareStatistic:
    def test_basic_case(self):
        observed = [10.0, 20.0, 30.0]
        expected = [20.0, 20.0, 20.0]
        result = chi_square_statistic(observed, expected)
        assert result == pytest.approx(10.0, abs=1e-4)

    def test_perfect_fit(self):
        observed = [20.0, 20.0, 20.0]
        expected = [20.0, 20.0, 20.0]
        assert chi_square_statistic(observed, expected) == 0.0

    def test_single_element(self):
        assert chi_square_statistic([15.0], [10.0]) == pytest.approx(2.5, abs=1e-4)

    @pytest.mark.parametrize(
        "observed,expected",
        [
            ([10.0, 20.0], [15.0, 15.0]),
            ([50.0, 50.0], [40.0, 60.0]),
        ],
    )
    def test_parametrized(self, observed, expected):
        result = chi_square_statistic(observed, expected)
        assert result >= 0.0

    def test_length_mismatch_raises(self):
        with pytest.raises(ValueError, match="same length"):
            chi_square_statistic([1.0, 2.0], [1.0])

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="not be empty"):
            chi_square_statistic([], [])

    def test_zero_expected_raises(self):
        with pytest.raises(ValueError, match="must be > 0"):
            chi_square_statistic([5.0], [0.0])


class TestChiSquarePValue:
    def test_significant_result(self):
        # chi2=20 with df=3 should be significant
        p = chi_square_p_value_approx(20.0, 3)
        assert p <= 0.01

    def test_non_significant(self):
        p = chi_square_p_value_approx(1.0, 3)
        assert p == 1.0

    def test_highly_significant(self):
        p = chi_square_p_value_approx(50.0, 5)
        assert p <= 0.001

    def test_df_1(self):
        # 3.841 is the critical value for p=0.05, df=1
        p_below = chi_square_p_value_approx(3.0, 1)
        assert p_below == 1.0
        p_above = chi_square_p_value_approx(4.0, 1)
        assert p_above == 0.05

    def test_df_1_returns_intermediate_001_bracket(self):
        # 6.635 is the critical value for p=0.01, df=1
        p = chi_square_p_value_approx(7.0, 1)
        assert p == 0.01

    def test_df_1_still_returns_0001_for_more_extreme_values(self):
        # 10.828 is the critical value for p=0.001, df=1
        p = chi_square_p_value_approx(11.0, 1)
        assert p == 0.001

    def test_df_greater_than_30(self):
        p = chi_square_p_value_approx(80.0, 40)
        assert p <= 0.01

    def test_invalid_df(self):
        with pytest.raises(ValueError, match="degrees of freedom"):
            chi_square_p_value_approx(5.0, 0)

    @pytest.mark.parametrize("df", [1, 5, 10, 20, 30])
    def test_returns_valid_bracket(self, df):
        for chi2 in [0.5, 5.0, 15.0, 30.0, 50.0]:
            p = chi_square_p_value_approx(chi2, df)
            assert p in (0.001, 0.01, 0.05, 1.0)


class TestOddsRatio:
    def test_basic(self):
        # 2x2 table: OR = (a*d)/(b*c) = (10*40)/(20*5) = 400/100 = 4.0
        assert odds_ratio(10, 20, 5, 40) == 4.0

    def test_unity(self):
        assert odds_ratio(10, 10, 10, 10) == 1.0

    def test_zero_denominator(self):
        assert odds_ratio(10, 0, 5, 40) == math.inf
        assert odds_ratio(10, 20, 0, 40) == math.inf

    def test_symmetric(self):
        # OR(a,b,c,d) = 1/OR(c,d,a,b) when finite
        or1 = odds_ratio(10, 20, 5, 40)
        or2 = odds_ratio(5, 40, 10, 20)
        assert or1 * or2 == pytest.approx(1.0, abs=1e-4)


class TestAutocorrelationLag1:
    def test_alternating(self):
        series = [0, 1, 0, 1, 0, 1, 0, 1, 0, 1]
        ac = autocorrelation_lag1(series)
        assert ac < 0  # negative autocorrelation

    def test_constant(self):
        assert autocorrelation_lag1([1, 1, 1, 1, 1]) == 0.0

    def test_too_short(self):
        assert autocorrelation_lag1([1, 0]) == 0.0
        assert autocorrelation_lag1([1]) == 0.0
        assert autocorrelation_lag1([]) == 0.0

    def test_clustered(self):
        series = [0, 0, 0, 0, 0, 1, 1, 1, 1, 1]
        ac = autocorrelation_lag1(series)
        assert ac > 0  # positive autocorrelation

    def test_random_like(self):
        series = [1, 0, 1, 1, 0, 0, 1, 0, 1, 0]
        ac = autocorrelation_lag1(series)
        assert -1.0 <= ac <= 1.0


class TestZScore:
    def test_basic(self):
        assert z_score(15.0, 10.0, 2.5) == 2.0

    def test_zero_std(self):
        assert z_score(15.0, 10.0, 0.0) == 0.0

    def test_negative(self):
        assert z_score(5.0, 10.0, 2.5) == -2.0

    def test_at_mean(self):
        assert z_score(10.0, 10.0, 5.0) == 0.0


class TestBetaBinomialPosteriorMean:
    def test_uses_laplace_smoothing(self):
        assert beta_binomial_posterior_mean(successes=1, trials=3) == pytest.approx(0.4, abs=1e-6)

    def test_returns_prior_mean_when_no_trials(self):
        assert beta_binomial_posterior_mean(successes=0, trials=0) == 0.5

    def test_rejects_invalid_counts(self):
        with pytest.raises(ValueError, match="successes must be between 0 and trials"):
            beta_binomial_posterior_mean(successes=3, trials=2)


class TestRedFlagPower:
    def test_large_sample_high_power(self):
        p = red_flag_power(100, 0.5)
        assert p >= 0.80

    def test_small_sample_low_power(self):
        p = red_flag_power(3, 0.5)
        assert p < 0.30

    def test_zero_n(self):
        assert red_flag_power(0, 0.5) == 0.0

    def test_negative_n(self):
        assert red_flag_power(-1, 0.5) == 0.0

    def test_p0_zero(self):
        assert red_flag_power(50, 0.0) == 0.0

    def test_p0_one(self):
        assert red_flag_power(50, 1.0) == 0.0

    def test_p0_near_one(self):
        p = red_flag_power(50, 0.9)
        assert 0.0 <= p <= 1.0

    def test_moderate_sample(self):
        p = red_flag_power(30, 0.5)
        assert 0.30 <= p <= 0.90

    @pytest.mark.parametrize("n,p0", [(1, 0.5), (5, 0.3), (50, 0.7), (200, 0.5)])
    def test_returns_bounded(self, n, p0):
        p = red_flag_power(n, p0)
        assert 0.0 <= p <= 1.0

    def test_custom_delta(self):
        p_small = red_flag_power(30, 0.5, delta=0.05)
        p_large = red_flag_power(30, 0.5, delta=0.30)
        assert p_small < p_large

    def test_monotonic_in_n(self):
        p10 = red_flag_power(10, 0.5)
        p50 = red_flag_power(50, 0.5)
        p200 = red_flag_power(200, 0.5)
        assert p10 <= p50 <= p200


class TestRedFlagConfidenceLabel:
    def test_high(self):
        assert red_flag_confidence_label(0.85) == "high"

    def test_moderate(self):
        assert red_flag_confidence_label(0.60) == "moderate"

    def test_low(self):
        assert red_flag_confidence_label(0.30) == "low"

    def test_boundary_high(self):
        assert red_flag_confidence_label(0.80) == "high"

    def test_boundary_moderate(self):
        assert red_flag_confidence_label(0.50) == "moderate"

    def test_none(self):
        assert red_flag_confidence_label(None) is None
