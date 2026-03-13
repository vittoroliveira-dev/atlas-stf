"""Tests for core statistical functions."""

import math

import pytest

from atlas_stf.core.stats import (
    autocorrelation_lag1,
    beta_binomial_posterior_mean,
    chi_square_p_value_approx,
    chi_square_statistic,
    odds_ratio,
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
