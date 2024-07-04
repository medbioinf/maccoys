# std imports
from typing import Callable, Dict, List, Tuple

# 3rd party imports
import numpy as np
import pandas as pd
import scipy
from statsmodels.stats.diagnostic import lilliefors as statsmodels_lilliefors


SIGNIFICANCE_LEVEL_ALPHA: float = 0.05
"""Significance level for Anderson-Darling test
"""


def anderson_darling(x: List[float], dist: str) -> Tuple[float, float]:
    """Anderson-Darling test for goodness of fit.

    Args:
        x (List[float]): Array of values to be tested
        cdf_fn (Callable[[Any], Any]): CDF function to be tested against

    Returns:
        Tuple[float, float]: Test statistic and p-value
    """
    test_res = scipy.stats.anderson(x, dist=dist)
    pvalue: float = -1.0  # not an actual p-value, 0.0 means reject H0
    # 1. Choose alpha (0.05)
    # 2. Get index (i) of sig level >= alpha
    # 3. Check if crit val[i] > test stat == not significant (fail to reject H0)
    matching_sig_level_indexes: np.ndarray = np.where(
        np.array(test_res.significance_level) <= SIGNIFICANCE_LEVEL_ALPHA * 100
    )[0]
    if matching_sig_level_indexes[0].size > 0:
        crit_value_idx = (
            matching_sig_level_indexes.min()
        )  # get largest significance level, they are order descending
        if test_res.critical_values[crit_value_idx] > test_res.statistic:
            pvalue = 1.0
        else:
            pvalue = 0.0
    return (test_res.statistic, pvalue)


def kolmogorov_smirnov(x: List[float], dist: str) -> Tuple[float, float]:
    """Kolmogorov-Smirnov test for goodness of fit.

    Args:
        x (List[float]): Array of values to be tested
        cdf_fn (Callable[[Any], Any]): CDF function to be tested against

    Returns:
        Tuple[float, float]: Test statistic and p-value
    """
    dist_mod = DISTRIBUTIONS[dist]
    fitting_args = dist_mod.fit(x)
    test_res = scipy.stats.kstest(x, dist_mod.cdf, args=fitting_args)
    return (test_res.statistic, test_res.pvalue)


def cramer_von_mises(x: List[float], dist: str) -> Tuple[float, float]:
    """Cramer-von Mises test for goodness of fit.

    Args:
        x (List[float]): Array of values to be tested
        cdf_fn (Callable[[Any], Any]): CDF function to be tested against

    Returns:
        Tuple[float, float]: Test statistic and p-value
    """
    if len(x) < 2:
        return (np.nan, np.nan)
    dist_mod = DISTRIBUTIONS[dist]
    fitting_args = dist_mod.fit(x)
    test_res = scipy.stats.cramervonmises(x, dist_mod.cdf, args=fitting_args)
    return (test_res.statistic, test_res.pvalue)


def lilliefors(x: List[float], dist: str) -> Tuple[float, float]:
    """Lilliefors test for goodness of fit.

    Args:
        x (List[float]): Array of values to be tested
        cdf_fn (Callable[[Any], Any]): CDF function to be tested against

    Returns:
        Tuple[float, float]: Test statistic and p-value
    """
    if len(x) < 4 and dist == "norm" or len(x) < 3 and dist == "exp":
        return (np.nan, np.nan)
    return statsmodels_lilliefors(x, dist=dist)


DISTRIBUTIONS: Dict[str, scipy.stats.rv_continuous] = {
    "exp": scipy.stats.expon,
    "halfnorm": scipy.stats.halfnorm,
    "chi2": scipy.stats.chi2,
    "norm": scipy.stats.norm,
}
"""Distribution to be tested
"""


GOODNESS_OF_FIT_TESTS: Tuple[
    Tuple[str, List[str], Callable[[List[float], str], Tuple[float, float]]], ...
] = (
    ("Anderson-Darling", ["norm", "expon"], anderson_darling),
    ("Kolmogorov-Smirnov", list(DISTRIBUTIONS.keys()), kolmogorov_smirnov),
    ("Cramer-von Mises", list(DISTRIBUTIONS.keys()), cramer_von_mises),
    ("Lilliefors", ["norm", "exp"], lilliefors),
)
"""Name of goodness of fit test, list of supported distributions to test against, and test function
"""


def calc_goodnesses(scores: List[float]) -> List[Tuple[str, str, float, float]]:
    """
    Calculate goodness of fit for the PSM distirbution

    Parameters
    ----------
    score: List[float]
        PSMs scores

    Returns
    -------
    List[Tuple[str, str, float, float]]
        Test name, distribution name, test statistic, p-value
    """
    goodnesses = []

    if len(scores) == 0:
        for test_name, dist_names, _ in GOODNESS_OF_FIT_TESTS:
            for dist_name in dist_names:
                goodnesses.append((test_name, dist_name, np.nan, np.nan))
        return goodnesses

    for test_name, dist_names, test_fn in GOODNESS_OF_FIT_TESTS:
        for dist_name in dist_names:
            goddness = test_fn(scores, dist_name)
            goodnesses.append((test_name, dist_name, goddness[0], goddness[1]))

    return goodnesses

