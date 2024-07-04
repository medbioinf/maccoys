# std import
from typing import List

# 3rd party imports
import numpy as np
import scipy  # type: ignore


def calculate_exp_score(scores: List[float]) -> List[float]:
    """
    Calculates the new score based on the assumption that
    the PSM-distribution is an exponential distribution.

    Parameters
    ----------
    scores : List[float]
        PSM scores

    Returns
    -------
    List[float]
        Exponential scores
    """
    if len(scores) == 0:
        return [np.NaN for _ in range(len(scores))]
    location, scale = scipy.stats.expon.fit(scores)
    fitted_expo = scipy.stats.expon(location, scale)
    return [1 - fitted_expo.cdf(x) for x in scores]


def calculate_distance_score(scores: List[float]) -> List[float]:
    """
    Calculates the distance between `base_score` of row n and n+1

    Parameters
    ----------
    scores : List[float]
        PSM scores

    Returns
    -------
    List[float]
        Distance scores
    """
    if len(scores) == 0:
        return [np.NaN for _ in range(len(scores))]
    dist_score = np.zeros(len(scores), dtype=float)
    last_score = float("-inf")
    for i in range(0, len(scores) - 1):
        score = scores[i + 1] - scores[i]
        dist_score[i] = max(score, last_score)
        last_score = dist_score[i]
    dist_score[len(scores) - 1] = np.NaN
    return dist_score
