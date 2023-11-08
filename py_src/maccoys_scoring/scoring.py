# std import
from pathlib import Path
from typing import List

# 3rd party imports
import numpy as np
import pandas as pd
import scipy  # type: ignore


def calculate_exp_score(psms: pd.DataFrame, base_score_col: str) -> np.ndarray:
    """
    Calculates the new score based on the assumption that
    the PSM-distribution is a exponential distribution.

    Parameters
    ----------
    psms : pandas.DataFrame
        DataFrame PSMs of the search engine

    Returns
    -------
    np.ndarray
        New PSM scores
    """
    if psms.empty:
        return np.full(len(psms[base_score_col]), np.NaN, dtype=float)
    org_scores = psms[base_score_col]
    location, scale = scipy.stats.expon.fit(org_scores)
    fitted_expo = scipy.stats.expon(location, scale)
    scores = [1 - fitted_expo.cdf(x) for x in org_scores]
    return np.array(scores)


def calculate_distance_score(
    psms: pd.DataFrame, base_score_col_name: str
) -> np.ndarray:
    """
    Calculates the distance between `base_score` of row n and n+1

    Parameters
    ----------
    psms : pandas.DataFrame
        DataFrame PSMs of the search engine
    base_score_col_name : str
        Name of the column containing the base scores for this distance score

    Returns
    -------
    np.ndarray
        Distance scores
    """
    if psms.empty:
        return np.full(len(psms[base_score_col_name]), np.NaN, dtype=float)
    base_score = psms[base_score_col_name]
    dist_score = np.zeros(len(base_score), dtype=float)
    last_score = float("-inf")
    for i in range(0, len(base_score) - 1):
        score = base_score[i + 1] - base_score[i]
        dist_score[i] = max(score, last_score)
        last_score = dist_score[i]
    dist_score[len(base_score) - 1] = np.NaN
    return np.abs(dist_score)
