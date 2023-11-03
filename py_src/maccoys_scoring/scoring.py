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


def rescore_psm_file(
    psms_file_path: Path,
    sep: str,
    header_row: int,
    exp_score_base_col: str,
    exp_score_col: str,
    dist_score_base_col: str,
    dist_score_col: str,
):
    """
    Adds the exponential and distance score to the given PSM file

    Parameters
    ----------
    psms_file_path : Path
        Path to the PSM file
    sep : str
        Separator of the PSM file, e.g. "\t" for tab-separated
    header_row : int
        Number of header rows, e.g. Comet's PSM files
        has a comment/revision in first line, so header_row=1
    exp_score_base_col : str
        Name of the column containing the base scores for the exponential score
    exp_score_col : str
        Name of the news column containing the exponential scores
    dist_score_base_col : str
        Name of the column containing the base scores for the distance score
    dist_score_col : str
        Name for the new column containing the distance scores
    """
    psms = pd.read_csv(psms_file_path, sep=sep, header=header_row)
    psms[exp_score_col] = calculate_exp_score(psms, exp_score_base_col)
    psms[dist_score_col] = calculate_distance_score(psms, dist_score_base_col)

    content_before_header: List[str] = []
    if header_row > 0:
        with psms_file_path.open("r") as psm_file:
            # Write comment/revision lines
            for _ in range(header_row):
                content_before_header.append(next(psm_file))

    with psms_file_path.open("w") as psm_file:
        # Write comment/revision lines
        for line in content_before_header:
            psm_file.write(line)
        # Write the rest
        psms.to_csv(
            psm_file,
            sep=sep,
            index=False,
        )
