"""
Wrapper for the PyNomaly library to calculate local outlier probability based in a Polars dataframe.
"""

import numpy as np
import polars as pl
from PyNomaly import loop # type: ignore[import]

def calculate_local_outlier_probability(
    psms: pl.DataFrame, features: list[str], n_neighbors: int, score_column_name: str
) -> pl.DataFrame:
    """
    Calculats the local outlier probability for the given features in the PSM dataframe.
s
    Parameters
    ----------
    psms : pl.DataFrame
        The PSM dataframe containing the features to calculate the local outlier probability for.
    features : list[str]
        Columns with features to consider for the local outlier probability calculation.
    n_neighbors : int
        The number of neighbors to use for the local outlier probability calculation.
    score_column_name : str
        The name of the column to store the local outlier probability in.

    Returns
    -------
    pl.DataFrame
        The PSM dataframe with the local outlier probability added as a new column.
    """
    # Fit the model and calculate the local outlier probability
    loop_score = loop.LocalOutlierProbability(
            psms[features].to_numpy(writable=False),
            n_neighbors=n_neighbors,
            use_numba=True,
        ).fit().local_outlier_probabilities
    # For some reason, the loop_score is of type object, so we need to convert it to float64
    loop_score = loop_score.astype(np.float64)
    psms = psms.with_columns(pl.Series(score_column_name, loop_score, dtype=pl.Float64))
    return psms
