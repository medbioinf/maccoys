"""Functions to handle Comet output TSV files.
"""

# import std
from pathlib import Path
from typing import List

# 3rd party import
import pandas as pd

HEADER_ROW: int = 1
SEP: str = "\t"


def read(comet_tsv_path: Path) -> pd.DataFrame:
    return pd.read_csv(
        comet_tsv_path,
        sep=SEP,
        header=HEADER_ROW,
    )


def overwrite(comet_tsv_path: Path, psms: pd.DataFrame):
    content_before_header: List[str] = []
    with comet_tsv_path.open("r") as psm_file:
        # Write comment/revision lines
        for _ in range(HEADER_ROW):
            content_before_header.append(next(psm_file))

    with comet_tsv_path.open("w") as psm_file:
        # Write comment/revision lines
        for line in content_before_header:
            psm_file.write(line)
        # Write the rest
        psms.to_csv(
            psm_file,
            sep=SEP,
            index=False,
        )
