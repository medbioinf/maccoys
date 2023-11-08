"""
MaCcoyS Scoring entrypoint for executing the module directly with `python -m maccoys_scoring`
"""

# std imports
import argparse
from pathlib import Path

# 3rd party imports
import pandas as pd

# internal import
from maccoys_scoring.io.comet_tsv import (
    read as read_comet_tsv,
    overwrite as overwrite_comet_tsv,
)
from maccoys_scoring.scoring import calculate_distance_score, calculate_exp_score
from maccoys_scoring.search_engine_type import SearchEngineType


def add_scoring_cli(subparser: argparse._SubParsersAction):
    parser = subparser.add_parser("scoring", help="Rescore PSMs")

    parser.add_argument(
        "psms_file",
        type=str,
        help="Path to PSM file",
    )
    parser.add_argument(
        "exp_score_base_col",
        type=str,
        help="Name of the column containing the base scores for the exponential score",
    )
    parser.add_argument(
        "exp_score_col",
        type=str,
        help="Name of the news column containing the exponential scores",
    )
    parser.add_argument(
        "dist_score_base_col",
        type=str,
        help="Name of the column containing the base scores for the distance score",
    )
    parser.add_argument(
        "dist_score_col",
        type=str,
        help="Name for the new column containing the distance scores",
    )

    def rescore_func(cli_args):
        search_engine_type = SearchEngineType.from_str(cli_args.search_engine_type)
        psm_file_path = Path(cli_args.psms_file).absolute()

        psms = pd.DataFrame()
        match search_engine_type:
            case SearchEngineType.COMET:
                psms = read_comet_tsv(
                    psm_file_path,
                )

        psms[cli_args.exp_score_col] = calculate_exp_score(
            psms, cli_args.exp_score_base_col
        )

        psms[cli_args.dist_score_col] = calculate_distance_score(
            psms, cli_args.dist_score_base_col
        )

        match search_engine_type:
            case SearchEngineType.COMET:
                overwrite_comet_tsv(
                    psm_file_path,
                    psms,
                )

    parser.set_defaults(func=rescore_func)


def main():
    """
    Main entrypoint for the CLI
    """
    cli = argparse.ArgumentParser(
        prog="MaCcoyS Scoring",
        description="Rescores PSMs based on the assumption that the PSM-distribution is a exponential distribution.",
    )

    cli.add_argument(
        "search_engine_type",
        type=str,
        choices=SearchEngineType.get_all_names(),
        help="Search engine used to generate the PSM file",
    )

    subparser = cli.add_subparsers()
    add_scoring_cli(subparser)
    # TODO: add CLI for annotation here

    # Call function for CLI args
    cli_args = cli.parse_args()
    cli_args.func(cli_args)


if __name__ == "__main__":
    main()
