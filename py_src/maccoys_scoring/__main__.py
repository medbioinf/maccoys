"""
MaCcoyS Scoring entrypoint for executing the module directly with `python -m maccoys_scoring`
"""

# std imports
import argparse
from pathlib import Path

# internal import
from maccoys_scoring.scoring import rescore_psm_file


def add_scoring_cli(subparser: argparse._SubParsersAction):
    parser = subparser.add_parser("scoring", help="Rescore PSMs")

    parser.add_argument(
        "psms_file",
        type=str,
        help="Path to PSM file",
    )
    parser.add_argument(
        "sep",
        type=str,
        help="Separator of the PSM file, e.g. `$'\t'` (in bash) for tab-separated. Be aware, that your shell might interpret the separator, so you might need to escape it as shown",
    )
    parser.add_argument(
        "header_row",
        type=int,
        help="Zero-based index of the header row, e.g. Comet's PSM files has a comment/revision in first line, so header_row=1",
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
        if len(cli_args.sep) > 1:
            print("Separator must be a single character")
            return 1
        rescore_psm_file(
            Path(cli_args.psms_file).absolute(),
            cli_args.sep,
            cli_args.header_row,
            cli_args.exp_score_base_col,
            cli_args.exp_score_col,
            cli_args.dist_score_base_col,
            cli_args.dist_score_col,
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

    subparser = cli.add_subparsers()
    add_scoring_cli(subparser)
    # TODO: add CLI for annotation here

    # Call function for CLI args
    cli_args = cli.parse_args()
    cli_args.func(cli_args)


if __name__ == "__main__":
    main()
