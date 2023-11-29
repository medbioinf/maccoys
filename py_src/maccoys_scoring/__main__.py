"""
MaCcoyS Scoring entrypoint for executing the module directly with `python -m maccoys_scoring`
"""

# std imports
import argparse
import logging
from pathlib import Path
import asyncio

# internal import
from maccoys_scoring.scoring import rescore_psm_file
from maccoys_scoring.annotate import process_file


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

    async def rescore_func(cli_args):
        logging.info("Rescoring")
        if len(cli_args.sep) > 1:
            print("Separator must be a single character")
            return 1
        await rescore_psm_file(
            Path(cli_args.psms_file).absolute(),
            cli_args.sep,
            cli_args.header_row,
            cli_args.exp_score_base_col,
            cli_args.exp_score_col,
            cli_args.dist_score_base_col,
            cli_args.dist_score_col,
        )

    parser.set_defaults(func=rescore_func)


def add_annotation_cli(subparser: argparse._SubParsersAction):
    parser = subparser.add_parser(
        "annotate", help="Annotate matches with domain information"
    )

    parser.add_argument(
        "tsv_file",
        type=str,
        help="Path to tsv file",
    )
    parser.add_argument(
        "api_url",
        type=str,
        help="URL of MaCPepDB API",
    )

    async def annotate_func(cli_args):
        logging.info("Processing")
        await process_file(cli_args.tsv_file, cli_args.api_url)

    parser.set_defaults(func=annotate_func)


def configure_logging():
    log_format = "%(asctime)s [%(levelname)s] %(module)s.%(funcName)s(): %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_format)


def main():
    """
    Main entrypoint for the CLI
    """
    configure_logging()

    cli = argparse.ArgumentParser(
        prog="MaCcoyS Scoring",
        description="Rescores PSMs based on the assumption that the PSM-distribution is a exponential distribution.",
    )

    subparser = cli.add_subparsers()
    add_scoring_cli(subparser)
    add_annotation_cli(subparser)

    # Call function for CLI args
    cli_args = cli.parse_args()
    loop = asyncio.get_event_loop()
    tasks = [
        loop.create_task(cli_args.func(cli_args)),
    ]
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()


if __name__ == "__main__":
    main()
