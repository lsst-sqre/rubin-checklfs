"""Parser for checker"""
import argparse
import os

from .util import str_bool

PREFIX = "LFSCHECKER_"


def parse(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "-i",
        "--input-directory",
        default=os.environ.get(PREFIX + "INPUT_DIRECTORY", "."),
        help=(
            "Directory containing OID input JSON files [env: "
            + PREFIX
            + "INPUT_DIRECTORY, '.']"
        ),
    )
    parser.add_argument(
        "-g",
        "--input-glob",
        default=os.environ.get(PREFIX + "INPUT_GLOB", "."),
        help=(
            "Glob matching OID input JSON filenames [env: "
            + PREFIX
            + "INPUT_GLOB, 'oids--*.json']"
        ),
    )
    parser.add_argument(
        "-p",
        "--project",
        default=os.environ.get(PREFIX + "PROJECT", "data-curation-prod-fbdb"),
        help=(
            "GCP Project [ env: "
            + PREFIX
            + "PROJECT, 'data-curation-prod-fbdb']"
        ),
    )
    parser.add_argument(
        "-b",
        "--bucket",
        default=os.environ.get(PREFIX + "BUCKET", "rubin-us-central1-git-lfs"),
        help=(
            "GCP Bucket [ env: "
            + PREFIX
            + "BUCKET, 'rubin-us-central1-git-lfs']"
        ),
    )
    parser.add_argument(
        "-o",
        "--original_bucket",
        default=os.environ.get(
            PREFIX + "ORIGINAL_BUCKET", "git-lfs.lsst.codes-us-west-2"
        ),
        help=(
            "GCP Bucket [ env: "
            + PREFIX
            + "BUCKET, 'git-lfs.lsst.codes-us-west-2']"
        ),
    )
    parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        default=str_bool(os.environ.get(PREFIX + "DEBUG", "")),
        help="enable debugging [env: " + PREFIX + "DEBUG, False]",
    )
    return parser
