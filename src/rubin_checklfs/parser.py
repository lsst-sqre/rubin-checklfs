"""Parser for checker"""
import argparse
import os

from .util import str_bool

ENV_PREFIX = "LFSCHECKER_"


def parse(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "-m",
        "--map-directory",
        default=os.environ.get(ENV_PREFIX + "MAP_DIRECTORY", "."),
        help=(
            "Directory containing OID JSON files [env: "
            + ENV_PREFIX
            + "MAP_DIRECTORY, '.']"
        ),
    )
    parser.add_argument(
        "-x",
        "--dry-run",
        action="store_true",
        default=str_bool(os.environ.get(ENV_PREFIX + "DRY_RUN", "")),
        help="enable debugging [env: " + ENV_PREFIX + "DRY_RUN, False]",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        default=str_bool(os.environ.get(ENV_PREFIX + "DEBUG", "")),
        help="enable debugging [env: " + ENV_PREFIX + "DEBUG, False]",
    )
    parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        default=str_bool(os.environ.get(ENV_PREFIX + "DEBUG", "")),
        help="enable debugging [env: " + ENV_PREFIX + "DEBUG, False]",
    )
    return parser


def add_input_parms(
    parser: argparse.ArgumentParser,
) -> argparse.ArgumentParser:
    parser.add_argument(
        "--branch-pattern",
        default=os.environ.get(ENV_PREFIX + "BRANCH_PATTERN", r"v\d.*"),
        help=(
            "branch pattern to match for copy [env: "
            + ENV_PREFIX
            + 'LFSMIGRATOR_BRANCH_PATTERN, "r"v\\d.*""]'
        ),
    )
    parser.add_argument(
        "--full-map",
        action="store_true",
        default=str_bool(os.environ.get(ENV_PREFIX + "FULL_MAP", "")),
        help=(
            "generate full OID map for repo [env: "
            + ENV_PREFIX
            + "FULL_MAP, False]"
        ),
    )
    return parser


def add_bucket_parms(
    parser: argparse.ArgumentParser,
) -> argparse.ArgumentParser:
    parser.add_argument(
        "-p",
        "--project",
        default=os.environ.get(
            ENV_PREFIX + "PROJECT", "data-curation-prod-fbdb"
        ),
        help=(
            "GCP Project [ env: "
            + ENV_PREFIX
            + "PROJECT, 'data-curation-prod-fbdb']"
        ),
    )
    parser.add_argument(
        "-b",
        "--bucket",
        default=os.environ.get(
            ENV_PREFIX + "BUCKET", "rubin-us-central1-git-lfs"
        ),
        help=(
            "GCP Bucket [ env: "
            + ENV_PREFIX
            + "BUCKET, 'rubin-us-central1-git-lfs']"
        ),
    )
    parser.add_argument(
        "-o",
        "--original_bucket",
        default=os.environ.get(
            ENV_PREFIX + "ORIGINAL_BUCKET", "git-lfs.lsst.codes-us-west-2"
        ),
        help=(
            "GCP Bucket [ env: "
            + ENV_PREFIX
            + "BUCKET, 'git-lfs.lsst.codes-us-west-2']"
        ),
    )
    parser.add_argument(
        "-g",
        "--input-glob",
        default=os.environ.get(ENV_PREFIX + "INPUT_GLOB", "oids--*.json"),
        help=(
            "Glob matching OID input JSON filenames [env: "
            + ENV_PREFIX
            + "INPUT_GLOB, 'oids--*.json']"
        ),
    )
    return parser


def add_remediation_parms(
    parser: argparse.ArgumentParser,
) -> argparse.ArgumentParser:
    parser.add_argument(
        "--stop-after-check",
        action="store_true",
        default=str_bool(os.environ.get(ENV_PREFIX + "STOP_AFTER_CHECK", "")),
        help="enable debugging [env: "
        + ENV_PREFIX
        + "STOP_AFTER_CHECK, False]",
    )
    parser.add_argument(
        "--remediation-output-file",
        default=os.environ.get(ENV_PREFIX + "REMEDIATION_OUTPUT_FILE", ""),
        help=(
            "Remediation output file (optional) [env: "
            + ENV_PREFIX
            + "REMEDIATION_OUTPUT_FILE, '']"
        ),
    )
    return parser
