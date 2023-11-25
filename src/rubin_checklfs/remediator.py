"""This does the checking to see whether all the files in Git LFS that
are present in the original bucket are present in the output bucket.
It relies on the OidMapper class to have constructed an input document
showing how oids map to repositories.  While it would be possible to
construct this in memory and pass it around between classes, that
process is slow and it's generally much better to be able to
checkpoint and resume from data stored on disk if necessary.

Note that the s3 "paths" differ: the source is "data/<oid>" and the
target is "<owner>/<repo>/<oid>", so we can't just do a naive bucket
copy.

This assumes two very important things, and will not work if either of these
assumptions are violated:

1) The process context is authenticated to AWS so that it can inspect the
   contents of the original s3 bucket.
2) The process context is authenticated to GCP so that it can both inspect
   the contents of the target s3 bucket and upload to it if needed.
"""
import asyncio
import contextlib
import json
import logging
import os
import tempfile
from pathlib import Path

import boto3
from google.cloud import storage

from .parser import ENV_PREFIX, add_bucket_parms, add_remediation_parms, parse
from .util import path


class Remediator:
    def __init__(
        self,
        map_directory: Path,
        input_glob: str,
        project: str,
        bucket: str,
        original_bucket: str,
        stop_after_check: bool,
        remediation_input_file: str,
        remediation_output_file: str,
        dry_run: bool,
        debug: bool,
        quiet: bool,
        logger: logging.Logger | None,
    ) -> None:
        self._map_dir = map_directory.resolve()
        self._input_glob = input_glob
        self._project = project
        self._orig_bucket = original_bucket
        self._stop_after_check = stop_after_check
        self._dry_run = dry_run
        self._debug = debug
        self._quiet = quiet

        self._remediation_input_file: Path | None = None
        if remediation_input_file:
            self._remediation_input_file = path(
                remediation_input_file
            ).resolve()
        self._remediation_output_file: Path | None = None
        if remediation_output_file:
            self._remediation_output_file = path(
                remediation_output_file
            ).resolve()

        if logger is not None:
            self._logger = logger
        else:
            self._logger = logging.getLogger(__name__)
            ch = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            ch.setFormatter(formatter)
            if ch not in self._logger.handlers:
                self._logger.addHandler(ch)
        self._logger.setLevel("INFO")
        if self._debug:
            self._logger.setLevel("DEBUG")
            self._logger.debug("Debugging enabled for Migrator")
        if self._quiet:
            if self._debug:
                self._logger.debug(
                    "'debug' and 'quiet' do not make sense together. "
                    "'debug' wins."
                )
            else:
                self._logger.setLevel("CRITICAL")

        self._oids: dict[str, list[str]] = {}
        self._missing_oids: dict[str, set[str]] = {}
        self._missing_oids_by_repo: dict[str, set[str]] = {}

        self._bucket = storage.Bucket(
            client=storage.Client(project=project), name=bucket
        )

    async def execute(self) -> None:
        """execute() is the only public method.  It loads OIDs from its
        input files, and checks to see if the corresponding objects
        exist."""
        if self._remediation_input_file is not None:
            self._logger.info(
                f"Loading missing oids from file "
                f"'{str(self._remediation_input_file)}'"
            )
            await self._load_input_remediation_file()
        else:
            await self._load_oids()
            await self._check_oids()
        if not self._missing_oids:
            return
        if self._remediation_output_file is not None:
            await self._write_remediation_file()
        if self._stop_after_check:
            return
        await self._remediate()

    async def _load_oids(self) -> None:
        inp_files = self._map_dir.glob(self._input_glob)
        for i_f in inp_files:
            with open(i_f, "r") as inp:
                obj = json.load(inp)
            self._oids.update(obj)

    async def _check_oids(self) -> None:
        for repo in self._oids:
            oids = self._oids[repo]
            self._logger.info(f"Checking {len(oids)} objects for repo {repo}")
            for oid in oids:
                blob = storage.Blob(name=f"{repo}/{oid}", bucket=self._bucket)
                self._logger.debug(
                    f"Checking bucket {self._bucket.name} for object "
                    f"{repo}/{oid}"
                )
                if not blob.exists():
                    if repo not in self._missing_oids:
                        self._missing_oids[repo] = set()
                    if oid not in self._missing_oids_by_repo:
                        self._missing_oids_by_repo[oid] = set()
                    self._missing_oids[repo].add(oid)
                    self._missing_oids_by_repo[oid].add(repo)
                    self._logger.warning(
                        f"Bucket {self._bucket.name} is missing "
                        f"object {repo}/{oid}"
                    )

    async def _load_input_remediation_file(self) -> None:
        pass

    async def _write_remediation_file(self) -> None:
        pass

    async def _remediate(self) -> None:
        s3 = boto3.client("s3")
        for oid in self._missing_oids_by_repo:
            with tempfile.TemporaryDirectory() as tmpdirname:
                with contextlib.chdir(tmpdirname):
                    self._logger.debug(
                        "Downloading content from AWS bucket "
                        f"{self._orig_bucket}/{oid}"
                    )
                    s3.download_file(self._orig_bucket, f"data/{oid}", oid)
                    for repo in self._missing_oids_by_repo[oid]:
                        blob = storage.Blob(
                            name=f"{repo}/{oid}", bucket=self._bucket
                        )
                        self._logger.info(
                            "Uploading content to "
                            f"bucket {self._bucket.name}/{repo}/{oid}"
                        )
                        blob.upload_from_filename(oid)


def _get_remediator() -> Remediator:
    """Parse arguments and return the remediator object."""
    parser = parse("Remediate Git LFS")
    add_bucket_parms(parser)
    add_remediation_parms(parser)
    parser.add_argument(
        "--remediation-input-file",
        default=os.environ.get(ENV_PREFIX + "REMEDIATION_INPUT_FILE", ""),
        help=(
            "Remediation output file (optional) [env: "
            + ENV_PREFIX
            + "REMEDIATION_INPUT_FILE, '']"
        ),
    )
    args = parser.parse_args()
    return Remediator(
        map_directory=path(args.map_directory).resolve(),
        input_glob=args.input_glob,
        project=args.project,
        bucket=args.bucket,
        original_bucket=args.original_bucket,
        remediation_input_file=args.remediation_input_file,
        remediation_output_file=args.remediation_output_file,
        stop_after_check=args.stop_after_check,
        logger=None,
        dry_run=args.dry_run,
        quiet=args.quiet,
        debug=args.debug,
    )


def main() -> None:
    remediator = _get_remediator()
    asyncio.run(remediator.execute())


if __name__ == "__main__":
    main()
