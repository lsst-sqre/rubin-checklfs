import asyncio
import contextlib
import json
import logging
import tempfile
from pathlib import Path

import boto3
from google.cloud import storage

from .parser import parse
from .util import path


class Remediator:
    def __init__(
        self,
        input_directory: Path,
        input_glob: str,
        project: str,
        bucket: str,
        original_bucket: str,
        debug: bool,
    ) -> None:
        self._input_dir = input_directory
        self._input_glob = input_glob
        self._project = project
        self._orig_bucket = original_bucket
        self._debug = debug

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

        self._oids: dict[str, list[str]] = {}
        self._missing_oids: dict[str, set[str]] = {}
        self._missing_oids_by_repo: dict[str, set[str]] = {}

        self._bucket = storage.Bucket(
            client=storage.Client(), project=project, name=bucket
        )

    async def execute(self) -> None:
        """execute() is the only public method.  It loads OIDs from its
        input files, and checks to see if the corresponding objects
        exist."""
        await self._load_oids()
        await self._check_oids()
        if not self._missing_oids:
            return
        await self._remediate()

    async def _load_oids(self) -> None:
        inp_files = self._input_dir.glob(self._input_glob)
        for i_f in inp_files:
            with open(i_f, "r") as inp:
                obj = json.load(inp)
            self._oids.update(obj)

    async def _check_oids(self) -> None:
        for repo in self._oids:
            oids = self._oids[repo]
            self._logger.info(f"Checking {len(oids)} for repo {repo}")
            for oid in oids:
                blob = storage.Blob(name=f"{repo}/{oid}", bucket=self._bucket)
                self._logger.debug(
                    f"Checking bucket {self._bucket.name} for object "
                    f"{repo}/{oid}"
                )
                # We assume the runtime context is already authenticated
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

    async def _remediate(self) -> None:
        s3 = boto3.client("s3")
        for oid in self._missing_oids_by_repo:
            with tempfile.TemporaryDirectory() as tmpdirname:
                with contextlib.chdir(tmpdirname):
                    with open(oid, "wb") as fi:
                        self._logger.debug(
                            "Downloading content from AWS bucket "
                            f"{self._orig_bucket}/{oid}"
                        )
                        s3.download_fileobj(self._orig_bucket, oid, fi)
                        for repo in self._missing_oids_by_repo[oid]:
                            blob = storage.Blob(
                                name=f"{repo}/{oid}", bucket=self._bucket
                            )
                            self._logger.info(
                                "Uploading content to "
                                f"bucket {self._bucket.name}/{repo}/{oid}"
                            )
                            with open(oid, "rb") as fo:
                                blob.upload_from_file(fo)


def _get_remediator() -> Remediator:
    """Parse arguments and return the remediator object."""
    parser = parse("Remediate Git LFS")
    args = parser.parse_args()

    return Remediator(
        input_directory=path(args.input_directory).resolve(),
        input_glob=args.input_glob,
        project=args.project,
        bucket=args.bucket,
        original_bucket=args.original_bucket,
        debug=args.debug,
    )


def __main__() -> None:
    remediator = _get_remediator()
    asyncio.run(remediator.execute())
