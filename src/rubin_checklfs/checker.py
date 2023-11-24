import asyncio
import json
from pathlib import Path

from google.cloud import storage

from .parser import parse
from .util import path


class Checker:
    def __init__(
        self,
        input_directory: Path,
        input_glob: str,
        project: str,
        bucket: str,
        debug: bool,
    ) -> None:
        self._input_dir = input_directory
        self._input_glob = input_glob
        self._project = project
        self._bucket = bucket
        self._debug = debug

        self._oids: dict[str, list[str]] = {}
        self._missing_oids: dict[str, set[str]] = {}

    async def execute(self) -> None:
        """execute() is the only public method.  It loads OIDs from its
        input files, and checks to see if the corresponding objects
        exist."""
        await self._load_oids()
        await self._check_oids()
        await self._report()

    async def _load_oids(self) -> None:
        inp_files = self._input_dir.glob(self._input_glob)
        for i_f in inp_files:
            with open(i_f, "r") as inp:
                obj = json.load(inp)
            self._oids.update(obj)

    async def _check_oids(self) -> None:
        for repo in self._oids:
            oids = self._oids[repo]
            for oid in oids:
                blob = storage.Blob(name=f"{repo}/{oid}", bucket=self._bucket)
                self._logger.debug(
                    f"Checking bucket {self._bucket} for oid {oid}"
                )
                # We assume the runtime context is already authenticated
                if not blob.exists():
                    if repo not in self._missing_oids:
                        self._missing_oids[repo]: set[str] = set()
                    self._missing_oids[repo].add(oid)
                    self._logger.warning(
                        f"Bucket {self._bucket} is missing oid {oid}"
                    )

    async def _report(self) -> None:
        if not self._missing_oids:
            return
        print(json.dumps(self._missing_oids, sort_keys=True, indent=2))


def _get_checker() -> Checker:
    """Parse arguments and return the checker object."""
    parser = parse()
    args = parser.parse_args()
    return Checker(
        input_directory=path(args.input_directory).resolve(),
        input_glob=args.input_glob,
        project=args.project,
        bucket=args.bucket,
        debug=args.debug,
    )


def __main__() -> None:
    checker = _get_checker()
    asyncio.run(checker.execute())
