import asyncio
import contextlib
import logging
import os
import tempfile
from pathlib import Path
from urllib.parse import ParseResult

import git

from .oid_mapper import OidMapper
from .parser import (
    ENV_PREFIX,
    add_bucket_parms,
    add_input_parms,
    add_remediation_parms,
    parse,
)
from .remediator import Remediator
from .util import path, str_bool, url


class Looper:
    def __init__(
        self,
        input_file: Path,
        map_directory: Path,
        full_map: bool,
        branch_pattern: str,
        input_glob: str,
        project: str,
        bucket: str,
        original_bucket: str,
        remediation_output_file: str,
        logger: logging.Logger | None,
        stop_after_scan: bool,
        stop_after_check: bool,
        dry_run: bool,
        quiet: bool,
        debug: bool,
    ) -> None:
        self._repo_file = input_file
        self._map_directory = map_directory
        self._branch_pattern = branch_pattern
        self._full_map = full_map
        self._input_glob = input_glob
        self._project = project
        self._bucket = bucket
        self._orig_bucket = original_bucket
        self._remediation_output_file = remediation_output_file
        self._stop_after_scan = stop_after_scan
        self._stop_after_check = stop_after_check
        self._dry_run = dry_run
        self._debug = debug
        self._quiet = quiet

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

    async def execute(self) -> None:
        repo_urls = await self._read_repo_file()
        for repo_url in repo_urls:
            await self._map_repo(repo_url)
        if self._stop_after_scan:
            return
        await self._check_and_remediate_repos()

    async def _map_repo(self, repo_url: ParseResult) -> None:
        with tempfile.TemporaryDirectory() as tmpdirname:
            with contextlib.chdir(tmpdirname):
                await self._process_repo(repo_url)

    async def _process_repo(self, repo_url: ParseResult) -> None:
        path_parts = repo_url.path.split("/")
        owner = path_parts[-2]
        repo_name = path_parts[-1]
        target = Path(Path(owner) / repo_name)
        Path.mkdir(Path(owner))
        Path.mkdir(target)
        git.Repo.clone_from(repo_url.geturl(), target)
        oid_mapper = OidMapper(
            map_directory=self._map_directory,
            repo_directory=target,
            owner=owner,
            repository=repo_name,
            branch_pattern=self._branch_pattern,
            logger=self._logger,
            full_map=self._full_map,
            dry_run=self._dry_run,
            quiet=self._quiet,
            debug=self._debug,
        )
        await oid_mapper.execute()

    async def _check_and_remediate_repos(self) -> None:
        remediator = Remediator(
            map_directory=self._map_directory,
            input_glob=self._input_glob,
            project=self._project,
            bucket=self._bucket,
            stop_after_check=self._stop_after_check,
            original_bucket=self._orig_bucket,
            remediation_input_file="",
            remediation_output_file=self._remediation_output_file,
            dry_run=self._dry_run,
            debug=self._debug,
            quiet=self._quiet,
            logger=self._logger,
        )
        await remediator.execute()

    async def _read_repo_file(self) -> list[ParseResult]:
        repo_urls: list[ParseResult] = []
        with open(self._repo_file, "r") as fh:
            for ln in fh:
                # Look for comments and ignore anything after '#'
                m_p = ln.find("#")
                if m_p != -1:
                    ln = ln[:m_p]
                # Strip whitespace
                ln = ln.strip()
                # Strip '.git' from end if it's there
                if ln.endswith(".git"):
                    ln = ln[:-4]
                # Anything left?
                if not ln:
                    continue
                repo_url = url(ln)
                if repo_url.scheme != "https":
                    self._logger.warning(
                        "Repository URL scheme must be 'https', not "
                        + f"{repo_url.scheme}; skipping {repo_url}"
                    )
                    continue
                repo_urls.append(repo_url)
        return repo_urls


def _get_looper() -> Looper:
    parser = parse("Remediate list of LFS repo URLs")
    add_input_parms(parser)
    add_bucket_parms(parser)
    add_remediation_parms(parser)
    parser.add_argument(
        "-f",
        "--input-file",
        default=os.environ.get(ENV_PREFIX + "INPUT_FILE", "lfsrepos.txt"),
        help=(
            "Directory containing OID JSON files [env: "
            + ENV_PREFIX
            + "INPUT_FILE, 'lfsrepos.txt']"
        ),
    )
    parser.add_argument(
        "--stop-after-scan",
        action="store_true",
        default=str_bool(os.environ.get(ENV_PREFIX + "STOP_AFTER_SCAN", "")),
        help="enable debugging [env: "
        + ENV_PREFIX
        + "STOP_AFTER_SCAN, False]",
    )
    args = parser.parse_args()
    return Looper(
        input_file=path(args.input_file).resolve(),
        map_directory=path(args.map_directory).resolve(),
        full_map=args.full_map,
        branch_pattern=args.branch_pattern,
        input_glob=args.input_glob,
        project=args.project,
        bucket=args.bucket,
        original_bucket=args.original_bucket,
        remediation_output_file=args.remediation_output_file,
        stop_after_check=args.stop_after_check,
        stop_after_scan=args.stop_after_scan,
        logger=None,
        dry_run=args.dry_run,
        quiet=args.quiet,
        debug=args.debug,
    )


def main() -> None:
    looper = _get_looper()
    asyncio.run(looper.execute())


if __name__ == "__main__":
    main()
