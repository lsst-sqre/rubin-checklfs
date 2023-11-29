"""This generates a JSON document showing which oids (representing Git
LFS file content) come from which repositories, for all tags and a
regular expression-matched set of branches for those repositories.

This makes two assumptions, and will not work if either is violated:

1) The git repositories must be readable by the process context.
2) It is vital that Git LFS is *not* installed in the process context,
   because operation relies on being able to inspect the LFS stub files
   rather than downloading the pointed-to content.

This process could be done via Git LFS operations and checking the
file contents (via sha256sum) for each checkout.  Since Rubin
observatory generates a tag per weekly release, and has done so for
several years, and since the LFS files are large and therefore take a
while to generate a checksum for, and are also numerous for some
repositories, that approach proved infeasible.

Generating the JSON document is not, strictly speaking, necessary: we
could just store that information in memory and pass it to the
remediator.  However, it is extremely helpful to be able to checkpoint
progress and restart.
"""

import asyncio
import contextlib
import json
import logging
import re
from pathlib import Path

from git import Repo

from .parser import add_input_parms, parse


class OidMapper:
    """This class relies on **not** having Git LFS installed: it walks
    through the stub files on each branch of a particular repository,
    extracts the OIDs, and constructs a map of which oids belong to which
    repository.  It then writes that map to an output path.
    """

    def __init__(
        self,
        repo_directory: Path,
        map_directory: Path,
        owner: str,
        repository: str,
        branch_pattern: str,
        logger: logging.Logger | None,
        full_map: bool,
        dry_run: bool,
        debug: bool,
        quiet: bool,
    ) -> None:
        self._dir = repo_directory.resolve()
        self._map_dir = map_directory.resolve()
        self._repo = Repo(self._dir)
        self._owner = owner
        self._repository = repository
        self._branch_pattern = branch_pattern
        self._full_map = full_map
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
        self._oids: dict[str, bool] = {}
        self._selected_branches: list[str] = []
        self._tags: list[str] = []
        self._checkout_lfs_files: dict[str, dict[str, str]] = {}

    async def execute(self) -> None:
        """execute() is the only public method.  It performs the git
        operations necessary to extract the LFS stub file contents.
        """
        with contextlib.chdir(self._dir):
            await self._select_branches()
            await self._select_tags()
            await self._loop()
            await self._write_map()

    async def _select_branches(self) -> None:
        origin = "origin/"
        l_o = len(origin)
        mpat = "^" + origin + self._branch_pattern
        self._selected_branches = [
            x.name[l_o:]
            for x in self._repo.remote().refs
            if (x.name == "origin/main")
            or (x.name == "origin/master")
            or re.match(mpat, x.name) is not None
        ]
        self._logger.debug(f"Selected branches: {self._selected_branches}")

    async def _select_tags(self) -> None:
        client = self._repo.git
        client.fetch("--tags")
        self._tags = [x for x in client.tag("-l").split("\n") if x]
        self._logger.debug(f"Tags: {self._tags}")

    async def _loop(self) -> None:
        checkouts = self._selected_branches.copy()
        if self._tags:
            checkouts.extend(self._tags)
        self._logger.debug(f"Checkouts to attempt: {checkouts}")
        if len(checkouts) > 0:
            self._logger.info(
                f"{len(checkouts)} checkouts to attempt for "
                f"{self._owner}/{self._repository}"
            )
        for co in checkouts:
            await self._loop_over_item(co)

    async def _locate_co_gitattributes(self) -> Path | None:
        ga = list(self._dir.glob("**/.gitattributes"))
        if not ga:
            return None
        if len(ga) > 1:
            raise RuntimeError(f"Multiple .gitattributes files found: {ga}")
        return ga[0]

    async def _get_co_lfs_file_list(self, git_attributes: Path) -> list[Path]:
        """Assemble the list of LFS-managed files by interpreting the
        .gitattributes file we found."""
        files: list[Path] = []
        with open(git_attributes, "r") as f:
            for line in f:
                fields = line.strip().split()
                if not await self._is_lfs_attribute(fields):
                    continue
                files.extend(
                    await self._find_co_lfs_files(git_attributes, fields[0])
                )
        fileset = set(files)
        self._logger.debug(f"Included files: {fileset}")
        excluded_files = await self._get_co_excluded_file_list(git_attributes)
        excset = set(excluded_files)
        self._logger.debug(f"Excluded files: {excset}")
        resolved_fileset = fileset - excset
        lfsfiles = list(resolved_fileset)
        if lfsfiles:
            self._logger.debug(
                f"LFS file list for {self._owner}/{self._repository}"
                f" -> {lfsfiles}"
            )
        return lfsfiles

    async def _get_co_excluded_file_list(
        self, git_attributes: Path
    ) -> list[Path]:
        """Assemble the list of LFS-managed files by interpreting the
        .gitattributes file we found."""
        files: list[Path] = []
        pdir = git_attributes.parent
        with open(git_attributes, "r") as f:
            for line in f:
                # There's probably something better than this, but....
                # it'll do for the Rubin case.
                if line.find("!filter !diff !merge") != -1:
                    match = "**/" + line.split()[0]
                    exf = list(pdir.glob(match))
                    files.extend(exf)
                    self._logger.debug(f"Excluded file {match} -> {exf}")
        return files

    async def _is_lfs_attribute(self, fields: list[str]) -> bool:
        """It's not clear that this is ever really formalized, but in
        each case I've seen, "filter", "diff", and "merge" are set to
        "lfs", and it's almost always not a binary file ("-text").  I
        think that's just what `git lfs track` does, but whether it's
        documented, I don't know.

        Apparently not quite, since we have one repo which uses "-crlf"
        (lsst-dm/phosim_psf_tests).
        """
        if not fields:
            return False
        notext = fields[-1]
        if notext != "-text" and notext != "-crlf":
            self._logger.debug(
                f"{' '.join(fields)} does not end with '-text' or '-crlf'"
            )
            return False
        mids = fields[1:-1]
        ok_flds = ("filter", "diff", "merge")
        for m in mids:
            if m.find("=") == -1:
                continue  # Definitely not right
            k, v = m.split("=")
            if k not in ok_flds:
                self._logger.debug(f"{k} not in {ok_flds}")
                return False
            if v != "lfs":
                self._logger.debug(f"{k} is '{v}', not 'lfs'")
                return False
        return True

    async def _find_co_lfs_files(
        self, git_attributes: Path, match: str
    ) -> list[Path]:
        """The .gitattributes file is defined at:
        https://git-scm.com/docs/gitattributes

        Those can be in arbitrary directories and only concern things
        at or below their own directory.

        In Rubin Git LFS repositories, there is only one
        .gitattributes file, but it may not be at the root of the
        repo.

        Our strategy is pretty simple: do "**/" prepended to the
        match, starting with the directory in which the .gitattributes
        file was found.
        """
        pdir = git_attributes.parent
        match = "**/" + match
        files = list(pdir.glob(match))
        self._logger.debug(f"{match} -> {[ str(x) for x in files]}")
        return files

    async def _loop_over_item(self, co: str) -> None:
        client = self._repo.git
        client.checkout(co)
        self._logger.debug(f"Checking out/fetching '{co}'")
        client.fetch()
        client.reset("--hard")
        git_attributes = await self._locate_co_gitattributes()
        if git_attributes is None:
            self._logger.debug(
                f"No .gitattributes file for checkout '{co}' "
                " -- nothing to check"
            )
            return
        lfs_files = await self._get_co_lfs_file_list(git_attributes)
        if not lfs_files:
            self._logger.debug(
                f"No LFS files managed in checkout '{co}' "
                " -- nothing to check"
            )
            return
        for path in lfs_files:
            fn = str(path)
            if co not in self._checkout_lfs_files:
                self._checkout_lfs_files[co] = {}
            self._checkout_lfs_files[co][fn] = ""
        await self._update_oids(co, lfs_files)

    async def _update_oids(self, checkout: str, files: list[Path]) -> None:
        for fn in files:
            if fn.is_symlink():
                # A symlink either points elsewhere into someplace inside the
                # repo, in which case we'll check it there, or it points
                # somewhere else entirely, in which case we can't check it.
                self._logger.debug(
                    f"Skipping symlink {str(fn)} -> {fn.resolve()}"
                )
                del self._checkout_lfs_files[checkout][str(fn)]
                continue
            with open(fn, "r") as f:
                try:
                    for ln in f:
                        line = ln.strip()
                        fields = line.split()
                        if not fields:
                            continue
                        if fields[0] != "oid":
                            continue
                        oid = fields[1]
                        self._checkout_lfs_files[checkout][str(fn)] = oid
                        self._oids[oid] = True
                        self._logger.debug(
                            f"oid '{oid}' @ [{checkout}] -> {str(fn)}"
                        )
                        break
                except UnicodeDecodeError:
                    self._logger.warning(
                        f"Failed to decode {str(fn)} as text; skipping "
                        "(probably stored directly, not in LFS)"
                    )

    async def _write_map(self) -> None:
        filename = Path(f"oids--{self._owner}--{self._repository}.json")
        out = {
            f"{self._owner}/{self._repository}": [
                x.split(":")[1] for x in self._oids.keys()
            ]
        }
        with open(self._map_dir / filename, "w") as f:
            json.dump(out, f, sort_keys=True, indent=2)
        if not self._full_map:
            return
        filename = Path(f"fullmap--{self._owner}--{self._repository}.json")
        out2 = {f"{self._owner}/{self._repository}": self._checkout_lfs_files}
        with open(self._map_dir / filename, "w") as f:
            json.dump(out2, f, sort_keys=True, indent=2)


def _get_oid_mapper() -> OidMapper:
    """
    Parse arguments and return the OID mapper for that repository.
    """
    parser = parse(description="Map all LFS OIDs for a repository")
    add_input_parms(parser)
    parser.add_argument(
        "-r",
        "--repo-directory",
        "--repo-dir",
        help="directory of repo to migrate (no default)",
    )
    parser.add_argument(
        "-u",
        "--owner",
        "--user",
        help="owner (usually organization) for repository (no default)",
    )
    parser.add_argument("-n", "--repository", help="repository name")
    args = parser.parse_args()
    if not args.owner or not args.repository:
        raise RuntimeError("Both owner and repository must be specified")
    return OidMapper(
        map_directory=args.map_directory,
        full_map=args.full_map,
        branch_pattern=args.branch_pattern,
        owner=args.owner,
        repository=args.repository,
        repo_directory=args.repo_directory,
        dry_run=args.dry_run,
        quiet=args.quiet,
        debug=args.debug,
        logger=None,
    )


def main() -> None:
    om = _get_oid_mapper()
    asyncio.run(om.execute())


if __name__ == "__main__":
    main()
