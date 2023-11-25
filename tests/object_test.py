from pathlib import Path

from rubin_checklfs.looper import Looper
from rubin_checklfs.oid_mapper import OidMapper
from rubin_checklfs.remediator import Remediator


def test_remediator_object_init() -> None:
    remediator = Remediator(
        map_directory=Path("."),
        input_glob="oids--*.json",
        project="data-curation-prod-fbdb",
        bucket="rubin-us-central1-git-lfs",
        original_bucket="git-lfs.lsst.codes-us-west-2",
        remediation_input_file="",
        remediation_output_file="",
        stop_after_check=False,
        logger=None,
        quiet=False,
        dry_run=False,
        debug=False,
    )
    assert remediator._map_dir.is_dir()
    assert remediator._input_glob == "oids--*.json"
    assert remediator._project == "data-curation-prod-fbdb"
    assert remediator._bucket is not None
    assert remediator._orig_bucket == "git-lfs.lsst.codes-us-west-2"
    assert remediator._remediation_input_file is None
    assert remediator._remediation_output_file is None
    assert remediator._logger is not None
    assert remediator._dry_run is False
    assert remediator._quiet is False
    assert remediator._debug is False


def test_oid_mapper_object_init() -> None:
    oid_mapper = OidMapper(
        owner="lsst-dm",
        repository="milestones",
        map_directory=Path("."),
        repo_directory=Path("."),
        branch_pattern=r"v\d.*",
        logger=None,
        full_map=False,
        quiet=False,
        dry_run=False,
        debug=False,
    )
    assert oid_mapper._map_dir.is_dir()
    assert oid_mapper._dir.is_dir()
    assert oid_mapper._repo is not None
    assert oid_mapper._owner == "lsst-dm"
    assert oid_mapper._repository == "milestones"
    assert oid_mapper._branch_pattern == r"v\d.*"
    assert oid_mapper._full_map is False
    assert oid_mapper._logger is not None
    assert oid_mapper._dry_run is False
    assert oid_mapper._quiet is False
    assert oid_mapper._debug is False


def test_looper_object_init() -> None:
    looper = Looper(
        input_file=Path("lfsrepos.txt"),
        map_directory=Path("."),
        input_glob="oids--*.json",
        project="data-curation-prod-fbdb",
        bucket="rubin-us-central1-git-lfs",
        original_bucket="git-lfs.lsst.codes-us-west-2",
        remediation_output_file="",
        stop_after_check=False,
        stop_after_scan=False,
        branch_pattern=r"v\d.*",
        full_map=False,
        logger=None,
        quiet=False,
        dry_run=False,
        debug=False,
    )
    assert looper._repo_file is not None
    assert looper._map_directory.is_dir()
    assert looper._input_glob == "oids--*.json"
    assert looper._project == "data-curation-prod-fbdb"
    assert looper._bucket is not None
    assert looper._orig_bucket == "git-lfs.lsst.codes-us-west-2"
    assert looper._branch_pattern == r"v\d.*"
    assert looper._full_map is False
    assert looper._remediation_output_file == ""
    assert looper._logger is not None
    assert looper._dry_run is False
    assert looper._quiet is False
    assert looper._debug is False
