from pathlib import Path

from rubin_checklfs.remediator import Remediator


def test_object() -> None:
    remediator = Remediator(
        input_directory=Path("."),
        input_glob="oids--*.json",
        project="data-curation-prod-fbdb",
        bucket="rubin-us-central1-git-lfs",
        original_bucket="git-lfs.lsst.codes-us-west-2",
        debug=False,
    )
    assert remediator._input_dir.is_dir()
    assert remediator._input_glob == "oids--*.json"
    assert remediator._project == "data-curation-prod-fbdb"
    assert remediator._bucket is not None
    assert remediator._orig_bucket == "git-lfs.lsst.codes-us-west-2"
    assert remediator._debug is False
