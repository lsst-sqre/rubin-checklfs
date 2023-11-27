Rubin LFS Checker
=================

Ensure that all the LFS objects referenced in any tags, or at the tip of
branches `main`, `master`, or `re(r'v\d.*')`, exist at GCP.

It's slightly more sophisticated than that: notably, you can change the
source and target buckets, change the branch match pattern, and stop
after the object mapping or resume from an object map.  This allows
reasonable checkpointing.

Important Assumptions
---------------------

This checker/migrator will not work if any of the following assumptions
are violated:

1. The git repositories must be readable by the process context.
2. It is vital that Git LFS is *not* installed in the process context,
   because operation relies on being able to inspect the LFS stub files
   rather than downloading the pointed-to content.
3. The process context is authenticated to AWS so that it can inspect the
   contents of the original s3 bucket.
4. The process context is authenticated to GCP so that it can both inspect
   the contents of the target s3 bucket and upload to it if needed.

Note that the first two and the last two are somewhat separate, in that
you could generate the list of objects needing transfer with the
`check_lfs` program and the `--remediation-output-file` and
`--stop-after-check` options, and then later on, transfer those objects
with `remediate_lfs` and the `--remediation-input-file` option.

Setup
-----

`pip install .` in the top-level directory.  Eventually maybe it'll be
on pypi.  You probably want a virtualenv so you don't clutter up your
system Python.  You need at least Python 3.11.

Usage
-----

Assuming that you've set up the process context with the above
assumptions correctly validated, and that the virtualenv is activated so
the various scripts are on your `PATH`, then all you need to do is
collect a list of repositories that need migrating.  Currently these
must be in URL format, and the scheme must be `https`
(e.g. `https://github.com/lsst-dm/milestones`).  The last two components
of the URL will be taken to be the owner and repository name of the
referenced repository.  This is a reasonable assumption for
GitHub-hosted repositories, which captures the Rubin Observatory use
case.

The list, current as of 24 November 2023, can be found in [Rubin LFS
Repos](assets/lfsrepos.txt).  Note that these have already been
migrated, although it does no harm to rerun the command.  For the
purpose of these instructions, we'll assume the file specifying the
repositories is in the current directory as `lfsrepos.txt`

This will write files in the first stage that will be loaded in the
second stage, so either you need to be in a directory you can write to,
or you need to specify a writeable directory with the `--map-directory`
option.

Assuming that your working directory is writeable, and you don't mind
putting JSON map files in it, just run `check_lfs --input-file
lfsrepos.txt`, and sit back and wait.

Commands
--------

There are three commands that can be used.  Invoking any of them with
the `--help` option will give full usage details.

`check_lfs` operates on a file listing repositories to be checked for
migration, and loops over all those repositories.  This is probably all
you're going to need.

`oid_mapper` operates on a directory containing a checked-out
repository, and generates a list of object IDs for all referenced LFS
objects on all tags and all branches matching the default branch pattern
(which is "v" followed by a digit followed by anything else) for that
repository.  In general you will not need to run this directly.

`remediate_lfs` operates on a directory containing JSON files produced
by `oid_mapper`, or it can take an input file produced if
``--stop-after-check`` was used with ``check_lfs``.  If you need to
split the mapping and the file-moving because of process authentication
concerns, doing it this way may be useful.
