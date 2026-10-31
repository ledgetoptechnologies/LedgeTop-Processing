# Offline backup and restore prototype

This prototype is not a deployment or cutover procedure. It assumes the
operator has stopped and excluded all writers, including legacy instances,
and separately provisioned independent storage. Cooperative locks and mount
checks do not establish those facts. In particular, writes through another
mount namespace or storage mapping are not fenced by a read-only bind mount.

Backup requires the stopped source mounted read-only at its own mount root,
with no nested mounts. The new destination must be on a different filesystem
and its parent must be private to the current user. It must not be an alias
of the source. The authority lease and coordinator lock files must already
exist and be available for exclusive locking. The inventory output can be
captured from the backup CLI with `--emit-inventory`; retain it and the same
32-byte key separately from the backup, under the operator's control.

Restore requires the **exact backup directory** itself to be mounted read-only
as its own mount root, with no nested mounts. Mounting only its parent and
pointing to the backup subdirectory is refused. The new restore destination
must meet the same independent-filesystem and private-parent requirements.
The original authority lease must be accessible and the backup's coordinator
lock must be lockable. Restore checks its source against the separately held
inventory before creating the destination and compares source, destination,
and final source after copying. Existing destinations are never reused.

The inventory is an operator-trusted record, not a signed or authenticated
manifest. Keyed file and row digests do not authenticate the whole document;
protecting its origin and integrity is an operator prerequisite. A failure or
lost response leaves the destination untouched for separate investigation;
do not infer completion or automatically delete or retry into it. This is not
yet a tested operational recovery procedure.

The opt-in disposable Docker test `tests.test_stopped_viewer_mount_e2e` creates
synthetic SQLite and file data, backs it up, remounts that exact backup
directory read-only using a Docker volume subpath, restores to another
filesystem, checks rows and bytes, and removes its own temporary resources.
Run it on a Docker host with `python:3.12-slim` available by setting
`LEDGETOP_DOCKER_MOUNT_E2E=1` and invoking the module with `unittest`.

With `LEDGETOP_IMAGE_COMPAT_E2E=1` also set, this test requires the exact local
predecessor and candidate images pinned in the test. The predecessor builds a
synthetic terminal Viewer task and photo; the candidate initializes its actual
coordinator/Viewer handler on a separate copy; keyed database schema and row
digests and the photo are compared; and the predecessor initializes against the
independently restored backup. Backup and restore run as UID/GID 10001. No
listening API, real processing, live state, writer-fence or daemon-restart
recovery is tested; the synthetic task was marked terminal for the fixture.
This image-specific result does not qualify later candidate revisions.
