# Worker directory publication

The Linux worker factory publishes its configured local state root through
`ops/private_worker_directory.py` before granting a daemon ownership lease. It
then publishes the scratch root under that lease, before engine admission.
The helper opens every path component relative to the preceding directory
descriptor with `O_DIRECTORY | O_NOFOLLOW`. Missing components are created with
mode 0700; existing root ownership and private permissions are checked without
changing permissions.

All directory descriptors are synced from the leaf through the filesystem root.
Their visible device/inode identities are checked before and after syncing. This
covers newly created ancestor entries as well as existing directories whose
publication history is unknown. A symlink, path replacement, unsupported sync or
I/O error stops factory admission and retains any created directories. There is
no cleanup or permission-repair fallback.

`DaemonStateLock` also syncs the exact locked file and its containing directory
before registering the live ownership capability. Failed publication closes the
file descriptor without registering ownership. Later startup may inspect the
retained lock file and obtain its actual OS lease; a lock file's existence alone
never proves a running owner.

This requests the configured local filesystem's durability guarantees; it does
not prove hardware power-loss behavior, supply storage quotas, authenticate NAS
access or make a distributed transaction. The trusted host must retain the
ownership lease throughout engine work. Existing worker components continue to
check their root, file, process and lease bindings. This helper does not activate
the new sequential adapters or change a running service.

The production factory remains Linux-only. Its Windows branch exists for
portable synthetic ownership tests and makes no claim of directory-fsync
durability on Windows.

Synthetic tests exercise nested creation and bottom-up sync, existing roots,
permission refusal, symlink/file ancestors, failed sync with retained directories
and closed descriptors, path replacement, and failed lease-file publication.
Factory, driver, runtime and Native intermediate regressions exercise their
existing ownership interfaces against the updated implementation.
