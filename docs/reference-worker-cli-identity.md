# Canonical worker CLI identity

Keep the Docker entrypoint `python -m ops.reference_worker_daemon`. Its
`__main__` dispatch calls `ops.reference_worker_daemon.main` from the canonical
imported module. All active ownership classes and the live `_STATE_OWNERS`
registry therefore come from that same module.

Previously the CLI called the `main` function defined in its `__main__`
namespace. Its `DaemonStateLock` was a different class from the canonical one
imported by the indexed profile's exact-type capability check. Imported factory
tests used the canonical module and did not exercise that distinction.

The actual cached image's unchanged daemon and indexed source bytes were
verified, and its inert `--help` execution demonstrated those distinct class
identities without constructing an owner, opening state or starting a listener.
The second real empty-lab worker also exited before service startup despite a
healthy native backend. Its generic log does not expose the exact exception;
the namespace defect independently reproduces the strict ownership rejection.

The fix changes dispatch only. It does not relax type checks, alias classes,
replace the ownership registry, repair failed state or restart an existing
worker. Unit tests execute the file's real `__main__` dispatch with a probe
canonical entrypoint and actual state lock, and retain foreign-owner refusal.
A newly built, pinned worker image still needs real canonical entrypoint and
service readiness qualification before any maintenance or processing claim.
