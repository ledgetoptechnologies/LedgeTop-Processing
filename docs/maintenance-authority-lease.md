# Cooperative maintenance authority lease

Managed maintenance and ingress commands share an OS lock keyed by the local
Docker Engine ID. The lock path derives from the OS account's home directory,
not `XDG_STATE_HOME`, so different service environments for the same account
cannot select different locks. The lock covers configuration reads and the
subsequent mutation. The updater adapter also takes it before binding its
journal and during each operation. The existing supervisor lock remains
separate: a credential-rebinding operation must hold both locks and must keep
them through the whole rebinding transaction.

This is a cooperative source change only. Installed or copied older tools do
not acquire this lease; another OS user and direct Docker clients can bypass
it. Do not rebind a live updater until its old executable paths and any active
processes have been fenced and a reviewed, crash-recoverable binding migration
exists. This change neither rotates credentials nor migrates adapter journals.
It must not be treated as a sandbox or proof that a worker has stopped.
