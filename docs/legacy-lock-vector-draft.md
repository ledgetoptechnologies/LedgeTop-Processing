# Disabled legacy lock vector prototype

This draft extends the separate PR83 enrolled-fence prototype to retain two to
16 legacy state locks. An offline caller enrolls existing lock files in slot
order after independently excluding all writers. Candidate startup reads the
exact stored device/inode and authority for every slot, acquires the locks in
numeric device/inode order before opening its own queue, and retains all file
descriptors for the queue lifetime. Partial startup releases all acquired
descriptors. Duplicate, missing, changed or same-as-candidate locks fail closed.
The normal candidate queue lock remains distinct and is still acquired normally.

Runtime paths are fixed at `/legacy-state-vector/<slot>/coordinator.lock`.
Nothing mounts them or enables enrollment automatically. There is no reviewed
installer, container recipe, updater, rollback, filesystem-topology or inventory
integration. Old binaries may ignore the new marker; existing host policies and
the mandatory required-fence flag must prohibit their startup or downgrade. A
noncooperating process can ignore flock, replace a path after inspection, start
from an unrecorded copy or use an unprotected Docker daemon. This is not a host
authorization policy or a cross-host fence. Reboot, daemon restart, remount,
lock-file replacement, ambiguous enrollment and exact live images require
separate review and fail-closed recovery before any deployment.
