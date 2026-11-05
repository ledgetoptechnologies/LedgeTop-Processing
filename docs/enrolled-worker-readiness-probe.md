# Existing worker readiness observation

`ops.enrolled_worker_readiness_probe` supplies a read-only body for the tracked
SSH observer. It does not connect through SSH itself, enter a maintenance
context, create a worker lock, restart a service, or update any configuration.

`serialize_probe(worker_entry, challenge_uuid)` embeds the observer and reviewed
bounded subprocess implementation. It checks the expected SSH UID, interpreter,
and independently hashed read-only installed source before importing the existing
worker handover code. The new observer module need not be installed on the
worker. The caller must use the existing authenticated SSH identity and a
tracked, fresh request; completion requires the separate remote process
retirement proof. A cached packet is not current readiness for another effect.

`probe(worker_entry, challenge_uuid)` reuses
`ReferenceWorkerHandover.ready(require_idle=True)` without calling `__enter__`.
That verifies the existing private configuration/source pins and sends a fresh
nonce over authenticated, verified TLS. The Systemd control performs read-only
`show` calls with bounded output and owned client process cleanup. The proof
must match every expected worker, engine, authority, generation, source,
configuration and activation field. Process identity is checked again afterward.

The closed response is `{schemaVersion: 1,
protocol: "ltds-enrolled-worker-readiness-probe-v1", challenge, nodeId,
settingsSha256, readiness, process}`. `readiness` is the full existing DTO;
`process` is the existing control's exact PID/start/unit observation. Errors in
the serialized body are sanitized. Settings hashes use the transport's sorted,
compact ASCII JSON encoding. Credentials stay in the existing private config.

The focused Linux suite passed 7/7 tests in 17.792 seconds under one CPU,
512 MiB, UID 10001, isolated networking and a read-only source bind. It includes
real TLS/nonce authentication, standalone serialized execution, identity and
config mismatches, source-footprint equivalence, and refusal of service mutation
commands. Systemd identity is synthetic in those fixtures; actual installed
workers, SSH retirement and cutover readiness remain separate qualification.
The test child uses a 30-second bound for repeated source scans on the Windows
bind. Production TLS checks and the existing bounded Systemd checks are unchanged.
