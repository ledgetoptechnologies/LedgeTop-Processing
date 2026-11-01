# Coordinator process interruption rehearsal

Do not execute process termination tests until exact scripts, ownership and
the container watchdog have been independently reviewed. All resources are
synthetic and isolated in the reviewed network-none retained container. The
specific candidate below was independently reviewed and run once; later edits
require independent review before further execution.

The supervisor owns independent direct Popen handles for the coordinator,
synthetic engine, and worker. The coordinator process owns Queue, SQLite,
ReferenceStore and the actual HTTPS listener; it creates one synthetic job,
grants its input transfer after the worker requests it, and publishes an atomic
ready marker containing only synthetic identifiers and listener port. The
supervisor never holds that coordinator's Queue authority lock. Fixed synthetic
credentials remain in an owner-private retained configuration file.

After engine commit and before synthetic engine completion, require a live
coordinator handle and an explicit persisted-job/grant marker; kill that exact
coordinator handle, wait and reap, verify the listener cannot connect, and do
not start a replacement if termination is uncertain. Release engine completion
while the coordinator is absent. Verify the worker has an exact pending outbox
identity and retains the original synthetic archive. Restart a new coordinator
child on the same persistent queue and same listener port; do not resubmit the
job or transfer grant. Require the worker's receipt to match the coordinator's
single persisted receipt and the original outbox payload/digest, and retrieve
identical bytes through the actual result proxy. Verify one engine execution.
Coordinator process SIGKILL does not prove host power-loss durability.

Every direct child is logged and tracked from creation through verified reap;
startup failures, including ambiguous readiness or bind failure, abort rather
than killing by name. Retain all evidence and container. A reviewed outer
Windows Job and disposable container with init bound all descendants. Any
unreaped or inaccessible resource fails closed and is reported, not silently
cleaned. No shared or production resource, guessed PID, or recursive deletion.

## Synthetic run, 2026-11-01

Processing snapshot `9a0e23ee29287e6b6455ae412f789dea403abdb2`, local image
`sha256:aa943ce411423c7582288f18707ff66eddcbaeb510502d747a72aeb34f30d159`,
run label `dd64c1303b754229813657ef2597c57b`. Synthetic container
`6f12f4d568bfddd2e655279ffa655fa7a0312fc7dd7813021440fd199703362a`
exited 0 and its exact label/name/ID were verified. The inner process group
exited 0, fixture cleanup reported verified, and Windows Job active count was
zero. No uncertainty was reported. Both container and root
`C:\Users\fstor\AppData\Local\Temp\ledgetop-acceptance-outer-ueh4ay9r`
were retained.

The first coordinator process was killed and reaped after synthetic engine
commit; the worker persisted its pending result while the listener was offline.
The new coordinator reopened the queue, the exact outbox payload and receipt
matched across worker and coordinator, proxy bytes matched, and each synthetic
engine action occurred once. This uses a synthetic engine and does not prove
real ODM behavior, Viewer acceptance, host power loss, or production readiness.
The previous worker-restart test and its failed first attempt are recorded in
`WORKER_PROCESS_OUTAGE_PLAN.md`.

Snapshot SHA-256 values:

* `worker_process_coordinator.py`: `ea496c1c1326b2332d60e392fbd625b29ad85acb9266b06f627b6e5c526dc70d`
* `coordinator_process_restart_supervisor.py`: `4c03632aacb921794e572af1cd2dbf2d67f1fdba40406f91f91ceab73a66fdbc`
* `coordinator_process_restart_watchdog.py`: `d5e16abaab0443b5da6c00d640d4970cebcc527296d88731b5efa659c1b9ec2b`
* `worker_process_outage_fixture.py`: `18d2b0e6013a6c8ce7156ecb1a6c4145ff817ad1abefa396840711d76566c07f`
* `worker_process_outage_engine.py`: `f7f3fbc6f3ff0af14c1ef4fad1c6343e4500a09681636e627e028c5e0035903e`
