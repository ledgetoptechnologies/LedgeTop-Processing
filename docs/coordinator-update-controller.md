# Dual-host coordinator update controller

`ops/run_coordinator_update_bridge.py` is a one-shot controller on Beau's existing
Windows SSH host. It opens independent authenticated SSH connections to
`bkoltz@192.168.50.88` and `bkoltz@192.168.50.89`; the cluster does not need a key
for the main PC. It makes no TrueNAS changes and installs no listener or timer.

The controller never decides that a deployment is safe from a saved status file.
The main child holds the actual canonical main NodeGate. The cluster updater
holds the cluster NodeGate, requests fresh nonce challenges, and receives replies
through the controller. The reply proves the main gate at that check, not an
atomic distributed transaction. The updater must retain both durable node drains
through cutover and check the gates around every mutation and node reopening.

The controller uses the existing OpenSSH executable and known-host records with
strict checking, batch authentication, no agent forwarding, no SSH forwarding,
and no connection multiplexing. It creates no SSH credentials or grants. Unknown
host keys or authentication failure hold the operation. The remote program is
constant Python code plus one POSIX-quoted JSON argument; there is no variable
shell program or shell invocation on Windows.

## Preparation and source binding

Stage the exact reviewed source bundle on both PCs at
`/home/bkoltz/.local/share/ledgetop-processing-reference/reference-<full-commit>/source`.
Run the Windows controller from a byte-identical extraction of that bundle.
The bridge hashes its own source and the imported NodeGate source. Windows Git
line-ending conversion can change those hashes; a checkout that differs from
the deployed bundle must not be used to authorize the bridge.

The existing updater's separate `prepare` action creates the private checkpoint
and operation ID without holding the bridge. Its reviewed gate configuration
contains exactly `schemaVersion`, `cluster`, `main`, `sessionNonce`, and
`timeoutSeconds`. Each node binding contains its exact Engine ID, node ID,
authority, localhost coordinator origin, gate generation, and helper hash.
Keep the local JSON byte-identical to the reviewed cluster configuration.
Preparation and configuration alone do not authorize deployment.

The main holder's probe additionally checks the exact existing main NodeODM
container ID/name and the prepared main image, then reads its private localhost
API to prove no queued or retained tasks. Every challenge repeats this read-only
probe. Main's current Docker recipe has no Docker healthcheck; API readiness is
checked directly. Any task, identity drift, unavailable API, or malformed reply
holds the operation without changing that processing container.

After source review, synthetic acceptance, immutable candidate-image validation,
and the updater's own activation review gate, invoke the controller with explicit
`activate` or `rollback`, full source commit, local candidate image ID, verified
main processing image ID, prepared operation ID, cluster receipt directory,
cluster gate-configuration path, local gate-configuration path, and a new absolute
local evidence directory. Use `python -B -m ops.run_coordinator_update_bridge
--help` for argument names. There is no default image, tag, operation, action,
SSH host, or retry. Rollback remains subject to the updater's compatibility and
no-reopen fences.

## Failure handling and evidence

The cluster's stdout is exclusively bridge protocol. Updater summaries and SSH
diagnostics go to separate `main-ssh.stderr` and `cluster-ssh.stderr` files in the
new evidence directory. Existing evidence is never overwritten. These files are
private operational evidence and should be inspected locally before sharing.
No registry or API token is passed through controller arguments or bridge frames.

A successful relay requires the bridge's normal close protocol and actual zero
exit statuses from both owned SSH children. EOF, a ready frame, matching target
image, or a process exit alone is insufficient. Failure closes the controller's
owned transport handles and reaps its child processes with bounded waits; it
never sends a remote unlock, clears a persisted reservation, invokes Docker,
restores a database, or retries a mutation. Remote gate release is proven by a
normal holder exit only. Lost transport or cleanup uncertainty requires inspecting
the existing operation and gate state, preserving the durable drains.

The focused controller tests use real local subprocesses and binary pipes for
launch failure, relay failure, nonzero exits, handle cleanup, and evidence
preservation. They do not prove an actual SSH deployment; bridge ownership and
updater state transitions have separate tests and require runtime acceptance.
