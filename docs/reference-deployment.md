# Reference runtime deployment preflight

This records the read-only October 27, 2026 preflight as `bkoltz` on cluster
`192.168.50.89` and main `192.168.50.88`. No deployment configuration, certificates,
credentials, services, containers, gate bindings or task data were changed.
Only process/container metadata, filesystem metadata, health, engine info/options
and read-only queue state were inspected. No processing task was submitted.

The source qualification in [reference-pipeline.md](reference-pipeline.md) is not
an installed-runtime or production-ingestion qualification. The NAS broker HTTPS
mount and its production configuration were not installed or verified in this
preflight. No real-photo trial is authorized by these instructions.

## Verified installed state

| Item | Cluster `.89` | Main `.88` |
| --- | --- | --- |
| Host account | `bkoltz`, UID/GID 1000 | `bkoltz`, UID/GID 1000 |
| Host Python | 3.14.4 | 3.10.12 |
| Installed source root | `/home/bkoltz/.local/share/ledgetop-processing-prototype/viewer-ba01f37/source` | `/home/bkoltz/.local/share/ledgetop-processing-node/source` |
| Recorded source revision | `ba01f372d31a2354ff4321dc5ae7ebf9f2b5f310` | Installation metadata records `3a6ad0dbc2fb73ae08763d14ec387267d8970526` |
| Actual engine | ODM 3.6.2, loopback `127.0.0.1:13001` | ODX 3.8.5, loopback `127.0.0.1:13004` |
| Proposed TCP 18443 / 18445 | Neither listening | Neither listening |
| User manager / linger | Running / **no** | Running / **yes** |
| Free space on home filesystem | About 233 GiB | About 1.38 TiB |

Available port numbers do not establish firewall reachability or certificate
trust. Verify those after the explicit TLS listener is staged. Free space is a
snapshot; scratch admission must continue using the current capacity probe and
configured safety floor.

The sole coordinator is Docker container
`ledgetop-processing-prototype-c3fcb3a`, image
`sha256:40a4d54c3b7311384ba031cf05acb3e760237ada4d6d2434ab1392fabd5bc401`.
It uses Python 3.13.17, `/app`, UID/GID `10001:10001`, bridge networking and a
read-only root filesystem. Its only persistent mount is named volume
`ledgetop-processing-prototype-c3fcb3a-state` at `/prototype-state`, read/write.
The state directory is UID 10001, mode 0755; `jobs.sqlite3` is UID 10001, mode 0600.
No reference module, reference configuration mount or reference port is installed
in that container. `PROCESSING_REFERENCE_CONFIG` is absent.

Native mappings remain `127.0.0.1:18080 → 8080` and
`192.168.50.89:3005 → 8080`. The existing native Viewer provider remains
`http://192.168.50.89:3005`, with its existing credentials and behavior. Its private
runtime environment is
`/home/bkoltz/.local/share/ledgetop-processing-prototype/viewer-ba01f37/runtime.env`,
owned by UID 1000, mode 0600. Credential values must not enter this document,
commands printed in review records, logs or evidence files.

The queue authority is `7131882e-38ce-4d3e-99cc-4ba5a94d2ee8`. Inspection found
12 historical jobs: ten succeeded, two failed, and none running, held or queued.
Both nodes were active. Authenticated engine discovery was ready and returned
74 identical common options. These observations must be refreshed before a cutover.

## Existing physical ownership

Both canonical host gates are owned by `bkoltz`, idle, generation **1**, and bound
to the same authority above with coordinator origin **`http://127.0.0.1:18080`**.

| Node | Physical Docker Engine ID |
| --- | --- |
| Cluster | `b557939c-6ac3-4e73-8b14-31caf7390f23` |
| Main | `45bc889d-d645-442b-8fb9-d78b77b14fe9` |

The gates live under
`/home/bkoltz/.local/state/LedgeTop-Processing/node-gates/<sha256-engine-id>/gate.json`
and were UID 1000, mode 0600. Preserve their account, default state-home location,
authority, origin spelling and generation. Installing the reference transport
does not require provisioning or handing over either gate.

Main's enabled, active user service
`/home/bkoltz/.config/systemd/user/ledgetop-processing-tunnel.service` already maps
main `127.0.0.1:18080` to the cluster coordinator. Preserve it. Worker configuration
must distinguish `gateCoordinatorOrigin=http://127.0.0.1:18080` from the proposed
transport `coordinatorOrigin=https://192.168.50.89:18443`. Substituting the HTTPS
origin into the existing gate binding would fail ownership checks.

## Private TLS mount and process ownership

Docker's inspected UID map was `0 → 0` for the full range: no user-namespace
remapping. A read-only directory bind at `/run/processing-reference` can therefore
carry UID-10001-owned runtime configuration and key files directly. Use a real
directory path, without symlink components; do not use the `/var/run` alias.

The container's private JSON and TLS key must be UID/GID `10001:10001`, mode
0400 or 0600, beneath a private UID-10001 directory. The runtime explicitly checks
file ownership; a UID-1000 mode-0600 configuration will fail even if a separate
access grant makes it readable. Public certificates and CA material may be
read-only regular files. Mount this directory read-only, preserving the existing
state volume and container security restrictions. Do not loosen existing native
environment-file permissions to solve access to the new mount.

Host workers run as UID 1000. Their separate private JSON, TLS keys, state and
scratch directories must instead be owned by `bkoltz`, with files mode 0600/0400
and directories mode 0700. Do not reuse the coordinator's UID-10001 private copy.
The coordinator TLS server uses its configured certificate/key; the explicit CA
validates downstream worker HTTPS. Bearer authentication remains required; this
configuration does not require client certificates.

Cluster has noninteractive sudo available for an authorized ownership/linger
installation. Main did not have noninteractive sudo. Neither host had `setfacl`
or `getfacl`; ACL tooling is unnecessary for the ownership scheme above.

## Staged installation

1. Stage the reviewed immutable coordinator image and versioned worker source.
   Prepare approved PKI and private configuration separately, without starting
   units or changing the active container. Require certificate SANs matching the
   configured HTTPS origins. Confirm the NAS broker origin, CA, scoped grant
   policy and credentials before any driver intake. Keep the initial reference
   Viewer credential **staging-only**; do not introduce a production reference
   credential during infrastructure qualification. Keep every reference Viewer,
   private worker and worker-result-reader credential distinct from each other
   and from all existing native/API/operator/engine credentials.
2. Recheck queue, gate and maintenance state. Drain intake for the planned
   cutover and wait for safe idle ownership. Stop only the existing coordinator
   process, take a consistent backup of its existing state volume and record its
   exact immutable image, native environment and port mappings for rollback.
3. Replace that single logical coordinator with the reviewed image, preserving
   the original named volume, authority, native environment and both native port
   mappings. Add `.89:18443` for the configured reference TLS listener and the
   read-only private mount. Set `PROCESSING_REFERENCE_CONFIG` to its absolute
   in-container JSON path. The listener must bind the appropriate container
   interface so the published port is usable. Do not launch a second application
   process, temporary coordinator or second state volume for staging.
4. Stage one `bkoltz` systemd user unit on each host. Use a versioned source root
   as `WorkingDirectory` and `/usr/bin/python3 -m ops.reference_worker_daemon
   --config ABSOLUTE_PRIVATE_JSON` as the command. This preserves package imports
   and the existing account's canonical gate location. Configure fixed loopback
   engine origins, the observed engine-version pins, exact gate identity and
   generation, separate HTTPS coordinator/broker origins, private result readers,
   explicit allowed environments and conservative scratch/output budgets. Serve
   worker results over verified HTTPS on `.89:18445` and `.88:18445`.
5. Initially leave `cleanupEnabled=false` and restrict new reference work to
   staging. Verify TLS trust, authentication separation, authority, health,
   dynamic options and queue visibility without submitting processing jobs.
   Confirm that the native Viewer registration at port 3005 is unchanged. Recheck
   that only the original coordinator owns the queue and that no engine mutation
   or unexpected admission occurred.
6. Enable unattended worker startup only after qualification. Main already has
   linger; cluster needs explicitly approved linger enablement for user units to
   survive logout/start at boot. Verify the real service identities and active
   result listeners. Firewall changes, if necessary, need separate authorized
   administration; no rule change was attempted in this preflight.

The workers' daemon lock covers in-flight driver operations and active result
readers through shutdown. Stop their units gracefully and wait for draining;
a timeout or forced kill does not authorize another owner or removal of stale
read pins. Uncertain engine mutation and gate receipts remain for reconciliation.

## Rollback and retained data

Before admitting reference jobs, rollback is a drained stop of the new workers
and sole coordinator, followed by the recorded original image/environment/native
port mappings against the **same** state volume. Start only one coordinator.
Verify the original authority, complete job history, native capabilities and
engine/gate state. Keep the replacement image and private deployment records for
inspection rather than deleting evidence while ownership is uncertain.

Do not restore an old database snapshot over jobs admitted since that snapshot.
Once reference jobs exist, first reconcile their execution and delivery state;
an older image cannot be assumed to dispatch or recover reference-profile rows.
Preserving queue history takes precedence over returning immediately to an old
binary. Gate reprovisioning, authority replacement and engine-task removal are
not rollback steps.

Retain worker inputs, archives and engine task data during initial qualification.
Coordinator metadata ACK, successful download and terminal-failure acceptance do
not authorize file deletion. Result cleanup requires its separate exact Viewer
acceptance, terminal-state, pin and grace checks. Input-cache deletion additionally
requires an authenticated durable source-retention/release lifecycle. The
source-retention validator alone does not install issuer purge pins, global
receipt-role uniqueness or that lifecycle. No input deletion is activated here.

## Subsequent source-only staging

After the preflight, authorized install preparation staged committed source
`8ae9fe09bbe049071aee027caffae1cfc8b8d5b1` on **both** hosts at
`/home/bkoltz/.local/share/ledgetop-processing-reference/reference-8ae9fe0/source`.
The Git archive contained only `processing/`, `ops/` and `Dockerfile`: 50 files,
634,880 archive bytes, SHA256
`60830f7c4340cbce322602b28ef575844b720d67e39997ba06bf882f2a057ea9`.
Both received archives and every extracted file were hash-verified. New version
and source directories are UID 1000, mode 0700; files are UID 1000, mode 0600 or
0700 according to their committed executable bit. Existing targets were not
overwritten, and only the exact transferred archive was removed after verification.

Each host retains its nonsecret per-file staging receipt at
`/home/bkoltz/.local/share/ledgetop-processing-reference/reference-8ae9fe0/staging-receipt.json`.
Receipt SHA256 is
`7ec3f384f9c277a206c25caa8885f5a58708ed74268f5a8b6247d46436d59710` on cluster and
`ed846305cc75ff83426f7f1d121868cfa2ce8d2a2b9ff3adb014ec17a01429e2` on main.
Independent re-reading verified all 50 file hashes on each host. The source is
inactive: no Docker build/pull/run, configuration or PKI generation, unit install,
gate change or listener activation occurred. Current installations remain intact.

A second authorized source-only stage placed commit
`10a3848b5da0fa591fd8be7ef642e122bb1f1724` at
`/home/bkoltz/.local/share/ledgetop-processing-reference/reference-10a3848/source`
on both hosts, retaining the earlier staged version. This archive again contained
only `processing/`, `ops/` and `Dockerfile`: 55 files, 696,320 bytes, SHA256
`1a664b77b90812f1ff8d9fa18e081c1476f043bb9fbc8719853ad264569a2c5a`.
The same exclusive extraction, private ownership and per-file verification checks
passed. The transferred archives were removed after verification, and a second
independent read verified all 55 extracted hashes on each host.

The newer version's adjacent `staging-receipt.json` has SHA256
`05ff31e16f34c5c68575c8b78a8621423884256fdfa920b6e0c604acee43da1b` on cluster and
`72c0a63ed48a28095d00f4e8df3d5b357d0751aca0794e1c001e4e6d7f249959` on main.
Both reference ports remained unbound after staging. No Docker build/run,
credentials, PKI, configuration, units or gates were changed by that source stage.

Read-only recreation inspection also confirmed that the 24 keys in the existing
private native runtime environment file exactly match the running container,
without reporting their values. Preserve its bridge network, init process,
`unless-stopped` restart, read-only root, UID/GID 10001, dropped capabilities,
`no-new-privileges`, 64-process limit, 0.5 CPU, 256 MiB memory, 512 MiB memory/swap
total, private IPC, 64 MiB shared memory, JSON-file logging, native ports, state
volume and inherited healthcheck. Its default hostname is a generated container
identity, not a gate or queue authority.

The exact pinned Python base image from `Dockerfile`,
`python:3.13-slim-bookworm@sha256:e32be0e55814a08977501ca863271d9bd02443710db8f2afaf8372e2c4508530`,
was present locally on cluster as Linux/amd64. Docker client/server were 29.1.3;
Buildx was absent. A future authorized local build can use the cached base with
`--pull=false --network=none`, after selecting a supported local builder. Builder
execution was not tested and no image was built or pulled in this inspection.

An additional read-only host check imported `processing.reference_driver`,
`processing.reference_runtime`, `processing.treedar_admission` and
`ops.reference_worker_daemon` from that staged source using each host's actual
`python3 -B` (cluster 3.14.4, main 3.10.12). Both passed. No configuration was
loaded, daemon started, database opened or bytecode written by that import check.

## Private TLS and configuration preparation

The reviewed `ops/provision_reference_tls.py` was executed on cluster in the new
`reference-10a3848/tls` private directory. It created a Processing-only CA and
separate coordinator, cluster-worker and main-worker server certificates. The
leaf SANs bind their actual `.89`/`.88` addresses and loopback; leaf certificates
expire January 26, 2027. Renewal is still an installation responsibility, not an
enabled automatic service. The CA certificate SHA256 is
`d35b9882b6f7244ecc09ff934ddb524a27fb2080ac4350087c5b96529a636318`.
No machine trust store was changed. The CA private key stayed on cluster; only
the main leaf key/certificate and public CA were transferred to main over SSH.
Main verified the certificate chain, IP SAN, matching private key and mode/owner.

Each host's worker TLS copy is UID 1000 with directory 0700/files 0600. The
cluster coordinator mount copy is numeric UID/GID 10001, with the same private
modes. Public CA material was also copied to the local Codex artifact directory
for future endpoint-scoped Viewer trust; no private key was copied there.

`ops/provision_reference_config.py` created five distinct new role credentials
in private cluster files and generated the staging-only reference runtime JSON.
It does not generate a NAS broker route or worker intake configuration. The
author copy remains UID 1000; the separate mount copy is UID 10001. No generated
credential values appear in this record. Native credentials remain unchanged.

An initial network-disabled image build from private staged source succeeded,
but its UID-10001 import check failed: Docker's plain `COPY` retained root-owned
0600 files and 0700 directories. That inert image was rejected before cutover.
The Dockerfile now explicitly copies the package with numeric owner 10001; the
replacement image must pass actual as-user imports and configuration validation
before replacing the live coordinator. This does not loosen host source modes.
