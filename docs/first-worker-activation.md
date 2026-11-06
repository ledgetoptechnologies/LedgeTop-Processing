# Guarded first-worker activation

`ops.first_worker_activation` supplies a controller-owned journal and concrete
read-only/actuator adapters. It runs only after the **same existing cutover** has
sealed its bootstrap gate. It does not create a scheduler, open admission,
enable a unit, stop an old worker, replace data, generate credentials or widen
root privileges. Installation remains a separate prerequisite.

The exact existing operation directory holds an immutable registration, a
bounded activation journal and immutable effect observations per reviewed node.
Both the existing host operation lock and main cutover journal lock serialize
each action. Reload and start intents are durable before the corresponding
tracked SSH dispatch. An uncertain result is inspection-only, including after
controller death. A missing journal after registration, missing accepted proof,
corrupt binding or unretired remote helper holds progress. Matching bytes never
authorize another start. Known accepted observations survive a lost journal ACK
through their separate immutable proofs and fresh verification.

The actual main driver refuses conflicting progression while activation is
pending, including before rollback changes its direction metadata. Workers from
the installer's reserved `/opt/ledgetop-processing-reference/<UUID>/source`
namespace require a completed activation receipt before bootstrap stop or final
readiness. Preexisting managed workers keep the existing authenticated read-only
probe behavior; their absence of an activation file grants no first-start
authority. Final readiness binds the accepted activation receipt and unchanged
actual process/instance/source/config/engine/catalog facts to a fresh TLS nonce.

## Actual observations

Before service effects, the controller verifies the current bootstrap container
recipe and writer census, authenticated frozen authority/startup/plan/owner,
zero active operations, and actual main/external kernel flocks on the original
physical files. Bootstrap has no SourceReaders registry: its authenticated
`absent` observation is deliberately distinguished from final reader custody.

The narrow SSH helper runs through the reviewed interpreter with `-I -B`, exact
source footprint and existing service UID. Source/config/unit/interpreter and
versioned installed-tree pins are checked fresh, including dependency bytes,
`include-system-site-packages=false`, private config/unit descriptors and opaque
CA/certificate/key fingerprints. The helper checks actual host/kernel boot,
manager startup environment, inactive/disabled unit, loaded systemd policy,
original physical Gate lock, exact native image/loopback binding, current options,
zero `/info` count **and** a bounded stable `/task/list`/task-info census.
Declared retained terminal tasks are quarantined by exact native container/image,
task UUID, terminal status and complete task-info hash; no result is removed or
treated as Viewer-accepted. Active, unknown, undeclared or changing tasks hold.

Native ingress is a separate exact policy input: private root, gateway config
hash, closed-state hash, generated closed nginx config hash and exact proxy ID.
The helper reuses existing ingress inspection, proves saved old nginx workers
are drained, checks the current503 response and binds the exact backend ID.
It does not close/open/reload ingress. No queued job or private engine task is
created by these observations. The original Gate lock is opened without create
or chmod and released before the new factory must acquire it; frozen admission,
closed ingress and verified historical producer closure remain required.

Only service-user `daemon-reload` (necessarily manager-wide, without starting or
enabling other units) and the existing
`SystemdWorkerControl.command('start')` are actuators. There is no sudo, enable,
stop, reboot or root-worker command. Each command uses an existing-key,
host-key-verified, identity-tracked SSH helper with exact request/source binding
and actual helper retirement proof. Readiness reuses the existing authenticated
TLS probe without entering managed handover. The readiness DTO's `bootId` is an
API-instance UUID; the Linux kernel boot identity is checked separately.

## Closed receipt inputs and explicit command

The new install-only receipt adds `installedTreeSchemaVersion:1` and an opaque
`installedTreeSha256`. Existing receipts without these fields retain their
original meaning and cannot authorize this first-start path. No missing pin is
filled from an already running worker. Private certificate/key bytes and token
values never enter a manifest, receipt, log or command argument.

```text
python -I -B -m ops.first_worker_activation --plan <existing-private-plan> --bundle <existing-private-bundle> --receipt <same-operation-directory> --worker-manifest <private-manifest> --install-receipt <private-install-receipt> --ingress-policy <private-policy> --native-inventory <private-quarantine-inventory> --node <reviewed-node> --action prepare
```

Use the existing reviewed-source execution mechanism for isolated Python; the
example assumes the module is installed in that mechanism. Actions `advance`
and `inspect` are also explicit and operate on the same inputs. There is no
implicit apply loop, approval flag, token argument or fallback worker identity.
The main driver's existing final-readiness/resume path is the sole authority
that may reopen the coordinator.

The `pilotClosureSha256` qualification must contain the exact typed historical
receipt validated by `ops.historical_pilot_closure`, covering all three retained
operations and original source/guard/exec-session/terminal records. Its opaque
retained-file manifest must match both the accepted full baseline and a fresh
small no-follow read. The original private Docker PID namespace must be stopped.
The failed e8 producer's terminal exit1 proves local lifecycle termination, not
successful data processing. No arbitrary `pilotClosed` boolean is accepted.

This patch is **not installed or connected-host qualified**. Portable synthetic
boundary tests and static source checks do not prove real SSH, user systemd,
native ingress or first-worker startup. Linux durable-journal/helper tests and a
bounded actual activation qualification remain required before live use. On any
uncertain outcome the coordinator remains frozen and all data/receipts remain.
