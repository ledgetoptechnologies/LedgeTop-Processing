# Canonical private ODM worker service

The executable service is the existing host entrypoint
`python -m ops.reference_worker_daemon --config <owner-private.json>`. It uses
`load_config` → `build_worker` → `WorkerService`; no second controller, queue or
execution wrapper is introduced. Importing it does not start anything. No
installed worker or private configuration is changed by this source update.

An optional, closed private configuration field selects the new service profile:

```json
"serviceProfile": {"schemaVersion": 1, "mode": "odm_only"}
```

The profile is absent by default. It permits the configured cluster or main node
and rejects `executionRouting`, including a TreeDar-null router. Thus this profile
does not install TreeDar, combined execution, a metadata issuer, source custody
or Gaussian splats. Legacy configurations keep their existing behavior, and the
new health route remains absent. The profile does not change scheduling policy
or permit a worker to steal another node's jobs.

`--check-config` validates the existing owner-private configuration and TLS file
metadata, then exits without opening worker state, acquiring a gate, creating a
listener or processing a job. Output is a fixed safe message; configuration
values, tokens and paths are never printed. Credentials/TLS must come from the
already reviewed private setup; this service creates none.

## Actual execution and lifecycle

The canonical factory owns the real DaemonStateLock and ReaderSession. It checks
the preprovisioned physical NodeGate's authority/node/origin/generation, a fixed
literal loopback backend and exact configured engine version. It assembles the
actual ReferenceDriver, NativeEngine HTTP client, BrokerClient, ScratchLedger,
worker/failure outboxes, result readers and authenticated TLS result server.
Every new claim refreshes the engine option catalog through the existing path.
Sharing, allowed environments and cleanup remain explicit existing private
configuration decisions; no new source-deletion permission is added.

WorkerService drives the real driver on its own thread, retains the established
retry/backoff behavior, and serves results concurrently on its TLS thread.
Shutdown stops new reads, joins handlers and the last driver stage, then releases
only the actual owned reader/state leases. A timeout or uncertain close is not
permission to abandon work or unlock another owner. An in-flight job must finish
its bounded stage; operators still use the reviewed drain/update procedure.

The original input grant authorizes transfer. Once actual ODM execution starts,
existing execution collection, durable outbox delivery and acceptance do not
renew that grant or require a new payload read. An expired grant still blocks
pre-start/fetch; the service does not extend TTLs, reset byte budgets, create a
new claim, or reseal provenance to pass a later check. Heavy-slot settlement and
retained-result acceptance/cleanup keep their separate existing semantics.

## Authenticated, bounded health

For the explicit profile only, the private TLS listener adds
`GET /ltds/worker/v1/health`. It uses the existing configured result-reader role;
missing credentials and the distinct worker-claim credential receive401. It is
read-only, rejects other methods/bodies, and delegates every result route to the
unchanged result API. It supplies no job/engine/control actions.

The response has `scope: "service_liveness"`: actual service state and thread
liveness, frozen configured infrastructure identity, and aggregate counts only.
`identityScope: "configured_service"` labels nodeId, authorityId, workerId,
workerInstanceId, engineId, gateGeneration and engineVersion from the validated
factory configuration. These bounded values cannot contain paths; they are
copied before later caller mutations. They allow an explicit configuration
mismatch to hold, not to infer offline fallback or fresh native/Docker admission.
No client/job IDs,
project data, claim tokens, credentials, filesystem paths or upstream exception
details or credential hashes are returned. Before the actual WorkerService
binds, it returns503.

The work snapshot requires the actual state owner and a nonblocking driver lock.
A busy driver reports `workState: "busy"` and unknown counts, never invented
emptiness. Otherwise a SQL-only immutable read uses no writable driver
transaction, rejects sidecars/oversized history, bounds SQLite progress, and
reads only grouped stages/counts. `empty` means no retained driver rows; `idle`
means only completed history; `pending_work` and `awaiting_acceptance` describe
actual retained states. Invalid state/ownership/history fails closed with503.
Health does not validate an artifact or attest coordinator/backend admission
readiness, container identity, source authority or scientific success.

## Synthetic composition proof

`tests.test_reference_worker_service.WorkerServiceTests` exercises the actual
factory and TLS listener: closed/default-off profile, configuration-only CLI,
empty/busy and pre-start health, role isolation, unchanged read-only bytes,
exclusive owner and joined shutdown/reopen. No fake successful processing is
used to establish health.

The separately opted-in `CanonicalWorkerJointTests` reuse the unchanged pinned
Viewer867 Node provider/broker/acceptance fixture and construct a **new canonical
factory worker**. The transport fixture's old manual driver remains unstepped
and its row count stays zero. All actual processing steps occur on the canonical
WorkerService thread; tests never call its Driver.step manually.

The actual chain includes TLS coordinator APIs, Node transfer broker, native
HTTP fixture, factory driver/ledger/gate/outbox, canonical TLS result server,
coordinator result proxy, durable Node acceptance, and only then result/native
cleanup. The Node fixture declares `published:false`; the `abc` photo and empty
ZIP establish transport/durability only, not photogrammetry or Viewer publication.
Both original worker and Viewer source bytes remain retained.

The outage case advances only worker/source-grant validation clocks past the
original unchanged grant's expiry **after actual initialize/upload/commit**.
It stops the actual coordinator TLS listener and Queue, lets the synthetic native
backend become terminal, and proves automatic service polling seals/retains the
same work while repeated deliveries fail. Reopening the same authority/endpoint
delivers the identical outbox under the original claim, reservation and gate
receipt, with no second engine start or payload download. Node accepts durably,
then cleanup proceeds. Source broker accounting stays one nine-byte budget with
three bytes consumed; no TTL/budget/identity is rewritten. Only the outbox retry
clock advances separately; coordinator and grant records are not edited.

Run the joint cases separately in the already cached Node24/Python image with
the same read-only verified Viewer867 snapshot and
`VIEWER_REFERENCE_ROOT=/viewer` used by `docs/reference-joint-flow.md`.
Ordinary Python-only runs explicitly skip those two Node cases. Record actual
source pins, image/snapshot pins and separate results in qualification receipts.

## Activation and later pipeline gates

The coordinator image currently copies `processing/` only. It cannot run this
`ops` host entrypoint unchanged. A separate worker image/package must include
the reviewed ops dependency closure; none is built or installed here. Actual
host-daemon deployment also needs exact node/container/image/config bindings,
approved private worker/result credentials and TLS, reachable configured
coordinator/broker/result origins, owned state/scratch, and the canonical gate.
Existing deployed canonical gates are UID1000; a worker container must qualify
its UID and the same gate mount/network namespace rather than create a private
replacement. No main-node compute caps or backend recipe are changed here.

Loopback/version/gate checks qualify this source/synthetic assembly. They do not
attest the actual Docker/Compose image/CID recipe; activation must qualify those
identities separately. Fresh genuine-LiDAR stage admission still requires the
actual authenticated acquisition/custody/metadata issuer contract. A new combined
TreeDar stage after a long ODM run cannot use an expired transfer grant as fresh
metadata authority. That separate gate stays closed; this ODM-only service does
not fake an issuer, borrow a caller boolean, replace original receipts or enable
unqualified final transports.
