# TreeDar runtime changes and retained results

TreeDar controller journals keep their immutable configuration. Changing an image,
source pin, version or policy creates a separate controller journal under
`treedar-runtimes/`. A stable delivery journal retains old result manifests,
metadata acknowledgements and Viewer acceptance across those changes. Historical
reads do not construct old controllers or require their images and source files.

The runtime manager records the physical/storage binding separately from
immutable runtime epochs and per-job runtime pins. Each epoch includes the exact
image, source, engine version, controller journal and full configuration/policy
digest. Existing route descriptors keep their five-field runtime contract.
New starts must use the active epoch; retired jobs cannot invoke a replacement
controller. Retained results keep their original engine version.

An update activates during worker construction, before its listener and driver
threads start. The factory holds its state-root ownership lock and exact reader
session; activation additionally holds the canonical NodeGate. It checks:

- The gate is idle and still belongs to the expected node, engine and authority.
- Driver, route, delivery, controller, outbox and scratch histories account for
  the same jobs. Unknown, orphaned or unfinished intents block the switch.
- Every old execution has terminal proof, a delivered projection, an exact
  durable coordinator metadata acknowledgement and a terminal reservation.
- The candidate configuration and read-only host/image/source probes match.
  An unregistered candidate journal containing execution intents is refused.

Pending Viewer acceptance is allowed: it protects data, while engine completion
and metadata acknowledgement establish that the engine can change. Acceptance,
read pins, cleanup grace and object identity remain separate deletion gates.

One synchronous SQLite transaction publishes the new active pointer, epoch and
job backfill, along with its census receipt and legacy downgrade fence. A lost
commit response is resolved by rereading the exact pointer and descriptor; it
does not authorize another engine action or an unconditional overwrite.
Historical evidence is read without changing old journals or their clock state.

The original v1 binding is preserved exactly, including explicit absence for a
fresh managed database. Successful publication marks the canonical binding so a
pre-managed worker cannot silently restart against the old configuration.
Pointer, marker and preserved evidence must agree in both directions. A failed
publication leaves the old binding usable. Legacy backfill requires explicit
opt-in and exact closed evidence; missing history is never inferred.

Ordinary restart on the exact active runtime is different from activation. It
attaches to that runtime without requiring a closed migration census or an idle
reservation, allowing the existing driver/controller to recover a pending job.
It still validates ownership and runtime bindings, and uncertain engine starts
are not replayed.

Shutdown stops new reads and joins the driver before the final handler drain and
lease release. Direct factory closure also waits for the driver stage mutex.
Uncertain drain or close keeps ownership held. This prevents another factory
from migrating state while an old driver mutation remains in flight.

The complete container-update workflow must drain scheduling before stopping the
worker, qualify the intended image, start the updated worker, verify health and
then reopen its own maintenance plan. This component provides the runtime and
retained-result boundary; automatic registry detection, release qualification and
scheduled update supervision remain separate work. No runtime or capability is
enabled by importing these classes or by the source qualification tests.

The final source checkpoint passed all 688 isolated Linux tests with no skips.
The focused core and composed-factory suites cover old results/receipts after
upgrade, missing old image/source, new TreeDar and ODM jobs, pending same-runtime
restart with one total launch, candidate orphan intents, stale actors, unknown
commit responses, frozen pre-managed constructor refusal and blocked-driver
shutdown. Independent source review found no blocking issue. See
`evidence/reference-treedar-runtime-20261028.json`.

Actual main-PC algorithm/bootstrap qualification is separately recorded in
`evidence/main-treedar-bootstrap-20261028.json`. It does not exercise managed
migration on the live host, the NAS issuer or the Viewer handoff.
