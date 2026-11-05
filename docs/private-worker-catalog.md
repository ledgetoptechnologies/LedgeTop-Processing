# Dynamic private worker catalog

Reference processing discovery now uses the existing per-node worker HTTPS
origin, result-reader bearer and coordinator client CA. Native NodeODM remains
at the worker's configured loopback origin; no engine token is sent to the
coordinator. This change does not alter installed bindings, firewall rules,
native ingress or deployment configuration.

The worker's existing listener adds only `POST /ltds/worker/v1/catalog`, with the
closed read-only request `{schemaVersion: 1, nonce: <UUID>}`. It authenticates the
existing fixed node/worker result-reader principal before reading engine
metadata. It returns a nonce-bound identity, validated `/info` projection and
bounded `/options` definitions. Unknown native routes, including job submission,
remain unavailable. Raw or URL-encoded native/result credentials reflected in
engine metadata cause a closed error.

Catalog reads do not require an idle engine, acquire the Driver/NodeGate lock,
create a job, or change processing state. Idle and busy engines both expose
fresh flags, `maxImages` and `taskQueueCount`. Local Docker identity is checked
before and after metadata reads, and the observed engine version must match the
worker's current pin. Engine changes still require their existing update flow.

The coordinator validates nonce, authority, worker instance and configured
physical binding. It reuses the exact option intersection and fingerprint
algorithm. A worker claim refresh obtains options and capacities from one
fresh fleet observation; failed nodes remain unavailable independently, with
no fallback to directly querying native engine endpoints. Ordered node limits
and the existing one-job-per-node scheduler remain authoritative.

Workers must include the relay before the updated coordinator uses its default
discovery path. Older workers returning 404 stay unavailable. Readiness and
source/policy pins must be regenerated and reviewed for the new immutable
candidate; the module does not authorize activation. Native ingress may be
restricted only after the installed relay is separately qualified.
