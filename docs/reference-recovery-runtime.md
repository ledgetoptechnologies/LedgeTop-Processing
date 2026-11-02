# Optional original-claim recovery configuration

The private reference runtime configuration retains its existing schema version
1 unchanged. Schema 1 has no `recoveryBindings` field and does not construct a
claim recovery service or create its startup/receipt tables. Existing four-field
`ReferenceSettings` callers retain `recovery_bindings=None`.

Schema version 2 explicitly opts into the existing original-claim recovery
service on its supported POSIX coordinator host. It requires every schema 1
field and this additional closed map:

```json
{
  "schemaVersion": 2,
  "recoveryBindings": {
    "cluster": {
      "engineId": "physical-cluster",
      "authorityId": "11111111-1111-4111-8111-111111111111",
      "coordinatorOrigin": "http://127.0.0.1:18080",
      "gateGeneration": 1,
      "outputBudget": 1000
    },
    "main": {
      "engineId": "physical-main",
      "authorityId": "11111111-1111-4111-8111-111111111111",
      "coordinatorOrigin": "http://127.0.0.1:18080",
      "gateGeneration": 1,
      "outputBudget": 1000
    }
  }
}
```

This is a fragment with invented identities, not a complete deployable config.
The existing listener, Viewer/worker credentials and stream budget remain
required. The recovery map must exactly cover the configured queue nodes. Each
entry has only the five shown fields. IDs, canonical gate origin, positive safe
integer generation and whole worker output reservation are validated into frozen
`RecoveryNodeBinding` instances while loading the private file, before Queue is
opened. All nodes must name the same single coordinator authority. Boolean/float
integers, unknown fields, partial coverage and a recovery
field under schema 1 are rejected.

## Schema version 3: pin Viewer accepted-copy receipt issuers

Schema 3 retains every schema 2 field and adds a closed `viewerReceiptKeys`
map. It contains only public Ed25519 PEM keys, keyed by the exact Viewer key ID
and bound to its issuer ID. The Processing coordinator validates each key at
startup and verifies signed accepted-copy receipts before persisting the
legacy acceptance and signed sibling in one database transaction. Workers use
the same public pins before recording local acceptance, and engine-cache
cleanup requires signed receipts that cover the complete result-object closure.
Schema 1 and 2 remain readable but have no receipt pins; their unsigned
acceptances cannot authorize this cleanup.

```json
{
  "schemaVersion": 3,
  "viewerReceiptKeys": {
    "viewer-prod-key-2026": {
      "issuerId": "ledgetop-viewer-production",
      "publicKeyPem": "-----BEGIN PUBLIC KEY-----\\n...\\n-----END PUBLIC KEY-----\\n"
    }
  }
}
```

This is an abbreviated fragment, not a deployable configuration. Schema 3
requires the complete schema 1 listener, credential, worker and stream budget
fields; `recoveryBindings` remains optional and is validated when present. Do not place a private signing key in Processing;
the corresponding signer remains in Viewer. An empty key map is accepted for
configurations that do not use signed result cleanup, but it leaves that cleanup
held.

## Schema version 4: durable outbound Viewer acceptance

Schema 4 retains the schema 3 fields and adds `viewerAcceptance`, an environment
map with one or both of `production` and `staging`. Each entry has exactly an
HTTPS Viewer `origin`, the Viewer service-auth `keyId`, and an absolute
`secretFile` path. The referenced key file must be a single-link regular file
owned and private to the runtime account on POSIX; Windows ACL privacy remains
the provisioner's responsibility. The key is read into process memory only and
is never serialized in statuses or delivery records. Distinct service keys are
required for each configured environment, and the public Ed25519 receipt pins
must be non-empty.

When configured, `ReferenceRuntime` runs one bounded background transfer at a
time. It persists a stable acceptance envelope and lease in the same SQLite
database as job metadata, resumes Viewer object offsets after interruption,
replays commit/status after a lost response, verifies the signed accepted-copy
receipt and only then stores the receipt pair. It streams result bytes through
the coordinator's existing `ResultProxy`; it does not create another complete
local archive copy. If Viewer is unavailable, transfer metadata and worker
source results remain for retry. Production and staging use their separately
configured Viewer origins and credentials.

This sender is an additive source capability: schemas 1 through 3 remain
readable and do not activate outbound transfer. Schema 4 does not itself
configure a deployed host. The current Viewer acceptance route accepts only
single-engine ODM results; TreeDar manifests and roles are held by the sender
until the Viewer-side result contract explicitly supports them. Neither
Gaussian splats nor TreeDar is enabled by this setting.

```json
{
  "schemaVersion": 4,
  "viewerReceiptKeys": {
    "viewer-prod-key-2026": {
      "issuerId": "ledgetop-viewer-production",
      "publicKeyPem": "-----BEGIN PUBLIC KEY-----\\n...\\n-----END PUBLIC KEY-----\\n"
    }
  },
  "viewerAcceptance": {
    "production": {
      "origin": "https://viewer.example",
      "keyId": "processing-transfer-v1",
      "secretFile": "/private/processing/viewer-production.key"
    }
  }
}
```

This is an abbreviated fragment, not a deployable configuration. The complete
schema 1 listener, credentials, workers and stream budget remain required.

Bindings are operator-provisioned expectations about existing physical nodes;
requests cannot choose them. The authority must be the actual persistent Queue
authority already shared by the nodes' gates. The origin is the gate's configured
coordinator origin, which may differ from the TLS recovery API origin. Generation
and output budget must match the original worker's physical fence and complete
reservation. This configuration neither creates those local resources nor proves
their live state. Workers still need fresh ParentGuard and actual local evidence
before execution resumes; historical recovery receipts are not permission.

## Initialization order

`app.make_handler` loads and validates the private file before constructing Queue.
After Queue exists, `ReferenceRuntime` rechecks exact node coverage, binding types
and every authority against that actual Queue before TLS contexts, service
assembly or listener creation. A mismatch aborts startup; `make_handler` closes
its already-created Viewer service and Queue. No authority is rewritten to make
the configuration match.

`ReferenceControlAPI` passes the optional binding map to `ReferenceWorkerAPI`
during construction, which constructs `ClaimRecoveryService` before the assembled
API is returned. Its startup epoch/restart hold transaction therefore completes
before any new request can claim work through this API. Reconstructing Runtime
or Control against the same owned Queue epoch does not re-hold a resumed job;
the service verifies the same configured binding digest for that epoch.

Both the Viewer dispatcher and reference listener remain unstarted during
`make_handler`. `app.main` constructs the native HTTP server after that function
returns. `CoordinatorService.run` starts the dispatcher and listeners afterward.
The recovery configuration/factory consequently finishes before those background
services admit or claim work. This order is exercised with the real Queue and
configured API routes; tests mock certificate loading only for synthetic TLS
fixture files.

No configuration files, credentials, certificates, capabilities, outer result
protocols or deployed services are changed by this source addition. Recovery
remains disabled unless an operator explicitly provisions a valid schema 2 file.
