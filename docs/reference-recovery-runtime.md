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
