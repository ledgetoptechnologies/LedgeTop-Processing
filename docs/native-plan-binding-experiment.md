# Disabled native plan-binding experiment

`NativePlanBindings` is an internal, unwired experiment. Its literal qualification
flag defaults to false; neither environment configuration nor an HTTP route
enables it. It binds proposed canonical plan bytes to an already authenticated
manifest binding and stores a separate SQLite record. It does not reserve disk,
accept objects, admit a job or allow dispatch. Its receipt explicitly reports
`engineEvidence: {state: unavailable}`, `capacityReady: false` and
`dispatchReady: false`.

The internal experiment accepts a conservative strict subset of bool, int,
float, enum and string options against the exact coordinator-selected catalog
definitions. It rejects coercion, null option values, JSON and unknown domains.
The domain examples are synthetic; no engine-version, binary, configuration or
effective-settings profile is qualified. The engine may cap or inject
max-concurrency. The receipt reports effective options unavailable. The
selected-catalog digest and persisted definitions are coordinator observations,
not evidence of a particular engine binary, configuration or default. Catalog
discovery must be healthy at first binding, while exact replay and status use
the persisted record even if discovery is unavailable. Catalog observation is
not a physical writer fence. Name, option order and the difference between null
and empty outputs are retained in the proposed plan bytes.

This is a provisional internal profile, not a frozen wire agreement. Its
unkeyed digests do not protect against a writer able to alter both data and
digests. Existing binaries do not recognize its table, and this draft does not
install a startup guard for that table: external old-writer exclusion, migration,
rollback and durable history retirement remain unqualified. The existing
manifest binding blocks legacy operations only in cooperating current code.

Read-only inspection of the installed .88 NodeODX 2.4.0 and .89 NodeODM 3.6.2
sources found byte-identical `filterOptions` sections (SHA-256
`2637a76165bb7c156a685e64b408c12f9e1e3c44599256f47b3e56ec5bd909ff`).
Those filters allow legacy coercions and can inject or cap max-concurrency;
this experiment intentionally rejects coercions. It also rejects zero for
`positive` and `negative` domains even though the inspected filter permits it.
Decimal domain bounds are interpreted as binary64, without claiming exact
real-number containment. The .88 live catalog had bool,
int, float, enum and string definitions with both recognized and JSON domains.
The .89 unauthenticated catalog request returned an error object, so its actual
catalog was not observed. The coordinator cannot independently attest the
observed engine source or configuration, and its catalog GET framing is not
qualified. These observations do not authorize this profile or execution.

The process-exit test uses `os._exit` immediately after SQLite reports the bind
committed, then restarts the coordinator fixture and checks replay while catalog
discovery fails. It does not simulate power loss, prove filesystem durability,
or establish physical fencing.
