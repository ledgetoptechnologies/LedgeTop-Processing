# Disabled native plan-binding experiment

`NativePlanBindings` is an internal, unwired experiment. Its literal qualification
flag defaults to false; neither environment configuration nor an HTTP route
enables it. It binds proposed canonical plan bytes to an already authenticated
manifest binding and stores a separate SQLite record. It does not reserve disk,
accept objects, admit a job or allow dispatch. Its receipt explicitly reports
`engineEvidence: {state: unavailable}`, `capacityReady: false` and
`dispatchReady: false`.

Only the empty option list can be bound. The scalar validator exercises the
jointly proposed bool, string, null and big-endian binary64 shapes, but no
nonempty option is accepted: no engine-version/domain profile is qualified.
The empty selected-catalog digest identifies `[]`; it is not evidence of a
particular engine, version, configuration or default. Catalog discovery must be
healthy at first binding, while exact replay and status use the persisted
record even if discovery is unavailable. Catalog observation is not a physical
writer fence. Name, option order and the difference between null and empty
outputs are retained in the proposed plan bytes.

This is a provisional internal profile, not a frozen wire agreement. Its
unkeyed digests do not protect against a writer able to alter both data and
digests. Existing binaries do not recognize its table, and this draft does not
install a startup guard for that table: external old-writer exclusion, migration,
rollback and durable history retirement remain unqualified. The existing
manifest binding blocks legacy operations only in cooperating current code.

The process-exit test uses `os._exit` immediately after SQLite reports the bind
committed, then restarts the coordinator fixture and checks replay while catalog
discovery fails. It does not simulate power loss, prove filesystem durability,
or establish physical fencing.
