# Native capacity metadata proposal (unwired)

This is a provisional, disabled internal object for cross-repository review;
it has no route, advertisement, negotiation, admission effect or production
qualification. Its exact fields are not yet an agreed wire contract.

All byte quantities are JavaScript-safe integer bytes. `input.legacyMaxTaskBytes`
is the configured cumulative ceiling for legacy uploads, including auxiliary
files; `input.manifestMaxTaskBytes` is the lesser of that value and the separate
20 GiB native-manifest ceiling. The manifest path remains disabled and does not
provide a receiving path. `request.maxMultipartBodyBytes` is the entire multipart wire-body ceiling
including framing. A separate part limit and object/auxiliary limits are unknown
in this proposal. Neither known ceiling indicates available disk space. Temporary
spooling is per part; `maxConcurrentBytes: null` is unknown, not an upper
bound. Expansion values for the coordinator task, engine workspace and result
are unknown. Resource roles list the coordinator spool, task input and archive
and the engine workspace and output separately, but each is null: neither
opaque stable identity, shared-device aliases nor live free bytes are established.
Environment, node, config revision, fingerprint and freshness are also unknown.
Null never means zero or unlimited. `admission: unknown` must fail closed; it
does not authorize a transfer. The existing task identity
reservation is not physical capacity allocation: the capacity reservation is
unavailable. These values do not establish admission or node readiness.

The producer reads the existing configured task ceiling and transport constant;
it changes neither. Coordination must settle units, field meaning, historical
versioning and unknown-value handling before any wire exposure. Dynamic free
space can change immediately, multiple roots may share one filesystem, and
external writers, retention and workload-dependent engine expansion remain
unaccounted. Non-null resource IDs and aliases require authenticated identity,
scope and freshness semantics; no resource identity is invented by this draft.
