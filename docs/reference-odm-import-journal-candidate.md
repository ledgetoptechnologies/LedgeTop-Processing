# Inactive reference-owned ODM import journal candidate

This isolated integration uses the reference Queue's own `jobs.sqlite3` and
holds its lifecycle lock while the existing first-proof journal opens its
transaction. It reads the reference job, prepared Viewer principal/task/attempt,
activation record, sealed internal result, durable Viewer transfer and signed
accepted-copy receipt in the same SQLite transaction that records the proof.
The configured principal, environment, issuer and signing key registries are
trusted installation inputs; they are never taken from the proof itself. Missing
records, changed bindings, changed pins or an unavailable Queue owner hold.

Only the ODM single-archive verifier is supported. Its model/output IDs and
inventory hashes remain signed Viewer assertions. This adds no HTTP route,
does not activate a job, perform a transfer, or authorize cleanup. Separate
v1/v2 proof tables remain independent with no supersession semantics.

The reference v2 activation and authenticated grant remain deliberately blocked.
Tests directly inject **synthetic** completed activation/copy records to exercise
database atomicity and restart; these records are not evidence of an executable
end-to-end path. The writer-fenced Viewer receipt, independently authenticated
principal-to-key configuration, durable task/job prebinding and current key
rotation policy still need real integration, shared contracts and review. No
claim is made that directory fsync proves all storage hardware power-loss cases.
The adapter inherits the journal's private-directory and POSIX sync requirements;
it does not claim Windows support. It also inherits the reference store's schema
and metadata assumptions and is not a general-purpose untrusted-database reader.
