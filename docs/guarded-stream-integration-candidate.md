# Guarded stream integration candidate

This branch combines the accepted-copy, scheduler, guarded node, node-gate
schema, failure-hold, and cleanup-census candidates with the experimental
bounded mTLS upload path. It is an isolated test candidate, not a deployment
configuration or a claim of production readiness.

The streaming endpoint reserves aggregate spool bytes before receiving a file,
verifies its digest, and records the mutation intent before upload. Uncertain
uploads keep their spool and reservation. Confirmed uploads also currently
remain charged: there is no qualified lifecycle for releasing these temporary
copies. The default 20 GiB cap therefore cannot admit a 142.85 GiB dataset in
full. A direct NAS-to-main path, suitable capacity and processing/output
headroom must be proven before large-job use; the coordinator must not spool
the complete dataset on the cluster PC.

The test client and listener do not yet establish a production job-scoped
authority binding, connect the ordinary deployed Viewer scheduler to remote
streaming, or exclude direct engine, older binary, alternate gate path, or
in-process writers. Spool path/ledger integrity against same-user replacement,
orphan reconciliation, quotas across all writers, and safe release after result
acceptance are still open. The listener's bounded shutdown requires an actual
supervisor to wait for active operations. Guarded failure and cancellation
remain held until physical stop is independently established.

The updater also requires exact reviewed migrations for new receipt tables and
the additive columns present on the current live baseline. Neither this merge
nor local tests authorizes startup, migration, cleanup, or deployment.
