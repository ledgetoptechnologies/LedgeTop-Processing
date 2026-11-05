# Two-node Viewer pipeline acceptance

Test-only candidate based on Processing
`2654915902a83f9f9bf2f7b8c65363828025de5f`. Production code and admission guards
are unchanged. Three independent actual Viewer SQLite attempts use the production
credential/environment through the real V2 source producer, protected Reference
TLS listener, source broker, workers, result proxy, signed copy receipt and Viewer
ingest paths. Only the ODM execution edge is a deterministic synthetic transport.
Each input is one 30-byte synthetic photo payload, with a generated valid small
OBJ/GLB/EPT result archive. No client photos or NAS paths are used.

The new test verifies:

- The first small production job starts on cluster; while its processing slot is
  occupied, the next small job starts on main. Both real NodeGates retain their
  independent physical reservations simultaneously.
- A third job remains queued with no assigned node. Protected status and log
  requests bind the correct production job, attempt and node.
- Cluster result-metadata delivery receives synthetic HTTP 503 responses while
  main completes its actual Viewer acceptance and ingest. Cluster retains its
  durable outbox and physical reservation; the queued third job can then start
  on the now-free main node.
- Cluster later retries the identical persisted metadata envelope. All three
  attempts durably copy the exact archive into their own Viewer-managed paths,
  recover a lost Viewer ACK, retain independent storage/acceptance receipt IDs,
  ingest the expected assets and complete without duplicate engine initialization.

Existing JS fixtures accept an optional environment/principal while retaining
staging defaults. The existing roundtrip fixture now explicitly creates its
private temporary coordinator root with mode 0700, satisfying the unchanged
freeze guard independently of the shell umask.

## Qualification

Final bounded Linux run: **2/2 passed, no skips, 37.281 seconds**:

```
tests.test_two_node_viewer_pipeline
tests.test_viewer_result_v2_roundtrip.ViewerResultV2RoundTripTests.test_actual_viewer_source_intake_result_copy_lost_ack_and_worker_cleanup
```

The environment reused frozen Viewer
`6718d1beca0f7fbf8cbaae2bbec14326e1e053bb`, Node 24.22.2 and the qualified
production dependency cache. The raw cached lock hash is
`a0eaead118642843905756fcfe3f53b5336b5a437e22a2f80677d6f06087fb12`; the
LF-exported Viewer lock hash is
`b528bce371d92ed5ce3ddc8eff726ab9ad6ecc58ced27ef8d1070c232d083363`.
Both locks were verified equal as JSON and after CRLF-to-LF normalization.
Runtime image:
`sha256:57f01267c88439d872f6782b54511e314614a76145e32cfe2fad003e5ca0fac8`.
The runner was limited to one CPU/2 GiB, read-only code/dependency mounts,
network none except container loopback, and an owned temporary volume.
Task-owned listeners/processes exited and the container/volume were removed.

An initial diagnostic corrected an exact empty-next DTO assertion to include
both `job: null` and `execution: null`. A subsequent default-staging compatibility
check exposed the old temporary-root mode; only that fixture's mkdir mode was
corrected. The final complete two-case selection passed. PowerShell 5's Docker
label-template quoting required JSON-based ownership inspection for cleanup;
the initially retained owned volume was verified and removed.

This proves synthetic orchestration, transport, independent custody and actual
Viewer ingest. The native processing tasks are simulated, both workers run in
one bounded test container, and imported models remain staged with required
derivatives pending. Installed two-PC operation, real photogrammetry, publication
and large-dataset capacity are separate acceptance gates.
