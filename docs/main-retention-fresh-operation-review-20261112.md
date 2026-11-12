# Fresh main-retention operation review packet

This is a command/input review packet, not an execution record. Runtime source
comes from commit `9ef04bafd096aa89007dc053cc4f5181537ad82c`. The prior e30 archive
executed many CRLF files, which differ from the intended Git LF manifest hashes.
That historical suite is not LF byte qualification. Root subsequently verified
all 52 Python members of canonical LF archive9c1b16be against the complete source
map, with zero missing/extra/diffs, and ran a fresh suite:133 cases,128 passed,
5 expected skips. Output SHA256:
`ea1cb35fbca4f3f761af9922cb336be8ba16311cd4ad3fa731e002d66e0adb3e`.
This resolves source-byte qualification, not the separate live-operation gates.
The subsequent mount-order fix is runtime07c2e029, qualified from canonical LF
archive8855a3d8:51 Python files remain exact9efLF, and the combined helper is
SHA256 `9302ff0b1a2786c12aa94c6269e915025fc0b8823e2f9875ec3f21f0c912d594`.
Root's separate135-case suite passed130 with5expected skips; independent review
passed the order-only mount fingerprint change. All fresh-operation gates remain.
Documentation successors do not change its Python inventory. The draft manifest
`docs/evidence/main-retention-live-qualification-DRAFT-20261112.json` proposes
six gate-to-receipt bindings and retains `passed=false`, `completedGates=[]`.
Root and independent review must approve those bindings before issuing a separate
root-private approved manifest. Do not pass the draft to a live preparation.

The native receipts establish the restricted root0/gid0/noattrs container-layer
profile and GNUtar bind-data ACL/xattr restore. They do not establish generic
Docker export/copy attribute fidelity, live installation, real GPU processing,
or acceptance of a client result. The actual target GPU device query was reported
by root separately and is explicitly labeled as such in the draft.

## Private input schema

The inputs JSON has exactly two top-level keys:

* `readOnlyObservers`: map from each independently approved full container ID to
  `{metricsOnlyReviewed: true, imageId, applicationConfigSha256,
  hostConfigSha256, mountsSha256}`. Hashes bind complete canonical inspected
  objects, not selected command/environment fields. `mountsSha256` uses
  `mounts_fingerprint`: sort the complete mount dictionaries by their full
  canonical JSON bytes, then SHA256 the canonical JSON array. Only array order
  is normalized; all fields (including unknown fields) and repeated entries are
  preserved. Do not hash Docker's nondeterministic raw mount-array order.
  Both preparation and cleanup verify the exact seal and absence of privileged/
  SYS_ADMIN authority before exempting its RO bind/volume mounts from generic
  overlap resolution. Unsealed RO proc/sys/nsfs and all RW unsupported sources
  still hold. Valid opaque namespace roots remain opaque mountinfo identities;
  they never become ordinary filesystem paths.
  Do not print raw objects or
  infer an exemption from a container name. An empty map approves no observers.
* `cleanupTemplates`: nonempty list with one template per authorized engine task,
  without duplicates. Only M4E `c4e4121a-7848-4290-a3c9-c0537699bbb1` and Tim
  `0f7e6743-63a2-4ff0-91f8-b141569d1a3e` are allowed. Todd is excluded.

Each cleanup template supplies `schemaVersion:1`, fresh `authorityId`, `nodeId`
`main`, `engineId`, original `containerId`, original `imageId`, `viewerTaskId`,
integer `generation`, `jobId`, `engineTaskId`, `nodeGateGeneration`,
`nativeStatus:40`, `coordinatorArchiveDurable:true`,
`finalInventoryComplete:true`, `futureCacheConsumers:[]`, `selectedCaches`,
`archiveRelativePath`, `archiveSha256`, and complete `finalInventory`.
Each final inventory item supplies native `relativePath`, optional differing
ZIP `archivePath`, `byteSize` and `sha256`. Capture these from verified actual
receipts and files; do not substitute an ACK alone for archive/file proof.
`selectedCaches` is a unique nonempty subset of `opensfm/features`,
`opensfm/matches`, `opensfm/undistorted/images`.

The controller injects the new `operationId`, owned `maintenancePlanId`,
`maintenanceRevision`, `maintenanceTargetImage` and approved observer map at
plan time. It rechecks current authenticated authority, successful bound job,
owned reservation, stopped original container and readers. The operator must
review the resulting full plans before the cache-application phase.

Credentials contain the existing distinct `PROCESSING_API_TOKEN` and
`PROCESSING_PROTOTYPE_OPERATOR_TOKEN` values. Keep credentials, approved
qualification, inputs and all operation receipts root-owned mode600, single-link
and without symlinks. The operation directory is new root-owned mode700.
No credentials, client paths or raw container command/environment arrays belong
in this packet or public PR.

## Explicit command sequence for review

These are shell templates, not commands that have run. Root must substitute the
exact reviewed source/private paths, issue one fresh UUID and review all hashes.
`OP` must be `/var/lib/lt-main-combined-` plus that UUID's canonical lowercase
32-character hex. It must not exist before preparation. Never use an old
operation directory or rerun a consumed stage.

```sh
# Set these only to independently reviewed absolute paths and canonical JSON SHAs.
SOURCE=/var/lib/REVIEWED_SOURCE_BUNDLE
OP=/var/lib/lt-main-combined-NEW_UUID_HEX
CREDS=/var/lib/REVIEWED_PRIVATE_CREDENTIALS
QUAL=/var/lib/REVIEWED_APPROVED_QUALIFICATION.json
INPUTS=/var/lib/REVIEWED_INPUTS.json
QUAL_SHA=REVIEWED_CANONICAL_QUALIFICATION_SHA256
INPUTS_SHA=REVIEWED_CANONICAL_INPUTS_SHA256

# Default is static: no Runtime construction, source scan, API or Docker reads.
(cd "$SOURCE" && python3 -B -m ops.combined_main_retention)

# ONE preparation only; creates fresh private receipts, schedules no maintenance.
(cd "$SOURCE" && python3 -B -m ops.combined_main_retention --prepare \
  --credentials "$CREDS" --receipt "$OP" \
  --qualification "$QUAL" --qualification-sha256 "$QUAL_SHA" \
  --inputs "$INPUTS" --inputs-sha256 "$INPUTS_SHA")

# Root reviews the new preparation and uses the returned canonical digest.
PREP_SHA=REVIEWED_RETURNED_PREPARATION_SHA256
(cd "$SOURCE" && python3 -B -m ops.combined_main_retention --stage begin \
  --credentials "$CREDS" --receipt "$OP" --prepared-sha256 "$PREP_SHA")

# After clean stop and sealed original TMP proof: writes plans, not cache deletion.
(cd "$SOURCE" && python3 -B -m ops.combined_main_retention --stage plan-cleanup \
  --credentials "$CREDS" --receipt "$OP" --prepared-sha256 "$PREP_SHA")

# Separate review checks exact files, readers, archive proof and planned capacity.
CLEANUP_SHA=REVIEWED_RETURNED_CLEANUP_SET_SHA256
(cd "$SOURCE" && python3 -B -m ops.combined_main_retention --stage apply-cleanup \
  --credentials "$CREDS" --receipt "$OP" --prepared-sha256 "$PREP_SHA" \
  --cleanup-set-sha256 "$CLEANUP_SHA")

# Full capacity/backup/export/retained recreation/pin/owned admission checks remain.
(cd "$SOURCE" && python3 -B -m ops.combined_main_retention --stage finish \
  --credentials "$CREDS" --receipt "$OP" --prepared-sha256 "$PREP_SHA")
```

CLI JSON digests are SHA256 of canonical `sort_keys=True,separators=(',',':')`
UTF-8 JSON, not the possibly pretty-printed file's raw-byte hash. Source hashes
are raw executing Python file bytes and require the exact Linux LF checkout.
Private files still retain their separately captured byte hashes for custody.

Before `begin`, root must recheck live role/configuration, terminal jobs and
accepted retained outputs, no active uploads/readers, known launchers fenced,
approved observers, native expiry timing and exact command ownership. `begin`
revalidates idle claims, schedules and begins its own reservation, fences restart,
closes ingress only with owned UPDATING authority, captures original TMP and
stops cleanly. Unknown tracked readers, nonroot/attributed TMP, claim races,
capacity deficits or phase errors hold for independent inspection. No automatic
KILL, replay, old-image start or database restore is an authorized fallback.
