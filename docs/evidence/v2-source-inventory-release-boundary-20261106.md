# V2 source inventory release boundary

This review compares Processing `c87d6ec1935da6a58d3c39c92228ff1171da8f34` with Viewer `60a8cb86d8ab4493e449061b190763d36e93914c`. It is source review, not proof of installed configuration or a successful job.

## Selected route and scope

The Viewer source-intake worker is wired conditionally: `apps/viewer/server/worker.js` constructs `PreparedSubmissionOutbox` and `SourceIntakeClientV2` when reference source intake is enabled, and `processingWorker.js` sends a pinned coordinator through `processSubmitReferenceV2`. Processing `processing/reference_api.py` exposes `/ltds/v2/capabilities` and `/ltds/v2/jobs` subject to configured authority and intake. The Viewer's durable internal intent uses a v3-shaped prepared envelope; that does not make a separate Processing V3 HTTP submission, activation, result, or accepted-copy API active. The Viewer `PREPARED_SUBMISSION_OUTBOX_DRAFT.md` wording that the component is unmounted is stale for this selected V2 worker wiring and must not be used to describe the whole V2 route as dormant.

The last observed Viewer container had source intake disabled and no corresponding authority/signing configuration in that container environment. The current coordinator has not been shown to have the required NAS spool mount. No conclusion follows about keys stored elsewhere. These checks do not establish release readiness.

## Inventory limitation and bounded pilot

Viewer `coordinatorPipelineManifest` maps each selected source object's name to `path.basename(relativePath)` and rejects duplicate submitted names case-insensitively. `referenceProtocol.js` enforces a basename-only manifest. Preclaim source attestation checks source IDs, original paths, sizes and hashes against the held snapshot and records an input inventory digest, but its selected Processing object names are also basenames. Thus unique names from nested folders can be flattened for engine submission; equal basenames in separate folders are rejected. Original path evidence in the Viewer snapshot is not an end-to-end path-preserving Processing/engine contract.

A restricted, no-GCP photo pilot can proceed only after independently confirming the selected files have supported roles, case-insensitively unique submitted basenames and unchanged bytes, and after the installed route, storage and safety gates are met. Inform the operator that engine inputs are flattened; do not imply arbitrary nested source inventories are preserved. This is a constraint for a narrowly qualified pilot, not acceptance of the full inventory requirement. The Viewer currently sets `treedar.requested` and `gaussian_splat.requested` false in this route; neither is proven by the photo pilot.

## Release criteria beyond that pilot

- Agree and implement a versioned Viewer-to-Processing-to-engine inventory mapping that preserves dataset and file IDs, exact relative paths, roles, sizes and hashes, including nested paths and duplicate basenames without overwriting, silent renaming or traversal. Define and retain a reversible engine filename mapping if an engine requires flat names.
- Prove the exact mapping and returned provenance survive submit replay, queue/restart, source transfer, processing, accepted-result copy, and recovery; negative tests must cover case collisions, duplicate basenames, unsafe paths, changed source bytes and unauthorized or stale bindings.
- Bind each GCP run to the immutable attempt's raw source CSV IDs, paths and hashes; selected image IDs and paths, dimensions and correspondences; CRS, height-source choice and datum; exact generated ODM bytes, count and hash. Retain the original CSVs and reject missing, stale, ambiguous or over-limit evidence. Neither reinterpretation nor a silent datum substitution is acceptable. NAVD88 conversion remains unqualified until supported transformation and reference data are verified.
- Test Viewer and Processing transfer and metadata limits together; hold inputs exceeding either side's agreed bounds rather than truncating. Make redundant spool copies eligible for cleanup only after Viewer outputs are verified ready plus the agreed 24-hour retention, with cleanup disabled during the first pilot.
- Keep the separate V3 API and Gaussian splats disabled until their complete cross-service contracts and failure paths are implemented and qualified. Complete genuine-LiDAR TreeDar routing and output acceptance separately; a photo-only success does not qualify TreeDar.

The broader installed gates remain: user-controlled TrueNAS access and durability checks, authenticated spool/worker configuration, direct-engine bypass prevention, per-node scheduling, durable result acknowledgement and retry, then small real end-to-end jobs before larger client workloads.
