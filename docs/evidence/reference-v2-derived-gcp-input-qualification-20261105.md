# Receipt-bound V2 ODM GCP input qualification

Runtime/test candidate `4babfe1496c9824693dbd88c7ff7b8f393b34cf4` is based on
Processing `51cff44`. The shared `reference_odm_inputs` validator permits only
the exact V2 attempt-derived GCP list reconstructed from accepted source/derived
receipts and the authenticated coordinator's source acceptance binding. It is
used by Driver, execution Router and ODM worker before backend mutation.
It preserves the image-only V1 path and refuses generic auxiliary input. No
special `gcp` option, new credential, parallel controller or source deletion is
introduced. Staged/combined GCP still fails before stage-plan creation.

The native wire remains multipart field `images`, with filename `gcp_list.txt`.
NodeODM explicitly uses that field for image and optional GCP text uploads;
its server classifies `.txt` files separately. References:
[official upload contract](https://github.com/OpenDroneMap/NodeODM/blob/master/docs/index.adoc),
[server upload classification](https://github.com/OpenDroneMap/NodeODM/blob/master/libs/taskNew.js).

## Executed evidence

- Focused Linux suite: **76/76 passed**, 4.113 seconds. Modules:
  `tests.test_reference_odm_inputs`, `tests.test_reference_engine_worker`,
  `tests.test_reference_driver`, `tests.test_reference_execution`.
  One CPU/1 GiB, read-only source and no external network. This run preceded
  adding the explicit combined-stage refusal test.
- Actual Viewer GCP whole-object roundtrip: **1/1 passed**, 10.168 seconds,
  frozen Viewer `6718d1beca0f7fbf8cbaae2bbec14326e1e053bb`.
- Final clean regression: **13/13 passed**, 35.249 seconds, frozen Viewer
  `2317d40029f5b0dbbb002cd319c8f25d321a1bfb`. This comprises the nine closure
  tests plus photo-only whole/range and GCP whole/range roundtrips. The preceding
  command had one incorrect method name (loader error); that command was
  corrected and the complete selection passed. No code failure was hidden.

The paired runs used Node 24.22.2, exact production dependencies with lock hash
`a0eaead118642843905756fcfe3f53b5336b5a437e22a2f80677d6f06087fb12`,
one CPU/2 GiB, read-only source/dependency mounts, networking disabled except
container loopback and a task-owned temporary disk volume. Runtime image:
`sha256:57f01267c88439d872f6782b54511e314614a76145e32cfe2fad003e5ca0fac8`.
The relevant GCP/import/snapshot/worker/source-intake implementations are
unchanged between the two frozen Viewer commits. All task containers and
owned volumes exited/were removed; no host/NAS or production service changed.

Reproduce the final selection inside the documented bounded Linux pairing
environment from `docs/local-viewer-pairing-qualification.md`:

```sh
python -m unittest \
  tests.test_reference_odm_inputs \
  tests.test_viewer_result_v2_roundtrip.ViewerResultV2RoundTripTests.test_actual_viewer_source_intake_result_copy_lost_ack_and_worker_cleanup \
  tests.test_viewer_result_v2_roundtrip.ViewerResultV2RoundTripTests.test_actual_viewer_range_v2_fetch_result_copy_lost_ack_and_worker_cleanup \
  tests.test_viewer_result_v2_roundtrip.ViewerResultV2RoundTripTests.test_actual_viewer_gcp_snapshot_reaches_native_multipart_without_raw_csv \
  tests.test_viewer_result_v2_roundtrip.ViewerResultV2RoundTripTests.test_actual_viewer_range_v2_gcp_snapshot_reaches_native_multipart_without_raw_csv
```

## What the paired case establishes

The actual Viewer Emlid parser, GCP repository and pixel correspondence create
the attempt snapshot. Viewer signs its held source/preclaim evidence, uploads
the raw source and derived closure, and replays lost POST/PUT/completion replies.
The actual Processing intake, broker/range transfer, Driver, Router and worker
reach `NativeEngine.upload`. A loopback HTTP peer captures the real multipart:
exactly `photo.jpg` and `gcp_list.txt`, both under `images`; no raw CSV upload.
The captured GCP body matches its receipt's byte size and digest. Image count
remains one although the engine manifest has two objects. Raw CSV custody is
marked `permanent_viewer_hold`. Result-copy acceptance, ingestion and worker
cache cleanup complete without a second engine initialization.

The follow-up test-only assertions additionally check the captured exact CRS,
synthetic geographic coordinates, ftUS-to-metre height and selected pixel
`25 75 photo.jpg`, and verify unchanged raw CSV hashes in Viewer and the source
spool after worker cleanup. Those explicit assertions await the root combined
qualification; they were added after the clean 13-test run above.

The case uses a synthetic Emlid CSV and synthetic photo bytes. Engine
initialization, execution and output archive remain synthetic; only its upload
multipart is exercised over a real local HTTP connection. Physical mount/host
probes are injected. No real photogrammetry, real NAS durability, client-data
processing or rendered GCP UI acceptance is established. CSV auto-discovery,
invalid-format handling, import idempotency and the 50-foot/manual-marking UI
have their separate Viewer tests; this case does not claim those UI behaviors.
