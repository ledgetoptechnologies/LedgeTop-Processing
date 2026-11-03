# Inactive Viewer exporter and Processing receiver pairing

Run with a clean detached Viewer checkout at `63970eaf056ec80230ceabd508823916c78e4af2`:

```powershell
$env:PINNED_VIEWER_ROOT = '<pinned Viewer checkout>'
$env:PAGED_VIEWER_PAIR_REQUIRED = '1'
python -m unittest tests.test_paged_viewer_export_pair
```

The test calls the real Viewer repositories and exporter on an in-memory
synthetic database, sends the exact emitted frame bytes to the Processing
receiver, and builds a separately pinned synthetic binding and processing
closure from the emitted metadata. It tests a synthetic 16,832-photo metadata
inventory, LiDAR metadata, a GCP correspondence, replay, incorrect pin,
truncation and page reordering. Every emitted frame's byte size, canonical
encoding, sequence index and domain-separated digest is checked. One run
emitted 33 source frames (4,160,588 bytes) and 33 selected frames (1,657,762
bytes); identifiers and digests change between runs. No image files are read or processed. The
binding is test-constructed and does not establish independent authority,
complete source custody, or immutable source content. Receiver results remain
held. No network transport, activation, engine capacity or cleanup is proven.

The prior mixed-case GCP ordering discrepancy is resolved for this vector:
Viewer emits `Control, control` and the Processing source verifier accepts the
unchanged frames while remaining held. Viewer now rejects `C:photo.jpg` and
unsupported DNG MIME before export. These vectors do not establish
comprehensive path or MIME parity.
