# Inactive Viewer exporter and Processing receiver pairing

Run with a clean detached Viewer checkout at `a6ed23b38ea66171fc8884867840b2d699062926`:

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
truncation and page reordering. No image files are read or processed. The
binding is test-constructed and does not establish independent authority,
complete source custody, or immutable source content. Receiver results remain
held. No network transport, activation, engine capacity or cleanup is proven.

Mixed-case GCP source IDs expose a real incompatibility: Viewer emits
`control, Control` using locale-sensitive comparison, whereas Processing
requires ascending code-point order. The test checks the actual exported bytes
are rejected; it does not reorder or alter them. Viewer also emits a `C:photo.jpg`
path that Processing rejects; unsupported DNG MIME is held by Viewer. These
specific negative vectors do not establish comprehensive path or MIME parity.
