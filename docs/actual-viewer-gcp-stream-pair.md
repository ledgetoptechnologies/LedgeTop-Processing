# Synthetic Viewer GCP evidence pairing

Use a clean Viewer checkout at `4ed7dcd566380c3c0cd7f03a765ee1badf2004d5`:

```powershell
$env:PINNED_VIEWER_ROOT='<clean pinned Viewer checkout>'
$env:GCP_VIEWER_PAIR_REQUIRED='1'
python -m unittest tests.test_actual_viewer_gcp_stream_pair
```

Actual Viewer repositories populate an in-memory synthetic database; the test
compares its stored evidence JSON and generated content bytes with exported
streams and pins, validates evidence and generated file through Processing
decoders, and invokes the inactive semantic bridge using stored bytes. It checks a real producer
split across a multi-byte UTF-8 character, tampering and missing trailer,
individual stream and combined sizes, and source/selected/evidence/file page
hashes. Test-only chunk extraction additionally compares stored file bytes;
it is not itself a decoder. The quota helper checks aggregate declared sizes,
not a combined authenticated Processing receiver or a boundary-sized stream.

Viewer reports the dataset file count excluding the generated record; the
versioned Processing verifier accepts the unchanged actual count and rejects
the wrong count. The semantic bridge requires independent trusted image path,
dimensions and complete correspondence records, and verifies pixel bounds.
The test's trusted fixtures are synthetic copies, not a real independent
Viewer authority. No same-transaction authenticated loader or source-file
custody exists.
This is not an authenticated source, file custody or execution test; no route,
grant or cleanup is enabled.
