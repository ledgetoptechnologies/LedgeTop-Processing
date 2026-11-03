# Synthetic Viewer GCP evidence pairing

Use a clean Viewer checkout at `4ed7dcd566380c3c0cd7f03a765ee1badf2004d5`:

```powershell
$env:PINNED_VIEWER_ROOT='<clean pinned Viewer checkout>'
$env:GCP_VIEWER_PAIR_REQUIRED='1'
python -m unittest tests.test_actual_viewer_gcp_stream_pair
```

Actual Viewer repositories populate an in-memory synthetic database; the test
compares its stored evidence JSON and generated content bytes with exported
streams and pins, validates evidence via the Processing decoder, and invokes
the inactive semantic bridge using stored bytes. It checks a real producer
split across a multi-byte UTF-8 character, tampering and missing trailer,
individual stream and combined sizes, and source/selected/evidence/file page
hashes. Test-only chunk extraction compares stored file bytes; it is not a
Processing file-stream decoder. The Processing candidate has no such decoder
at this commit and therefore file-stream trailer, tamper and UTF-8 enforcement
are not proven on that side. The test checks actual size accounting, not a
72 MiB boundary-sized input or a combined Processing receiver quota.

The GCP source frame contains an additional generated record but Viewer reports
only the dataset file count. Processing currently expects all records in that
count; the test confirms this mismatch is held. It does not adjust the count.
This is not an authenticated source, file custody or execution test; no route,
grant or cleanup is enabled.
