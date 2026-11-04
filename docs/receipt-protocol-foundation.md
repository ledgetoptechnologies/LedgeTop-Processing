# Inactive receipt protocol foundation

The reference protocol, Viewer result manifest and Viewer v1 signed receipt
verifier in this branch are isolated building blocks from Processing candidate
`bccf633`. No route, runtime configuration, cleanup authority or image change
is enabled by this branch. A valid signature alone does not authorize removal
of source, engine or result data.

For isolated local validation, install the test-only pinned dependencies in a
dedicated environment, then run:

```
python -m pip install --require-hashes --only-binary=:all: -r tests/requirements-receipt-foundation.txt
python -m unittest tests.test_reference_protocol tests.test_viewer_result_manifest tests.test_viewer_acceptance_receipt -v
```

The locked wheel hashes cover Linux x86-64 Python 3.10 used by CI and Windows
x86-64 Python 3.10 and 3.13; other platforms need separately reviewed hashes.

The test vectors contain synthetic data and public test signing material.
The runtime container intentionally does not install the test dependency: it
must not import the signed receipt module until a separate reviewed integration
adds runtime dependency, trust configuration and its security boundaries.
The v1 verifier does not implement the proposed v2 acceptance protocol, and
neither module proves authenticated peer identity, durable custody or replay
fencing. Those remain separate work.

The existing v1 result manifest accepts a caller-specified subset of objects;
the caller must establish the required closure from trusted policy. Version 2
manifest acceptance requires the full manifest order. Neither behavior itself
authorizes cleanup.
