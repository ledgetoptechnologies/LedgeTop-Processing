# Paired synthetic Viewer repository source test

`tests.test_viewer_repository_source_snapshot_pair` executes the actual Viewer
database migrations, ProcessingRepository and GcpRepository from clean commit
`5513f92b6cf45221475a3c38347e3ca1dbf0c32d` against an in-memory SQLite
database. It finalizes synthetic byte-hashed source records, creates a task,
GCP set and correspondence, and captures an attempt's actual persisted
provenance, GCP text and immutable correspondence evidence. One `auto` role is
set directly in the synthetic database to represent a legacy finalized dataset;
no Viewer code or real data is modified. The test checks that editing the live
correspondence after attempt creation does not change the captured evidence.

The Processing candidate checks the resulting source IDs, paths, hashes,
roles, snapshot and proposed source policy and remains held. Tests separately
verify synthetic fixture bytes, missing/changed supplied mappings, changed
attempt identity, and duplicate reference-v1 basenames. No source materializer
is run. The source-byte check is fixture-only and is not an authenticated
production reader, custody proof or test of an actual lost file.

Set `PINNED_VIEWER_ROOT` to a clean detached checkout at that commit and
`PYTHONPATH` to the hash-pinned rfc8785 candidate dependency, then run:

```
python -m unittest tests.test_viewer_repository_source_snapshot_pair
```

The Viewer repository can produce attempt-bound evidence in this synthetic
case. Real trusted snapshot/selection loading, authentication, transport,
coordinator authority, source reservation, runtime file custody checks,
failure recovery and deployment are not implemented or proven by this test.
