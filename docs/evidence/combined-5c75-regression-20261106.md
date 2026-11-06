# Combined Processing regression — November 6, 2026

Three bounded Linux test selections passed against frozen test/controller source
`5c75dbcd489fab76aab5a6d33ebf1f8b48a64cc4` and local derived candidate image
`sha256:aeff9d1b2cb90f6fe5762a77bcd04e04c9c67e5ee5cee7af2213ae5837761dad`.
The image reports runtime revision `ac0a25b57be1d1716983a666654d4fdbe80d5bb3`;
the later combined source includes test and documentation changes. The frozen
source archive SHA256 was
`91d7253b963e9a7529289a5718316a93d26a7f293b83b58cce7b7b13c15157d3`.

| Selection | Result | Captured log SHA256 |
| --- | --- | --- |
| Acceptance/legacy-push fence, activation, worker/runtime configuration, prestart, TLS and retention | 158 passed; zero skips | `bf775f294de9da7ee5e75119ac969b4cd27d6d4765296cb7832c6f29f9581d77` |
| Actual composer preparation and fixture-profile guards | 8 passed; zero skips | `cf2f445d1c638282e1e6b975b4dd2b683a4b6d8229bae36403f5b6725746a824` |
| Corrected TreeDar engine/delivery, routing, reference delivery, transfer worker and result-manifest cases | 82 passed; zero skips | `828ad6f0cc7b68aba8bd304e935514a285af16a7c9640e4dfeaa9fe63e2080ff` |

Some cases appear in both the first and third selections; the counts must not
be presented as 248 distinct tests. Each container ran with one CPU, a 768 MiB
memory limit, nonroot UID 10001, no network, read-only root and source, dropped
capabilities and no-new-privileges. All three test containers were auto-removed
and their label-filtered inventories were empty; no named volumes were created.
The frozen archive and logs remain in the private task evidence directory.

These are synthetic regression results. They do not establish genuine-LiDAR
TreeDar processing, Viewer TreeDar ingestion, installed host/SSH/systemd
readiness, end-to-end real Viewer jobs, or a production deployment. TreeDar
capability remains disabled until its separate integration and qualification
gates are satisfied.
