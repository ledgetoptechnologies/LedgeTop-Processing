# Synthetic durable acceptance pair

This standalone local regression connects the Processing transfer client to the
Viewer acceptance API and SQLite ledger. It generates temporary TLS keys,
mounts both source trees read-only, and publishes the disposable Node
container only on localhost. It drops one chunk response and one commit
response, then checks that status recovery returns the signed receipt without
another upload or commit. It uses a minimal router and synthetic data; it does
not qualify the production Express mount, runtime configuration, deployed
storage, or cleanup authorization.

Requirements: Python with `cryptography`, Docker, and an already-local
`node:24-bookworm-slim` image. Set `LTDS_VIEWER_SOURCE_DIR` to a pinned Viewer
checkout with the acceptance API and ledger, then from the Processing checkout
run:

```sh
python tests/support/durable_viewer_acceptance/run_pair.py
```

Record both repository commits with the result. The container is stopped on
exit and temporary certificates and ledger data are deleted. Do not point this
test at a deployed service or real data.
