# Disabled direct worker configuration preparation

Schema 6 can optionally carry `directWorkers` with `enabled: false` and an
entry for every configured node. Each entry references an HTTPS origin, numeric
connection IP, CA, client certificate and private key, bearer file, and pinned
Ed25519 worker key with issuer/key IDs. Runtime startup validates these files,
TLS key pair, identities and distinct secrets; it does not start direct work.

`ops/prepare_reference_runtime_config.py` can include these references in its
final candidate, but it does not create, copy or enroll credentials or prove
they exist at their eventual mounted paths. The normal runtime loader performs
the file and TLS checks. `enabled: true` is rejected until authenticated worker
readiness, reservation lifecycle, restart and sender wiring are implemented.
No listener or scheduler activation is implied by this preparation profile.
