# Disabled direct worker configuration preparation

Schema 6 can optionally carry `directWorkers` with `enabled: false` and an
entry for every configured node. Each entry references an HTTPS origin, numeric
connection IP, CA, client certificate and private key, bearer file, and pinned
Ed25519 worker key with issuer/key IDs. Normal runtime startup checks the
configuration shape but does not open disabled direct credentials. The explicit
`validate_disabled_direct_files=True` offline loader option checks files, TLS
key pair and secret separation; it does not start direct work.

`ops/prepare_reference_runtime_config.py` can include these references in its
final candidate, but it does not create, copy or enroll credentials or prove
they exist at their eventual mounted paths. The offline loader option performs
file and TLS checks. These checks do not prove safe CA
ownership or rule out path substitution between checking and TLS loading; an
immutable, protected trust-root and key loading strategy remains required before
activation. `enabled: true` is rejected until authenticated worker
readiness, reservation lifecycle, restart and sender wiring are implemented.
No listener or scheduler activation is implied by this preparation profile.
