# Source range grant renewal candidate

This Processing branch adds a candidate client for the Viewer source-range v2
renewal protocol. It is not a runtime release. Viewer must mount the matching
routes and enable the protected handler before the client can be activated.

## Contract and safety properties

- Renewal uses `POST /api/processing/source-range/v2/grants/renew`; durable
  acceptance uses `POST /api/processing/source-range/v2/grants/renew/ack`.
- Both requests use verified HTTPS, no redirects and bounded canonical JSON
  (4 MiB). In production the request must go through the trusted TLS proxy,
  which injects `X-Viewer-Proxy-Secret`; the worker does not receive or store
  that shared proxy secret. Range GETs carry the scoped grant bearer.
- The driver saves the exact renewal request and id before sending it. A lost
  renewal response retries that same request. It saves the exact successor grant
  and ack body before acknowledging. A lost ack response retries that exact ack;
  only a validated ack promotes the successor to the active grant.
- Successors must extend expiry while preserving job, generation, source,
  reservation, object closure and every transfer budget. The range client can
  renew between chunks without resetting its durable request, byte or attempt
  ledger. An expired source lease still blocks renewal.
- Temporary authority outages and `grant_not_expired` responses retain the
  same durable request for retry. Stale reservations, rejected renewals,
  malformed replies and conflicting acknowledgements hold the job with its
  pending journal intact for reconciliation.
- `sourceRangeV2Enabled` defaults to `false`. Do not enable it until the paired
  Viewer routes are mounted, protected, and `brokerOrigin` is verified to route
  through the trusted TLS proxy that injects the gate header. A direct app
  origin must fail closed.

## Candidate verification

On Windows, the focused suites ran 62 tests: 46 passed and 16 Linux-only tests
were skipped. All 62 then passed in an isolated Linux container running as an
unprivileged user, with a read-only repository mount, no network, a 1 CPU
limit, 1 GiB memory cap and automatic container removal:

```text
python -m unittest tests.test_source_range_transfer_candidate tests.test_reference_driver tests.test_reference_worker_daemon_routing
Ran 62 tests ... OK
```

The coverage includes maximum-length 10,000-object renewal messages, replay
after lost request and ack responses, renewal during a multi-range object,
unchanged transfer budgets, default-off daemon configuration, and short local
writes. The short-write case confirms that partial bytes are not committed,
the exact range request ID is replayed, and request/byte budgets are charged
only once. The platform-gated Linux daemon composition tests account for the
Windows skips.

No live renewal request, production route, worker node, or client dataset was
used. The Viewer renewal handler is still candidate-only and default-off.
