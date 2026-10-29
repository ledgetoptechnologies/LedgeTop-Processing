# Indexed private transfer capability

This additive source API supports the separately owned indexed-fetch driver. It
creates no route, job, reservation, renewal, worker configuration or runtime.
Legacy `ValidatedGrant`, `validate_grant` and `download_object` retain their
existing shapes and behavior.

`validate_indexed_grant(raw, job=trusted_private_job, clock=trusted_clock,
monotonic=time.monotonic)` executes the existing complete grant validator once.
It bounds the original closure to10,000 objects, the manifest to the existing
4MiB ceiling and the private grant to2MiB. These are admission limits, not an
increase to any public wire limit. Caller-provided job metadata is not newly
authenticated by this factory: it must already be the persisted original job
whose authority the driver owns and validates.

The returned `IndexedValidatedGrant` is an opaque process-local capability. It
has no assignable instance fields. A weak identity registry contains a detached
immutable ID map of primitive descriptors, original job/reservation/generation/
environment, manifest and complete grant digests, nonce, budget and original
expiry. The token is private and excluded from repr. No public constructor,
mapping or arbitrary `TransferObject` can substitute download authority. The
read-only identity properties let the journal owner compare the original seal;
`object_ids` is an immutable tuple in original manifest order, and
`object_for(id)` returns a fresh detached descriptor in O(1).

`BrokerClient.download_indexed_object(handle, id, destination)` obtains exactly
one descriptor by dictionary lookup and uses the **same** private publisher as
legacy downloads. HTTPS origin/TLS verification, forbidden redirects, exact
response URL, identity content encoding, bounded streaming reads, size/hash
verification, file and directory fsync and exclusive hard-link publication all
remain shared. Redirect refusal closes the original response before rejecting
it. An existing or racing destination is never replaced. No bearer or HTTP
exception detail is included in transfer failures.

Each indexed publication checks the client trusted wall clock against the
factory's initial sample and its process high-water mark, and also checks an
original monotonic deadline. Checks run before filesystem/network admission,
before each streaming read and before publication. Clock rollback, nonfinite
time, expiry or monotonic regression permanently holds that handle. A frozen
wall clock cannot extend the original monotonic lifetime. A new factory call is
not renewal: cold recovery must supply the original persisted grant and trusted
persisted clock high-water mark, preserving the original expiry and reservation.
The journal must independently bind its original claim/reservation and perform
its own durable clock/recovery validation. The capability is not serialized or
automatically restored.

`handle.check_expiry(clock=trusted_clock)` exposes the same sealed high-water/
monotonic gate without a request, for reconciling an already published object or
checking a final phase transition. Immutable `object_for` lookup alone does not
assert freshness. Full validation time consumes the original monotonic lifetime;
the factory rechecks expiry before returning a capability.

One API invocation performs at most one GET; it never retries, rotates a nonce
or reconciles an engine mutation. The owned driver must durably charge the
existing three-per-object attempt limit and its separate aggregate ceiling
`min(3*N, N+10)` (at most10,010 requests), including uncertain attempts, before
calling this API. This module does not provide that journal or broker policy.

The Python module, its private registry and trusted clock/opener configuration
are trusted application code, not a sandbox against arbitrary code rewriting
module globals. Tests deliberately exercise forged/unregistered handles,
detached descriptor mutation and field/map substitution through the public API.

`tests/test_reference_indexed_transfer.py` includes a10,000-object admission and
lookup counter: exactly one complete validation and one mapping lookup per
object, with manifest rescans prohibited on the hot lookup path. Actual loopback
HTTPS tests verify trusted TLS, exact published bytes/hash, corruption refusal,
redirect refusal, no-replace races and expiry. Clock/size/fsync/error tests and
the unchanged legacy transfer suite cover the shared publisher. These synthetic
tests do not prove actual NAS transfers, client datasets, durable indexed-driver
recovery, broker quota configuration or production activation.
