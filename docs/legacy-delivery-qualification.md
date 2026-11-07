# Legacy delivery qualification (not yet wired into cutover)

The retained legacy route is required for the coordinator transition. A
SHA-256 of an arbitrary `legacyDeliverySha256` file by itself is not evidence
that the candidate can read old results.

`ops.legacy_delivery_qualification.observe` uses the candidate's actual legacy
reader to verify and read every current retained archive. It returns a canonical
typed observation containing the candidate image/source and reader SHA, the
authority, HMACed full `jobs`, `viewer_tasks`, and `viewer_files` table digests,
and HMACed archive path/content/size/mode records. It never returns the census
key, client paths, task identifiers, or raw rows. It reads with a queue/freeze
inspection shim: it does **not** prove real HTTP, runtime leases or a freeze.
Any missing, unsafe, non-successful or unverifiable archive is a hard failure.

The producer's image and source arguments are assertions. Before trusting the
observation, an independently reviewed runner must verify Docker actually ran
the exact pinned image, the reader module is from that image with no overriding
mount, the state volume was mounted read-only, and the expected reviewed helper
code and census key were used. Keep the key out of argv and logs. The included
`tests/fixtures/observe_legacy_qualification.py` is an explicit inspection
adapter, not a complete or self-authenticating launcher. Enforce an external
wall-clock/read budget for an actual large inventory.

At preflight, `validate_artifact(raw, plan, reader_sha256=...)` validates the
canonical typed envelope and the independently pinned image, source, authority
and reader SHA. This alone is not authority to resume admission.
After `capture-full-custody` has produced its **real, frozen, writer-excluded**
baseline, and before any step that opens the final admission gate, the driver
must also call `validate_against_baseline(raw, plan, baseline, reader_sha256=...)`.
It requires a full retained-custody baseline with matching authority, the same
three table counts and HMACs, the exact archive count, and each archive's path,
content, size and mode in that baseline. Any change requires a fresh observation
and reviewed artifact; never patch the receipt to match. Historical incarnations
or removed tasks with retained archives may cause a conservative hold because
the reader only serves current tasks. The baseline's independent writer/lease
and projection guards remain mandatory.

The two functions are separate building blocks only; the driver does not call
them yet. A qualification is not complete until the producer identity and
read-only mounts are independently verified, preflight and baseline validation
are wired into the actual driver, interruption/error cases are tested, and the
installed path is checked. The point-in-time observation must never be described
as a frozen-state or live delivery proof.
