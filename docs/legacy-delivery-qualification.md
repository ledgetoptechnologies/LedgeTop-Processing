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

The observer's image and source arguments are assertions. The separate
`capture_legacy_delivery` runner inspects the exact image and task-owned
container, verifies the UID, isolated network, read-only root, state-volume and
helper mounts, and feeds the private census key through stdin. It seals the
observation, exact helper hashes and volume binding with that key. Run it only
from the independently archive-verified controller and pin the expected reader
SHA from the reviewed startup policy. This does not defend against a hostile
process with the same OS user and key. The runner bounds its child duration;
the inspection adapter itself does not provide a separate byte-budget deadline.

At preflight, `validate_capture` verifies the keyed envelope, exact helper hashes,
state volume and canonical typed observation with the pinned image, source,
authority and reader SHA. This alone is not authority to resume admission.
After `capture-full-custody` has produced its **real, frozen, writer-excluded**
baseline, and before any step that opens the final admission gate, the driver
also calls `validate_capture_against_baseline`.
It requires a full retained-custody baseline with matching authority, the same
three table counts and HMACs, the exact archive count, and each archive's path,
content, size and mode in that baseline. Any change requires a fresh observation
and reviewed artifact; never patch the receipt to match. Historical incarnations
or removed tasks with retained archives may cause a conservative hold because
the reader only serves current tasks. The baseline's independent writer/lease
and projection guards remain mandatory.

The preflight and final resume paths contain those calls in this candidate
source, but they have not been used in an installed cutover. A qualification
is not complete until the runner and driver changes receive independent review,
the exact inputs are repinned, interruption/error cases are qualified, and the
installed path is checked. The point-in-time observation must never be described
as a frozen-state or live delivery proof.
