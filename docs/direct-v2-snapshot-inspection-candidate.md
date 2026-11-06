# Direct V2 signed source snapshot inspection candidate

`DirectV2Reservations.inspect_held_snapshot` is a read-only, unmounted
prerequisite for a future source-custody receipt. It resolves the reservation
and owner from the coordinator database, requires a separately configured
owner/environment/issuer/key snapshot pin, revalidates the stored signed source
attestation and manifest digests, then verifies the actual Viewer held-snapshot
signature, intent hash, bounded canonical source pages and source-object
ID/basename/size/hash closure. Full relative paths remain internal. It returns
only `source_pages_verified_not_custody`; no output authorizes processing,
source release, a coordinator receipt or deletion.

This method is not exposed by an HTTP route; a future adapter must authenticate
the Viewer caller and provide protected transport, framed input limits,
authorization, safe timeout, and signer configuration/rotation. It does not
persist this inspection, independently prove that Viewer still retains files,
validate the selected-page or GCP-evidence chunk streams, qualify live source
file identity, or compute the currently undefined `originalPathInventorySha256`
in the proposed receipt. GCP-bearing snapshots are explicitly refused until
their complete metadata and chunk streams have an agreed verifier. The source
page check follows Viewer's exact-path and case-insensitive basename collision
policy using Python Unicode lowercase; Unicode-version differences versus JS
remain to be qualified. It cannot prove
worker custody, writer confinement, current mount integrity, crash recovery or
an authenticated explicit release. Viewer must continue to hold originals.

Focused tests exercise rejection and a synthetic Node signature over bytes
generated with the actual Viewer page encoder/canonicalizer. The reservation
method fixture mocks its database reservation and source-attestation verifier;
it is not a paired live Viewer-to-coordinator test. Agreement on the wire
format, snapshot key authority and original-path digest remains outstanding.
