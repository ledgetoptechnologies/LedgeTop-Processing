# Inactive durable archive claim prototype

This is an unqualified test prototype. Do not migrate or deploy it. Even with
claims disabled, archive inspection holds are monotonic and cannot be cleared
through ordinary status updates. `install_archive_claim_schema` is an explicit
offline migration primitive, not an upgrade procedure; no environment setting
or operator workflow exposes this feature. The exact archive-status schema must
already be installed before the claim schema can be installed or used.

When enabled directly in synthetic tests, a database claim is committed before
first archive capture or manual download. The database stores only a hash of
the claim token. Publication verifies the claim and removes it in the same
transaction that records the archive path. An interrupted operation retains a
claim indefinitely and subsequent cooperating callers refuse to write. This is
not a lease or proof that a writer has exited. The scanner excludes claimed
incarnations before selecting a candidate. Recorded-archive restore also claims
before writing and releases its claim only after receipt verification and a
matching publication check. Public status labels a claim as
active or unresolved without asserting that its owner has failed or exposing
claim material. There is no claim resolver.

The startup check prevents this version with its flag disabled from opening a
database with the schema installed. It does not stop older binaries that know
nothing about the schema. No upgrade, downgrade, physical writer fence or
cross-host filesystem authority has been qualified. Benign pre-write failures
leave a hold, and a completed archive
whose database publication fails remains held even if a receipt is present.
Any resolution must prove old writers are stopped and independently verify the
exact task identity, immutable archive and durable receipt first.
