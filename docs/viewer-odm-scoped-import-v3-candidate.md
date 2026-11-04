# Held ODM import scope v3 proposal

This is an unmounted proposal, not the deployed Viewer signer contract. It signs
an outer object over the complete independently signed v2 confirmation. Exact
outer fields: `protocol` = `ltds-viewer-odm-scoped-import-confirmation-v3`,
`schemaVersion` = 3, `issuerId`, `keyId`, `coordinatorAuthorityId` (UUID),
`viewerPrincipalId` (identifier), `activationId` (UUID),
`preparedEnvelopeSha256` (lowercase SHA-256), `confirmationV2` (unchanged
complete signed ODM v2 object), and `signature` (base64url Ed25519).

The signature covers `LTDS-VIEWER-ODM-SCOPED-IMPORT-CONFIRMATION-V3` followed
by a zero byte and the canonical JSON of all outer fields except `signature`.
The signer identity must equal the nested v2 signer. The verifier first checks
all existing v2 result, accepted-copy, archive and task/attempt coverage and
then the outer signature against an independently provisioned scope and key.
Actual signature domain, digest definition and trust provisioning require Viewer
agreement. The verifier does not authenticate its construction inputs, persist
first-replay state, authorize removal, or attest the Viewer has deployed this
protocol. Its output always says held.
