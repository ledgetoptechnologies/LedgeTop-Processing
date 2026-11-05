# Actual projected writer inventory

The production `DockerCLI.writer_inventory()` path from immutable patch
`0b4cfc43f0166895b9f16c7b6e17c37579eb9d06` was exercised against the actual
local Docker Engine. It is a read-only metadata qualification, not a cutover,
original lock proof or production-host enrollment.

## Bounds and result

- Frozen source archive SHA256:
  `38e174c81356323fb5da3a2a83d051cfda9eeaee77cf74007e93377490577b21`.
- Existing image:
  `sha256:378559e42456017b965d1e2e38b56eb627274734aeb317a758d84c0bc99bb5ab`.
- UID10001, supplemental socket GID0, one CPU,256MiB, read-only root/source/CLI,
  network none and no published ports. Eight-second observer alarm.
- 96 containers accepted in0.375seconds using three metadata commands:
  full ID list, one bounded projected inspect, full ID list closure.
- Inventory SHA256:
  `8c83cf45326d835e113d5a6945825098cac6750997d41aa50ce2d95682e58deb`.
- No configuration/environment fields were requested or read. No paths,
  container identities, payloads or credentials appeared in the result.

The first read-only compatibility diagnostic correctly refused unrelated
Docker Desktop bind mounts whose absolute Sources use Windows drive spelling.
The reviewed parser recognizes only explicit absolute drive forms for binds;
empty, relative, UNC and control-character Sources remain refused. Volumes and
the original named-state volume must retain actual POSIX absolute Sources.
No drive-to-POSIX normalization or alias assumption is introduced.

Fifteen native parser/boundary tests passed in0.018seconds, covering96/512
container batching, inventory change, duplicate/missing/invalid identities,
closed states, truncated/oversize data, command failure, forbidden extra fields,
volume names and source/parent/root/child bind aliases, and empty tmpfs handling.

## Lifecycle and remaining work

Operation `db1bb860-1a4f-409f-9616-eacb936879e1`, controller
`ltds-writer-inventory-db1bb8601a4f409f`, exact container
`59410d4c20533447bfc5ca83f1899d84f00b6d2a2129df79e0295332fa591b8f`.
Inspected PID0, exit0 and no OOM, then removed only the exact owned controller.
Exact operation-label inventory was empty. No volume was created.

The actual complete forward and declared-v2 rollback qualifications are still
required. Existing authority, schema, receipts, startup windows and application
timeouts remain unchanged. The completed probe does not authorize a live cutover.
