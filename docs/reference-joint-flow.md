# Isolated Viewer/Python ODM reference handoff

`tests.test_reference_joint_flow` exercises the actual reference-v1 modules
across Node and Python using verified loopback TLS. It adds no production
configuration or Viewer application wiring. Node's fixture explicitly returns
`published:false`; no project/model/catalog publication or geometry validation
occurs. The backend is NativeNodeFixture's three-byte `abc` photo and fixed empty
ZIP, and performs no photogrammetry. No NAS, real photos or remote node is used.

The actual chain is Viewer ReferenceProvider submission/status/grant/result
client → Python Queue/ReferenceStore, ReferenceControlAPI and WorkerAPI → actual
ReferenceDriver/BrokerClient → Viewer ReferenceTransferBroker and TLS broker
handler → actual NativeEngine HTTP fixture → WorkerOutbox/delivery receiver →
worker ReferenceResults/result API → coordinator ResultProxy → Node provider
download and ReferenceAcceptanceLedger → coordinator acceptance → actual worker
acceptance application, EngineCacheCleanup and ScratchCleanupExecutor.

Actual DaemonStateLock, NodeGate, ScratchLedger and reader leases are used. The
fixture sharing/capacity policy allows only this synthetic isolated result flow;
it is not a real project authorization decision. No transport method is replaced
with a fake provider/broker implementation. The existing unchanged Viewer joint
helper supplies the synthetic operator endpoint and source file. Tests observe
the exact original claim request/token binding, one reservation and physical
receipt, one nine-byte grant budget with three bytes spent and no leaked read
pins, and one initialize/upload/commit/download/remove operation each.

Three cases currently qualify:

- Normal delivery: producer result/native cache remain before acceptance; Node
  persists the exact result bytes and durable acceptance/outbox before sending
  its acknowledgement. Accepted producer result cleanup follows.
- Worker completion acknowledgement loss: the real delivery receiver commits,
  while the synthetic server substitutes HTTP503 only for that response. The
  actual sender retries the identical sealed outbox envelope. Only its explicit
  retry clock advances; coordinator/expiration clocks and SQL retry rows do not.
- Viewer crash: the existing helper exits86 after its acceptance ledger commit.
  The producer retains result/native cache while the coordinator still awaits
  acceptance. Restarting the helper over the same durable files replays the
  original receipt, then permits cleanup. No source file is deleted.

Source retention remains separate. `referenceSourceRetention.js` is pinned as
reference material but the joint helper does not call it or supply an issuer.
Its pure validator is not a durable NAS retention pin or cleanup authorization.
Both original worker inputs and the fixture Viewer source remain byte-identical
after result cleanup. No source-retention receipt is invented by these tests.

## Pinned source and separate execution

The oracle loads the unchanged modules/helper/fixture from immutable Viewer
commit `867dd3f26c16203b5fdceec3af1671846ffa208c`. Each of the nine files is
checked against its exact Git-byte SHA256 before Node starts. Provider's Windows
worktree CRLF export has a different raw hash but LF-normalizes exactly to the
Git blob; this is not a semantic source difference. `PINS` in the test records
the exact Git/archive bytes used here. Source modules are imported from the
snapshot, never copied into Processing or edited in the Viewer worktree.

From this repository, create a new snapshot outside either checkout:

```powershell
python -m tests.snapshot_reference_joint_viewer --repo "C:/Projects/Ledge Top Operations/viewer-orchestrator-reference-v1-20261027" --output "C:/Users/fstor/.codex/artifacts/processing-joint-flow-20261028/viewer-867-final"
```

Run that snapshot read-only in the already cached image
`sha256:3d27e5c11e5786e309ec3e03f93ae536eb36e6e5eb3714d5eb3300a36157add0`
(Node24 and Python3.11), with `--pull never --network none --read-only`, bounded
CPU/memory/PIDs, a private `/tmp` tmpfs, Processing mounted at `/work`, snapshot
at `/viewer`, working directory `/work` and
`VIEWER_REFERENCE_ROOT=/viewer PYTHONDONTWRITEBYTECODE=1`:

```text
python3 -B -W error::ResourceWarning -m unittest -v tests.test_reference_joint_flow
```

Default Processing-only runs explicitly skip these three tests when the
snapshot opt-in is absent or the platform is not Linux. An opted-in missing,
changed or wrong-version dependency fails; it never falls back to duplicated
business code or installs dependencies. The normal cached Python-only test
image has no Node executable, so this separately executed oracle must remain
visible in qualification receipts rather than be counted as default-suite
coverage. The tests clean only their own temporary synthetic state/processes.

Production Viewer index/provider/worker mounting, real source custody, data
validation, combined outer-v2 delivery and installed acceptance remain distinct
integration work. This modular handoff is not evidence those paths are enabled.
