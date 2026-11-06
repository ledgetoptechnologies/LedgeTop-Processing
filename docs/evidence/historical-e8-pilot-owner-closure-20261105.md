# Historical e8 producer termination

Read-only inspection on November5 traced the original one-shot invocation for
`lt-aa5-e8f8fe018ae87130994c3a92d36f2f26` to direct Processing child
`/root/pilot_release`, session `01a18cdd-afe9-7ee2-96a4-47bfef3298ed`, parent
`01a16555-50cc-7ee2-9b43-189cac48a431`.

Exact JSONL is `C:/Users/fstor/.codex/sessions/2026/10/30/rollout-2026-10-30T07-51-41-01a18cdd-afe9-7ee2-96a4-47bfef3298ed.jsonl`.
Original source `pr66-viewer-candidate-d8/real_lifecycle_owner-DRAFT.py` has SHA
`ce0100de374426a0bfa00ec9dcb6fc89c7f034fc561b68cb1ec0d2f38f89d333`.
The actual launch asserted that exact hash, required exactly one occurrence of
each of two literal review guards, removed them and compiled the result in
memory. It supplied the exact literal operation. Effective source SHA was
`1fbf2e2712d22671e62a71a34286d4f7af5e36226b8f4131cf5b860ab9c7c512`.
Guard hashes/lengths were `8e81d1dfd2afe80495f880e2e8caca04700747c463bf46ed7c9908fb51d726b0`/67
and `8d7a0ad14641ce8511d1262c0babb18df3666c2e5271b906766b3a1cc5521d08`/56 bytes.
The scripts were not executed during this inspection.

All following times are October31,2026 UTC; hashes cover complete original JSONL
lines including newline. Offsets are zero-based bytes:

| Record | Time | Offset/bytes | SHA256 |
| --- | --- | --- | --- |
| Invocation `call_5dcbe5b118cd46848eaf7bd38192b6dd` | 07:03:20.291Z | 72131294/1522 | `04c76a30d5862db2594f144d42dbd350a374bfc9edcf447d461cba32c4f14bff` |
| Returned exec session99958 | 07:03:30.491Z | 72133734/901 | `c271f67f1fb5896d8e616c0ac4618bcb897b670eac3130b53ac3d03a6162fc45` |
| Exact CommandExecution terminal, failed/exit1 | 07:12:41.491Z | 72735185/4063 | `264e1617ba5191776d9d18c2f2e0a4c2690c8393fd30b672a6c6e32117ee4ddc` |
| Confirming poll output `call_1b7b7c57d5d3434fa8dc83726d4df277` | 07:12:46.942Z | 72745475/1479 | `f31593d7100386408abf2cc69666009bd07c001ba929a044e8b30388541f8d9e` |

99958 is a tool execution-session identifier, **not a Windows PID**. The public
local journal contains module/preflight/acquire/bootstrap intent+done followed
by pilot-intent. It does not claim teardown/release completion. The terminal
`PILOT_HOLD`/exit1 proves local producer termination, not processing success.

The reviewed source's pilot child waits for a one-byte gate after assignment to
a Windows Job Object. `LimitFlags=0x2000` supplies KILL_ON_JOB_CLOSE; no breakaway
or detached flags appear. Its finally path terminates remaining job processes,
checks active-process count and closes the handle. The earlier bootstrap child
has its own bounded wait/kill/reader cleanup, completed before bootstrap-done.
There is no scheduled task, Windows service or persistent local resumer in this
one-shot source. No claim is made about a deliberate future trusted command.

Combined source/invocation/session/terminal bindings replace the missing old
supervisor-PID record. They do not prove remote child or native job completion.
Current original private Docker namespace termination, retained opaque pilot
manifest, physical Gate, native terminal inventory, ingress and worker readiness
remain separate actual checks. No credential, transcript payload, client data,
host process, service or retained data was changed by this investigation.
