# Historical pilot producer closure evidence

Read-only review on November 5, 2026 traced the original local acquire/release
supervisors for these two pilot operations:

- pr53: `lt-aa5-045fe15bf4766f6984da90be919257d5`.
- pr66: `lt-aa5-23bce8ed95d85869c2ff243f8c481fc7`.

The actual producer is the historical `/root/qualification_review` child,
session `01a18cf4-f8a1-71c3-9143-31d1b67e68de`, in:

```text
C:/Users/fstor/.codex/sessions/2026/10/30/rollout-2026-10-30T08-17-07-01a18cf4-f8a1-71c3-9143-31d1b67e68de.jsonl
```

This record supplements independent current custody checks. It grants no
activation or release authority and does not qualify any live deployment.

## Exact source and invocation bindings

Each original invocation reads its declared source bytes, checks the exact
SHA-256, verifies one occurrence of a 64-byte DRAFT guard, removes that one guard
and compiles/executes the resulting source with the exact literal operation ID.
The removed guard SHA-256 is:

```text
2f7bca166840cf2b014f89831ec7bfc22286abde0161acea569a07fa581f7c41
```

| Invocation | Original source SHA-256 | Effective source SHA-256 after the declared single removal |
| --- | --- | --- |
| pr53 acquire | `65c80f8e7d69dee6491ad6c318519cc0f345fdc0a439649399fe5a30b84b59e0` | `400e3e531d8447b64b773a33188a42e5177742dd0e7028753e032b112dd48d36` |
| pr53 release | `81279d6392901c792873fb4842e6305a69827bc4631ff83f0e4815b2a29efd66` | `7e016129df469ad6c5893629ff313f8f6580bd09735c37189d4d173cfb91fca8` |
| pr66 acquire | `65c80f8e7d69dee6491ad6c318519cc0f345fdc0a439649399fe5a30b84b59e0` | `400e3e531d8447b64b773a33188a42e5177742dd0e7028753e032b112dd48d36` |
| pr66 release | `3c329e21e277af07496ed796eddedd0e5c370768a3add826d591c134c966c644` | `4ef8549f3d79d25ad61eb6e92dd5d2360b0e786213909f6e673590f048b4ad50` |

Source paths are the respective `pr53-aa5b33b-pilot` or
`pr66-ad3c085-pilot` directories under `C:/Users/fstor/.codex/artifacts/`, files
`two-node-acquire-supervisor-DRAFT.py` and
`two-node-release-supervisor-DRAFT.py`.

The current pr66 acquisition file was rewritten later, on October 31, and now
hashes `2214f5f04b35645c5067c935ef8d5dbe8a212061378709ad94f0379a01a809de`.
That later threaded implementation is **not** the source invoked by session
39989. The original acquisition bytes are recoverable from the retained pr53
file, whose exact hash matches both original invocation checks. No logged code
was executed to reconstruct these hashes; only the explicitly declared literal
single-string transformation was applied to the hash-matched source in memory.

Those actual original source bodies use synchronous `subprocess.run` with
explicit finite timeouts and captured output. They contain no detached spawn,
`Popen`, threads, timers, persistent registration or top-level infinite loop.
The wrappers pipe source into an ordinary foreground Python interpreter.

## Local tool lifecycle records

All timestamps below are October 30, 2026, UTC. Offsets are zero-based byte
offsets in the exact qualification-review JSONL above. Record hashes are SHA-256
of the complete original line, including its newline.

| Invocation | Tool call ID | Invocation time | Invocation offset / bytes | Invocation record SHA-256 |
| --- | --- | --- | --- | --- |
| pr53 acquire | `call_ac81927654b64e819bddeee1dfc76524` | 20:42:00.822Z | 19737432 / 1171 | `447c830f871bac97f69af7e827929b75d7faac4ec7e63ec0199bcbd1e7df18e8` |
| pr53 release | `call_9f856429f7e24ff29db7372d46eaa91b` | 20:56:28.927Z | 20238239 / 1181 | `1c06ad3dcfbbf398864b309b1e906ad487181363e99202a12c5c3d11ab52eaab` |
| pr66 acquire | `call_420c4ec8b0494831a7b45bdee44787aa` | 22:34:52.424Z | 24672317 / 1174 | `43d8c1e802c2bcc4d35d5dc8c4739bdeab26787c2581d5c1af2bd6feec464365` |
| pr66 release | `call_483dec794df548268d3eba23ed89d94e` | 22:44:34.097Z | 24911752 / 1170 | `6398d0cf3111bc20252348d00e8f1ae5d0a0505c90c32dc25d9ec2b64637165f` |

The matching tool outputs return the following execution sessions:

| Invocation | Exec session | Returned-session time | Output offset / bytes | Output record SHA-256 |
| --- | --- | --- | --- | --- |
| pr53 acquire | `21072` | 20:42:11.065Z | 19739510 / 802 | `cbe9f77dc3bafdc500ad54a04299b908a87463a16ca19a3d53f513ce804d6324` |
| pr53 release | `46744` | 20:56:39.117Z | 20240328 / 676 | `340998b6777e11b59a87436d4aab60899c8bb83da5b4d3f4cdeb683947f79f77` |
| pr66 acquire | `39989` | 22:35:02.639Z | 24674393 / 802 | `4b24a5957b6de7691a600f3faf0d83a413d3dc78d09174864224b77bf612b8f8` |
| pr66 release | `72598` | 22:44:44.298Z | 24913824 / 676 | `a41e80f9d2037777f8a83caa39da7b8c360c8f0e8978cb0279299d363b427fe5` |

Each terminal `CommandExecution` event has the same exec session, contains the
full exact original launch command and literal operation, and reports
`status=completed`, `exit_code=0`:

| Invocation | Terminal time | Terminal offset / bytes | Terminal record SHA-256 |
| --- | --- | --- | --- |
| pr53 acquire | 20:42:11.626Z | 19741148 / 2221 | `15545d4bf4dd9ce4edb9c69b11d7a449b1783af08f357280f2b8a76c00fc12c5` |
| pr53 release | 20:56:46.721Z | 20245227 / 2376 | `bf7336a4ed6aae4804c4d73e15d07f3bf01e7f188a692888e6beeba3e82daecd` |
| pr66 acquire | 22:35:03.685Z | 24676031 / 2224 | `1aec13ec93b21a30a3f82bef58c4cb301f9a280ef5ca0099d0fcd6eadfbeb0a7` |
| pr66 release | 22:44:50.457Z | 24916770 / 2356 | `c77cc34624e1b011356348e30781450aed3c79dc3d9258e6ed0606e22f9207b5` |

These numerical IDs are **tool execution-session identifiers, not Windows
process IDs**. This review does not infer Windows kernel Job flags, PID identity
or a present-day process census from them. The linked terminal records plus
one-shot synchronous source establish closure of these original producers.
They do not prevent a trusted human or agent from deliberately issuing a new
future command.

## Bounded search and evidence limits

The discovery pass used binary timestamp seeking, not whole-file grep. Limits
were 15 seconds, 32 MiB aggregate bytes, maximum 2 MiB per JSONL record, at most
32 seek iterations per file, and window 20:30–22:50Z on October 30. It consumed
18,047,730 bytes in less than one second across only these three direct children:

- `01a18cdd-afe9-7ee2-96a4-47bfef3298ed` (`pilot_release`).
- `01a18cd0-48cd-7a31-b032-a31438b2c27d` (`contract_budget`).
- `01a18cf4-f8a1-71c3-9143-31d1b67e68de` (`qualification_review`).

Subsequent exact-offset followups read bounded 8–30 KiB record neighborhoods and
one 300 KiB earlier-source window. The 2.93 GB root rollout and unrelated
sessions were not scanned. Earlier 20:38–20:40 source-write/syntax-check commands
in the producer session were terminal; the target's first effectful acquisition
is the explicit 20:42 invocation identified above. No transcript bodies, client
data or credential values are copied into this evidence document.

This historical review does **not** prove current remote-child absence, current
original Docker/Gate/kernel lease identity, engine idle state, native-writer
retirement, bootstrap-frozen coordinator identity, Viewer accepted-copy custody
or first-worker readiness. Those require their separate fresh authenticated and
physical proofs. No host process, private input, configuration, service,
credential, native engine or NAS data was changed, and no processing test ran.
