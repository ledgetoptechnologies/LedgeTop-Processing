# Synthetic native binding interoperability

An isolated loopback rehearsal used coordinator commit
`1a8dd57ca9f21f72fd18caa4715a68381bc2b6ac` and Viewer commit
`9cb9972accaf1283248c6238a58430dd934d6c22`. The test checks six Viewer
exported files against SHA-256 hashes of their Git blobs. The coordinator was
exported from the specified commit with LF settings; every exported coordinator
file was not independently compared to its blob. The image ID was
`sha256:0bb8832e2ce0cf718ae6b5419fa79501a75f303a82ff4b32200888329f7ef668`.

The single explicitly selected test passed in a network-none, unprivileged,
1-CPU, 768-MiB container. It used the real Viewer candidate client, a real
loopback coordinator HTTP listener with qualification patched in the fixture,
and an encrypted synthetic Viewer capability. The test checks exact persisted
binding replay after an in-process coordinator listener/service restart, wrong capability and wrong
environment, one durable coordinator binding, no task creation, and status-only
recovery without another reservation or bind. Response loss is simulated by
throwing after the client has consumed the successful HTTP response; it is not
a network disconnect or process-death test. The receipt remains
`identity_bound`, `capacityReady: false`.

Successful run ID `native-binding-afec2afd09acbed56bd4515e955c96c8` returned
one passing test. Its exact owned container was verified exited and removed;
the evidence directory was retained. Two earlier containers and evidence
directories were retained after failures before any Viewer request: the first
export had CRLF bytes, and the second used a mistyped expected hash. The
wrapper now forces LF Git export and the six hashes were independently checked.

This does not qualify TLS, production authority, physical writer fencing,
process-crash durability, upload, allocation, dispatch, or large-job support.
The Viewer code is an unwired candidate; qualification remains disabled.
The wrapper retains evidence and uncertain containers. Its evidence child is
0755 for the unprivileged container and may be read by local users who can
traverse the supplied parent; use a private parent. Its explicit unittest target
avoids running inherited parent tests in this rehearsal; an opt-in full test
discovery would also discover those inherited tests.
