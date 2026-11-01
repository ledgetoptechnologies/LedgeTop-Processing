# Coordinator release boundary evaluation

This is an evaluation of a smaller direct-main review boundary, not a deployment
or a claim that the selection is minimal. It starts from main
`7200fbf49908820f6b76d77a164fd23e191655ae`. The staged `processing/` tree and
coordinator `Dockerfile` are byte-identical to candidate
`3459d9a9867fe9ffcd5981522d3f6f40053d3522`; the full candidate branch and its
history remain unchanged. This selection copies file contents rather than
carrying the full candidate commit ancestry. The evaluation copies the v1 reference contract,
Viewer Compose/docs, selected native/reference/maintenance tests and their
test-only dependencies. It also corrects the README's stale Actions and
Viewer descriptions. It does not include the separate rehearsal helpers.

The running .89 coordinator was observed with both Viewer enabled and a
reference configuration, so neither interface should be omitted on the
assumption that it is unused. The entire candidate coordinator source is
retained as a conservative superset. Optional worker/combined/v2 modules in
that source can require `ops.reference_worker_daemon` or v2 contract files,
which are not packaged in the coordinator image or included by this boundary.
This evaluation does not qualify those separate runtimes, their deployment,
or every possible dynamic import. Reference listener tests are synthetic;
they are not a live or complete end-to-end Viewer/worker acceptance.

Local checks used network-disabled Docker containers. The workflow's Python
3.10 checks used official `python@sha256:5be5aaecb962a41567bc8041f1e9e3432cb085b037fe057f9a562630b2e4b370`
(Python 3.10.22), pulled before the tests. Discovery ran the tests present in
this reduced selection, not the full candidate test tree:

- `python -m unittest discover -v`: 399 tests passed at 1.5 CPU / 768 MiB.
- TreeDar prototype-worker discovery: 34 tests passed at 1 CPU / 512 MiB.
- `.github/scripts/test_publish_images.py`: 5 tests passed in that container.

A prior Python 3.12 run also passed those steps; it preceded the final
fixture-only extension of the unknown-identity retry deadline. The final
Python 3.10 discovery ran with that stabilization.

The first selected-suite run had missing reference and maintenance fixture
dependencies and a known replacement-identity test timeout. The first full
discovery then ran 398 tests with seven baseline maintenance-fixture errors;
the next ran 398 with only the same replacement timeout. After restoring the
candidate maintenance test, the replacement test was made deterministic by
holding the normal service/store locks across its known-identity reset; a
separate unit test asserts the conservative unknown-identity delay. Runtime
behavior was not changed by those test corrections. The unknown-gap test mocks
the identity read/scan and does not prove a real concurrent process race or
post-deadline recovery.

The existing nonempty coordinator updater remains pinned to a different old
image and rejects the synthetic nonempty Viewer tree. Neither this boundary nor
the isolated predecessor-copy rehearsal proves safe writer exclusion, live
cutover, rollback after candidate writes, Viewer custody, or engine retention.
The archive registry and admission closed-start options remain unqualified and
disabled. No live system was changed.
