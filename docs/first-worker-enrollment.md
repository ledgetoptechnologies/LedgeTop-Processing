# First reference worker enrollment

`ops/enroll_reference_worker.py` is an explicit Linux, root-owned **installation**
operation. It supplies `prepare`, read-only `inspect`, and install-only `enroll`.
It never enables/starts/stops a service, submits a job, changes a native image,
creates or adopts a worker lease/receipt, clears a gate, or modifies old source
preparations, ACLs or NAS data. Existing workers require the managed handover;
this helper refuses their files/state instead of taking them over.

## Reviewed inputs

The root-owned private JSON manifest binds a UUID operation, exact host boot and
Docker identity, existing service account/home, original canonical idle-gate
file/hash/authority/generation/origin, exact private native container/image,
loopback port, engine version and current option hash. Root privilege must be
present for the exact noninteractive invocation; the helper never prompts or
attempts a privilege workaround. A failed sudo listing is not evidence that all
specific privileged commands are denied.

The `source`, `dependencies` and `config` artifact descriptors each contain
`path`, `sha256`, `bytes`, `ownerUid`. Source additionally binds `commit`,
`footprintSha256` and `requirementsSha256`. Dependencies additionally bind
`pythonSha256`, actual `pythonVersion` and the exact reviewed distribution pins.
Configuration bytes and credentials remain in the existing private artifact,
never in the manifest, output or unit argv. No fabricated package hash is an
acceptable input.

The install root is a new
`/opt/ledgetop-processing-reference/<operationId>`. Build a canonical plain TAR
from the approved Git archive and a separately reviewed, host-compatible,
self-contained **copied** venv artifact. Both TARs must contain regular files and
directories only: no symlinks (including normal venv interpreter/lib64 aliases),
hard links, special files, traversal or duplicate paths. Do not point the worker
at an old writable prep or dereference arbitrary foreign trees automatically.
The dependency artifact must contain `bin/python` and `pyvenv.cfg`, with
`include-system-site-packages = false`; package inputs must be independently
reviewed for each actual host Python profile.

Prepare streams/hash-verifies the exact archives with bounded memory, creates
only the new root-owned version tree, and publishes code/dependency files as
service-readable and non-writable. It invokes the reviewed interpreter **as the
existing service user**, with `-I -B`, to verify actual package versions, source
footprint/read-only access and existing daemon `load_config`. It never calls
`build_worker` during preflight. Source and dependency byte identities remain
sealed in a bounded root-owned journal. Private config/unit parents are created
as the service user without chmod/chown of existing directories; exclusive file
publication is anchored to their verified directory descriptors.

The configuration must use the exact manifest node/authority/gate/engine/source
bindings, maintenance readiness with no borrowed activation, and new
operation-specific worker state/scratch paths beneath the service home. Those
paths must be absent before enrollment, preventing old outbox/lease adoption.
Existing TLS/role material must already exist and pass the existing private
configuration checks. Initial credential/PKI preparation is a separate reviewed
step; this helper generates none.

The unit hash is the SHA-256 of `render_unit(manifest).encode()`. Its command and
termination policy reuse the existing user-unit contract, with exact versioned
source/interpreter paths instead of `current`. It pins HOME and XDG_STATE_HOME,
disables user-site startup, and literally removes declared loader/Python startup
overrides. No unsupported environment globs are used.

```text
sudo -n <reviewed-interpreter> -I -B <reviewed-installer-entry> prepare --manifest <root-private-manifest>
sudo -n <reviewed-interpreter> -I -B <reviewed-installer-entry> inspect --manifest <root-private-manifest>
sudo -n <reviewed-interpreter> -I -B <reviewed-installer-entry> enroll --manifest <root-private-manifest>
```

Use the reviewed execution bundle/source mechanism to import the helper; these
placeholder commands are not authorization to execute a writable script as root.
`enroll` exclusively creates the new private config and exact user-unit file,
without daemon-reload, start, enable or mutable current-link changes. Known
durable checkpoints replay against original file/directory identities. Unknown
partial copies stay held; matching bytes do not adopt foreign files. Outputs
`serviceStartIssued:false`, `serviceEnableIssued:false` and
`engineMutationIssued:false` describe **this installer's actions**, not current
service state or readiness. Another actor starting a unit cannot be hidden by
these flags. Current readiness requires the separate authenticated observation.

## Explicit activation handoff

Installation is not the endpoint of the workflow. After independent full-phase
and source/lease custody review, pause the **same** coordinator cutover operation
at its bootstrap-frozen boundary, then perform a separately journaled activation:

1. Revalidate exact installed source/dependency/config/unit pins, original
   authority, authenticated frozen startup/plan/owner and current physical source
   custody. Independently prove old pilot/writer retirement, closed native
   ingress, exact idle NodeGate and actual engine/image/task census. Neither an
   empty gate snapshot nor `/info` queue count alone suffices.
2. As the existing service user, daemon-reload only the exact reviewed unit.
   Reuse `SystemdWorkerControl` to verify loaded fragment, no drop-ins/reload,
   exact command and inactive process state. Enumerate actual manager environment
   with `ActualBoundary.assert_clean_manager_environment`: refuse unreviewed
   `LD_*`/`PYTHON*` keys, retain only its opaque hash/decision, and never print
   values. Literal unit removals cover known overrides; unexpected future keys
   still hold. Confirm the default XDG gate namespace matches the manifest.
3. Commit the current operation's exact start intent before
   `SystemdWorkerControl.command('start')`. The coordinator stays durably frozen,
   blocking claims throughout initial worker startup. No additional scheduler is
   created. A lost start reply is inspection-only; never repeat an uncertain
   start based on PID absence.
4. Use `ReferenceWorkerHandover.ready` **without** `__enter__`/`stop_old`, or the
   existing tracked worker probe, for fresh authenticated TLS nonce/config/source/
   authority/engine/generation/process proof. Verify dynamic catalog/capacity via
   the existing worker HTTPS relay, including while the driver is busy. Retire
   the exact probe process. Do not replace this with a cached ready boolean.
5. Bind those current receipts into the existing full-phase final-readiness
   gates; only that single owner may reopen the coordinator. Enabling unattended
   boot is a later explicit action after installed acceptance. On ambiguity keep
   the coordinator frozen and installed bytes/results retained; never reset
   worker state, resolve a gate or force-stop a service as a shortcut.

The separate controller-owned implementation is documented in
[guarded activation](first-worker-activation.md); it is not activation authority
inside the installer. Connected-host qualification remains outstanding. No live installation or activation has
been performed by this work. Current installed facts and actual missing inputs
are recorded in [the dated inventory](evidence/installed-worker-inventory-20261105.md).
