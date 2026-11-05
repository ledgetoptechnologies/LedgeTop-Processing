# Actual disposable configuration staging qualification

Receipt recorded 2026-11-05 23:13:50 UTC. This is a local Docker component test,
not processing-PC enrollment, production configuration staging, complete cutover
or client-data processing. No NAS, engine, service, existing configuration
permissions or client files were changed.

## Exact inputs

- Controller source: `e38a3bd34ae981ec34001e71e51a5b06562fd019`, frozen with
  `git archive` and mounted read-only. This includes the canonical writer-stdin
  recovery fix; it does not replace the separately pinned runtime source.
- Image: `sha256:378559e42456017b965d1e2e38b56eb627274734aeb317a758d84c0bc99bb5ab`.
- Image revision/runtime source: `18750d4d599026c18e569a9c67135e6c9fdc7d1a`.
  The independently reviewed frozen runtime tree was mounted read-only. All 15
  reviewed runtime-file hashes had to match the actual immutable image before
  the production writer was dispatched.
- Native Docker CLI SHA256:
  `6c71cfe71faff949a602946a4df4799b563232e79d0b8b6fd343d0ef081c9621`.
  The fixture verified its actual read-only bind, regular/root-owned single-link
  identity and bounded descriptor hash. Desktop's read-only mode0777 handling
  is an explicit test profile, not production host enrollment.
- Reviewed daemon: `7d7ff0fd-d590-413a-9301-42a3a08135f4`.
- Outer operation: `dacb604a618644ea85334e938cabf09f`.

The outer controller used one CPU, 768 MiB, UID10001/GID10001 with socket group0,
read-only root, network none, dropped capabilities, no-new-privileges and bounded
scratch. Its only persistent writable data was a new task-owned artifact volume.
The production staging helper had only one new synthetic configuration-root
mount, UID/GID10001, read-only image, network none, one CPU/128 MiB/64 PIDs,
dropped capabilities, no-new-privileges and logging disabled. It mounted no
processing state, authority leases or NAS path.

## Successful result

Execution `9dc4fd`, owned session `30819`, terminal `eac526`, exit0. The first
call yielded after 10.008 seconds; the same session then completed successfully.
The runner did not record exact wall-clock start/end timestamps, so that yield
duration is not presented as an exact total duration.

Three checkpoints passed:

1. Production closed helper recipe and actual create, followed by loss of its
   confirmed reply, recovered through inspection without another create.
2. Actual immutable-image package/source checks and exclusive UID10001 writer;
   lost confirmed start reply recovered through inspection. Exact directory700,
   file600, UID/GID10001 and content hashes were checked through the existing
   typed archive reader, including after the original fixture was stopped.
3. Existing synthetic old configuration bytes, mode0440, owner/group, inode and
   size remained unchanged. Lost confirmed helper-remove reply recovered through
   inspection without another removal.

Observed mutation counts were exactly `{create:1,start:1,rm:1}`. These injected
losses happened after confirmed daemon completion; they do not claim recovery
from every arbitrary partial Docker effect. Production `ConfigPreparation`
phase methods, writer, recipe validation and cold readback ran unchanged. The
fixture explicitly substituted synthetic enrollment IDs/payload-policy inputs
and a fixture-only global lock; `productionEnrollment=false` and
`fullCutover=false` remained explicit.

All inner task containers and their new configuration volume were removed by
ownership-checked fixture cleanup. The exact successful outer controller
`014d17d1c8dc767bdb02592dd92756ca4b12516f94a7457ee1c4e49dc4e012b4`
and artifact volume `ltds-config-stage-artifacts-dacb604a618644ea` were then
removed only after terminal exit0, owner/operation verification and no remaining
mount users. Final operation-label container/volume inventories were empty.
Unowned running containers and the shared builder were left alone.

## Earlier failure and disposition

The prior `97cde57` controller failed before writer start with
`reviewed_config_writer_changed`. Creation and lost-create inspection had
succeeded. The receipt's canonical JSON reordered settings keys, while the
writer source used Python dictionary insertion order. `e38a3bd` canonicalizes
settings and payload ordering before constructing the exact reviewed stdin;
it retains the SHA equality gate. The new canonical-receipt/reordered-input
regression passed natively (1/1 in 0.001 seconds). The preceding writer/CLI suite
passed 12/12 Linux cases at `79c675b`.

Failed outer `a14b2d9e936d54779a4c5035ee923b800aba71535def6cfb37a8a084330c46e0`
ran from 23:07:47.880082585Z to 23:07:51.828638354Z, exited1, PID0, with no
unknown mutation. The known helper was never started and was confirmed absent.
Cold retained-artifact inspection was bounded to 9,728 TAR bytes: two private700
directories, five private600 JSON metadata files totaling 3,364 bytes and one
known empty private lock file. The preparation receipt remained `created`.
There were no links, database, client data or secret keys in those artifacts.

After root review authorized exact synthetic cleanup, fresh owner/operation and
PID0/exit1 checks passed. The exact failed outer and its artifact volume
`ltds-config-stage-artifacts-592b9668c9df4ccf` were removed without force or
cascade; operation-label inventories were empty (`306d3e`). A first cleanup
guard conservatively refused before deletion because the Docker listing used
truncated IDs; `--no-trunc` fixed the observation without weakening identity
comparison. This receipt preserves the public diagnostic after artifact removal.
