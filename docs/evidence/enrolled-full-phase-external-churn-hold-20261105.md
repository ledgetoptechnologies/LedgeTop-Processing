# Actual forward attempt: external inventory churn

This is an incomplete, correctly held qualification. It did not reach candidate
startup or admission. No installed PC, TrueNAS dataset, client data or real
processing task was used.

## Frozen inputs and result

- Harness `951ef9a`; archive SHA256
  `fcbe8edbdb1decfbf969f95c967c27da32af396bb8e904b6e9051be19fcdae04`.
- Candidate image
  `sha256:378559e42456017b965d1e2e38b56eb627274734aeb317a758d84c0bc99bb5ab`,
  packaged processing source `18750d4d599026c18e569a9c67135e6c9fdc7d1a`.
- Production projected full writer inventory, unchanged schema/custody guards
  and normal application deadlines; qualification-only outer budget900seconds.
- Disposable real693 predecessor, real PID1 holder/dual kernel flocks, full
  original-state census and immutable TAR copy/validation.

The controller ran from23:09:02.196824408Z through23:10:01.355945751Z on
November5,2026:59.159seconds. The900-second budget was **not** reached.

Five phases completed through `custody-verified`. Journal index5 retained
`copy-verified-backup` pending with dispatch attempt5 and `admissionOpened=false`.
The copy body and first observation completed. During the second fresh
verification, the full before/after Docker ID sets differed, producing
`writer_inventory_changed_during_observation`. It did not silently adopt the
changed namespace, replay a mutation, reset its window or open admission.

The real predecessor exited0; no bootstrap/final candidate was created. The
known-safe inner cleanup completed, with independently empty task-label
container and volume inventories. This deletes only disposable fixture state;
it is not a production rollback or source-data cleanup policy.

## Bounded event evidence and attribution limit

Only container create/destroy events in the exact operation interval were
requested. No broad logs, configuration or environment values were read.

The available event timeline contains an external, unlabeled create at
23:10:00.114036Z. Its container ID SHA256 is
`c75cb8c84e0a97dc648ff10e4eb86d427cb78d0592ae676325080a57d286f89e`.
It has neither this operation's labels nor any observed `org.ledgetop` or
Compose owner tag. Another external destroy occurred at23:09:36.118477Z
(ID SHA256 `75fca389032ee2f2a8227cdbb52bad40e72768a0f1955791df35ed79ab34868e`).

The three owned inner destroy events begin later, at23:10:00.878123Z,
23:10:00.964728Z and23:10:01.054722Z, during cleanup. They cannot account
for the preceding verification failure. Tracked `docker exec` observers do not
create Docker container identities.

The failed guard did not persist its exact before/after ID sets. The external
create is strongly correlated with the failure time, but is not presented as
a reconstructed exact-set proof. The event history can be bounded/evicted; it
is not asserted to be a complete lifetime ledger. No foreign resource was
stopped, changed or removed.

## Retained artifact proof and disposition

Operation `54103935-f4bf-4b40-b078-84f90d4064b5` has stopped outer controller
`ltds-phase-forward-54103935f4bf4b40`, exact ID
`461d6948ec245963997f0639bffacff15fd69f1e565787d89749289c49ad940c`.
PID0, exit1, no OOM and its exact operation label were independently checked.
Private artifact volume `ltds-phase-forward-artifacts-54103935f4bf4b40` was
initially retained with only that stopped controller as a mount reference.

Cold read-only copies of the journal, backup receipt, baseline and TAR were
recovered without restarting it. Unchanged production `validate_tar` again
matched the durable receipt:467968archive bytes,202files,13directories,
262429content bytes, clean stream closure and client exit0. Archive SHA256:
`b1fe610e83c9a6ae34e5bf2ab5c75dd43cbd1acec7ff5f32b4b36813d6e2f351`.
`restoreAuthorized=false`; the copied archive is never original lease authority.

The root agent reviewed the cold TAR proof and explicitly authorized exact
resource cleanup. Fresh owner/operation label, PID0, exit1 and no OOM were
checked before removing only the stopped outer controller, without force.
The exact volume name and owner were checked, then zero remaining mount users
were proved before removing only that volume, without force. Filtered operation
container **and** volume inventories were empty afterward. Private diagnostic
copies remain; no keys were printed or foreign resources touched. The operation
was not replayed: its disposable original fixtures are gone.

## Next step

Coordinate a quiet local Docker namespace window with other sessions before
one fresh operation. Keep the full inventory fence unchanged. A complete
forward run and separate declared-v2 rollback are still required; the elapsed
hold is evidence of a protected concurrency boundary, not successful cutover.
