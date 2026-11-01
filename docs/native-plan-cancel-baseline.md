# Existing native-plan cancellation ordering

The disabled v1 candidate uses the same coordinator SQLite authority for plan
binding and reservation cancellation. A new plan binding rechecks the
reservation under `BEGIN IMMEDIATE` before inserting; cancellation also uses
`BEGIN IMMEDIATE`. If cancellation commits first, a later plan insertion holds.
If the plan insertion commits first, cancellation can subsequently close the
reservation; the existing plan row is retained. A plan reply may arrive after
that cancellation and is only a historical acceptance.

The authenticated v1 plan status returns the plan receipt while the reservation
is active. After cancellation it returns a conflict before checking the plan
row, whether cancellation happened before or after plan binding. The cancel
response reports a cancelled reservation but does not distinguish those cases.
Clients must hold on this ambiguity. No data is deleted and no execution is
authorized by these receipts.

Two synthetic tests use separate HTTP client processes against one actual
test-only coordinator. Test-controlled transaction barriers force each commit
ordering; the plan-first case also checks the held result and retained plan row
after an in-process coordinator fixture restart. This is not a process-kill,
power-loss, external-writer or network-partition test.

This ordering cannot detect a Viewer-local context change until a corresponding
authenticated cancellation reaches and commits at the coordinator. It is not
an operation-generation revocation protocol, an old-writer fence, or an
authorization to enable the disabled binding route.
