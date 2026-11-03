# Host mutation journal candidate

This is an unmounted, disabled library candidate. It records operation intent
under an existing bound `NodeGate` reservation before an adapter issues an
engine mutation. A completed operation with the same identity returns the
saved acknowledgment digest. A pending operation always holds; callers must
not resend it merely because a process restarted or a reply was lost.

The gate lock must remain held throughout each external operation. The journal
records only hashes supplied by its caller: it does not itself authenticate
the coordinator, verify an HTTP response, query Docker, or attest the engine
identity. `NodeGate` protects cooperating processes in the same OS-user gate
directory; direct engine clients, other users and different state homes can
bypass it. It does not guard existing Viewer or NodeODM traffic.

The candidate intentionally supplies no completion/release or journal-reset
path. Its marker remains in `gate.json` even after external gate resolution;
it cannot yet serve a subsequent reservation without a reviewed lifecycle and
migration. A missing or incomplete journal remains held for inspection. Do not
provision this candidate against live nodes or change any deployed gate.

Linux tests use a temporary state root and actual competing processes and
exercise process termination before and after a recorded acknowledgment. They
do not prove filesystem power-loss behavior, trusted response verification,
HTTP integration, rollback safety, or physical isolation.
