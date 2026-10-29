"""Old-writer oracle for opt-in input-copy accounting (synthetic data only).

Run inside the PREVIOUS worker image with this source tree mounted read-only at
/new, for example:

  docker run --rm --network none --read-only --tmpfs /tmp -e PYTHONPATH=/app \
    -v <this checkout>:/new:ro --entrypoint python <old worker image> \
    /new/scripts/input_copy_old_image_oracle.py

The image's own processing.scratch_policy is the old writer; the mounted copy is
loaded under a separate module name. Prints one JSON summary; exits 1 on failure.
"""
from contextlib import closing
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys
import tempfile

import processing.scratch_policy as old


def load_new():
    spec = importlib.util.spec_from_file_location('new_scratch_policy', '/new/processing/scratch_policy.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def reserve(ledger, reservation):
    return ledger.reserve(job_id='j-' + reservation, generation=1, reservation_id=reservation,
        manifest_sha256='a' * 64, input_bytes=3, output_budget=10, environment='staging',
        ttl_seconds=60, worker_ready=True)


def main():
    new = load_new()
    results = dict(oldModule=old.__file__, newModule='/new/processing/scratch_policy.py', python=sys.version.split()[0])
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        options = dict(capacity_probe=lambda _: 10**9, clock=lambda: 100.0, safety_floor_bytes=0, cleanup_grace_seconds=0)

        # 1. Cold reopen: the old code must refuse a marked ledger before writing.
        cold = root / 'cold.sqlite'
        marked = new.ScratchLedger(cold, root, input_copy_accounting=True, **options)
        reserve(marked, 'r1')
        marked.enable_input_copy_accounting()
        before = digest(cold)
        try:
            old.ScratchLedger(cold, root, **options)
            results['coldReopen'] = 'ACCEPTED'
        except old.PolicyConflict as error:
            results['coldReopen'] = 'refused: ' + str(error)
        results['coldBytesUnchanged'] = digest(cold) == before

        # 2. Live writer: an old ledger opened before the latch is fenced afterwards.
        live = root / 'live.sqlite'
        writer = old.ScratchLedger(live, root, **options)
        reserve(writer, 'r2')
        opted = new.ScratchLedger(live, root, input_copy_accounting=True, **options)
        opted.enable_input_copy_accounting()
        before = digest(live)
        try:
            reserve(writer, 'r3')
            results['liveWrite'] = 'ACCEPTED'
        except old.PolicyConflict as error:
            results['liveWrite'] = 'refused: ' + str(error)
        results['liveBytesUnchanged'] = digest(live) == before

        # 3. Both markers: an old writer that already latched control accounting
        #    is fenced once input accounting is latched alongside it.
        both = root / 'both.sqlite'
        writer = old.ScratchLedger(both, root, **options)
        reserve(writer, 'r5')
        writer.record_allocated('r5', 3)
        writer.prepare_control_charge('r5', charge_id='c5', scope_sha256='e' * 64, parent_binding_sha256='f' * 64,
            expected_allocated=3, allowance_bytes=2, maximum_scope_charges=1)
        opted = new.ScratchLedger(both, root, input_copy_accounting=True, **options)
        opted.enable_input_copy_accounting()
        with closing(sqlite3.connect(both)) as db:
            stored = json.loads(db.execute("SELECT value FROM metadata WHERE key='config'").fetchone()[0])
        results['bothMarkers'] = sorted(key for key in stored if key.endswith('Version'))
        before = digest(both)
        try:
            reserve(writer, 'r6')
            results['bothLiveWrite'] = 'ACCEPTED'
        except old.PolicyConflict as error:
            results['bothLiveWrite'] = 'refused: ' + str(error)
        try:
            old.ScratchLedger(both, root, **options)
            results['bothColdReopen'] = 'ACCEPTED'
        except old.PolicyConflict as error:
            results['bothColdReopen'] = 'refused: ' + str(error)
        results['bothBytesUnchanged'] = digest(both) == before

        # 4. Unmarked ledgers stay fully compatible with the old code.
        plain = root / 'plain.sqlite'
        new.ScratchLedger(plain, root, **options)
        reserve(old.ScratchLedger(plain, root, **options), 'r4')
        results['unmarkedCompatible'] = True

    ok = (results['coldReopen'].startswith('refused') and results['liveWrite'].startswith('refused')
          and results['coldBytesUnchanged'] and results['liveBytesUnchanged']
          and results['bothMarkers'] == ['accountingVersion', 'inputCopyAccountingVersion']
          and results['bothLiveWrite'].startswith('refused') and results['bothColdReopen'].startswith('refused')
          and results['bothBytesUnchanged'])
    results['pass'] = ok
    print(json.dumps(results, indent=2, sort_keys=True))
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())
