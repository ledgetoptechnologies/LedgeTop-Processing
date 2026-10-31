"""Hold d738's own state lock so the old coordinator cannot start on that state.

d738 refuses to start ("prototype state already owned") when another process
holds ``flock`` on ``<state>/coordinator.lock``, and it takes that lock before
writing its database. d738 has no authority lease, so this holder is the
independent fence for every retained copy of its state. Run one holder
container per volume, with the volume mounted read-only, ``--network none`` and
a restart policy that brings it back after it dies. Never recreate a holder to
add a volume: start another one, so no fenced volume is ever released.

``--verify`` runs inside the holder container (``docker exec``). It proves from
``/proc/locks``, now rather than from historical logs, that the current holder
process owns an exclusive flock on every named lock. It exits 1 otherwise.

The holder never creates, writes or deletes anything. A missing lock file
refuses, because creating one would mean this is not the state it was meant to
fence.
"""

import fcntl
import json
import os
from pathlib import Path
import signal
import sys


def _lock_path(directory):
    path = Path(directory) / 'coordinator.lock'
    if path.is_symlink() or not path.is_file():
        raise SystemExit('fence refused: no coordinator.lock in %s' % directory)
    return path


def hold(directories):
    descriptors = []
    for directory in directories:
        descriptor = os.open(_lock_path(directory), os.O_RDONLY | os.O_NOFOLLOW)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(descriptor)
            raise SystemExit('fence refused: state is owned by a running coordinator: %s' % directory)
        descriptors.append(descriptor)
    return descriptors


def holder_pids(proc='/proc'):
    pids = set()
    for name in os.listdir(proc):
        if not name.isdigit() or int(name) == os.getpid():
            continue
        try:
            arguments = Path(proc, name, 'cmdline').read_bytes().split(b'\0')
        except OSError:
            continue
        # Only the Python holder itself; an init process (docker-init) carries
        # the same arguments but holds no lock.
        if (os.path.basename(arguments[0]).startswith(b'python')
                and any(item.endswith(b'state_fence.py') for item in arguments) and b'--verify' not in arguments):
            pids.add(int(name))
    return pids


def held_flocks(text):
    """Map (major, minor, inode) to the PIDs holding an exclusive flock on it."""
    held = {}
    for line in text.splitlines():
        fields = line.split()
        if '->' in fields:  # a blocked waiter, not a holder
            continue
        if len(fields) < 6 or fields[1] != 'FLOCK' or fields[3] != 'WRITE':
            continue
        major, minor, inode = fields[5].split(':')
        held.setdefault((int(major, 16), int(minor, 16), int(inode)), set()).add(int(fields[4]))
    return held


def verify(directories, locks_text=None, pids=None):
    pids = holder_pids() if pids is None else pids
    held = held_flocks(Path('/proc/locks').read_text() if locks_text is None else locks_text)
    report = []
    for directory in directories:
        info = _lock_path(directory).stat()
        owners = held.get((os.major(info.st_dev), os.minor(info.st_dev), info.st_ino), set())
        report.append(bool(pids) and bool(owners & pids))
    return {'holders': len(pids), 'locks': len(report), 'held': sum(report)}, all(report) and len(pids) == 1


def main(argv=None):
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments[:1] == ['--verify']:
        result, ok = verify(arguments[1:] or [])
        print(json.dumps(dict(result, verified=ok)), flush=True)
        return 0 if ok and arguments[1:] else 1
    if not arguments:
        raise SystemExit('usage: state_fence.py [--verify] STATE_DIR [STATE_DIR ...]')
    descriptors = hold(arguments)
    print(json.dumps({'fenced': len(descriptors)}), flush=True)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    while True:
        signal.pause()


if __name__ == '__main__':
    raise SystemExit(main())
