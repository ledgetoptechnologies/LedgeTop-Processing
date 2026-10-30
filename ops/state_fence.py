"""Hold d738's own state lock so the old coordinator cannot start on that state.

d738 refuses to start ("prototype state already owned") when another process
holds ``flock`` on ``<state>/coordinator.lock``, and it takes that lock before
writing its database. d738 has no authority lease, so this holder is the
independent fence for every retained copy of its state (the original and any
verification volume). Run it in its own container with the volumes mounted
read-only, ``--network none`` and a restart policy that brings it back after a
Docker daemon restart.

It never creates, writes or deletes anything. A missing lock file refuses,
because creating one would mean this is not the state it was meant to fence.
"""

import fcntl
import json
import os
from pathlib import Path
import signal
import sys


def hold(directories):
    descriptors = []
    for directory in directories:
        path = Path(directory) / 'coordinator.lock'
        if path.is_symlink() or not path.is_file():
            raise SystemExit('fence refused: no coordinator.lock in %s' % directory)
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(descriptor)
            raise SystemExit('fence refused: state is owned by a running coordinator: %s' % directory)
        descriptors.append(descriptor)
    return descriptors


def main(argv=None):
    directories = list(sys.argv[1:] if argv is None else argv)
    if not directories:
        raise SystemExit('usage: state_fence.py STATE_DIR [STATE_DIR ...]')
    descriptors = hold(directories)
    print(json.dumps({'fenced': len(descriptors)}), flush=True)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    while True:
        signal.pause()


if __name__ == '__main__':
    main()
