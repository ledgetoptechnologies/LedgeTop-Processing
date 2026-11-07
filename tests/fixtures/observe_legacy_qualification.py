"""Run inside the exact isolated candidate; caller supplies census key on stdin.

The caller independently verifies the running image ID and read-only mount. The
result is point-in-time evidence and must be compared with frozen full custody.
"""
import sys
sys.path.insert(0, '/qualification')
sys.path.insert(0, '/app')
from ops.enrolled_coordinator_transport import canonical
from ops.legacy_delivery_qualification import observe

if __name__ == '__main__':
    if len(sys.argv) != 3:
        raise SystemExit(2)
    key = sys.stdin.buffer.read(33)
    if len(key) != 32:
        raise SystemExit(2)
    value = observe('/prototype-state', key=key, candidate_image=sys.argv[1],
                    candidate_source=sys.argv[2], nodes=('cluster','main'),
                    max_archive_bytes=10**12)
    sys.stdout.buffer.write(canonical(value))
