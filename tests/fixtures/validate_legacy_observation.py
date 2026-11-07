"""Validate point-in-time observation only; this does not verify frozen custody."""
import json
import sys
sys.path.insert(0, '/qualification')
sys.path.insert(0, '/app')
from ops.enrolled_coordinator_cutover import AUTHORITY
from ops.legacy_delivery_qualification import validate_artifact

if __name__ == '__main__':
    if len(sys.argv) != 4:
        raise SystemExit(2)
    plan = dict(authorityId=AUTHORITY, candidate=dict(imageId=sys.argv[1], sourceCommit=sys.argv[2]))
    value = validate_artifact(sys.stdin.buffer.read(1024*1024+1), plan, reader_sha256=sys.argv[3])
    print(json.dumps(dict(archives=len(value['archives']), bytes=sum(row['bytes'] for row in value['archives'].values()),
                          scope=value['scope'])))
