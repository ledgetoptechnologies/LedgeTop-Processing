"""Unpublished native capacity metadata experiment; not an admission API.

No caller is wired to this module.  Values describe configured limits, not
free space, physical allocation, node readiness, or permission to submit.
"""

from .viewer_limits import MAX_UPLOAD_BYTES
from .native_input_manifest import MAX_INPUT_BYTES as MANIFEST_MAX_INPUT_BYTES

NATIVE_CAPACITY_QUALIFIED = False


def capability(store):
    """Return the proposed closed v1 object under test-only qualification."""
    if NATIVE_CAPACITY_QUALIFIED is not True:
        raise RuntimeError('native capacity contract is not qualified')
    maximum = store.max_input_bytes
    if type(maximum) is not int or not 1 <= maximum <= 10**15:
        raise ValueError('invalid configured task input limit')
    return {
        'schemaVersion': 1,
        'unit': 'bytes',
        'scope': {'environment': None, 'node': None, 'configRevision': None,
                  'fingerprint': None, 'freshness': 'unknown'},
        'input': {'legacyMaxTaskBytes': maximum,
                  'manifestMaxTaskBytes': min(maximum, MANIFEST_MAX_INPUT_BYTES),
                  'maxObjects': None,
                  'maxAuxiliaryObjects': None},
        'request': {'maxMultipartBodyBytes': MAX_UPLOAD_BYTES, 'maxPartBytes': None},
        'temporary': {'strategy': 'per_part_spool', 'filesystemRole': 'coordinator_spool',
                      'maxConcurrentBytes': None},
        'expansion': {'coordinatorTaskBytes': None, 'engineWorkspaceBytes': None,
                      'resultBytes': None},
        'reservation': {'state': 'unavailable', 'physical': False},
        'resources': {'coordinatorSpool': None, 'coordinatorTaskInput': None,
                      'coordinatorArchive': None, 'engineWorkspace': None,
                      'engineOutput': None},
        'admission': 'unknown',
    }
