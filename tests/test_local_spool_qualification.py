import copy
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from ops import local_spool_qualification as q
from ops.enrolled_coordinator_transport import TransportHeld, canonical
from processing.source_spool import SourceSpoolError


@unittest.skipUnless(os.name == 'posix', 'runtime spool qualification uses Linux filesystem semantics')
class LocalSpoolQualificationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.mount = Path(self.temp.name)
        self.root = self.mount / 'objects'
        self.root.mkdir(mode=0o700)
        self.marker = self.mount / '.spool-id'
        self.marker.write_text('a' * 64)
        self.marker.chmod(0o600)
        self.settings = dict(root=str(self.root), mountPath=str(self.mount), mountId='a' * 64,
            markerName='.spool-id', safetyFloorBytes=0, maxObjectBytes=1024**3, chunkBytes=4096)
        self.config = canonical(dict(schemaVersion=6, sourceSpool=self.settings))

    def observation(self):
        with patch('os.path.ismount', return_value=True):
            return q.observe(self.config, required_free_bytes=4096)

    def test_actual_write_readback_fsync_and_retention(self):
        retained = self.root / 'retained-client-object'
        retained.write_bytes(b'retained')
        before = retained.stat()
        observation = self.observation()
        q.validate(canonical(observation), self.config, expected_uid=os.geteuid(),
            now=observation['observedUnixSeconds'])
        self.assertEqual(retained.read_bytes(), b'retained')
        self.assertEqual(retained.stat().st_ino, before.st_ino)
        self.assertEqual(list(self.root.iterdir()), [retained])

    def test_reject_changed_config_identity_and_expired_or_insufficient_evidence(self):
        observation = self.observation()
        now = observation['observedUnixSeconds']
        changed = copy.deepcopy(self.settings); changed['mountId'] = 'b' * 64
        with self.assertRaises(TransportHeld):
            q.validate(canonical(observation), canonical(dict(schemaVersion=6, sourceSpool=changed)),
                expected_uid=os.geteuid(), now=now)
        for field, value in [('rootInode', -1), ('runtimeUid', os.geteuid() + 1),
                             ('availableBytes', 0), ('probeBytes', 0)]:
            bad = dict(observation, **{field: value})
            with self.assertRaises(TransportHeld):
                q.validate(canonical(bad), self.config, expected_uid=os.geteuid(), now=now)
        with self.assertRaises(TransportHeld):
            q.validate(canonical(observation), self.config, expected_uid=os.geteuid(), now=now + 86401)

    def test_missing_changed_marker_and_insufficient_space_fail_closed(self):
        self.marker.write_text('b' * 64)
        with patch('os.path.ismount', return_value=True):
            with self.assertRaises(SourceSpoolError): q.observe(self.config)
        self.marker.write_text('a' * 64)
        with patch('os.path.ismount', return_value=True):
            with self.assertRaises(SourceSpoolError): q.observe(self.config, required_free_bytes=1024**4)


if __name__ == '__main__': unittest.main()
