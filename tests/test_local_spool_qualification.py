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
        self.recipe = dict(imageId='sha256:' + 'c' * 64,
            mounts=[dict(type='bind', source=str(self.mount), target=str(self.mount), readOnly=False)])

    def observation(self):
        with patch('os.path.ismount', return_value=True):
            return q.observe(self.config, self.recipe, required_free_bytes=4096)

    def test_actual_write_readback_fsync_and_retention(self):
        retained = self.root / 'retained-client-object'
        retained.write_bytes(b'retained')
        before = retained.stat()
        observation = self.observation()
        q.validate(canonical(observation), self.config, self.recipe, expected_uid=os.geteuid(),
            now=observation['observedUnixSeconds'])
        self.assertEqual(retained.read_bytes(), b'retained')
        self.assertEqual(retained.stat().st_ino, before.st_ino)
        self.assertEqual(list(self.root.iterdir()), [retained])

    def test_reject_changed_config_identity_and_expired_or_insufficient_evidence(self):
        observation = self.observation()
        now = observation['observedUnixSeconds']
        changed = copy.deepcopy(self.settings); changed['mountId'] = 'b' * 64
        with self.assertRaises(TransportHeld):
            q.validate(canonical(observation), canonical(dict(schemaVersion=6, sourceSpool=changed)), self.recipe,
                expected_uid=os.geteuid(), now=now)
        for field, value in [('rootInode', -1), ('runtimeUid', os.geteuid() + 1),
                             ('availableBytes', 0), ('probeBytes', 0)]:
            bad = dict(observation, **{field: value})
            with self.assertRaises(TransportHeld):
                q.validate(canonical(bad), self.config, self.recipe, expected_uid=os.geteuid(), now=now)
        with self.assertRaises(TransportHeld):
            q.validate(canonical(observation), self.config, self.recipe, expected_uid=os.geteuid(), now=now + 86401)

    def test_missing_changed_marker_and_insufficient_space_fail_closed(self):
        self.marker.write_text('b' * 64)
        with patch('os.path.ismount', return_value=True):
            with self.assertRaises(SourceSpoolError): q.observe(self.config, self.recipe)
        self.marker.write_text('a' * 64)
        with patch('os.path.ismount', return_value=True):
            with self.assertRaises(SourceSpoolError): q.observe(self.config, self.recipe, required_free_bytes=1024**4)

    def test_different_host_bind_and_nested_shadow_mount_fail_closed(self):
        observation = self.observation()
        other = self.mount / 'different-bind'
        other.mkdir()
        wrong = copy.deepcopy(self.recipe)
        wrong['mounts'][0]['source'] = str(other)
        # Even if the recipe digest and claimed source are rewritten, live
        # host inode identity must match the actual observation.
        rewritten = dict(observation, bindSource=str(other), finalRecipeSha256=q.digest(wrong))
        with self.assertRaises(TransportHeld):
            q.validate(canonical(rewritten), self.config, wrong, expected_uid=os.geteuid(),
                now=observation['observedUnixSeconds'])
        shadow = copy.deepcopy(self.recipe)
        shadow['mounts'].append(dict(type='bind', source=str(other), target=str(self.root), readOnly=False))
        with self.assertRaises(TransportHeld):
            q.validate(canonical(observation), self.config, shadow, expected_uid=os.geteuid(),
                now=observation['observedUnixSeconds'])

    def test_final_container_identity_and_capacity_must_match(self):
        observation = self.observation()
        fields = ('finalConfigSha256', 'runtimeUid', 'rootDevice', 'rootInode',
                  'mountDevice', 'mountInode', 'markerDevice', 'markerInode', 'availableBytes')
        live = {field: observation[field] for field in fields}
        self.assertEqual(q.verify_live(observation, live), {k:v for k,v in live.items() if k != 'availableBytes'})
        for field in ('rootInode', 'mountInode', 'markerInode', 'runtimeUid'):
            bad = dict(live, **{field: live[field] + 1})
            with self.assertRaises(TransportHeld): q.verify_live(observation, bad)
        with self.assertRaises(TransportHeld): q.verify_live(observation, dict(live, availableBytes=0))


if __name__ == '__main__': unittest.main()
