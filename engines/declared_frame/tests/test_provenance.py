"""Real OpenSSH signature checks with throwaway keys made and deleted in a temp folder."""
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from support import NONCE, Refusals  # noqa: E402

from declared_frame import provenance  # noqa: E402

PEOPLE = ('test-sealer', 'test-operator', 'test-writer', 'test-outsider')
CONTAINER = {'network': 'none', 'read_only': True, 'cap_drop': ['ALL']}


@unittest.skipUnless(shutil.which('ssh-keygen'), 'needs ssh-keygen')
class ProvenanceTests(Refusals):
    @classmethod
    def setUpClass(cls):
        cls.folder = Path(cls.enterClassContext(tempfile.TemporaryDirectory()))
        lines = []
        for who in PEOPLE:
            key = cls.folder / who
            subprocess.run(['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-C', who, '-f', str(key)],
                           check=True, capture_output=True)
            lines.append(f"{who} {' '.join((cls.folder / (who + '.pub')).read_text().split()[:2])}")
        cls.signers = ('\n'.join(lines[:3]) + '\n').encode()

    def sign(self, who, role, raw):
        target = self.folder / 'payload'
        signature = self.folder / 'payload.sig'
        target.write_bytes(raw)
        signature.unlink(missing_ok=True)
        subprocess.run(['ssh-keygen', '-q', '-Y', 'sign', '-f', str(self.folder / who), '-n',
                        provenance.ROLES[role], str(target)], check=True, capture_output=True)
        return signature.read_bytes()

    def setUp(self):
        self.manifest = {
            'schema': provenance.MANIFEST, 'image': 'sha256:' + '1' * 64,
            'base_image': 'sha256:' + '2' * 64, 'engine_version': 'x',
            'engine_files': {'modules/gcp.py': '3' * 64},
            'candidate_files': {'contract.py': '4' * 64}, 'profile_sha256': '5' * 64,
            'grid_sha256': '6' * 64, 'proj_db_sha256': '7' * 64,
            'runtime': {'gdal': 'x', 'pyproj': 'x', 'proj': 'x'},
            'command': ['python3', 'collect'], 'container': CONTAINER}
        self.authorization = {
            'schema': provenance.AUTHORIZATION, 'manifest_sha256': None, 'run_nonce': NONCE,
            'operator': 'test-operator', 'writer': 'test-writer', 'ledger_id': 'ledger-1',
            'purpose': 'synthetic'}
        self.receipt = {'run_nonce': NONCE, 'manifest_sha256': None, 'authorization_sha256': None,
                        'ledger_id': 'ledger-1', 'container': 'c' * 12, 'observed': None}
        self.launcher = {'image': self.manifest['image'], 'command': self.manifest['command'],
                         'container': CONTAINER, 'container_id': 'c' * 64, 'exit_code': 0,
                         'receipt_sha256': None}
        self.signed_by = {'manifest': 'test-sealer', 'authorization': 'test-operator',
                          'receipt': 'test-writer'}

    def bundle(self):
        """Fill in honest links, then sign. Values a test set itself are left alone."""
        dump = lambda value: json.dumps(value, sort_keys=True).encode()
        fill = lambda value, key, item: value.__setitem__(
            key, item if value[key] is None else value[key])
        manifest = dump(self.manifest)
        fill(self.authorization, 'manifest_sha256', provenance.sha256(manifest))
        authorization = dump(self.authorization)
        fill(self.receipt, 'manifest_sha256', self.authorization['manifest_sha256'])
        fill(self.receipt, 'authorization_sha256', provenance.sha256(authorization))
        fill(self.receipt, 'observed', {key: self.manifest[key] for key in (
            'engine_version', 'engine_files', 'candidate_files', 'profile_sha256', 'grid_sha256',
            'proj_db_sha256', 'runtime')})
        receipt = dump(self.receipt)
        fill(self.launcher, 'receipt_sha256', provenance.sha256(receipt))
        return {
            'signers': self.signers, 'manifest': manifest, 'authorization': authorization,
            'receipt': receipt, 'launcher': self.launcher,
            'manifest_sig': self.sign(self.signed_by['manifest'], 'sealer', manifest),
            'authorization_sig': self.sign(self.signed_by['authorization'], 'operator',
                                           authorization),
            'receipt_sig': self.sign(self.signed_by['receipt'], 'writer', receipt)}

    def verify(self, bundle=None, **change):
        bundle = bundle or self.bundle()
        return provenance.verify_run(bundle, **{
            'trust_sha256': provenance.sha256(self.signers), 'sealer': 'test-sealer', **change})

    def test_honest_run(self):
        manifest, authorization, receipt = self.verify()
        self.assertEqual((manifest['image'], authorization['writer'], receipt['run_nonce']),
                         ('sha256:' + '1' * 64, 'test-writer', NONCE))

    def test_trust_root_is_the_reviewers(self):
        self.refused('trust_root_mismatch', self.verify, trust_sha256='0' * 64)
        bundle = self.bundle()
        bundle['signers'] += b'# added\n'
        self.refused('trust_root_mismatch', self.verify, bundle)

    def test_every_document_is_signed_by_its_own_role(self):
        for name, role in (('manifest', 'sealer'), ('authorization', 'operator'),
                           ('receipt', 'writer')):
            for who in ('test-outsider', 'test-sealer', 'test-writer'):
                self.setUp()
                if self.signed_by[name] == who:
                    continue
                self.signed_by[name] = who
                with self.subTest(document=name, who=who):
                    self.refused(f'{role}_signature_invalid', self.verify)

    def test_one_changed_byte_breaks_each_document(self):
        for name, role in (('manifest', 'sealer'), ('authorization', 'operator'),
                           ('receipt', 'writer')):
            bundle = self.bundle()
            bundle[name] += b' '
            self.refused(f'{role}_signature_invalid', self.verify, bundle)

    def test_a_signature_for_another_role_is_not_reused(self):
        bundle = self.bundle()
        bundle['receipt_sig'] = self.sign('test-writer', 'operator', bundle['receipt'])
        self.refused('writer_signature_invalid', self.verify, bundle)

    def test_roles_and_keys_are_separate(self):
        self.authorization['writer'] = 'test-operator'
        self.refused('roles_must_be_separate', self.verify)
        self.setUp()
        saved = type(self).signers
        self.addCleanup(setattr, type(self), 'signers', saved)
        lines = saved.decode().splitlines()
        shared = lines[1].split(' ', 1)[1]
        type(self).signers = '\n'.join(
            [lines[0], lines[1], 'test-writer ' + shared]).encode() + b'\n'
        self.refused('signing_keys_must_be_separate', self.verify)

    def test_run_is_bound(self):
        for target, key, value, code in (
                ('receipt', 'run_nonce', 'b' * 64, 'receipt_not_bound'),
                ('receipt', 'manifest_sha256', '0' * 64, 'receipt_not_bound'),
                ('receipt', 'authorization_sha256', '0' * 64, 'receipt_not_bound'),
                ('receipt', 'ledger_id', 'other', 'receipt_not_bound'),
                ('authorization', 'manifest_sha256', '0' * 64, 'authorization_mismatch'),
                ('authorization', 'run_nonce', 'short', 'authorization_mismatch'),
                ('authorization', 'purpose', 'production', 'authorization_mismatch')):
            self.setUp()
            getattr(self, target)[key] = value
            with self.subTest(key=key):
                self.refused(code, self.verify)

    def test_observed_runtime_must_equal_the_seal(self):
        for key in ('engine_version', 'engine_files', 'candidate_files', 'profile_sha256',
                    'grid_sha256', 'proj_db_sha256', 'runtime'):
            self.setUp()
            self.receipt['observed'] = {k: self.manifest[k] for k in (
                'engine_version', 'engine_files', 'candidate_files', 'profile_sha256',
                'grid_sha256', 'proj_db_sha256', 'runtime') if k != key}
            error = self.refused('observed_manifest_mismatch', self.verify)
            self.assertEqual(error.details, {'field': key})

    def test_launcher_is_only_compared(self):
        for key, value in (('image', 'sha256:' + '9' * 64), ('command', ['other']),
                           ('container', {'network': 'bridge'}), ('container_id', 'd' * 64),
                           ('exit_code', 1), ('receipt_sha256', '0' * 64)):
            self.setUp()
            self.launcher[key] = value
            self.refused('launcher_mismatch', self.verify)
        self.setUp()
        del self.launcher['image']
        self.refused('launcher_malformed', self.verify)

    def test_malformed_manifest(self):
        self.manifest['engine_files'] = {}
        self.refused('manifest_malformed', self.verify)
        self.setUp()
        self.manifest['extra'] = 1
        self.refused('manifest_malformed', self.verify)


if __name__ == '__main__':
    unittest.main(verbosity=2)
