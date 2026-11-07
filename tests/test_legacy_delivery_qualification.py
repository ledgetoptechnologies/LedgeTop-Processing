"""Synthetic reader observation versus actual frozen predecessor census."""
import copy
import hashlib
import json
import os
from pathlib import Path
import unittest

from ops import legacy_delivery_qualification as q
from ops.enrolled_coordinator_transport import canonical
from processing import legacy_viewer_readonly
from tests import test_coordinator_predecessor_census as fixture


@unittest.skipUnless(os.name == 'posix', 'reader requires Linux nofollow descriptors')
class LegacyDeliveryQualificationTests(unittest.TestCase):
    def setUp(self):
        self.old = fixture.ClosedPredecessorCensusTests()
        self.old.setUp(); self.addCleanup(self.old.doCleanups)
        self.image = 'sha256:' + 'a' * 64
        self.commit = 'b' * 40
        self.reader = hashlib.sha256(Path(legacy_viewer_readonly.__file__).read_bytes()).hexdigest()
        self.plan = dict(authorityId=self.old.authority, candidate=dict(imageId=self.image, sourceCommit=self.commit))

    def observation(self):
        return q.observe(self.old.root, key=self.old.key, candidate_image=self.image,
                         candidate_source=self.commit, nodes=('cluster','main'), max_archive_bytes=16*1024*1024)

    def verify(self, value, baseline=None):
        return q.validate_against_baseline(canonical(value), self.plan,
            self.old.census() if baseline is None else baseline, reader_sha256=self.reader)

    def test_exact_reader_output_matches_full_retained_custody(self):
        task, job, directory = self.old.tasks[0]
        receipt = json.loads((directory/'all.zip.receipt').read_text())
        intent = {name: receipt[name] for name in ('version','task','generation','job','node','engine')}
        (directory/'all.zip.intent').write_text(json.dumps(intent)); (directory/'all.zip.intent').chmod(0o600)
        value = self.observation()
        self.assertEqual(len(value['archives']), 7)
        self.assertEqual(self.verify(value), value)
        self.assertNotIn(task, json.dumps(value))

    def test_changed_candidate_or_reader_rejected(self):
        value = self.observation()
        for field, replacement in (('candidateImageId', 'sha256:'+'c'*64),
                                   ('candidateSourceCommit', 'd'*40), ('readerSha256', 'e'*64)):
            changed = dict(value, **{field: replacement})
            with self.assertRaises(Exception): self.verify(changed)

    def test_noncanonical_duplicate_and_invalid_scope_rejected(self):
        value = self.observation()
        for raw in (json.dumps(value, indent=2).encode(), b'{"protocol":"x","protocol":"x"}'):
            with self.assertRaises(Exception): q.validate_artifact(raw, self.plan, reader_sha256=self.reader)
        changed = dict(value, scope='installed-http-and-frozen')
        with self.assertRaises(Exception): self.verify(changed)

    def test_changed_archive_metadata_or_extra_archive_rejected(self):
        value = self.observation(); baseline = self.old.census()
        for field in ('bytes','content','mode'):
            changed = copy.deepcopy(value); row = next(iter(changed['archives'].values()))
            row[field] = {'bytes': row['bytes']+1, 'content':'f'*64, 'mode':0o400}[field]
            with self.assertRaises(Exception): self.verify(changed, baseline)
        changed = copy.deepcopy(value); changed['archives']['f'*64] = dict(next(iter(value['archives'].values())))
        with self.assertRaises(Exception): self.verify(changed, baseline)

    def test_predecessor_table_change_or_incomplete_census_rejected(self):
        value = self.observation(); baseline = self.old.census()
        for mutate in ('table','count','scope','validated'):
            changed = copy.deepcopy(baseline)
            if mutate == 'table': changed['tables']['jobs']['content'] = 'f'*64
            elif mutate == 'count': changed['archiveReceipts'] += 1
            elif mutate == 'scope': changed['validationScope'] = 'metadata-paths-receipts-only'
            else: changed['retainedFilesValidated'] = False
            with self.assertRaises(Exception): self.verify(value, changed)

    def test_bad_intent_and_unknown_file_fail_observation(self):
        directory = self.old.tasks[0][2]
        (directory/'all.zip.intent').write_text('{}'); (directory/'all.zip.intent').chmod(0o600)
        with self.assertRaises(Exception): self.observation()
        (directory/'all.zip.intent').unlink()
        (directory/'all.zip.partial').write_bytes(b'unknown'); (directory/'all.zip.partial').chmod(0o600)
        with self.assertRaises(Exception): self.observation()
