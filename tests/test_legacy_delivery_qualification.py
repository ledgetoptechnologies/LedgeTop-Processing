"""Synthetic reader observation versus actual frozen predecessor census."""
import copy
import hashlib
import json
import os
from pathlib import Path
import unittest

from ops import legacy_delivery_qualification as q
from ops import capture_legacy_delivery as runner
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

    def test_captured_observation_is_keyed_to_volume_helpers_and_frozen_baseline(self):
        observed = self.observation(); hashes = runner.helper_hashes()
        wire = q.seal_capture(observed,key=self.old.key,container_id='c'*64,state_volume='state',helper_hashes=hashes)
        baseline = self.old.census()
        checked = q.validate_capture_against_baseline(wire,self.plan,baseline,reader_sha256=self.reader,
            key=self.old.key,state_volume='state',helper_hashes=hashes)
        self.assertEqual(checked['observation'],observed)
        for kwargs in (dict(key=b'x'*32,state_volume='state',helper_hashes=hashes),
                       dict(key=self.old.key,state_volume='other',helper_hashes=hashes),
                       dict(key=self.old.key,state_volume='state',helper_hashes={'changed':'f'*64})):
            with self.assertRaises(Exception):
                q.validate_capture(wire,self.plan,reader_sha256=self.reader,**kwargs)
        changed=json.loads(wire); changed['observation']['archives'].clear()
        with self.assertRaises(Exception):
            q.validate_capture(canonical(changed),self.plan,reader_sha256=self.reader,key=self.old.key,
                               state_volume='state',helper_hashes=hashes)

    def test_capture_checks_exact_image_and_readonly_mount_before_start_and_cleans_owned_container(self):
        result = canonical(self.observation()); image=self.image; commit=self.commit
        class Docker:
            def __init__(self, bad=False): self.bad=bad; self.started=False; self.removed=False
            def image(self, ident): self.image_seen=ident
            def command(self,*args,**kwargs):
                if args[:2]==('volume','inspect'): return canonical([{'Name':'state'}])
                if args[:2]==('container','create'):
                    self.name=args[args.index('--name')+1]
                    self.token=args[args.index('--label')+1].split('=',1)[1]
                    bind=[item for item in args if item.startswith('type=bind,')][0]
                    self.folder=bind.split(',')[1].split('=',1)[1]
                    return ('d'*64+'\n').encode()
                if args[:2]==('container','start'):
                    self.started=True; self.assert_key=kwargs['input_bytes']; return result
                if args[:2]==('container','rm'):
                    self.removed=True; return b''
                raise AssertionError(args)
            def inspect(self, ident):
                return dict(Id=ident,Image=image,Name='/'+self.name,
                    Config=dict(Labels={runner.OWNER:self.token},User='10001:10001',Entrypoint=['python'],
                                Cmd=['/qualification/observer.py',image,commit]),
                    HostConfig=dict(NetworkMode='none',ReadonlyRootfs=True),
                    State=dict(Running=False,ExitCode=0),Mounts=[
                        dict(Type='volume',Name='state',Destination='/prototype-state',RW=self.bad),
                        dict(Type='bind',Source=self.folder,Destination='/qualification',RW=False)])
        for bad in (False,True):
            docker=Docker(bad)
            if bad:
                with self.assertRaises(Exception):
                    runner.capture(image=image,source_commit=commit,state_volume='state',authority=self.old.authority,
                                   reader_sha256=self.reader,key=self.old.key,docker=docker)
                self.assertFalse(docker.started)
            else:
                wire=runner.capture(image=image,source_commit=commit,state_volume='state',authority=self.old.authority,
                                    reader_sha256=self.reader,key=self.old.key,docker=docker)
                q.validate_capture_against_baseline(wire,self.plan,self.old.census(),reader_sha256=self.reader,
                    key=self.old.key,state_volume='state',helper_hashes=runner.helper_hashes())
                self.assertTrue(docker.started); self.assertEqual(docker.assert_key,self.old.key)
            self.assertTrue(docker.removed)
