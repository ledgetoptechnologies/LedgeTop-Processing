"""Owned synthetic RAW/ZIP fixtures; no engine, NAS or publication action."""
from contextlib import closing
from dataclasses import asdict,replace
import base64
import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from processing import reference_treedar_bundle as bundle,reference_protocol as p
from processing.scratch_policy import ScratchLedger

FIXTURE=json.loads((Path(__file__).resolve().parents[1]/'contracts/treedar-bundle-v1.contract.json').read_text())


class ManifestTests(unittest.TestCase):
    def setUp(self):self.limits=bundle.BundleLimits.from_mapping(FIXTURE['limits'])

    def test_shared_fixture_native_semantics_and_canonical_digest_match(self):
        value=bundle.validate_manifest(FIXTURE['manifest'],FIXTURE['expected'],self.limits)
        self.assertEqual(value,FIXTURE['manifest'])
        self.assertEqual({name:list(item) for name,item in bundle.NATIVE_FILES.items()},FIXTURE['nativeVocabulary'])
        self.assertEqual(bundle.manifest_sha256(value),value['manifestSha256'])
        self.assertEqual(next(item['role'] for item in value['files'] if item['nativePath']=='chm.tif'),'canopy_height')
        value['files'][0]['role']='changed';self.assertNotEqual(value,FIXTURE['manifest'])

    def test_strict_metadata_wire_grammar_schema_binding_roles_paths_and_body(self):
        wire=json.dumps(FIXTURE['manifest'],separators=(',',':'))
        cases=[wire.replace('"schemaVersion":1','"schemaVersion":1,"schemaVersion":1'),
            wire.replace('"schemaVersion":1','"schemaVersion":1,"schema\\u0056ersion":1'),
            wire.replace('"schemaVersion":1','"schemaVersion":1.0'),wire+'false',b'\xff',
            '{"__proto__":{}}','{"schemaVersion":NaN}']
        for value in cases:
            with self.subTest(value=str(value)[:40]),self.assertRaises(bundle.BundleHeld):bundle.validate_manifest(value,FIXTURE['expected'],self.limits)
        changes=[lambda v:v.update(schemaVersion=True),lambda v:v.update(format='other'),lambda v:v.update(extra=True),
            lambda v:v['binding'].update(generation=2),lambda v:v['binding'].update(environment='production'),
            lambda v:v['binding'].update(inputManifestSha256='0'*64),lambda v:v['binding'].update(pipelineDigest='0'*64),
            lambda v:v['binding']['sourceLidar'].update(objectId='odm-output'),
            lambda v:v['binding']['sourceLidar'].update(provenanceReceiptId='other-receipt'),
            lambda v:v['files'][0].update(nativePath='../escape'),lambda v:v['files'][0].update(nativePath='CHM.TIF'),
            lambda v:v['files'][0].update(role='dem'),lambda v:v['files'][0].update(format='unknown'),
            lambda v:v['files'].reverse(),lambda v:v['files'].append(v['files'][0]),lambda v:v['files'][0].update(byteSize=0),
            lambda v:v.update(rawBytes=1)]
        for change in changes:
            value=copy.deepcopy(FIXTURE['manifest']);change(value)
            if value['schemaVersion'] is not True:value['manifestSha256']=bundle.manifest_sha256(value)
            with self.subTest(change=change),self.assertRaises(bundle.BundleHeld):bundle.validate_manifest(value,FIXTURE['expected'],self.limits)
        with self.assertRaises(bundle.BundleHeld):bundle.validate_manifest(wire,FIXTURE['expected'],replace(self.limits,maxManifestBytes=10))

    def test_every_limit_is_explicit_and_safe(self):
        for key in FIXTURE['limits']:
            missing=dict(FIXTURE['limits']);missing.pop(key)
            with self.assertRaises(bundle.BundleHeld):bundle.BundleLimits.from_mapping(missing)
            with self.assertRaises(bundle.BundleHeld):bundle.BundleLimits.from_mapping(dict(FIXTURE['limits'],**{key:0}))
        with self.assertRaises(bundle.BundleHeld):replace(self.limits,maxFiles=65)
        with self.assertRaises(bundle.BundleHeld):replace(self.limits,maxManifestBytes=2*1024**2)


class OwnedGuard:
    """Test-only live OS lease/reservation and retained terminal-proof checks."""
    def __init__(self,case):
        self.case=case;self.pid=os.getpid();self.deny=False;self.deny_after=None;self.mutations=0;self.after=None
        policy=case.policy
        self.binding=dict(policySha256=p.digest(asdict(policy)),expectedSha256=p.digest(case.expected),
            publicationId=policy.publication_id,sourceStageProofSha256=policy.source_stage_proof_sha256)

    def check(self,context,*,mutation):
        import fcntl
        c=self.case
        assert os.getpid()==self.pid and not self.deny
        assert hashlib.sha256(c.stage_proof.read_bytes()).hexdigest()==c.policy.source_stage_proof_sha256
        if context['manifestSha256'] is not None:
            source=json.loads(c.stage_proof.read_bytes())
            files=[dict(nativePath=item['name'],role=bundle.NATIVE_FILES[item['name']][0],format=bundle.NATIVE_FILES[item['name']][1],byteSize=item['byteSize'],sha256=item['sha256']) for item in source['files']]
            expected_manifest=dict(schemaVersion=1,format=bundle.FORMAT,**c.expected,files=files,rawBytes=sum(item['byteSize'] for item in files))
            assert bundle.manifest_sha256(expected_manifest)==context['manifestSha256']
        assert context['expected']==c.expected and context['reservationId']=='bundle-reservation'
        assert context['allocationBase']==c.policy.allocation_base and context['remainingReservedBytes']==c.policy.remaining_reserved_bytes
        with closing(sqlite3.connect(c.ledger.path.as_uri()+'?mode=ro',uri=True)) as db:
            db.row_factory=sqlite3.Row;row=db.execute('SELECT * FROM reservations WHERE id=?',('bundle-reservation',)).fetchone()
            assert row['allocated']==c.policy.allocation_base and row['remaining']==c.policy.remaining_reserved_bytes and row['state']=='engine_started'
        probe=os.open(c.lease_path,os.O_RDWR)
        try:
            try:fcntl.flock(probe,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:pass
            else:fcntl.flock(probe,fcntl.LOCK_UN);raise AssertionError('actual owner lease missing')
        finally:os.close(probe)
        if mutation:
            self.mutations+=1
            if self.after:self.after()
            if self.deny_after is not None and self.mutations>self.deny_after:raise AssertionError('synthetic authority loss')


@unittest.skipUnless(sys.platform.startswith('linux') and hasattr(os,'pread'),'Linux owned partial descriptors and OS lease')
class PartialWriterTests(unittest.TestCase):
    def setUp(self):
        import fcntl
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name);self.root.chmod(0o700)
        self.raw=self.root/'jobs'/'bundle-reservation'/'raw';self.output=self.raw.parent/'bundles';self.state=self.raw.parent/'bundle-state'
        for directory in (self.raw,self.output,self.state):directory.mkdir(mode=0o700,parents=True)
        native=getattr(self,'native_source',None)
        if native:
            for source in sorted(Path(native).rglob('*')):
                if source.is_dir():continue
                target=self.raw/source.relative_to(native);target.parent.mkdir(mode=0o755,parents=True,exist_ok=True)
                with source.open('rb') as stream,target.open('xb') as dest:shutil.copyfileobj(stream,dest,1024**2)
                target.chmod(0o644)
        else:
            for name,body in FIXTURE['payloadsBase64'].items():
                target=self.raw/name;target.parent.mkdir(mode=0o755,parents=True,exist_ok=True)
                target.write_bytes(base64.b64decode(body));target.chmod(0o644)
        self.expected=copy.deepcopy(getattr(self,'expected_override',FIXTURE['expected']))
        self.limits=bundle.BundleLimits.from_mapping(FIXTURE['limits'])
        self.stage_proof=self.state/'source-stage-proof.json'
        stage=dict(scope='synthetic-owner-fixture-not-acquisition',engineTerminal=True,expected=self.expected,
            files=[dict(name=str(path.relative_to(self.raw)),byteSize=path.stat().st_size,sha256=hashlib.sha256(path.read_bytes()).hexdigest()) for path in sorted(self.raw.rglob('*')) if path.is_file()])
        self.stage_proof.write_bytes(p.canonical_json(stage));self.stage_proof.chmod(0o600)
        self.lease_path=self.state/'owner.lock';self.lease_fd=os.open(self.lease_path,os.O_RDWR|os.O_CREAT|os.O_EXCL,0o600);self.addCleanup(os.close,self.lease_fd)
        fcntl.flock(self.lease_fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        raw_bytes=sum(path.stat().st_size for path in self.raw.rglob('*') if path.is_file())
        parent_input_bytes=getattr(self,'parent_input_bytes_override',self.expected['binding']['sourceLidar']['byteSize'])
        self.allocation_base=parent_input_bytes+raw_bytes+self.stage_proof.stat().st_size+17
        self.ledger=ScratchLedger(self.root/'scratch.sqlite',self.root,capacity_probe=lambda _:1024**3,safety_floor_bytes=0)
        total=self.allocation_base+self.limits.maxArchiveBytes+self.limits.maxManifestBytes+self.limits.maxControlProofBytes+4096
        self.ledger.reserve(job_id=self.expected['binding']['jobId'],generation=self.expected['binding']['generation'],reservation_id='bundle-reservation',
            manifest_sha256=self.expected['binding']['inputManifestSha256'],input_bytes=parent_input_bytes,
            output_budget=total-parent_input_bytes,environment=self.expected['binding']['environment'],ttl_seconds=600,worker_ready=True)
        self.ledger.record_allocated('bundle-reservation',self.allocation_base);self.ledger.set_engine_state('bundle-reservation','engine_started')
        with self.ledger._transaction() as db:remaining=self.ledger._reservation(db,'bundle-reservation')['remaining']
        self.policy=bundle.BundlePolicy(str(self.raw),str(self.output),str(self.state),'synthetic-bundle-publication','bundle-reservation',self.allocation_base,remaining,
            hashlib.sha256(self.stage_proof.read_bytes()).hexdigest(),self.limits)
        self.guard=OwnedGuard(self);self.writer=self.open()
        self.fd=os.open(self.writer.partial_path,os.O_RDWR|os.O_CREAT|os.O_EXCL,0o600);self.addCleanup(os.close,self.fd)
        self.originals={str(path.relative_to(self.raw)):path.read_bytes() for path in self.raw.rglob('*') if path.is_file()}

    def open(self,policy=None):return bundle.BundlePartialWriter(policy or self.policy,self.expected,guard=self.guard)

    def persist_intent(self,writer=None,fd=None):
        writer=writer or self.writer;fd=self.fd if fd is None else fd
        intent=writer.intent_for(fd)
        with os.fdopen(os.open(writer.intent_path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600),'wb') as stream:
            stream.write(p.canonical_json(intent));stream.flush();os.fsync(stream.fileno())
        return intent

    def assert_originals(self):
        self.assertEqual({str(path.relative_to(self.raw)):path.read_bytes() for path in self.raw.rglob('*') if path.is_file()},self.originals)

    def adopt_limits(self,limits):
        self.policy=replace(self.policy,limits=limits);self.guard=OwnedGuard(self);self.writer=self.open()

    def test_actual_streaming_archive_matches_shared_manifest_and_preserves_parent_and_sources(self):
        expected=self.writer.prepare();self.assertEqual(expected,FIXTURE['manifest'])
        self.persist_intent();before=self.ledger.path.read_bytes();receipt=self.writer.write_to(self.fd)
        self.assertEqual(receipt['state'],'partial_complete_needs_durable_publication')
        self.assertNotIn('role',receipt);self.assertEqual(receipt['binding'],self.expected['binding'])
        self.assertEqual(receipt['allocation']['total'],self.policy.allocation_base+receipt['allocation']['newBytes'])
        self.assertEqual(receipt['allocation']['newBytes'],receipt['archive']['byteSize']+receipt['allocation']['manifestBytes']+receipt['allocation']['controlBytes'])
        self.assertEqual(self.ledger.path.read_bytes(),before)
        self.assertEqual(json.loads(self.writer.intent_path.read_bytes())['state'],'streaming')
        with zipfile.ZipFile(self.writer.partial_path) as archive:
            self.assertEqual(json.loads(archive.read(bundle.MANIFEST_NAME)),expected)
            for item in expected['files']:self.assertEqual(hashlib.sha256(archive.read('artifacts/'+item['nativePath'])).hexdigest(),item['sha256'])
            self.assertTrue(all(info.date_time==(1980,1,1,0,0,0) and not info.flag_bits&1 for info in archive.infolist()))
        self.assert_originals()

    def test_directory_census_refuses_unknown_or_excess_entries_before_sort_or_stat(self):
        class Entries:
            def __init__(self,names):self.names=iter(names);self.seen=0
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def __iter__(self):return self
            def __next__(self):
                from types import SimpleNamespace
                name=next(self.names);self.seen+=1;return SimpleNamespace(name=name)
        unknown=Entries('unknown-%s'%i for i in range(1000))
        with patch.object(bundle.os,'scandir',return_value=unknown),patch.object(bundle.Path,'lstat',side_effect=AssertionError('census must precede stat')):
            with self.assertRaises(bundle.BundleHeld):self.writer._inventory(hash_files=True)
        self.assertEqual(unknown.seen,1)
        self.adopt_limits(replace(self.limits,maxFiles=1));known=Entries(['chm.tif','dtm.tif']+['unknown']*1000)
        with patch.object(bundle.os,'scandir',return_value=known),patch.object(bundle.Path,'lstat',side_effect=AssertionError('bounded census must precede stat')):
            with self.assertRaises(bundle.BundleHeld):self.writer._inventory(hash_files=True)
        self.assertEqual(known.seen,2);self.assertEqual(os.fstat(self.fd).st_size,0);self.assert_originals()

    def test_growing_source_reads_only_observed_size_plus_one_byte(self):
        self.adopt_limits(replace(self.limits,maxRawBytes=10000))
        source=self.raw/sorted(self.originals)[0];initial=source.stat().st_size;real_read=os.read;observed=[]
        def grow_then_read(fd,count):
            with source.open('ab') as stream:stream.write(b'x'*1024**2)
            block=real_read(fd,count);observed.append((count,len(block)));return block
        with patch.object(bundle.os,'read',side_effect=grow_then_read):
            with self.assertRaises(bundle.BundleHeld):self.writer.prepare()
        self.assertEqual(observed,[(initial+1,initial+1)])
        self.assertLessEqual(sum(size for _,size in observed),10001)
        self.assertEqual(os.fstat(self.fd).st_size,0)
        for name,body in self.originals.items():
            with (self.raw/name).open('rb') as stream:self.assertEqual(stream.read(len(body)),body)

    def test_cumulative_raw_budget_refuses_before_hashing_next_file(self):
        maximum=sum(len(body) for body in self.originals.values())-1
        self.adopt_limits(replace(self.limits,maxRawBytes=maximum));read_bytes=0;real_read=os.read
        def count_read(fd,count):
            nonlocal read_bytes
            block=real_read(fd,count);read_bytes+=len(block);return block
        with patch.object(bundle.os,'read',side_effect=count_read):
            with self.assertRaises(bundle.BundleHeld):self.writer.prepare()
        self.assertLessEqual(read_bytes,maximum);self.assertEqual(os.fstat(self.fd).st_size,0);self.assert_originals()

    def test_missing_or_changed_durable_intent_refuses_before_output_write(self):
        with self.assertRaises(bundle.BundleHeld):self.writer.write_to(self.fd)
        self.assertEqual(os.fstat(self.fd).st_size,0);self.assert_originals()
        self.writer=self.open();intent=self.persist_intent();intent['state']='completed';self.writer.intent_path.write_bytes(p.canonical_json(intent))
        with self.assertRaises(bundle.BundleHeld):self.writer.write_to(self.fd)
        self.assertEqual(os.fstat(self.fd).st_size,0)

    def test_partial_failure_is_retained_and_neither_same_owner_nor_restart_overwrites_it(self):
        self.persist_intent();self.guard.deny_after=1
        with self.assertRaises(bundle.BundleHeld):self.writer.write_to(self.fd)
        size=os.fstat(self.fd).st_size;self.assertGreater(size,0);self.assert_originals()
        self.guard.deny_after=None
        with self.assertRaises(bundle.BundleHeld):self.writer.write_to(self.fd)
        reopened=self.open()
        with self.assertRaises(bundle.BundleHeld):reopened.write_to(self.fd)
        self.assertEqual(os.fstat(self.fd).st_size,size)

    def test_source_inventory_content_links_and_unknown_names_are_refused(self):
        self.writer.prepare();changed=self.raw/'chm.tif';changed.write_bytes(b'changed')
        with self.assertRaises(bundle.BundleHeld):self.writer.intent_for(self.fd)
        changed.write_bytes(self.originals['chm.tif']);other=self.raw/'unknown.bin';other.write_bytes(b'x')
        with self.assertRaises(bundle.BundleHeld):self.open().prepare()
        other.unlink();link=self.raw/'unknown-link';link.symlink_to(self.raw/'chm.tif')
        with self.assertRaises(bundle.BundleHeld):self.open().prepare()

    def test_terminal_source_inventory_is_not_replaced_by_freshly_hashed_modified_raw(self):
        changed=self.raw/'chm.tif';changed.write_bytes(b'changed before prepare')
        with self.assertRaises(bundle.BundleHeld):self.writer.prepare()
        self.assertEqual(os.fstat(self.fd).st_size,0)

    def test_known_native_hardlinks_symlinks_and_fifos_are_not_read_as_outputs(self):
        source=self.raw/'chm.tif';alias=self.root/'retained-alias';os.link(source,alias)
        with self.assertRaises(bundle.BundleHeld):self.writer.prepare()
        alias.unlink();source.unlink();source.symlink_to(self.raw/'dtm.tif')
        with self.assertRaises(bundle.BundleHeld):self.open().prepare()
        self.assertTrue(source.is_symlink());source.unlink();os.mkfifo(source,0o600)
        with self.assertRaises(bundle.BundleHeld):self.open().prepare()
        self.assertTrue(source.exists());self.assertEqual(os.fstat(self.fd).st_size,0)

    def test_exact_descriptor_path_mode_empty_size_and_no_append_are_required(self):
        os.write(self.fd,b'retained');os.lseek(self.fd,0,os.SEEK_SET)
        with self.assertRaises(bundle.BundleHeld):self.writer.intent_for(self.fd)
        self.assertEqual(self.writer.partial_path.read_bytes(),b'retained')
        self.assert_originals()

    def test_output_readonly_append_alias_and_public_modes_never_authorize_writes(self):
        for flags in (os.O_RDONLY,os.O_RDWR|os.O_APPEND):
            fd=os.open(self.writer.partial_path,flags)
            try:
                with self.assertRaises(bundle.BundleHeld):self.writer.intent_for(fd)
            finally:os.close(fd)
        self.writer.partial_path.chmod(0o644)
        with self.assertRaises(bundle.BundleHeld):self.writer.intent_for(self.fd)
        self.writer.partial_path.chmod(0o600)
        other=self.output/'other-empty';fd=os.open(other,os.O_RDWR|os.O_CREAT|os.O_EXCL,0o600)
        try:
            with self.assertRaises(bundle.BundleHeld):self.writer.intent_for(fd)
        finally:os.close(fd)
        self.assertEqual(os.fstat(self.fd).st_size,0)

    def test_owner_lease_and_terminal_source_proof_cannot_be_substituted(self):
        import fcntl
        fcntl.flock(self.lease_fd,fcntl.LOCK_UN)
        with self.assertRaises(bundle.BundleHeld):self.writer.prepare()
        fcntl.flock(self.lease_fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        self.stage_proof.write_bytes(b'changed')
        with self.assertRaises(bundle.BundleHeld):self.writer.prepare()
        self.assertEqual(os.fstat(self.fd).st_size,0)

    def test_derived_capacity_cannot_reset_parent_allocation_or_spend_unreserved_bytes(self):
        bad=replace(self.policy,remaining_reserved_bytes=1)
        self.guard.binding=dict(self.guard.binding,policySha256=p.digest(asdict(bad)))
        # Guard must compare this declaration to the real existing reservation.
        with self.assertRaises(bundle.BundleHeld):self.open(bad)
        self.assertEqual(os.fstat(self.fd).st_size,0);self.assert_originals()

    def test_archive_cap_error_retains_a_bounded_partial_and_unchanged_raw(self):
        self.adopt_limits(replace(self.limits,maxArchiveBytes=256));self.persist_intent()
        with self.assertRaises(bundle.BundleHeld):self.writer.write_to(self.fd)
        self.assertGreater(os.fstat(self.fd).st_size,0);self.assertLessEqual(os.fstat(self.fd).st_size,256)
        self.assert_originals();self.assertEqual(json.loads(self.writer.intent_path.read_bytes())['state'],'streaming')

    def test_raw_zip_and_control_allowance_must_fit_coexisting_disk_budget(self):
        self.adopt_limits(replace(self.limits,maxValidationDiskBytes=1));self.persist_intent()
        with self.assertRaises(bundle.BundleHeld):self.writer.write_to(self.fd)
        self.assertEqual(os.fstat(self.fd).st_size,0);self.assert_originals()

    def test_actual_intent_control_metadata_must_fit_its_separate_allowance(self):
        self.adopt_limits(replace(self.limits,maxControlProofBytes=1));self.persist_intent()
        with self.assertRaises(bundle.BundleHeld):self.writer.write_to(self.fd)
        self.assertEqual(os.fstat(self.fd).st_size,0);self.assert_originals()

    def test_lost_output_fsync_remains_an_unknown_partial_not_a_completed_publication(self):
        self.persist_intent();actual=os.fsync
        def lost(fd):
            if fd==self.fd:raise OSError('synthetic output flush outcome unknown')
            return actual(fd)
        with patch('processing.reference_treedar_bundle.os.fsync',side_effect=lost):
            with self.assertRaises(bundle.BundleHeld):self.writer.write_to(self.fd)
        self.assertGreater(os.fstat(self.fd).st_size,0);self.assert_originals()
        with self.assertRaises(bundle.BundleHeld):self.open().write_to(self.fd)

    def test_intent_and_existing_directory_publication_barriers_precede_first_zip_write(self):
        self.persist_intent();intent=self.writer.intent_path.stat();seen=set();actual_sync=os.fsync;actual_write=bundle._BoundedZip.write
        required={(intent.st_dev,intent.st_ino)}
        for root in (self.state,self.state.parent,self.output,self.output.parent):
            info=root.stat();required.add((info.st_dev,info.st_ino))
        def sync(fd):
            info=os.fstat(fd);seen.add((info.st_dev,info.st_ino));return actual_sync(fd)
        def write(stream,value):
            self.assertTrue(required<=seen);return actual_write(stream,value)
        with patch('processing.reference_treedar_bundle.os.fsync',side_effect=sync),patch.object(bundle._BoundedZip,'write',write):
            self.writer.write_to(self.fd)

    def test_missing_private_roots_are_never_created(self):
        bad=replace(self.policy,output_root=str(self.root/'missing'/'nested'))
        with self.assertRaises(bundle.BundleHeld):self.open(bad)
        self.assertFalse((self.root/'missing').exists())

    def test_forked_owner_cannot_reuse_the_writer_or_inherited_lease_guard(self):
        child=os.fork()
        if child==0:
            try:
                try:self.writer.prepare()
                except bundle.BundleHeld:pass
                else:os._exit(3)
                try:self.open()
                except bundle.BundleHeld:pass
                else:os._exit(4)
                os._exit(0)
            except BaseException:os._exit(5)
        _,status=os.waitpid(child,0);self.assertEqual(os.waitstatus_to_exitcode(status),0)

