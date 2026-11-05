"""Immutable TAR qualification uses synthetic contents; no live datasets."""
import copy
import json
import hashlib
import hmac
import io
from pathlib import Path
import tarfile
import unittest

from ops import enrolled_coordinator_custody_ops as c
from ops import coordinator_predecessor_census as old

KEY=b'synthetic-private-census-key-000'


def path_id(name):return hmac.new(KEY,b'path\0'+name.encode(),hashlib.sha256).hexdigest()


def fixture():
    files={'jobs.sqlite3':b'synthetic-original-sqlite-bytes','coordinator.lock':b'',
           'viewer-tasks/input.jpg':b'synthetic-photo','viewer-tasks/all.zip':b'synthetic-archive'}
    baseline=dict(profile=old.PROFILE,retainedFilesValidated=True,validationScope='full-retained-custody',rootMode=0o755,
        files={path_id(name):dict(bytes=len(value),mode=0o644 if name=='coordinator.lock' else 0o600,
            content=hmac.new(KEY,b'file\0'+value,hashlib.sha256).hexdigest()) for name,value in files.items()},
        directories={path_id('viewer-tasks'):dict(mode=0o700),path_id('empty-retained'):dict(mode=0o700)})
    return files,baseline


def archive(files,*,mutation=None,format=tarfile.PAX_FORMAT):
    output=io.BytesIO()
    with tarfile.open(fileobj=output,mode='w',format=format) as tar:
        for name,mode in (('.',0o755),('viewer-tasks',0o700),('empty-retained',0o700)):
            item=tarfile.TarInfo(name);item.type=tarfile.DIRTYPE;item.mode=mode;item.uid=item.gid=10001
            if mutation:mutation(item)
            tar.addfile(item)
        for name,value in files.items():
            item=tarfile.TarInfo('./'+name);item.uid=item.gid=10001;item.mode=0o644 if name=='coordinator.lock' else 0o600
            item.size=len(value)
            if mutation:mutation(item)
            tar.addfile(item,io.BytesIO(value))
    return output.getvalue()


class ArchiveCustodyTests(unittest.TestCase):
    def setUp(self):self.files,self.baseline=fixture()

    def validate(self,raw,**changes):
        args=dict(baseline=self.baseline,key=KEY,max_bytes=1048576,max_seconds=2);args.update(changes)
        return c.validate_tar(io.BytesIO(raw),**args)

    def test_complete_tar_preserves_every_original_byte_uid_mode_and_empty_directory(self):
        raw=archive(self.files);proof=self.validate(raw)
        self.assertEqual(proof['archiveSha256'],hashlib.sha256(raw).hexdigest())
        self.assertEqual(proof['fileCount'],4);self.assertEqual(proof['directoryCount'],2)
        self.assertFalse(proof['restoreAuthorized']);self.assertFalse(proof['originalAuthorityProven'])

    def test_changed_missing_and_unknown_contents_refuse(self):
        for files in (dict(self.files,**{'jobs.sqlite3':b'changed'}),
                      {k:v for k,v in self.files.items() if k!='coordinator.lock'},
                      dict(self.files,**{'unreviewed-file':b'unexpected'})):
            with self.subTest(files=list(files)):
                with self.assertRaises(c.CustodyHeld):self.validate(archive(files))

    def test_copy_cannot_promote_metadata_probe(self):
        bad=dict(self.baseline,retainedFilesValidated=False)
        with self.assertRaisesRegex(c.CustodyHeld,'full_original'):self.validate(archive(self.files),baseline=bad)

    def test_wrong_uid_gid_or_mode_refuses_before_content(self):
        for name,value in (('uid',0),('gid',1000),('mode',0o777)):
            def change(item):
                if item.name=='./jobs.sqlite3':setattr(item,name,value)
            with self.subTest(name=name):
                with self.assertRaises(c.CustodyHeld):self.validate(archive(self.files,mutation=change))

    def test_symlink_hardlink_and_traversal_are_never_extracted_or_accepted(self):
        for kind in (tarfile.SYMTYPE,tarfile.LNKTYPE):
            def change(item):
                if item.name=='./coordinator.lock':item.type=kind;item.linkname='/outside'
            with self.assertRaises(c.CustodyHeld):self.validate(archive(self.files,mutation=change))
        def traversal(item):
            if item.name=='./coordinator.lock':item.name='../outside'
        with self.assertRaises(c.CustodyHeld):self.validate(archive(self.files,mutation=traversal))

    def test_truncated_overbudget_bad_checksum_or_nonzero_trailer_holds(self):
        raw=archive(self.files)
        for value in (raw[:1700],bytes([raw[0]^1])+raw[1:],raw+b'not-zero'):
            with self.assertRaises(c.CustodyHeld):self.validate(value)
        with self.assertRaisesRegex(c.CustodyHeld,'byte_budget'):self.validate(raw,max_bytes=1024)

    def test_duplicate_path_and_unsupported_pax_override_hold(self):
        def duplicate(item):
            if item.name=='./coordinator.lock':item.name='./jobs.sqlite3'
        with self.assertRaises(c.CustodyHeld):self.validate(archive(self.files,mutation=duplicate))
        def pax(item):
            if item.name=='./jobs.sqlite3':item.pax_headers={'uid':'0'}
        with self.assertRaisesRegex(c.CustodyHeld,'unsupported_archive_extension'):
            self.validate(archive(self.files,mutation=pax))

    def test_pax_times_and_long_utf8_names_are_bounded_and_content_verified(self):
        name='viewer-tasks/'+'x'*130+'ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â©.jpg';value=self.files.pop('viewer-tasks/input.jpg');self.files[name]=value
        self.baseline['files'][path_id(name)]=self.baseline['files'].pop(path_id('viewer-tasks/input.jpg'))
        def times(item):item.pax_headers['mtime']='1770000000.123456'
        self.assertEqual(self.validate(archive(self.files,mutation=times))['fileCount'],4)


# This fixture uses real readonly serialized census child execution and a
# synthetic Docker/owner boundary. Actual tracked-exec/daemon qualification is
# deliberately separate; these cases do not claim original kernel ownership.
import os
import subprocess
import sys
import tempfile
import uuid
from unittest.mock import patch
from tests import test_coordinator_predecessor_census as old_fixture


class SyntheticTracker:
    def __init__(self,operation):self.operation=operation;self.cache={};self.calls=0;self.retired=True;self.lose_once=False
    def __call__(self,container,source,*,request_id,timeout,output_limit):
        if request_id not in self.cache:
            self.calls+=1
            process=subprocess.Popen([sys.executable,'-I','-'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            start=Path('/proc/'+str(process.pid)+'/stat').read_text().rsplit(')',1)[1].split()[19]
            output,error=process.communicate(source,timeout=timeout)
            if process.returncode!=0:raise AssertionError('serialized census child failed')
            self.cache[request_id]=dict(schemaVersion=1,protocol='ltds-cutover-exec-v1',operationId=self.operation,
                requestId=request_id,containerId=container,sourceSha256=hashlib.sha256(source).hexdigest(),
                process=dict(pid=process.pid,startTicks=start,retired=self.retired),bodySucceeded=True,result=json.loads(output))
        if self.lose_once:self.lose_once=False;raise OSError('synthetic lost CLI reply after child retired')
        return copy.deepcopy(self.cache[request_id])

    def inspect(self,container,request_id,source,*,maximum):
        if request_id not in self.cache:raise c.CustodyHeld('synthetic absent tracked request')
        packet=self.cache[request_id]
        if packet['containerId']!=container or packet['sourceSha256']!=hashlib.sha256(source).hexdigest():
            raise c.CustodyHeld('synthetic request binding changed')
        return copy.deepcopy(packet)


class SyntheticArchiveDocker:
    def __init__(self,root):self.root=root;self.calls=0;self.partial=False;self.closed=True;self.mutate_db=False
    def archive_to(self,container,root,sink,*,max_bytes,timeout):
        self.calls+=1;output=io.BytesIO()
        with tarfile.open(fileobj=output,mode='w') as tar:
            tar.add(self.root,arcname='.',recursive=True)
        raw=output.getvalue()
        if self.mutate_db:
            # Corrupt one payload byte without altering header framing.
            changed=bytearray(raw)
            with tarfile.open(fileobj=io.BytesIO(raw),mode='r:') as tar:
                target=next(item for item in tar if item.name.endswith('/jobs.sqlite3'))
                changed[target.offset_data]^=1
            raw=bytes(changed)
        if self.partial:sink(raw[:1000]);raise OSError('synthetic incomplete archive RPC')
        for offset in range(0,len(raw),65536):sink(raw[offset:offset+65536])
        return dict(clientExitCode=0,streamClosed=self.closed,bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest())


@unittest.skipUnless(os.name=='posix' and os.geteuid()==10001,'bounded Linux UID10001 synthetic orchestration')
class CustodyOrchestrationTests(unittest.TestCase):
    def setUp(self):
        self.fixture=old_fixture.ClosedPredecessorCensusTests();self.addCleanup(self.fixture.doCleanups);self.fixture.setUp()
        self.fixture.add_closed_direct_plans()
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.directory=Path(self.temp.name);self.directory.chmod(0o700)
        self.operation=str(uuid.uuid4());self.plan='a'*64;self.holder_id='b'*64
        self.settings=dict(schemaVersion=1,operationId=self.operation,planSha256=self.plan,
            authorityId=self.fixture.authority,stateRoot=str(self.fixture.root),leaseDirectory='/synthetic/original-lease')
        source=Path(__file__).resolve().parents[1]/'ops'
        modules={name:(source/(name+'.py')).read_bytes() for name in c.MODULE_NAMES}
        self.tracker=SyntheticTracker(self.operation);self.docker=SyntheticArchiveDocker(self.fixture.root)
        self.boundary=self.make_boundary()
        self.ops=c.CustodyOps(self.docker,holder_id=self.holder_id,holder_settings=self.settings,
            operation_directory=self.directory,key=KEY,key_sha256=hashlib.sha256(KEY).hexdigest(),
            module_sources=modules,module_sha256={name:hashlib.sha256(raw).hexdigest() for name,raw in modules.items()},
            reviewed_schema_bytes=old.SCHEMA_PATH.read_bytes(),policy_bytes=b'{}',policy_sha256=hashlib.sha256(b'{}').hexdigest(),
            tracked_exec=self.tracker,boundary_check=lambda role,context:copy.deepcopy(self.boundary),
            max_artifact_bytes=16*1048576,max_backup_bytes=64*1048576,max_seconds=10,safety_floor_bytes=1048576)

    def make_boundary(self):
        main=(self.fixture.root/'coordinator.lock').stat();db=self.fixture.path.stat();root=self.fixture.root.stat()
        main_key=[main.st_dev,main.st_ino,main.st_mode,main.st_nlink,main.st_uid,main.st_gid]
        external=[10,20,0o100600,1,10001,10001]
        operation=dict(authorityId=self.fixture.authority,root=str(self.fixture.root),
            database=str(self.fixture.path),databaseIdentity=[db.st_dev,db.st_ino],rootIdentity=[root.st_dev,root.st_ino],
            lease=str(self.fixture.root/'coordinator.lock'),leaseIdentity=main_key[:5],
            authorityLease=dict(directory='/synthetic/original-lease',fileIdentity=external))
        return dict(schemaVersion=1,role='holder',container=dict(id=self.holder_id,imageId='sha256:'+'c'*64,
            startedAt='2026-11-05T00:00:00Z',pid=123),
            holder=dict(settings=self.settings,pid=1,startTime='999',mainDescriptor=5,leaseDescriptor=6,
                        mainIdentity=main_key,leaseIdentity=external),
            physical=dict(operationBinding=operation,state=dict(main=main_key)),custody=None,
            writers=[dict(containerId='d'*64,running=False,pid=0,finishedAt='2026-11-05T00:00:00Z')])

    def intent(self,action):
        return dict(schemaVersion=1,operationId=self.operation,planSha256=self.plan,
            intentId=str(uuid.uuid5(uuid.UUID(self.operation),action)),action=action,direction='forward',step=1)

    def dispatch(self,intent):
        return dict(intentSha256=c.digest(intent),attempt=1,dispatchId=str(uuid.uuid4()))

    def test_capture_then_tar_backup_preserves_full_original_including_sqlite(self):
        baseline=self.ops.capture(self.intent('capture-full-custody'))
        original=self.fixture.path.read_bytes();intent=self.intent('copy-verified-backup')
        reference=self.ops.backup(baseline,intent,dispatch=self.dispatch(intent))
        document=self.ops.artifacts.load(reference,'backup');proof=document['body']['tarValidation']
        self.assertEqual(proof['fileCount'],len(self.fixture.census()['files']))
        self.assertEqual(self.fixture.path.read_bytes(),original)
        self.assertEqual(self.docker.calls,1);self.assertEqual(self.tracker.calls,3)
        archive_path=self.directory/document['body']['tarName']
        self.assertEqual(archive_path.stat().st_uid,os.geteuid());self.assertEqual(archive_path.stat().st_mode&0o777,0o600)
        self.assertFalse(proof['restoreAuthorized'])
        self.assertEqual(self.ops.inspect_backup(baseline,intent,dispatch=document['dispatch']),reference)
        self.assertEqual(self.tracker.calls,3);self.assertEqual(self.docker.calls,1)
        self.boundary['container']['startedAt']='2026-11-05T01:00:00Z';self.tracker.cache.clear()
        retained=self.ops.validate_backup_artifact(baseline,reference)
        self.assertFalse(retained['originalAuthorityProven']);self.assertFalse(retained['restoreAuthorized'])
        self.assertEqual(self.tracker.calls,3)
        with self.assertRaisesRegex(c.CustodyHeld,'boundary_changed'):
            self.ops.inspect_backup(baseline,intent,dispatch=document['dispatch'])

    def test_lost_capture_reply_reuses_retired_request_without_second_read(self):
        self.tracker.lose_once=True;intent=self.intent('capture-full-custody')
        with self.assertRaises(OSError):self.ops.capture(intent)
        reference=self.ops.capture(intent)
        self.assertEqual(self.tracker.calls,1)
        self.assertEqual(self.ops.artifacts.load(reference,'baseline')['body']['tracked']['resultSha256'],
                         c.digest(self.fixture.census()))

    def test_unretired_child_or_live_other_writer_cannot_supply_custody(self):
        self.tracker.retired=False
        with self.assertRaisesRegex(c.CustodyHeld,'retirement'):self.ops.capture(self.intent('capture-full-custody'))
        self.tracker.retired=True;self.boundary['writers'][0]['running']=True
        with self.assertRaisesRegex(c.CustodyHeld,'writers'):self.ops.capture(self.intent('other-capture'))
        self.assertFalse(any(self.directory.iterdir()))

    def test_partial_rpc_retains_private_data_without_receipt_or_replay(self):
        baseline=self.ops.capture(self.intent('capture-full-custody'));self.docker.partial=True
        intent=self.intent('copy-verified-backup');dispatch=self.dispatch(intent)
        with self.assertRaises(OSError):self.ops.backup(baseline,intent,dispatch=dispatch)
        name=self.ops.artifacts.name('backup',intent,dispatch,'tar')
        self.assertEqual((self.directory/name).stat().st_size,1000)
        self.assertFalse((self.directory/self.ops.artifacts.name('backup',intent,dispatch)).exists())
        with self.assertRaises(FileExistsError):self.ops.backup(baseline,intent,dispatch=dispatch)
        self.assertEqual(self.docker.calls,1)

    def test_valid_tar_without_rpc_closure_or_changed_sqlite_never_seals(self):
        for mode in ('closed','mutate_db'):
            with self.subTest(mode=mode):
                self.docker.closed=mode!='closed';self.docker.mutate_db=mode=='mutate_db'
                baseline=self.ops.capture(self.intent('capture-'+mode));intent=self.intent('backup-'+mode);dispatch=self.dispatch(intent)
                with self.assertRaises(c.CustodyHeld):self.ops.backup(baseline,intent,dispatch=dispatch)
                self.assertFalse((self.directory/self.ops.artifacts.name('backup',intent,dispatch)).exists())

    def test_backup_budget_or_disk_floor_refuses_before_cp(self):
        baseline=self.ops.capture(self.intent('capture-full-custody'));intent=self.intent('copy-verified-backup')
        self.ops.max_backup=1
        with self.assertRaisesRegex(c.CustodyHeld,'quota'):self.ops.backup(baseline,intent,dispatch=self.dispatch(intent))
        self.ops.max_backup=64*1048576
        class NoSpace:f_bavail=0;f_frsize=4096
        with patch.object(os,'statvfs',return_value=NoSpace()):
            with self.assertRaisesRegex(c.CustodyHeld,'disk_floor'):self.ops.backup(baseline,intent,dispatch=self.dispatch(intent))
        self.assertEqual(self.docker.calls,0)

    def test_corrupt_private_reference_and_directory_alias_fail_closed(self):
        reference=self.ops.capture(self.intent('capture-full-custody'));bad=dict(reference,sha256='0'*64)
        with self.assertRaisesRegex(c.CustodyHeld,'reference_changed'):self.ops.artifacts.load(bad,'baseline')
        path=self.directory/reference['name'];path.chmod(0o644)
        with self.assertRaisesRegex(c.CustodyHeld,'private_bounded'):self.ops.artifacts.load(reference,'baseline')
