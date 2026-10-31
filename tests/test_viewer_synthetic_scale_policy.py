"""Fixture-only small-object scale policy; no production capability."""
import io,os,time,unittest
from unittest.mock import patch
from ops.reference_worker_daemon import DaemonStateLock
from processing.direct_output_slot import _identity
from processing.viewer_archive_spool import ViewerArchiveSpool
from processing.viewer_service import NativeEngine
from processing.viewer_synthetic_supervisor import tick,_validate_scale_policy,SupervisorHeld
from processing.viewer_tasks import TaskStore
from tests.test_viewer_synthetic_submission import SyntheticSubmissionTests

class SyntheticScalePolicyTests(unittest.TestCase):
 def test_policy_bounds_are_explicit_and_small_input_only(self):
  _validate_scale_policy(10000,dict(version=1,maxObjects=10000,maxInputBytes=64*1024*1024))
  for limit,policy in [(10001,dict(version=1,maxObjects=10000,maxInputBytes=1)),(10000,dict(version=1,maxObjects=10000,maxInputBytes=64*1024*1024+1)),(65,dict(version=1,maxObjects=64,maxInputBytes=1)),(65,dict(version=2,maxObjects=10000,maxInputBytes=1))]:
   with self.subTest(limit=limit,policy=policy):
    with self.assertRaises(SupervisorHeld):_validate_scale_policy(limit,policy)
 def test_65_photo_plus_aux_actual_tls_fetch_and_fake_engine(self):
  original=TaskStore.commit
  def populate(store,identifier,*args,**kwargs):
   for index in range(64):store.upload(identifier,f'synthetic-{index:04}.jpg',io.BytesIO(b'x'),1,'production')
   store.upload(identifier,'synthetic-aux.txt',io.BytesIO(b'a'),1,'production')
   return original(store,identifier,*args,**kwargs)
  fixture=SyntheticSubmissionTests('test_exact_submission_once_and_replay_without_cleanup');self.addCleanup(fixture.doCleanups)
  with patch.object(TaskStore,'commit',populate):fixture.setUp()
  coordinator=fixture.base.base.fixture;root=coordinator.root/'scale-spool';root.mkdir(mode=0o700)
  fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY)
  try:identity=_identity(fd)
  finally:os.close(fd)
  spool=ViewerArchiveSpool(coordinator.claims,root=root,root_identity=identity,max_archive_bytes=65536,enabled=True);coordinator.api.viewer_spool=spool
  policy=dict(version=1,maxObjects=10000,maxInputBytes=64*1024*1024);start=time.monotonic()
  with DaemonStateLock(fixture.base.base.worker) as owner:
   worker=fixture.base.base.make(owner)
   result=tick(worker,NativeEngine(fixture.origin),coordinator.job,'enroll-1','production',input_root=fixture.base.inputs,input_root_identity=fixture.base.input_id,max_total=100000,max_object=100000,synthetic_fixture=True,max_objects=10000,synthetic_scale_policy=policy)
   self.assertEqual(result['kind'],'viewer-completion-attestation-receipt')
   with worker._db() as db:
    import json
    claim=json.loads(db.execute('SELECT claim FROM intent').fetchone()[0]);self.assertEqual((claim['objectCount'],claim['imageCount'],claim['inputBytes']),(66,65,74))
  self.assertEqual(len([x for x in fixture.calls if x[0]=='upload']),66)
  self.assertEqual(len(list(fixture.base.inputs.rglob('*.alloc'))),66)
  print('synthetic_objects=66 elapsed_seconds=%.2f'%(time.monotonic()-start),flush=True)
 def test_preflight_exact_count_bytes_and_capacity_fail_closed(self):
  import tempfile
  from types import SimpleNamespace
  from processing.viewer_synthetic_supervisor import _synthetic_scale_preflight
  with tempfile.TemporaryDirectory() as root:
   os.chmod(root,0o700);fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY)
   try:root_id=_identity(fd)
   finally:os.close(fd)
   owner=SimpleNamespace(root=root,_root_identity=(os.stat(root).st_dev,os.stat(root).st_ino),check=lambda _root:None)
   inputs=SimpleNamespace(worker=SimpleNamespace(owner=owner),claim={'objectCount':10000,'inputBytes':10008},policy={'inputRoot':root,'inputRootIdentity':root_id})
   policy=dict(version=1,maxObjects=10000,maxInputBytes=64*1024*1024)
   ample=SimpleNamespace(f_frsize=4096,f_favail=100000,f_bavail=1000000)
   with patch('processing.viewer_synthetic_supervisor.os.fstatvfs',return_value=ample):
    info=_synthetic_scale_preflight(inputs,10000,policy);self.assertEqual(info['journalEstimateBytes'],45154304)
    inputs.claim['inputBytes']=64*1024*1024+1
    with self.assertRaisesRegex(SupervisorHeld,'claim_limit'):_synthetic_scale_preflight(inputs,10000,policy)
    inputs.claim['inputBytes']=10008
   for fs in (SimpleNamespace(f_frsize=4096,f_favail=1,f_bavail=1000000),SimpleNamespace(f_frsize=4096,f_favail=100000,f_bavail=1)):
    with patch('processing.viewer_synthetic_supervisor.os.fstatvfs',return_value=fs):
     with self.assertRaisesRegex(SupervisorHeld,'capacity_low'):_synthetic_scale_preflight(inputs,10000,policy)
 def test_split_filesystems_check_both_capacity_and_journal_identity(self):
  import tempfile
  from types import SimpleNamespace
  from processing.viewer_synthetic_supervisor import _synthetic_scale_preflight
  if not os.path.isdir('/dev/shm') or os.stat('/tmp').st_dev==os.stat('/dev/shm').st_dev:
   self.skipTest('distinct tmpfs unavailable')
  with tempfile.TemporaryDirectory(dir='/tmp') as input_root,tempfile.TemporaryDirectory(dir='/dev/shm') as journal_root:
   fd=os.open(input_root,os.O_RDONLY|os.O_DIRECTORY)
   try:root_id=_identity(fd)
   finally:os.close(fd)
   st=os.stat(journal_root)
   owner=SimpleNamespace(root=journal_root,_root_identity=(st.st_dev,st.st_ino),check=lambda _root:None)
   inputs=SimpleNamespace(worker=SimpleNamespace(owner=owner),claim={'objectCount':66,'inputBytes':74},policy={'inputRoot':input_root,'inputRootIdentity':root_id})
   policy=dict(version=1,maxObjects=10000,maxInputBytes=64*1024*1024)
   ample=SimpleNamespace(f_frsize=4096,f_favail=100000,f_bavail=1000000)
   scarce=SimpleNamespace(f_frsize=4096,f_favail=100000,f_bavail=1)
   scarce_inodes=SimpleNamespace(f_frsize=4096,f_favail=1,f_bavail=1000000)
   for observed in ((scarce,ample),(ample,scarce),(scarce_inodes,ample),(ample,scarce_inodes)):
    with patch('processing.viewer_synthetic_supervisor.os.fstatvfs',side_effect=observed):
     with self.assertRaisesRegex(SupervisorHeld,'capacity_low'):_synthetic_scale_preflight(inputs,10000,policy)
   with patch('processing.viewer_synthetic_supervisor.os.fstatvfs',side_effect=(ample,ample)):
    _synthetic_scale_preflight(inputs,10000,policy)
   owner._root_identity=(st.st_dev,st.st_ino+1)
   with self.assertRaisesRegex(SupervisorHeld,'journal_root_changed'):_synthetic_scale_preflight(inputs,10000,policy)
 def test_scale_policy_is_bound_to_input_journal_across_owner_restart(self):
  from processing.viewer_node_inputs import ViewerNodeInputs,InputHeld
  fixture=SyntheticSubmissionTests('test_exact_submission_once_and_replay_without_cleanup');self.addCleanup(fixture.doCleanups);fixture.setUp()
  policy=dict(version=1,maxObjects=10000,maxInputBytes=64*1024*1024)
  def create(owner,selected):
   return ViewerNodeInputs(fixture.base.base.make(owner),fixture.base.base.fixture.job,'enroll-1','production',input_root=fixture.base.inputs,input_root_identity=fixture.base.input_id,max_total=100000,max_object=100000,synthetic_scale_policy=selected)
  with DaemonStateLock(fixture.base.base.worker) as owner:create(owner,policy)
  with DaemonStateLock(fixture.base.base.worker) as owner:
   with self.assertRaisesRegex(InputHeld,'input_policy_changed'):create(owner,dict(policy,maxObjects=9999))
