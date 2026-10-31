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
 @unittest.skipUnless(os.environ.get('OWNED_SCALE_PROBE_ID'),'explicit isolated scale run only')
 def test_10000_tiny_synthetic_objects_through_supervisor(self):
  import re,resource,stat
  from pathlib import Path
  root=Path('/work');info=root.lstat();token=os.environ['OWNED_SCALE_PROBE_ID'];marker=root/'owned-scale-marker'
  if (os.geteuid()==0 or not re.fullmatch('[a-f0-9]{32}',token) or not os.path.ismount(root) or info.st_uid!=os.geteuid()
      or stat.S_IMODE(info.st_mode)!=0o700 or marker.is_symlink() or marker.read_text()!=token
      or set(os.listdir('/sys/class/net'))!={'lo'} or os.environ.get('TMPDIR')!='/work'):
   raise RuntimeError('isolated owned work mount and network-none required')
  memory=Path('/sys/fs/cgroup/memory.max').read_text().strip();cpu=Path('/sys/fs/cgroup/cpu.max').read_text().split()
  if memory=='max' or int(memory)>768*1024*1024 or len(cpu)!=2 or cpu[0]=='max' or int(cpu[0])*2>int(cpu[1]):raise RuntimeError('resource limits required')
  original=TaskStore.commit;start=time.monotonic()
  def populate(store,identifier,*args,**kwargs):
   for index in range(9999):
    ext='jpg' if index<999 else 'txt';store.upload(identifier,f'synthetic-{index:05}.{ext}',io.BytesIO(b'x'),1,'production')
   return original(store,identifier,*args,**kwargs)
  fixture=SyntheticSubmissionTests('test_exact_submission_once_and_replay_without_cleanup');self.addCleanup(fixture.doCleanups)
  with patch.object(TaskStore,'commit',populate):fixture.setUp()
  coordinator=fixture.base.base.fixture;spool_root=coordinator.root/'scale-spool';spool_root.mkdir(mode=0o700)
  fd=os.open(spool_root,os.O_RDONLY|os.O_DIRECTORY)
  try:identity=_identity(fd)
  finally:os.close(fd)
  spool=ViewerArchiveSpool(coordinator.claims,root=spool_root,root_identity=identity,max_archive_bytes=65536,enabled=True);coordinator.api.viewer_spool=spool
  policy=dict(version=1,maxObjects=10000,maxInputBytes=64*1024*1024);setup_seconds=time.monotonic()-start;started=time.monotonic()
  with DaemonStateLock(fixture.base.base.worker) as owner:
   worker=fixture.base.base.make(owner)
   kwargs=dict(input_root=fixture.base.inputs,input_root_identity=fixture.base.input_id,max_total=100000,max_object=100000,synthetic_fixture=True,max_objects=10000,synthetic_scale_policy=policy)
   result=tick(worker,NativeEngine(fixture.origin),coordinator.job,'enroll-1','production',**kwargs)
   self.assertEqual(result['kind'],'viewer-completion-attestation-receipt')
   with worker._db() as db:
    import json
    claim=json.loads(db.execute('SELECT claim FROM intent').fetchone()[0]);self.assertEqual((claim['objectCount'],claim['imageCount'],claim['inputBytes']),(10000,1000,10008))
  elapsed=time.monotonic()-started
  self.assertEqual(len([call for call in fixture.calls if call[0]=='upload']),10000)
  self.assertEqual(len(list(fixture.base.inputs.rglob('*.alloc'))),10000)
  journal=fixture.base.base.worker/'viewer-inputs.sqlite';journal_bytes=journal.stat().st_size
  self.assertLess(journal_bytes,64*1024*1024)
  print('synthetic_scale_result objects=10000 photos=1000 bytes=10008 setup_seconds=%.2f run_seconds=%.2f journal_bytes=%d maxrss_kib=%d'%(setup_seconds,elapsed,journal_bytes,resource.getrusage(resource.RUSAGE_SELF).ru_maxrss),flush=True)
 def test_preflight_exact_count_bytes_and_capacity_fail_closed(self):
  import tempfile
  from types import SimpleNamespace
  from processing.viewer_synthetic_supervisor import _synthetic_scale_preflight
  with tempfile.TemporaryDirectory() as root:
   os.chmod(root,0o700);fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY)
   try:root_id=_identity(fd)
   finally:os.close(fd)
   inputs=SimpleNamespace(claim={'objectCount':10000,'inputBytes':10008},policy={'inputRoot':root,'inputRootIdentity':root_id})
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
