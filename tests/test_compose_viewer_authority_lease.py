"""Validate the rendered Compose lease, without starting containers."""
import json,os,shutil,subprocess,tempfile,unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]

@unittest.skipUnless(shutil.which('docker'),'Docker Compose CLI unavailable')
class ComposeAuthorityLeaseTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
  root=Path(self.temp.name);self.envfile=root/'runtime.env';self.envfile.write_text('PROCESSING_AUTHORITY_LEASE_DIR=/wrong/from-env\n')
  self.lease=root/'lease';self.lease.mkdir()
  self.env=dict(os.environ,PROCESSING_IMAGE='sha256:'+'a'*64,PROCESSING_RUNTIME_ENV=str(self.envfile),PROCESSING_STATE_VOLUME='synthetic-state',VIEWER_BIND_ADDRESS='127.0.0.1',PROCESSING_AUTHORITY_LEASE_HOST_DIR=str(self.lease))
 def render(self,env):
  return subprocess.run(['docker','compose','-f',str(ROOT/'compose.viewer.yaml'),'config','--format','json'],env=env,capture_output=True,text=True,timeout=15)
 def test_render_binds_explicit_existing_directory_and_overrides_runtime_env(self):
  p=self.render(self.env);self.assertEqual(p.returncode,0,p.stderr)
  config=json.loads(p.stdout);service=config['services']['coordinator'];self.assertEqual(service['environment']['PROCESSING_AUTHORITY_LEASE_DIR'],'/run/ledgetop-authority-lease')
  mounts=[m for m in service['volumes'] if m['target']=='/run/ledgetop-authority-lease'];self.assertEqual(len(mounts),1);mount=mounts[0]
  self.assertEqual(mount['type'],'bind');self.assertEqual(Path(mount['source']).resolve(),self.lease.resolve());self.assertFalse(mount.get('read_only',False));self.assertFalse(mount['bind']['create_host_path'])
  self.assertTrue(config['volumes']['state']['external'])
 def test_missing_host_directory_setting_fails_render(self):
  env=dict(self.env);env.pop('PROCESSING_AUTHORITY_LEASE_HOST_DIR',None)
  p=self.render(env);self.assertNotEqual(p.returncode,0);self.assertIn('PROCESSING_AUTHORITY_LEASE_HOST_DIR',p.stderr)
if __name__=='__main__':unittest.main()
