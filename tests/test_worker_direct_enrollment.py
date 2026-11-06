import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock,patch
import uuid

from ops import worker_direct_enrollment as enrollment
from ops import reference_worker_daemon as daemon


class WorkerEnrollmentTests(unittest.TestCase):
    def record(self):
        return dict(schemaVersion=1,protocol=enrollment.PROTOCOL,configPath='/configured/worker.json',
            configSha256='a'*64,authorityId=str(uuid.uuid4()),nodeId='cluster',workerId='worker',workerInstanceId='instance')

    def test_daemon_entrypoint_passes_independent_pin_and_checks_identity(self):
        record=self.record();config={k:record[k] for k in ('authorityId','nodeId','workerId','workerInstanceId')}
        config['pollSeconds']=1;driver=object();server=SimpleNamespace(server_close=Mock())
        with patch.object(enrollment,'load',return_value=record),patch.object(daemon,'load_config',return_value=config) as load, \
             patch.object(daemon,'build_worker',return_value=(driver,server)),patch.object(daemon.signal,'signal'), \
             patch.object(daemon,'WorkerService') as service:
            service.return_value.run.return_value=0
            self.assertEqual(daemon.main(['--config',record['configPath'],'--direct-enrollment','/etc/worker-enrollment.json']),0)
            load.assert_called_once_with(record['configPath'],expected_direct_config_sha256=record['configSha256'])
        with self.assertRaises(ValueError):enrollment.require_identity(record,dict(config,nodeId='wrong'))

    @unittest.skipUnless(os.name=='posix' and os.geteuid()==0,'root fixture needed to create root-owned enrollment')
    def test_root_owned_record_is_read_by_unprivileged_child_and_unsafe_parent_rejected(self):
        with tempfile.TemporaryDirectory(dir='/run') as directory:
            root=Path(directory);root.chmod(0o755);record=self.record()
            path=root/'record.json';path.write_text(json.dumps(record));path.chmod(0o644)
            source=("from ops.worker_direct_enrollment import load; "
                    "x=load(%r,config_path=%r); print(x['configSha256'])"%(str(path),record['configPath']))
            def child():
                return subprocess.run([sys.executable,'-c',source],capture_output=True,text=True,timeout=5,
                    preexec_fn=lambda:(os.setgid(10001),os.setuid(10001)),env=dict(os.environ,PYTHONPATH=os.getcwd()))
            result=child();self.assertEqual(result.returncode,0,result.stderr)
            self.assertEqual(result.stdout.strip(),record['configSha256'])
            root.chmod(0o777)
            result=child();self.assertNotEqual(result.returncode,0)
            root.chmod(0o755)
