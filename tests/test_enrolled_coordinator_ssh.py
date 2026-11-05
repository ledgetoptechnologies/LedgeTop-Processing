"""SSH argv and synthetic local process lifecycle; no connected SSH proof."""
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest
import uuid

from ops import enrolled_coordinator_ssh as s
from ops import enrolled_coordinator_exec_helper as helper
from ops import enrolled_coordinator_transport as t
from tests.test_enrolled_coordinator_deployment import bundle


class SSHArgumentsTests(unittest.TestCase):
    def test_existing_key_known_host_and_literal_interpreter_keep_private_source_off_argv(self):
        entry,_=bundle();worker=entry['workers'][0]
        worker['settings']['pythonExecutable']='/private/a folder/$(literal)/python'
        class Runner:
            def run(self,argv,**kwargs):self.argv=argv;self.options=kwargs;return b'{}'
        runner=Runner();client=s._ExistingSSH(worker,'a'*64,runner=runner)
        client.exec_python('a'*64,b'private-synthetic-config',timeout=2,output_limit=500)
        self.assertIn('StrictHostKeyChecking=yes',runner.argv)
        self.assertIn('BatchMode=yes',runner.argv)
        self.assertEqual(runner.argv[-2],'bkoltz@'+worker['sshHost'])
        self.assertEqual(shlex.split(runner.argv[-1]),[worker['settings']['pythonExecutable'],'-I','-B','-'])
        self.assertNotIn('private-synthetic-config',' '.join(runner.argv))
        self.assertEqual(runner.options['input_bytes'],b'private-synthetic-config')
        with self.assertRaises(t.TransportHeld):client.exec_python('b'*64,b'no request')


@unittest.skipUnless(os.name=='posix','Linux owner/remote-process lifecycle fixture')
class TrackedSSHFixtureTests(unittest.TestCase):
    def test_local_fixture_lost_reply_retires_exact_host_request_and_cannot_reexecute(self):
        value,plan=bundle();worker=value['workers'][0];worker['sshUid']=os.geteuid()
        operation=str(uuid.uuid4());request=str(uuid.uuid4());source=b'import json,time;time.sleep(.2);print(json.dumps({"proof":True}))'
        class Runner:
            child=None
            calls=0
            def run(self,argv,**kwargs):
                self.calls+=1
                if self.calls==1:
                    self.child=subprocess.Popen([sys.executable,'-I','-'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    self.child.stdin.write(kwargs['input_bytes']);self.child.stdin.close();self.child.stdin=None
                    raise t.TransportHeld('synthetic SSH reply lost')
                return t.BoundedSubprocess().run([sys.executable,'-I','-'],**kwargs)
        runner=Runner()
        import hashlib
        settings=dict(operationId=operation,requestId=request,sourceSha256=hashlib.sha256(source).hexdigest(),maximum=65536)
        if os.geteuid()!=10001:settings['ownerUid']=os.geteuid()
        root=helper.validate(settings)
        try:
            with tempfile.TemporaryDirectory() as directory:
                Path(directory).chmod(0o700)
                probe=s.TrackedWorkerProbe(worker,authority_id=plan['authorityId'],operation_id=operation,
                                           operation_directory=directory,runner=runner)
                with self.assertRaises(t.TransportHeld):probe(source,request_id=request)
                _,error=runner.child.communicate(timeout=3);self.assertEqual(runner.child.returncode,0,error.decode())
                result=probe.inspect_request(source,request_id=request)
                self.assertEqual(result['protocol'],'ltds-cutover-ssh-exec-v1')
                self.assertEqual(result['hostBindingSha256'],t.digest(worker))
                self.assertNotIn('containerId',result);self.assertTrue(result['bodySucceeded'])
                self.assertTrue(result['process']['retired']);self.assertEqual(result['result'],{'proof':True})
                before=runner.calls
                repeated=probe(source,request_id=request)
                self.assertEqual(repeated,result);self.assertEqual(runner.calls,before+1)
        finally:
            if runner.child is not None:
                if runner.child.poll() is None:runner.child.terminate()
                runner.child.communicate(timeout=3)
            self.assertEqual(root,helper.validate(settings));self.assertEqual(root.parent,Path('/tmp'))
            if root.exists():shutil.rmtree(root)


if __name__=='__main__':unittest.main()
