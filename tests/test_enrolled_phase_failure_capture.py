"""Private diagnostics retain failed state; no real Docker or HTTP here."""
from contextlib import redirect_stdout
import hashlib
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from ops import qualify_enrolled_phase_transport as q
from ops import enrolled_coordinator_transport as t


class FailureCaptureTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.op='11111111-1111-4111-8111-111111111111'
        self.identity='a'*64;self.image='sha256:'+'b'*64
        self.item=dict(Id=self.identity,Image=self.image,Config=dict(Env=['PRIVATE_TOKEN=never-on-stdout'],
            Labels={q.TASK_LABEL:self.op,t.ROLE_LABEL:'bootstrap'}),HostConfig={},Mounts=[],
            State=dict(Running=True,Pid=123,StartedAt='known-start',FinishedAt='zero',ExitCode=0,OOMKilled=False,Error=''))
        case=self
        class Docker:
            mutation_unknown=False
            def matching(self,filters):return [case.identity]
            def inspect(self,identity):return case.item
            def stop(self,*_):raise AssertionError('diagnostics may not stop')
            def start(self,*_):raise AssertionError('diagnostics may not start')
            def command(self,*_):raise AssertionError('diagnostics may not mutate')
        self.docker=Docker();self.records={}
        def retain(path,raw):
            self.records[Path(path).name]=raw
            return dict(path=str(path),sha256=hashlib.sha256(raw).hexdigest())
        self.retain=patch.object(q,'private_write',side_effect=retain);self.retain.start();self.addCleanup(self.retain.stop)
        self.plan=dict(predecessor={'imageId':'sha256:'+'c'*64},candidate={'imageId':self.image})

    def test_private_logs_and_fresh_health_capture_do_not_ack_or_replay(self):
        boundary=SimpleNamespace(fresh=lambda identity,source:dict(status=200,body={'status':'ok'}))
        output=io.StringIO()
        with patch.object(q,'private_docker_logs',return_value=(b'private-constructor-stderr',False)),redirect_stdout(output):
            value=q.capture_failure(self.docker,self.root,self.op,driver=SimpleNamespace(boundary=boundary),plan=self.plan)
        self.assertEqual(value['containers'][0]['freshDiagnosticHealth']['status'],200)
        self.assertTrue(value['containers'][0]['freshDiagnosticHealth']['separateReadOnlyObservation'])
        self.assertTrue(value['retainedForInspection']);self.assertEqual(value['failures'],[])
        self.assertIn('private-constructor-stderr',self.records[self.identity+'.docker-log'].decode())
        self.assertNotIn('never-on-stdout',output.getvalue());self.assertNotIn('private-constructor-stderr',output.getvalue())
        self.assertNotIn('PRIVATE_TOKEN',self.records['summary.json'].decode())

    def test_unavailable_logs_still_preserve_exact_state_and_hold(self):
        with patch.object(q,'private_docker_logs',side_effect=RuntimeError('unavailable')),redirect_stdout(io.StringIO()):
            value=q.capture_failure(self.docker,self.root,self.op,plan=self.plan)
        self.assertEqual(value['containers'][0]['state']['Pid'],123)
        self.assertEqual(len(value['failures']),1)
        self.assertTrue(value['retainedForInspection'])

    def test_foreign_owner_or_image_never_reads_logs(self):
        for mutation in ('owner','image'):
            with self.subTest(mutation=mutation):
                root=self.root/mutation;root.mkdir()
                if mutation=='owner':self.item['Config']['Labels'][q.TASK_LABEL]='foreign'
                else:
                    self.item['Config']['Labels'][q.TASK_LABEL]=self.op
                    self.item['Image']='sha256:'+'d'*64
                with patch.object(q,'private_docker_logs',side_effect=AssertionError('not authorized')),redirect_stdout(io.StringIO()):
                    value=q.capture_failure(self.docker,root,self.op,plan=self.plan)
                self.assertEqual(value['containers'],[]);self.assertEqual(len(value['failures']),1)


if __name__=='__main__':unittest.main()
