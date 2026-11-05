"""Structured CLI, bounded private subprocess I/O and real original lease FDs."""
from contextlib import closing
import copy
import io
import json
import os
from pathlib import Path
import signal
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import tarfile
import time
import unittest
import uuid
from unittest.mock import patch

from ops import enrolled_coordinator_transport as t
from ops import enrolled_coordinator_lease_helper as holder
from ops import enrolled_coordinator_exec_helper as remote


def recipe(*, bootstrap=True):
    return dict(schemaVersion=1,name='synthetic-bootstrap' if bootstrap else 'synthetic-final',
        imageId='sha256:'+'a'*64,user='10001:10001',workingDir='/app',
        environmentFile='/private/a folder/runtime.env',environmentSha256='b'*64,
        mounts=[dict(type='volume',source='synthetic-original-state',target='/prototype-state',readOnly=False),
                dict(type='bind',source='/private/tls',target='/run/processing-reference',readOnly=True)],
        ports=[] if bootstrap else [dict(containerPort='8080/tcp',hostIp='127.0.0.1',hostPort=18080)],
        network='none' if bootstrap else 'bridge',tmpfs={'/tmp':'rw,nosuid,nodev,noexec,size=64m,mode=1777'},
        labels={'org.ledgetop.purpose':'synthetic-test'},command=['python','-m','processing.app'],
        entrypoint=None,readOnly=True,init=True,pidsLimit=128,memoryBytes=768*1024**2,nanoCpus=10**9,
        stopSignal='SIGTERM',stopTimeout=-1,restartPolicy={'Name':'no','MaximumRetryCount':0})


class FakeRunner:
    def __init__(self, answer=b''):self.calls=[];self.answer=answer
    def run(self, argv, **kwargs):self.calls.append((argv,kwargs));return self.answer


class StructuredTransportTests(unittest.TestCase):
    def test_closed_bootstrap_recipe_has_no_network_or_ports_and_no_shell(self):
        args=t.create_arguments(recipe(),operation_id=str(uuid.uuid4()),role='bootstrap',intent_id=str(uuid.uuid4()))
        self.assertEqual(args[args.index('--network')+1],'none')
        self.assertNotIn('--publish',args)
        self.assertIn('/private/a folder/runtime.env',args)
        self.assertEqual(args[-4:],['sha256:'+'a'*64,'python','-m','processing.app'])
        self.assertNotIn('sh',args)
        self.assertEqual(args[args.index('--stop-timeout')+1],'-1')

    def test_closed_holder_recipe_requires_pid1_original_readonly_state_and_exact_external_mount(self):
        operation=str(uuid.uuid4());intent=str(uuid.uuid4())
        settings=dict(schemaVersion=1,operationId=operation,planSha256='a'*64,authorityId=str(uuid.uuid4()),
                      stateRoot='/prototype-state',leaseDirectory='/authority-leases')
        value=recipe();value['init']=False;value['mounts'][0]['readOnly']=True
        value['mounts'].append(dict(type='bind',source='/private/authority-leases',target='/authority-leases',readOnly=False))
        args=t.create_arguments(value,operation_id=operation,role='lease-holder',intent_id=intent,holder_settings=settings)
        self.assertNotIn('--init',args);self.assertIn('--no-healthcheck',args)
        self.assertEqual(args[-3:-1],['-I','-c']);self.assertIn('require_pid1=True',args[-1])
        self.assertIn('reviewed_readonly_package_required',args[-1])
        for change in ('init','state','external'):
            bad=copy.deepcopy(value)
            if change=='init':bad['init']=True
            elif change=='state':bad['mounts'][0]['readOnly']=False
            else:bad['mounts'][-1]['readOnly']=True
            with self.assertRaises(t.TransportHeld):
                t.create_arguments(bad,operation_id=operation,role='lease-holder',intent_id=intent,holder_settings=settings)

    def test_unreviewed_recipe_fields_mounts_and_public_network_refused(self):
        for change in (dict(privileged=True),dict(network='host'),dict(readOnly=False),dict(stopTimeout=10),
                       dict(restartPolicy={'Name':'always','MaximumRetryCount':0})):
            value=recipe();value.update(change)
            with self.assertRaises(t.TransportHeld):t.validate_recipe(value,bootstrap=True)
        for mount in (dict(type='bind',source='/private/tls,readonly=false',target='/safe',readOnly=True),
                      dict(type='bind',source='/private/shadow',target='/app/processing',readOnly=True),
                      dict(type='bind',source='/private/shadow',target='/etc/ld.so.preload',readOnly=True)):
            value=recipe();value['mounts'].append(mount)
            with self.assertRaises(t.TransportHeld):t.validate_recipe(value,bootstrap=True)

    def test_exact_container_id_and_no_kill_stop_are_required(self):
        runner=FakeRunner();docker=t.DockerCLI(runner=runner)
        docker.stop('a'*64)
        args,options=runner.calls[0]
        self.assertEqual(args[:3],['/usr/bin/docker','--host','unix:///run/docker.sock'])
        self.assertEqual(args[3:],['container','stop','--signal','SIGTERM','--timeout','-1','a'*64])
        self.assertTrue(options['mutation'])
        with self.assertRaises(t.TransportHeld):docker.stop('a-user-container-name')
        self.assertEqual(len(runner.calls),1)

    def test_inspect_requires_one_exact_id_and_duplicate_json_is_rejected(self):
        docker=t.DockerCLI(runner=FakeRunner(json.dumps([dict(Id='a'*64)]).encode()))
        self.assertEqual(docker.inspect('a'*64)['Id'],'a'*64)
        for answer in (b'[]',b'[{},{}]',b'[{"Id":"a","Id":"b"}]'):
            with self.assertRaises(t.TransportHeld):t.DockerCLI(runner=FakeRunner(answer)).inspect('a'*64)

    def test_helper_script_goes_only_over_stdin_and_is_not_echoed(self):
        runner=FakeRunner(b'{"ok":true}')
        t.DockerCLI(runner=runner).exec_python('a'*64,b'private-synthetic-key',mutation=True)
        args,options=runner.calls[0]
        self.assertNotIn('private-synthetic-key',' '.join(args))
        self.assertEqual(options['input_bytes'],b'private-synthetic-key')
        self.assertTrue(options['mutation'])

    def test_config_readback_requires_one_private_regular_tar_member(self):
        value=io.BytesIO()
        with tarfile.open(fileobj=value,mode='w') as archive:
            member=tarfile.TarInfo('runtime.json');member.uid=member.gid=10001;member.mode=0o600
            member.size=2;archive.addfile(member,io.BytesIO(b'{}'))
        runner=FakeRunner(value.getvalue())
        self.assertEqual(t.DockerCLI(runner=runner).read_private_file('a'*64,'/run/processing-reference/runtime.json'),b'{}')
        self.assertFalse(runner.calls[0][1]['mutation'])
        with self.assertRaises(t.TransportHeld):
            t.DockerCLI(runner=runner).read_private_file('a'*64,'/prototype-state/jobs.sqlite3')

    def test_archive_stream_seam_forwards_sink_bounds_and_exact_original_path(self):
        receipt=dict(clientExitCode=0,streamClosed=True,bytes=10240,sha256='b'*64)
        runner=FakeRunner(receipt);sink=lambda chunk:None
        result=t.DockerCLI(runner=runner).archive_to('a'*64,'/prototype-state',sink,max_bytes=50000000,timeout=45)
        self.assertEqual(result,receipt)
        args,options=runner.calls[0]
        self.assertEqual(args[3:],['container','cp','a'*64+':/prototype-state/.','-'])
        self.assertIs(options['sink'],sink);self.assertEqual(options['output_limit'],50000000)
        self.assertEqual(options['timeout'],45);self.assertFalse(options['mutation'])
        with self.assertRaises(t.TransportHeld):
            t.DockerCLI(runner=runner).archive_to('a'*64,'/different',sink,max_bytes=100,timeout=1)

    def test_gate_receipt_lookup_is_sql_read_only_not_a_mutating_rpc_replay(self):
        request=dict(requestId=str(uuid.uuid4()),planId=str(uuid.uuid4()),expectedRevision=0,
                     action='intent',startupId=str(uuid.uuid4()))
        source=t.gate_receipt_script(request)
        compile(source,'<synthetic-receipt>','exec')
        self.assertIn(b'?mode=ro',source)
        self.assertIn(b'PRAGMA query_only=ON',source)
        self.assertNotIn(b'connection.request',source)


@unittest.skipUnless(os.name=='posix','Linux selector pipes and private file modes')
class BoundedSubprocessTests(unittest.TestCase):
    def test_immutable_proof_cannot_be_replaced_and_rearmed_dispatch_has_distinct_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);root.chmod(0o700);proofs=t.PrivateProofs(root)
            intent=dict(intentId=str(uuid.uuid4()),action='synthetic')
            first=dict(intentSha256=t.digest(intent),attempt=1,dispatchId=str(uuid.uuid4()))
            second=dict(intentSha256=t.digest(intent),attempt=2,dispatchId=str(uuid.uuid4()))
            sha=proofs.retain(intent,{'outcome':'rejected'},dispatch=first,kind='rejection')
            self.assertEqual(proofs.retain(intent,{'outcome':'rejected'},dispatch=first,kind='rejection'),sha)
            with self.assertRaises(t.TransportHeld):proofs.retain(intent,{'outcome':'changed'},dispatch=first,kind='rejection')
            proofs.retain(intent,{'outcome':'rejected'},dispatch=second,kind='rejection')
            self.assertNotEqual(proofs.name(intent,first,kind='rejection'),proofs.name(intent,second,kind='rejection'))
            self.assertEqual(len(list(root.iterdir())),2)

    def test_fixed_host_operation_binding_survives_exit_and_refuses_new_receipt_directory(self):
        from ops import install_reference_coordinator as installer
        from tests.test_enrolled_coordinator_cutover import plan
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);root.chmod(0o700)
            one=root/'one';two=root/'two';one.mkdir(mode=0o700);two.mkdir(mode=0o700)
            value=plan()
            with patch.object(installer,'LOCK_PATH',root/'coordinator-install.lock'):
                operation=t.HostOperation(one)
                with operation.locked(value):pass
                with operation.locked(value):pass
                with self.assertRaises(t.TransportHeld):
                    with t.HostOperation(two).locked(value):pass
                other=copy.deepcopy(value);other['operationId']=str(uuid.uuid4())
                with self.assertRaises(t.TransportHeld):
                    with operation.locked(other):pass

    def test_proof_directory_replacement_and_symlink_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)/'proofs';root.mkdir(mode=0o700);proofs=t.PrivateProofs(root)
            root.rename(root.with_name('original'));root.mkdir(mode=0o700)
            with self.assertRaises(t.TransportHeld):proofs.retain({'intentId':str(uuid.uuid4())},{})

    def test_private_stdin_roundtrip_and_sanitized_stderr_failure(self):
        runner=t.BoundedSubprocess()
        self.assertEqual(runner.run([sys.executable,'-c','import sys;sys.stdout.buffer.write(sys.stdin.buffer.read())'],
            input_bytes=b'x'*200000,timeout=3,output_limit=300000),b'x'*200000)
        with self.assertRaises(t.TransportHeld) as caught:
            runner.run([sys.executable,'-c','import sys;sys.stderr.write("synthetic-secret");sys.exit(1)'],mutation=True)
        self.assertEqual(str(caught.exception),'mutation_outcome_unknown')

    def test_streamed_output_is_bounded_and_receipt_requires_clean_exit(self):
        import hashlib
        chunks=[]
        result=t.BoundedSubprocess().run([sys.executable,'-c','import sys;sys.stdout.buffer.write(b"x"*200000)'],
                         sink=chunks.append,output_limit=200000,timeout=3)
        self.assertEqual(result,dict(clientExitCode=0,streamClosed=True,bytes=200000,
                                    sha256=hashlib.sha256(b'x'*200000).hexdigest()))
        self.assertTrue(all(len(c)<=65536 for c in chunks))
        for source in ('import sys;sys.stdout.write("x"*101)',
                       'import sys;sys.stdout.write("x");sys.exit(1)'):
            with self.assertRaises(t.TransportHeld):
                t.BoundedSubprocess().run([sys.executable,'-c',source],sink=lambda _:None,output_limit=100)

    def test_exact_proof_replay_repeats_file_and_directory_durability_barriers(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);root.chmod(0o700);proofs=t.PrivateProofs(root)
            intent=dict(intentId=str(uuid.uuid4()))
            with patch.object(t.os,'fsync',side_effect=OSError('synthetic lost durability')):
                with self.assertRaises(OSError):proofs.retain(intent,{'bound':True})
            calls=[];real=os.fsync
            def sync(fd):calls.append(os.fstat(fd).st_mode);return real(fd)
            with patch.object(t.os,'fsync',side_effect=sync):proofs.retain(intent,{'bound':True})
            import stat
            self.assertTrue(any(stat.S_ISREG(mode) for mode in calls))
            self.assertTrue(any(stat.S_ISDIR(mode) for mode in calls))

    def test_output_overflow_and_deadline_are_bounded(self):
        runner=t.BoundedSubprocess()
        for script in ('import sys;sys.stdout.write("x"*100000)', 'import time;time.sleep(30)'):
            with self.assertRaises(t.TransportHeld):
                runner.run([sys.executable,'-c',script],timeout=.2,output_limit=100)

    def test_private_file_mode_and_symlink_refuse(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'private';path.write_bytes(b'private-synthetic-data');path.chmod(0o600)
            self.assertEqual(t.private_bytes(path),b'private-synthetic-data')
            path.chmod(0o644)
            with self.assertRaises(t.TransportHeld):t.private_bytes(path)
            path.chmod(0o600);link=path.with_name('link');link.symlink_to(path)
            with self.assertRaises(t.TransportHeld):t.private_bytes(link)

    def test_timed_out_client_does_not_leave_its_owned_descendant_running(self):
        with tempfile.TemporaryDirectory() as directory:
            marker=Path(directory)/'owned-child'
            source='''import os,pathlib,sys,time
child=os.fork()
if child==0:time.sleep(30)
else:
 p=pathlib.Path('/proc/'+str(child)+'/stat');start=p.read_text().rsplit(')',1)[1].split()[19]
 pathlib.Path(sys.argv[1]).write_text(str(child)+' '+start)
 time.sleep(30)
'''
            with self.assertRaises(t.TransportHeld):
                t.BoundedSubprocess().run([sys.executable,'-c',source,str(marker)],timeout=.5)
            self.assertTrue(marker.exists())
            pid,start=marker.read_text().split();path=Path('/proc/'+pid+'/stat');deadline=time.monotonic()+2
            while path.exists() and time.monotonic()<deadline:
                try:
                    if path.read_text().rsplit(')',1)[1].split()[19]!=start:return
                except FileNotFoundError:return
                time.sleep(.01)
            self.assertFalse(path.exists(),'owned command descendant remains after cleanup')


@unittest.skipUnless(os.name=='posix' and hasattr(os,'geteuid') and os.geteuid()==10001,
                     'synthetic Linux UID10001 original enrolled state')
class OriginalLeaseHolderTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        root=Path(self.temp.name);self.state=root/'state';self.state.mkdir(mode=0o755);self.state.chmod(0o755)
        self.database=self.state/'jobs.sqlite3';self.authority=str(uuid.uuid4())
        with closing(sqlite3.connect(self.database)) as db:
            db.execute('CREATE TABLE coordinator_identity(singleton INTEGER PRIMARY KEY CHECK(singleton=1),authority_id TEXT NOT NULL)')
            db.execute('INSERT INTO coordinator_identity VALUES(1,?)',(self.authority,))
            from processing.authority_lease import MARKER_DDL
            db.execute(MARKER_DDL);db.execute('INSERT INTO coordinator_authority_lease VALUES(1,1)');db.commit()
        self.database.chmod(0o600)
        self.main=self.state/'coordinator.lock';self.main.write_bytes(b'original-main-diagnostic');self.main.chmod(0o644)
        self.leases=root/'leases';self.leases.mkdir(mode=0o711);self.leases.chmod(0o711)
        self.external=self.leases/(self.authority+'.lease');self.external.write_bytes(b'original-external-diagnostic');self.external.chmod(0o600)
        self.settings=dict(schemaVersion=1,operationId=str(uuid.uuid4()),planSha256='a'*64,
            authorityId=self.authority,stateRoot=str(self.state),leaseDirectory=str(self.leases))
        self.proof=root/'owner.json';self.child=None;self.addCleanup(self.stop)

    def stop(self):
        if self.child is not None:
            if self.child.poll() is None:self.child.send_signal(signal.SIGTERM)
            try:self.child.communicate(timeout=3)
            except subprocess.TimeoutExpired:self.child.kill();self.child.communicate(timeout=3)
            self.child=None

    def start(self):
        self.child=subprocess.Popen([sys.executable,'-B','-c',
            'from ops.enrolled_coordinator_lease_helper import hold;import json,sys;raise SystemExit(hold(json.loads(sys.argv[1]),proof_path=sys.argv[2]))',
            json.dumps(self.settings),str(self.proof)],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        deadline=time.monotonic()+3
        while not self.proof.exists() and self.child.poll() is None and time.monotonic()<deadline:time.sleep(.01)
        if self.child.poll() is not None:
            _,error=self.child.communicate();self.fail('synthetic holder exited: '+error.decode())
        self.assertTrue(self.proof.exists())

    def test_actual_original_main_and_external_flocks_not_copy_or_self_report(self):
        import fcntl
        before={p:p.read_bytes() for p in (self.database,self.main,self.external)}
        self.start()
        observed=holder.observe(self.settings,expected_pid=self.child.pid,proof_path=str(self.proof))
        self.assertEqual(observed['settings'],self.settings)
        for path in (self.main,self.external):
            with path.open('rb') as file:
                with self.assertRaises(BlockingIOError):fcntl.flock(file,fcntl.LOCK_EX|fcntl.LOCK_NB)
        self.child.send_signal(signal.SIGTERM);_,error=self.child.communicate(timeout=3)
        self.assertEqual(self.child.returncode,0,error.decode())
        for path in (self.main,self.external):
            with path.open('rb') as file:fcntl.flock(file,fcntl.LOCK_EX|fcntl.LOCK_NB)
        self.assertEqual({p:p.read_bytes() for p in before},before)

    def test_forged_descriptor_proof_cannot_pass_actual_fd_and_flock_checks(self):
        self.start();proof=json.loads(self.proof.read_text())
        proof['mainDescriptor']=proof['leaseDescriptor'];self.proof.write_text(json.dumps(proof))
        with self.assertRaises(RuntimeError):holder.observe(self.settings,expected_pid=self.child.pid,proof_path=str(self.proof))

    def test_post_acquisition_check_failure_holds_original_locks_until_explicit_release(self):
        self._failure_retains('check')

    def test_proof_fsync_failure_holds_original_locks_until_explicit_release(self):
        self._failure_retains('fsync')

    def test_proof_and_failure_marker_errors_cannot_report_ready_from_visible_proof(self):
        self._failure_retains('double')

    def _failure_retains(self, fault):
        import fcntl
        source='''import json,sys
from unittest.mock import patch
from contextlib import ExitStack
import os
from ops.enrolled_coordinator_lease_helper import hold
from processing.authority_lease import AuthorityLease
original=AuthorityLease.check;calls=0
def check(self,*args,**kwargs):
 global calls
 calls+=1
 if calls>1:raise OSError('synthetic post-acquisition failure')
 return original(self,*args,**kwargs)
target='processing.authority_lease.AuthorityLease.check' if sys.argv[3]=='check' else 'ops.enrolled_coordinator_lease_helper.os.fsync'
options={'new':check} if sys.argv[3]=='check' else {'side_effect':OSError('synthetic injected failure')}
original_open=os.open
def opened(path,*args,**kwargs):
 if str(path).endswith('.held'):raise OSError('synthetic failure marker unavailable')
 return original_open(path,*args,**kwargs)
with ExitStack() as stack:
 stack.enter_context(patch(target,**options))
 if sys.argv[3]=='double':stack.enter_context(patch('ops.enrolled_coordinator_lease_helper.os.open',new=opened))
 raise SystemExit(hold(json.loads(sys.argv[1]),proof_path=sys.argv[2]))
'''
        self.child=subprocess.Popen([sys.executable,'-B','-c',source,json.dumps(self.settings),str(self.proof),fault],
                                    stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        held=Path(str(self.proof)+'.held');deadline=time.monotonic()+3
        target=self.proof if fault=='double' else held
        while not target.exists() and self.child.poll() is None and time.monotonic()<deadline:time.sleep(.01)
        self.assertTrue(target.exists());self.assertIsNone(self.child.poll())
        if fault=='double':
            time.sleep(.15);self.assertFalse(held.exists())
        with self.assertRaises(RuntimeError):holder.observe(self.settings,expected_pid=self.child.pid,proof_path=str(self.proof))
        for path in (self.main,self.external):
            with path.open('rb') as file:
                with self.assertRaises(BlockingIOError):fcntl.flock(file,fcntl.LOCK_EX|fcntl.LOCK_NB)
        self.child.send_signal(signal.SIGTERM);_,error=self.child.communicate(timeout=3)
        self.assertEqual(self.child.returncode,0,error.decode())
        for path in (self.main,self.external):
            with path.open('rb') as file:fcntl.flock(file,fcntl.LOCK_EX|fcntl.LOCK_NB)


@unittest.skipUnless(os.name=='posix','actual Linux remote-process lifecycle fixture')
class RemoteExecLifecycleTests(unittest.TestCase):
    def setUp(self):
        import hashlib
        self.source=b'import json,time;time.sleep(.3);print(json.dumps({"bounded":True}))'
        self.settings=dict(operationId=str(uuid.uuid4()),requestId=str(uuid.uuid4()),
                           sourceSha256=hashlib.sha256(self.source).hexdigest(),maximum=4096)
        self.root=remote.validate(self.settings);self.child=None;self.addCleanup(self.cleanup)

    def cleanup(self):
        if self.child is not None:
            if self.child.poll() is None:self.child.terminate()
            self.child.communicate(timeout=3)
        self.assertEqual(self.root.parent,Path('/tmp'))
        self.assertEqual(self.root,remote.validate(self.settings))
        if self.root.exists():shutil.rmtree(self.root)

    def launch(self,source=None):
        self.child=subprocess.Popen([sys.executable,'-I','-'],stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        self.child.stdin.write(remote.serializable_source(self.settings,source=self.source if source is None else source))
        self.child.stdin.close();self.child.stdin=None
        deadline=time.monotonic()+2
        while not (self.root/'start.json').exists() and self.child.poll() is None and time.monotonic()<deadline:time.sleep(.01)
        self.assertTrue((self.root/'start.json').exists())

    def test_actual_live_pid_cannot_retire_even_with_valid_start_record_then_exact_exit_recovers(self):
        self.launch()
        with self.assertRaises(RuntimeError):remote.inspect(self.settings)
        _,error=self.child.communicate(timeout=3);self.assertEqual(self.child.returncode,0,error.decode())
        value=remote.inspect(self.settings)
        self.assertTrue(value['processRetired']);self.assertEqual(value['result'],{'bounded':True})

    def test_child_death_without_exit_record_is_not_a_completed_observer(self):
        self.launch();self.child.terminate();self.child.communicate(timeout=3)
        with self.assertRaises((RuntimeError,FileNotFoundError)):remote.inspect(self.settings)
        with self.assertRaises(FileExistsError):remote.execute(self.settings,self.source)

    def test_exit_marker_written_while_its_pid_is_live_cannot_retire(self):
        self.root.mkdir(mode=0o700)
        identity=dict(settings=self.settings,pid=os.getpid(),start=remote.start_time(os.getpid()))
        remote.write(self.root/'start.json',identity)
        remote.write(self.root/'exit.json',dict(identity=identity,success=False,resultSha256=None))
        with self.assertRaises(RuntimeError):remote.inspect(self.settings)

    def test_tracked_lost_local_reply_retains_request_and_never_dispatches_twice(self):
        import hashlib
        source=self.source;case=self
        class LostReply:
            calls=0
            def exec_python(self,identifier,script,**kwargs):
                self.calls+=1
                if self.calls==1:
                    case.child=subprocess.Popen([sys.executable,'-I','-'],stdin=subprocess.PIPE,
                                                stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    case.child.stdin.write(script);case.child.stdin.close();case.child.stdin=None
                    raise t.TransportHeld('synthetic local reply lost; remote still running')
                raw=t.BoundedSubprocess().run([sys.executable,'-I','-'],input_bytes=script,timeout=3)
                return t.decode(raw)
        with tempfile.TemporaryDirectory() as directory:
            Path(directory).chmod(0o700);docker=LostReply()
            executor=t.TrackedExec(docker,self.settings['operationId'],directory)
            with self.assertRaises(t.TransportHeld):executor.dispatch('a'*64,self.settings['requestId'],source,maximum=4096)
            _,error=self.child.communicate(timeout=3);self.assertEqual(self.child.returncode,0,error.decode())
            result=executor.inspect('a'*64,self.settings['requestId'],source,maximum=4096)
            self.assertEqual(result['result'],{'bounded':True});self.assertTrue(result['process']['retired'])
            self.assertTrue(result['bodySucceeded']);self.assertNotIn('scriptExitCode',result)
            before=docker.calls
            with self.assertRaises(t.TransportHeld):executor.dispatch('a'*64,self.settings['requestId'],source,maximum=4096)
            self.assertEqual(docker.calls,before)


if __name__=='__main__':unittest.main()
