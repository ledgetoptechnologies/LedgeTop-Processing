import copy
from dataclasses import replace
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import json
import os
from pathlib import Path
import queue
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import uuid

from ops.coordinator_gate_bridge import (BridgeBinding,BridgeError,BridgeEOF,BridgeGateClient,
    GateSet,LocalGate,RootRelay,MainEngineProbe,MAIN_CONTAINER_ID,MAIN_CONTAINER_NAME,MAIN_INSPECT_TEMPLATE,
    helper_digest,incremental_helper_digest,_Pipe,_common,_main_http_json)
from processing.node_gate import NodeGate,GateBusy,canonical_gate_path


SESSION='a'*64
OP='coordinator-fixture-update'


def pair():
    first_read,second_write=os.pipe();second_read,first_write=os.pipe()
    return ((os.fdopen(first_read,'rb',buffering=0),os.fdopen(first_write,'wb',buffering=0)),
            (os.fdopen(second_read,'rb',buffering=0),os.fdopen(second_write,'wb',buffering=0)))


@unittest.skipUnless(os.name=='posix','actual canonical Linux flock and subprocess pipes')
class BridgeTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        env=patch.dict(os.environ,{'XDG_STATE_HOME':str(self.root/'gate-state')});env.start();self.addCleanup(env.stop)
        self.authority=str(uuid.uuid4());digest=helper_digest()
        self.main=BridgeBinding('fixture-main','main',self.authority,'http://127.0.0.1:18080',1,digest)
        self.cluster=BridgeBinding('fixture-cluster','cluster',self.authority,'http://127.0.0.1:18080',1,digest)
        for binding in (self.main,self.cluster):
            with NodeGate(binding.engine_id) as gate:gate.provision(binding.node_id,binding.coordinator_origin,self.authority)
        self.original={binding.engine_id:(canonical_gate_path(binding.engine_id)/'gate.json').read_bytes() for binding in (self.main,self.cluster)}

    def start_holder(self,binding=None):
        binding=binding or self.main
        code='''import json,sys\nfrom ops.coordinator_gate_bridge import BridgeBinding,serve_main\nb=BridgeBinding(**json.loads(sys.argv[1]))\ntry:serve_main(sys.stdin.buffer,sys.stdout.buffer,binding=b,operation_id=sys.argv[2],session_nonce=sys.argv[3],engine_probe=lambda:b.engine_id,timeout_seconds=1)\nexcept Exception:sys.exit(2)\n'''
        process=subprocess.Popen([sys.executable,'-B','-c',code,json.dumps(binding.__dict__),OP,SESSION],
            stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        self.addCleanup(self.cleanup_process,process)
        return process

    @staticmethod
    def cleanup_process(process):
        if process.poll() is None:process.terminate()
        try:process.wait(timeout=3)
        except subprocess.TimeoutExpired:process.kill();process.wait(timeout=3)
        for stream in (process.stdin,process.stdout,process.stderr):
            if stream:
                try:stream.close()
                except OSError:pass

    def start_executor(self,*,wait=False,extra=False,fail=False):
        ready=self.root/'executor-ready';release=self.root/'executor-release'
        code='''import json,sys,time\nfrom pathlib import Path\nfrom ops.coordinator_gate_bridge import BridgeBinding,BridgeGateClient,LocalGate,GateSet\nm,c=[BridgeBinding(**item) for item in json.loads(sys.argv[1])]\ntry:\n with GateSet(LocalGate(c,engine_probe=lambda:c.engine_id),BridgeGateClient(sys.stdin.buffer,sys.stdout.buffer,binding=m,operation_id=sys.argv[2],session_nonce=sys.argv[3],timeout_seconds=1)) as gates:\n  gates.check()\n  Path(sys.argv[4]).write_text('ready')\n  if sys.argv[6]=='wait':\n   while not Path(sys.argv[5]).exists():time.sleep(.01)\n  gates.check()\n if sys.argv[7]=='extra':print('unexpected stdout',flush=True)\n if sys.argv[8]=='fail':sys.exit(3)\nexcept Exception:sys.exit(2)\n'''
        process=subprocess.Popen([sys.executable,'-B','-c',code,json.dumps([self.main.__dict__,self.cluster.__dict__]),OP,SESSION,
            str(ready),str(release),'wait' if wait else 'now','extra' if extra else 'clean','fail' if fail else 'ok'],
            stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        self.addCleanup(self.cleanup_process,process)
        return process,ready,release

    def wait_file(self,path):
        deadline=time.monotonic()+3
        while not path.exists() and time.monotonic()<deadline:time.sleep(.01)
        self.assertTrue(path.exists())

    def assert_free(self,binding):
        with NodeGate(binding.engine_id):pass

    def test_actual_pipe_relay_holds_both_gates_and_closes_only_own_contexts(self):
        main=self.start_holder();cluster,ready,release=self.start_executor(wait=True)
        result=queue.Queue()
        relay=RootRelay(main,cluster,binding=self.main,operation_id=OP,session_nonce=SESSION,timeout_seconds=1)
        def run():
            try:result.put(relay.run())
            except Exception as error:result.put(error)
        thread=threading.Thread(target=run);thread.start();self.addCleanup(lambda:thread.join(timeout=3))
        self.wait_file(ready)
        for binding in (self.main,self.cluster):
            with self.assertRaises(GateBusy):NodeGate(binding.engine_id).__enter__()
        release.write_text('finish')
        thread.join(timeout=3);self.assertFalse(thread.is_alive());self.assertEqual(result.get_nowait(),0)
        self.assertEqual(cluster.wait(timeout=3),0);self.assertEqual(main.wait(timeout=3),0)
        for binding in (self.main,self.cluster):
            self.assert_free(binding)
            self.assertEqual((canonical_gate_path(binding.engine_id)/'gate.json').read_bytes(),self.original[binding.engine_id])
        with self.assertRaises(BridgeError):relay.run()

    def test_actual_incremental_pipe_client_and_root_relay_qualify_expanded_code_closure(self):
        self.main=replace(self.main,helper_sha256=incremental_helper_digest())
        self.cluster=replace(self.cluster,helper_sha256=incremental_helper_digest())
        self.test_actual_pipe_relay_holds_both_gates_and_closes_only_own_contexts()

    def test_main_holder_death_aborts_relay_and_executor_not_stale_ready(self):
        main=self.start_holder();cluster,ready,release=self.start_executor(wait=True)
        result=queue.Queue();relay=RootRelay(main,cluster,binding=self.main,operation_id=OP,session_nonce=SESSION,timeout_seconds=.5)
        def run():
            try:result.put(relay.run())
            except Exception as error:result.put(error)
        thread=threading.Thread(target=run);thread.start();self.wait_file(ready)
        main.kill();main.wait(timeout=3)
        thread.join(timeout=3);self.assertFalse(thread.is_alive());self.assertIsInstance(result.get(),BridgeError)
        cluster.wait(timeout=3)
        self.assert_free(self.main);self.assert_free(self.cluster)

    def test_extra_stdout_or_nonzero_executor_exit_cannot_be_success(self):
        for extra,fail in ((True,False),(False,True)):
            main=self.start_holder();cluster,_,_=self.start_executor(extra=extra,fail=fail)
            with self.subTest(extra=extra,fail=fail),self.assertRaises(BridgeError):
                RootRelay(main,cluster,binding=self.main,operation_id=OP,session_nonce=SESSION,timeout_seconds=1).run()
            main.wait(timeout=3);cluster.wait(timeout=3)

    def test_eof_releases_real_main_lease_without_reservation_mutation(self):
        main=self.start_holder();channel=_Pipe(main.stdout,main.stdin,timeout=1)
        ready=channel.read(time.monotonic()+1)
        self.assertEqual(ready['type'],'main-gate-ready')
        with self.assertRaises(GateBusy):NodeGate(self.main.engine_id).__enter__()
        main.stdin.close();self.assertEqual(main.wait(timeout=3),0)
        self.assert_free(self.main)
        self.assertEqual((canonical_gate_path(self.main.engine_id)/'gate.json').read_bytes(),self.original[self.main.engine_id])

    def test_partial_frame_absolute_deadline_releases_holder_not_per_byte_idle(self):
        main=self.start_holder();channel=_Pipe(main.stdout,main.stdin,timeout=1)
        channel.read(time.monotonic()+1)
        os.write(main.stdin.fileno(),b'{')
        with self.assertRaises(GateBusy):NodeGate(self.main.engine_id).__enter__()
        self.assertEqual(main.wait(timeout=3),2)
        self.assert_free(self.main)

    def test_actual_controller_death_closes_links_and_prevents_next_executor_check(self):
        code='''import json,sys\nfrom pathlib import Path\nfrom tests.test_coordinator_gate_bridge import BridgeTests,BridgeBinding,RootRelay,OP,SESSION\nf=BridgeTests('runTest');f.root=Path(sys.argv[1]);f.main,f.cluster=[BridgeBinding(**value) for value in json.loads(sys.argv[2])]\nm=f.start_holder();c,r,end=f.start_executor(wait=True)\nRootRelay(m,c,binding=f.main,operation_id=OP,session_nonce=SESSION,timeout_seconds=1).run()\n'''
        controller=subprocess.Popen([sys.executable,'-B','-c',code,str(self.root),json.dumps([self.main.__dict__,self.cluster.__dict__])],
            stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
        self.addCleanup(self.cleanup_process,controller)
        def kill_owned_group():
            try:os.killpg(controller.pid,signal.SIGTERM)
            except ProcessLookupError:pass
        self.addCleanup(kill_owned_group)
        self.wait_file(self.root/'executor-ready')
        controller.kill();controller.wait(timeout=3)
        deadline=time.monotonic()+3
        while True:
            try:self.assert_free(self.main);break
            except GateBusy:
                if time.monotonic()>=deadline:raise
                time.sleep(.01)
        # The executor still owns its local context while doing offline work;
        # its next fresh check sees EOF rather than authorizing another action.
        with self.assertRaises(GateBusy):NodeGate(self.cluster.engine_id).__enter__()
        (self.root/'executor-release').write_text('next check must fail')
        deadline=time.monotonic()+3
        while True:
            try:self.assert_free(self.cluster);break
            except GateBusy:
                if time.monotonic()>=deadline:raise
                time.sleep(.01)
        for binding in (self.main,self.cluster):
            self.assertEqual((canonical_gate_path(binding.engine_id)/'gate.json').read_bytes(),self.original[binding.engine_id])

    def test_binding_generation_reservation_or_helper_mismatch_never_ready(self):
        for binding in (replace(self.main,gate_generation=2),replace(self.main,authority_id=str(uuid.uuid4())),
                        replace(self.main,helper_sha256='f'*64)):
            process=self.start_holder(binding)
            self.assertEqual(process.wait(timeout=3),2);self.assertEqual(process.stdout.read(),b'')
            self.assert_free(self.main)
        with NodeGate(self.main.engine_id) as gate:
            gate.reserve('main',self.main.coordinator_origin,self.authority,1,'1'*32)
            before=gate.snapshot()
        process=self.start_holder();self.assertEqual(process.wait(timeout=3),2)
        with NodeGate(self.main.engine_id) as gate:self.assertEqual(gate.snapshot(),before)

    def test_actual_local_gate_live_identity_and_docker_identity_are_rechecked(self):
        engine=[self.main.engine_id]
        with LocalGate(self.main,engine_probe=lambda:engine[0]) as held:
            held.check();engine[0]='different'
            with self.assertRaises(BridgeError):held.check()
            engine[0]=self.main.engine_id
            state=held.gate.root/'gate.json';original=state.read_bytes()
            value=json.loads(original);value['generation']=2;state.write_text(json.dumps(value))
            with self.assertRaises(BridgeError):held.check()
            state.write_bytes(original)
        self.assert_free(self.main)

    def test_lock_inode_replacement_is_detected_and_never_unlocks_replacement_owner(self):
        with LocalGate(self.main,engine_probe=lambda:self.main.engine_id) as held:
            lock=held.gate.root/'gate.lock';lock.rename(held.gate.root/'old.lock')
            lock.touch(mode=0o600)
            with NodeGate(self.main.engine_id):
                with self.assertRaises(BridgeError):held.check()
                held.__exit__(None,None,None)
                with self.assertRaises(GateBusy):NodeGate(self.main.engine_id).__enter__()
        self.assert_free(self.main)

    def test_fork_child_cannot_check_or_unlock_parent_real_gate(self):
        with LocalGate(self.main,engine_probe=lambda:self.main.engine_id) as held:
            read,write=os.pipe();pid=os.fork()
            if pid==0:
                os.close(read);refused=0
                for action in (held.check,lambda:held.__exit__(None,None,None)):
                    try:action()
                    except BridgeError:refused+=1
                os.write(write,str(refused).encode());os._exit(0)
            os.close(write);self.assertEqual(os.read(read,10),b'2');os.close(read);os.waitpid(pid,0)
            held.check()
            with self.assertRaises(GateBusy):NodeGate(self.main.engine_id).__enter__()


class FramingAndClientTests(unittest.TestCase):
    def setUp(self):
        self.binding=BridgeBinding('fixture-main','main',str(uuid.uuid4()),'http://127.0.0.1:18080',1,helper_digest())
        self.identity=dict(pid=1,rootDevice=1,rootInode=2,lockDevice=1,lockInode=3,stateDevice=1,stateInode=4)
        self.base=_common(self.binding,OP,SESSION,'b'*64,self.identity)

    def peer(self,change=None,*,ready_change=None,trickle=False,no_reply=False,timeout=.3):
        client_streams,peer_streams=pair()
        for stream in (*client_streams,*peer_streams):self.addCleanup(stream.close)
        peer=_Pipe(*peer_streams,timeout=timeout);stop=threading.Event()
        def run():
            try:
                ready=dict(self.base,type='main-gate-ready')
                if ready_change:ready_change(ready)
                peer.write(ready,time.monotonic()+timeout)
                while not stop.is_set():
                    request=peer.read(time.monotonic()+1)
                    if request['type']=='main-gate-close':return
                    if no_reply:stop.wait(1);return
                    reply=dict(request,type='main-gate-checked')
                    if change:change(reply)
                    if trickle:
                        raw=json.dumps(reply).encode()+b'\n'
                        for byte in raw:
                            if stop.wait(.01):return
                            os.write(peer.wfd,bytes([byte]))
                    else:peer.write(reply,time.monotonic()+timeout)
            except (BridgeError,OSError):pass
        thread=threading.Thread(target=run);thread.start()
        self.addCleanup(lambda:(stop.set(),thread.join(timeout=2)))
        return BridgeGateClient(*client_streams,binding=self.binding,operation_id=OP,session_nonce=SESSION,timeout_seconds=timeout)

    def test_ready_alone_never_enters_gate_context(self):
        client=self.peer(no_reply=True,timeout=.15)
        with self.assertRaises(BridgeError):client.__enter__()
        with self.assertRaises(BridgeError):client.check()

    def test_fresh_nonce_and_concurrent_checks_serialize(self):
        client=self.peer(timeout=1)
        with client:
            self.assertEqual(client.sequence,1)
            errors=[]
            def check():
                try:client.check()
                except Exception as error:errors.append(error)
            threads=[threading.Thread(target=check) for _ in range(4)]
            for thread in threads:thread.start()
            for thread in threads:thread.join(timeout=2)
            self.assertEqual(errors,[]);self.assertEqual(client.sequence,5)

    def test_wrong_nonce_sequence_session_operation_engine_authority_or_helper_is_rejected(self):
        changes=[lambda value:value.update(nonce='e'*64),lambda value:value.update(sequence=2),
            lambda value:value.update(sessionNonce='c'*64),lambda value:value.update(operationId='peer-operation'),
            lambda value:value['binding'].update(engineId='wrong'),lambda value:value['binding'].update(authorityId=str(uuid.uuid4())),
            lambda value:value['binding'].update(helperSha256='e'*64),lambda value:value['binding'].update(generation=True)]
        for change in changes:
            with self.subTest(change=change):
                # JSON transport makes each nested binding independent.
                original=copy.deepcopy(self.base)
                client=self.peer(change=change)
                with self.assertRaises(BridgeError):client.__enter__()
                self.base=original

    def test_absolute_deadline_is_not_extended_by_trickling_response(self):
        client=self.peer(trickle=True,timeout=.15);start=time.monotonic()
        with self.assertRaises(BridgeError):client.__enter__()
        self.assertLess(time.monotonic()-start,.8)
        with self.assertRaises(BridgeError):client.check()

    def test_non_json_duplicate_fields_oversized_frame_or_partial_eof_rejected(self):
        for raw in (b'diagnostic stdout\n',b'{"a":1,"a":2}\n',b'x'*4097,b'{"a":'):
            client,peer=pair()
            for stream in (*client,*peer):self.addCleanup(stream.close)
            channel=_Pipe(*client,timeout=.2)
            def produce(raw=raw,stream=peer[1]):
                try:
                    offset=0
                    while offset<len(raw):offset+=os.write(stream.fileno(),raw[offset:])
                except OSError:pass
                finally:stream.close()
            thread=threading.Thread(target=produce,daemon=True);thread.start()
            try:
                with self.subTest(raw=raw[:20]),self.assertRaises(BridgeError):channel.read(time.monotonic()+.2)
            finally:
                client[0].close();thread.join(timeout=1)
            self.assertFalse(thread.is_alive())

    def test_stale_ready_wrong_session_or_identity_cannot_be_used(self):
        client=self.peer(ready_change=lambda value:value.update(sessionNonce='f'*64))
        with self.assertRaises(BridgeError):client.__enter__()
        client=self.peer(change=lambda value:value['gateIdentity'].update(lockInode=12345))
        with self.assertRaises(BridgeError):client.__enter__()


class MainProbeTests(unittest.TestCase):
    def setUp(self):
        self.image='sha256:'+'8'*64
        self.probe=MainEngineProbe(self.image,executable=sys.executable,endpoint='unix:///fixture.sock',timeout_seconds=.5)
        self.container=dict(id=MAIN_CONTAINER_ID,name=MAIN_CONTAINER_NAME,image=self.image,running=True,pid=123,health=None)
        self.info=dict(engine='odx',version='2.4.0',engineVersion='3.8.5',taskQueueCount=0)
        self.tasks=[];self.uploads='';self.kind='regular file 1\n';self.placeholder='*'
        self.top='PID COMMAND\n123 node /var/www/index.js --port 3000\n'
        self.identities=['fixture-main','fixture-main'];self.calls=[];self.deadlines=[]

    def run_probe(self):
        identities=iter(self.identities)
        def output(args,deadline=None):
            self.calls.append(args);self.deadlines.append(deadline)
            if args[0]=='info':return next(identities)+'\n'
            if args[:2]==['container','inspect']:return json.dumps(self.container)+'\n'
            if args[0]=='top':return self.top
            if args[0]=='exec':
                if args[2]=='/bin/ls':return self.uploads
                if args[2]=='/usr/bin/stat':return self.kind
                if args[2]=='/bin/cat':return self.placeholder
            raise AssertionError(args)
        def api(route,deadline,limit=65536):
            self.deadlines.append(deadline)
            return self.info if route=='/info' else self.tasks
        with patch.object(self.probe,'_output',side_effect=output),patch('ops.coordinator_gate_bridge._main_http_json',side_effect=api):
            return self.probe()

    def test_fixed_actual_main_no_healthcheck_is_valid_only_when_all_idle_proofs_match(self):
        self.assertEqual(self.run_probe(),'fixture-main')
        self.assertEqual(len(set(self.deadlines)),1)
        inspect=next(args for args in self.calls if args[:2]==['container','inspect'])
        self.assertEqual(inspect[-1],MAIN_CONTAINER_ID)
        self.assertEqual(inspect[3],MAIN_INSPECT_TEMPLATE)
        self.assertIn('{{with index .State "Health"}}{{json (index . "Status")}}{{else}}null{{end}}',inspect[3])
        self.assertNotIn('.State.Health',inspect[3])
        self.assertFalse(any('Env' in arg or 'Cmd' in arg for arg in inspect))
        self.assertTrue(all(args[0] in ('info','container','exec','top') for args in self.calls))

    def test_exact_regular_placeholder_and_known_server_wrapper_init_allowed(self):
        self.uploads='.gitignore\n'
        self.top='PID COMMAND\n1 /sbin/docker-init -- /usr/bin/node /var/www/index.js\n2 /bin/bash /usr/bin/node /var/www/index.js\n3 /usr/bin/node /var/www/index.js\n'
        self.assertEqual(self.run_probe(),'fixture-main')

    def test_explicit_incremental_cid_is_used_for_all_actual_probe_commands(self):
        current='a'*64
        self.probe=MainEngineProbe(self.image,expected_container=current,executable=sys.executable,endpoint='unix:///fixture.sock',timeout_seconds=.5)
        self.container['id']=current
        self.assertEqual(self.run_probe(),'fixture-main')
        for args in self.calls:
            if args[0]=='container':self.assertEqual(args[-1],current)
            if args[0] in ('exec','top'):self.assertEqual(args[1],current)
        self.container['id']=MAIN_CONTAINER_ID
        with self.assertRaises(BridgeError):self.run_probe()

    def test_explicit_probe_identity_drift_or_empty_container_cannot_fall_back_to_historical(self):
        with self.assertRaises(BridgeError):MainEngineProbe(self.image,expected_container='')
        self.probe.expected_container='a'*64
        with self.assertRaises(BridgeError):self.run_probe()
        self.assertEqual(self.calls,[])

    def test_foreign_container_name_image_stopped_unhealthy_or_invalid_pid_refused(self):
        for key,value in (('id','9'*64),('name','/peer'),('image','sha256:'+'9'*64),('running',False),('pid',0),('pid',True),('health','unhealthy')):
            old=self.container.copy();self.container[key]=value
            with self.subTest(field=key,value=value),self.assertRaises(BridgeError):self.run_probe()
            self.container=old

    def test_queue_task_or_wrong_dialect_refused(self):
        for change in ({'taskQueueCount':1},{'taskQueueCount':False},{'engine':'odm'}):
            original=self.info.copy();self.info.update(change)
            with self.subTest(change=change),self.assertRaises(BridgeError):self.run_probe()
            self.info=original
        self.tasks=[{'uuid':'historical-even-if-terminal'}]
        with self.assertRaises(BridgeError):self.run_probe()

    def test_unregistered_upload_symlink_or_wrong_placeholder_never_deleted(self):
        for listing,kind,body in (('incomplete-upload\n','regular file 1\n','*'),('.gitignore\n','symbolic link 1\n','*'),
                                 ('.gitignore\n','regular file 2\n','*'),('.gitignore\n','regular file 1\n','x')):
            self.uploads,self.kind,self.placeholder=listing,kind,body
            with self.subTest(listing=listing,kind=kind,body=body),self.assertRaises(BridgeError):self.run_probe()
        self.assertFalse(any(args[0] in ('rm','stop','start','create') for args in self.calls))

    def test_orphan_child_malformed_or_duplicate_server_is_not_idle(self):
        for tail in ('234 python /code/run.py\n','234 node /var/www/index.js\n','malformed\n','123 docker-init\n'):
            self.top='PID COMMAND\n123 node /var/www/index.js\n'+tail
            with self.subTest(tail=tail),self.assertRaises(BridgeError):self.run_probe()
        self.top='not-a-header\n123 node /var/www/index.js\n'
        with self.assertRaises(BridgeError):self.run_probe()

    def test_docker_engine_change_after_main_proofs_refused(self):
        self.identities=['fixture-main','different-engine']
        with self.assertRaises(BridgeError):self.run_probe()

    def test_final_engine_output_at_shared_absolute_deadline_cannot_succeed(self):
        clock=[100.0];identity_reads=[0]
        def output(args,deadline=None):
            if args[0]=='info':
                identity_reads[0]+=1
                if identity_reads[0]==2:clock[0]=deadline
                return 'fixture-main\n'
            if args[:2]==['container','inspect']:return json.dumps(self.container)
            if args[0]=='exec':return ''
            if args[0]=='top':return self.top
            raise AssertionError(args)
        def api(route,deadline,limit=65536):return self.info if route=='/info' else self.tasks
        with patch('ops.coordinator_gate_bridge.time.monotonic',side_effect=lambda:clock[0]),\
                patch.object(self.probe,'_output',side_effect=output),\
                patch('ops.coordinator_gate_bridge._main_http_json',side_effect=api):
            with self.assertRaises(BridgeError):self.probe()
        self.assertEqual(identity_reads[0],2)


class ReadonlyMainHTTPTests(unittest.TestCase):
    def request(self,mode,limit=65536):
        stop=threading.Event()
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*_):pass
            def do_GET(self):
                try:
                    if mode=='trickle-header':
                        for byte in b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{}':
                            if stop.wait(.02):return
                            self.connection.sendall(bytes([byte]))
                        return
                    self.send_response(302 if mode=='redirect' else 200)
                    self.send_header('Content-Type','application/json')
                    if mode!='trickle-body':self.send_header('Content-Length','2' if mode!='oversized' else '999999')
                    self.end_headers()
                    if mode=='trickle-body':
                        while not stop.wait(.02):self.wfile.write(b' ');self.wfile.flush()
                    else:self.wfile.write(b'{}')
                except OSError:pass
        class Server(ThreadingHTTPServer):
            daemon_threads=True;block_on_close=False
            def handle_error(self,*_):pass
        server=Server(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.01});thread.start()
        original=socket.create_connection
        def connect(address,timeout):
            self.assertEqual(address,('127.0.0.1',13004))
            return original(('127.0.0.1',server.server_port),timeout=timeout)
        try:
            with patch('ops.coordinator_gate_bridge.socket.create_connection',side_effect=connect):
                return _main_http_json('/info',time.monotonic()+.15,limit)
        finally:stop.set();server.shutdown();server.server_close();thread.join(timeout=1)

    def test_actual_owned_loopback_json_and_no_redirect_or_oversize(self):
        self.assertEqual(self.request('valid'),{})
        for mode in ('redirect','oversized'):
            with self.subTest(mode=mode),self.assertRaises(BridgeError):self.request(mode)

    def test_absolute_deadline_covers_trickling_headers_and_body(self):
        for mode in ('trickle-header','trickle-body'):
            start=time.monotonic()
            with self.subTest(mode=mode),self.assertRaises(BridgeError):self.request(mode)
            self.assertLess(time.monotonic()-start,.8)

if __name__=='__main__':unittest.main()
