"""Actual serialized helper/nonroot child; synthetic external transports only.

Requires an isolated Linux root runner with private tmpfs /opt and /tmp. Copies
the real reviewed Python modules/dependency distributions; never substitutes fake
production modules or disables the source/tree/descriptor/Gate guards. No SSH,
systemd, Docker socket, native engine, host credential or service is contacted.
"""
import copy
import hashlib
import importlib.metadata
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
import uuid

from ops import enroll_reference_worker as install
from ops import first_worker_activation_service as service
from ops.enrolled_coordinator_transport import canonical,digest,_terminate_owned
from ops.node_ingress import configuration


UID=GID=10001


@unittest.skipUnless(sys.platform.startswith('linux') and os.geteuid()==0,
                     'root-owned isolated Linux filesystem; child drops to service UID')
class SerializedServiceBodyTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue(os.path.ismount('/opt'),'qualification requires task-owned tmpfs /opt')
        self.assertFalse(os.statvfs('/opt').f_flag & os.ST_NOEXEC,
                         'copied interpreter requires explicit exec on task-owned tmpfs /opt')
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.base=Path(self.temp.name);self.base.chmod(0o750);os.chown(self.base,0,GID)
        self.home=self.base/'service-home';self.home.mkdir(mode=0o700);os.chown(self.home,UID,GID)
        self.mark=self.home/'external-calls.jsonl'
        self.operation=str(uuid.uuid4());self.root=Path('/opt/ledgetop-processing-reference')/self.operation
        self.root.parent.mkdir(mode=0o755,exist_ok=True);self.root.mkdir(mode=0o750);os.chown(self.root,0,GID)
        self.addCleanup(shutil.rmtree,self.root)
        self.source=self.root/'source';self.source.mkdir(mode=0o750)
        reviewed=Path(__file__).resolve().parents[1]
        for package in ('processing','ops'):
            for path in sorted((reviewed/package).rglob('*.py')):
                self.assertFalse(path.is_symlink())
                target=self.source/path.relative_to(reviewed);target.parent.mkdir(parents=True,exist_ok=True)
                shutil.copyfile(path,target)
        self.venv=self.root/'venv';(self.venv/'bin').mkdir(parents=True)
        shutil.copyfile(Path(sys.executable).resolve(),self.venv/'bin/python');(self.venv/'bin/python').chmod(0o550)
        lib=self.venv/'lib'/('python'+str(sys.version_info.major)+'.'+str(sys.version_info.minor))/'site-packages'
        lib.mkdir(parents=True)
        for name in ('cryptography','cffi','pycparser','rfc8785'):
            spec=importlib.util.find_spec(name);self.assertIsNotNone(spec)
            package=Path(spec.origin).parent
            shutil.copytree(package,lib/package.name,ignore=shutil.ignore_patterns('__pycache__','*.pyc'))
            dist=Path(importlib.metadata.distribution(name)._path)
            shutil.copytree(dist,lib/dist.name)
        backend=Path(importlib.util.find_spec('_cffi_backend').origin);shutil.copyfile(backend,lib/backend.name)
        (self.venv/'pyvenv.cfg').write_text('home = '+str(Path(sys.executable).resolve().parent)+
            '\ninclude-system-site-packages = false\nversion = '+sys.version.split()[0]+'\n')
        self.seal(self.source);self.seal(self.venv)
        self.authority=str(uuid.uuid4());self.engine='serialized-fixture-engine'
        self.gate=self.home/'.local/state/LedgeTop-Processing/node-gates'/hashlib.sha256(self.engine.encode()).hexdigest()/'gate.json'
        self.user_parent(self.gate.parent)
        state=dict(schema=1,engineId=self.engine,nodeId='main',authorityId=self.authority,generation=1,
                   coordinatorOrigin='http://127.0.0.1:18080',reservation=None)
        self.private(self.gate,canonical(state));self.private(self.gate.parent/'gate.lock',b'')
        self.config_path=self.home/'.config/ledgetop-processing/reference-worker.json'
        self.unit_path=self.home/'.config/systemd/user'/install.UNIT
        self.user_parent(self.config_path.parent);self.user_parent(self.unit_path.parent)
        tls=self.home/'tls';self.user_parent(tls)
        for name in ('ca.crt','worker.crt','worker.key'):self.private(tls/name,b'private synthetic TLS fixture; never parsed as a live certificate')
        footprint=install.source_footprint(self.source)
        self.manifest=dict(schemaVersion=1,protocol=install.PROTOCOL,operationId=self.operation,
            host=dict(hostname='serialized-fixture',bootId=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                      dockerEngineId=self.engine),service=dict(name='fixture-user',uid=UID,gid=GID,home=str(self.home)),
            installationRoot=str(self.root),source=dict(path=str(self.base/'unconsumed-source.tar'),sha256='a'*64,bytes=1,
                ownerUid=UID,commit='b'*40,footprintSha256=footprint,requirementsSha256='c'*64),
            dependencies=dict(path=str(self.base/'unconsumed-dependencies.tar'),sha256='d'*64,bytes=1,ownerUid=UID,
                pythonSha256=hashlib.sha256((self.venv/'bin/python').read_bytes()).hexdigest(),pythonVersion=sys.version.split()[0],
                versions=dict(cryptography='50.0.2',cffi='2.1.1',rfc8785='0.1.4',
                    pycparser='2.23' if sys.version_info[:2]==(3,10) else '3.12')),
            config=dict(path=str(self.base/'unconsumed-config.json'),sha256='0'*64,bytes=1,ownerUid=UID),unitSha256='0'*64,
            gate=dict(path=str(self.gate),sha256=hashlib.sha256(self.gate.read_bytes()).hexdigest(),nodeId='main',
                authorityId=self.authority,generation=1,coordinatorOrigin=state['coordinatorOrigin']),
            engine=dict(containerId='e'*64,imageId='sha256:'+'f'*64,version='3.8.5',loopbackPort=13004,catalogSha256='0'*64))
        self.options=[dict(name=name,type='bool',value=False) for name in ('pc-ept','gltf','3d-tiles')]
        from processing.options import _sha,_validate_options
        self.manifest['engine']['catalogSha256']=_sha(sorted(_validate_options(self.options),key=lambda x:x['name']))
        self.config=dict(schemaVersion=1,nodeId='main',workerId='fixture-worker',workerInstanceId='fixture-instance',
            authorityId=self.authority,engineId=self.engine,gateGeneration=1,gateCoordinatorOrigin=state['coordinatorOrigin'],
            engineOrigin='http://127.0.0.1:13004',engineToken='synthetic-private-native-token',engineVersion='3.8.5',
            caFile=str(tls/'ca.crt'),serverCertFile=str(tls/'worker.crt'),serverKeyFile=str(tls/'worker.key'),
            stateRoot=str(self.home/'.local/share/ledgetop-processing-reference/worker-state'/self.operation),
            scratchRoot=str(self.home/'.local/share/ledgetop-processing-reference/worker-scratch'/self.operation),
            maintenanceReadiness=dict(schemaVersion=1,bearer='synthetic-readiness-'+'r'*40,sourceRoot=str(self.source),
                sourceSha256=footprint,activation=None))
        raw=canonical(self.config);self.manifest['config'].update(sha256=hashlib.sha256(raw).hexdigest(),bytes=len(raw))
        self.manifest['unitSha256']=hashlib.sha256(install.render_unit(self.manifest).encode()).hexdigest()
        self.private(self.config_path,raw);self.private(self.root/'worker-config.json',raw)
        self.private(self.unit_path,install.render_unit(self.manifest).encode())
        install.validate_manifest(self.manifest)
        settings=dict(schemaVersion=1,unitName=install.UNIT,unitFile=str(self.unit_path),unitSha256=self.manifest['unitSha256'],
            pythonExecutable=str(self.venv/'bin/python'),configPath=str(self.config_path),sourceRoot=str(self.source),
            sourceSha256=footprint,readinessOrigin='https://127.0.0.1:18445',readinessCAFile=str(tls/'ca.crt'))
        self.worker=dict(nodeId='main',sshHost='192.0.2.88',sshUid=UID,settings=settings,
            expected=dict(nodeId='main',workerId='fixture-worker',workerInstanceId='fixture-instance',authorityId=self.authority,
                engineId=self.engine,gateGeneration=1,engineVersion='3.8.5',configSha256=self.manifest['config']['sha256'],
                sourceSha256=footprint,activation=None))
        self.ingress_root=self.home/'ingress';self.user_parent(self.ingress_root/'public-config')
        gateway=dict(proxyName='fixture-proxy',proxyImage='sha256:'+'1'*64,engineId=self.engine,backendName='fixture-native',
                     listenPort=3004,backendPort=13004,backendEngine='odx',allowedClients=['192.0.2.89'],backendToken='')
        self.private(self.ingress_root/'gateway.json',canonical(gateway))
        self.private(self.ingress_root/'state.json',canonical(dict(state='closed',containerId='2'*64,oldWorkers=[99],
                                                              expectedBackendImage=self.manifest['engine']['imageId'])))
        self.private(self.ingress_root/'ingress.lock',b'')
        self.private(self.ingress_root/'public-config/nginx.conf',configuration(3004,13004,True,['192.0.2.89']).encode(),mode=0o644)
        self.ingress=dict(root=str(self.ingress_root),configSha256=self.file_hash(self.ingress_root/'gateway.json'),
            stateSha256=self.file_hash(self.ingress_root/'state.json'),closedConfigSha256=self.file_hash(self.ingress_root/'public-config/nginx.conf'),
            containerId='2'*64)
        self.native=dict(schemaVersion=1,protocol='ltds-retained-native-quarantine-v1',nodeId='main',authorityId=self.authority,
            engineId=self.engine,containerId='e'*64,imageId='sha256:'+'f'*64,tasks=[])
        self.tree=install.Installer(self.manifest).observe_installed_tree()

    @staticmethod
    def file_hash(path):return hashlib.sha256(path.read_bytes()).hexdigest()

    @staticmethod
    def seal(root):
        for p in [*root.rglob('*'),root]:
            if p.is_symlink():raise AssertionError('fixture cannot adopt dependency symlinks')
            os.chown(p,0,GID);p.chmod(0o550 if p.is_dir() or p.stat().st_mode&0o111 else 0o440)

    def user_parent(self,path):
        pending=[]
        while not path.exists():pending.append(path);path=path.parent
        for p in reversed(pending):p.mkdir(mode=0o700);os.chown(p,UID,GID)

    @staticmethod
    def private(path,raw,mode=0o600):
        path.write_bytes(raw);os.chown(path,UID,GID);path.chmod(mode)

    def prefix(self):
        # Test-owned bootstrap imports ONLY the actual copied RO modules. It
        # patches external transports; actual serialized source guard runs
        # unchanged immediately afterward. No fake package/modules are supplied.
        return ('''import sys,os,json,subprocess,pwd,socket
from pathlib import Path
from types import SimpleNamespace
sys.dont_write_bytecode=True
sys.path.insert(0,'''+repr(str(self.source))+''')
from ops.enrolled_coordinator_transport import BoundedSubprocess
from ops.node_ingress import Ingress
from processing.viewer_service import NativeEngine
manifest='''+repr(self.manifest)+''';worker='''+repr(self.worker)+''';options='''+repr(self.options)+'''
marker=Path('''+repr(str(self.mark))+''')
def called(kind):
 with marker.open('a') as f:f.write(json.dumps(dict(kind=kind,uid=os.geteuid()))+'\\n')
def forbidden(*a,**k):raise AssertionError('real external effect forbidden')
subprocess.Popen=forbidden
socket.socket.connect=forbidden
pwd.getpwnam=lambda name:SimpleNamespace(pw_uid=10001,pw_gid=10001,pw_dir=manifest['service']['home'])
def fake_run(self,args,**kw):
 called('command')
 if args==['/usr/bin/hostname']:return b'serialized-fixture\\n'
 if args[:2]==['/usr/bin/systemctl','--user']:
  if args[2:] in (['show-environment'],['daemon-reload']):return b''
  if args[2]!='show':raise AssertionError('unreviewed service action')
  prop=next(x.split('=',1)[1] for x in args if x.startswith('--property='))
  if prop=='LoadState,ActiveState,MainPID,UnitFileState':return b'LoadState=loaded\\nActiveState=inactive\\nMainPID=0\\nUnitFileState=disabled\\n'
  if prop=='UnitFileState':return b'UnitFileState=disabled\\n'
  if prop=='Environment,UnsetEnvironment':
   from ops.enroll_reference_worker import UNSET_STARTUP_NAMES
   return ('Environment=HOME='+manifest['service']['home']+' XDG_STATE_HOME='+manifest['service']['home']+'/.local/state PYTHONNOUSERSITE=1\\nUnsetEnvironment='+' '.join(UNSET_STARTUP_NAMES)+'\\n').encode()
  if prop=='MainPID,ActiveState,SubState,ControlGroup,InvocationID,FragmentPath':return ('MainPID=0\\nActiveState=inactive\\nSubState=dead\\nControlGroup=\\nInvocationID=\\nFragmentPath='+worker['settings']['unitFile']+'\\n').encode()
  names=prop.split(',');python=worker['settings']['pythonExecutable'];config=worker['settings']['configPath']
  values=dict(FragmentPath=worker['settings']['unitFile'],DropInPaths='',NeedDaemonReload='no',Type='simple',KillMode='mixed',KillSignal='15',SendSIGKILL='no',UMask='0077',NoNewPrivileges='yes',ExecStartPre='',ExecStartPost='',ExecStop='',ExecStopPost='',ExecReload='',FailureAction='none',SuccessAction='none',ExecStart='{ path='+python+' ; argv[]='+python+' -B -m ops.reference_worker_daemon --config '+config+' ; ignore_errors=no ; start_time= ; stop_time= ; pid=0 ; code= ; status=0 }')
  return ''.join(name+'='+values[name]+'\\n' for name in names).encode()
 if args[:3]!=['/usr/bin/docker','--host','unix:///run/docker.sock']:raise AssertionError('unreviewed external command')
 a=args[3:]
 if a[:1]==['info']:return (manifest['host']['dockerEngineId']+'\\n').encode()
 if a[:2]==['inspect','--format']:
  if a[2]=='{{json .Id}}':return json.dumps(manifest['engine']['containerId']).encode()
  return json.dumps(dict(id=manifest['engine']['containerId'],image=manifest['engine']['imageId'],running=True,ports={'3000/tcp':[dict(HostIp='127.0.0.1',HostPort='13004')]})).encode()
 if a[:1]==['inspect']:
  proxy=dict(Id='2'*64,Image='sha256:'+'1'*64,State=dict(Running=True,Pid=101),HostConfig=dict(NetworkMode='host'),Config=dict(Entrypoint=['nginx'],Cmd=['-c','/config/nginx.conf','-g','daemon off;']),Mounts=[dict(Destination='/config',Type='bind',RW=False,Source='''+repr(str(self.ingress_root/'public-config'))+''')])
  return json.dumps([proxy]).encode()
 if a[:1]==['top']:
  return b'PID COMMAND\\n101 nginx\\n102 nginx\\n' if a[1]=='2'*64 else b'PID COMMAND\\n201 node /var/www/index.js\\n'
 if a[:1]==['exec'] and a[2:]==['/bin/ls','-A','/var/www/tmp']:return b''
 raise AssertionError('unreviewed Docker command')
def native(self,route,**kw):
 called('native-read')
 if route=='/info':return dict(engineVersion='3.8.5',taskQueueCount=0)
 if route=='/options':return options
 if route=='/task/list':return []
 raise AssertionError('unreviewed native route')
def http(self,port):called('ingress-http');return 503,None
BoundedSubprocess.run=fake_run
NativeEngine.read=native
Ingress.http=http
''').encode()

    def run_body(self,body):
        env={k:v for k,v in os.environ.items() if not k.startswith(('PYTHON','LD_'))}
        env.update(HOME=str(self.home),PYTHONDONTWRITEBYTECODE='1',PYTHONNOUSERSITE='1')
        process=subprocess.Popen([str(self.venv/'bin/python'),'-I','-B','-'],stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,stderr=subprocess.PIPE,user=UID,group=GID,extra_groups=[],start_new_session=True,
            env=env,cwd=str(self.source))
        try:
            output,error=process.communicate(self.prefix()+body,timeout=30)
            result=process.returncode
        finally:_terminate_owned(process)
        self.assertIsNotNone(process.returncode,'exact task-owned child retired')
        return result,output,error

    def body(self,action='observe'):
        challenge=str(uuid.uuid4())
        return service.serialize_service(self.worker,self.manifest,self.ingress,self.native,self.tree,action,challenge),challenge

    def test_real_serialized_body_observe_and_reload_reply_without_host_effects(self):
        for action in ('observe','reload'):
            with self.subTest(action=action):
                body,nonce=self.body(action);code,out,error=self.run_body(body)
                self.assertEqual(code,0,error.decode());value=json.loads(out)
                self.assertEqual((value['challenge'],value['action'],value['workerSha256']),(nonce,action,digest(self.worker)))
                self.assertEqual(value['manifestSha256'],digest(self.manifest));self.assertEqual(value['native']['taskCount'],0)
                self.assertEqual(value['process']['pid'],0)
        calls=[json.loads(line) for line in self.mark.read_text().splitlines()]
        self.assertTrue(calls);self.assertTrue(all(row['uid']==UID for row in calls))
        self.assertFalse(Path(self.config['stateRoot']).exists());self.assertFalse(Path(self.config['scratchRoot']).exists())

    def test_mutated_installed_source_fails_before_external_command(self):
        body,_=self.body();path=self.source/'processing/reference_driver.py'
        path.chmod(0o640);path.write_bytes(path.read_bytes()+b'\n# synthetic mutation\n');path.chmod(0o440)
        code,out,error=self.run_body(body)
        self.assertNotEqual(code,0);self.assertEqual(out,b'');self.assertFalse(self.mark.exists())
        self.assertIn(b'activation_readonly_source_changed',error)

    def test_invalid_serialized_manifest_fails_before_external_command(self):
        body,_=self.body();bad=copy.deepcopy(self.manifest);bad['gate']['generation']=True
        # Production serializer canonicalizes map ordering for exact replay.
        original=repr(json.loads(canonical(self.manifest))).encode();self.assertEqual(body.count(original),1)
        body=body.replace(original,repr(json.loads(canonical(bad))).encode(),1)
        code,out,error=self.run_body(body)
        self.assertNotEqual(code,0);self.assertEqual(out,b'');self.assertFalse(self.mark.exists())
        self.assertIn(b'bounded_enrollment_integer_required',error)


if __name__=='__main__':unittest.main()
