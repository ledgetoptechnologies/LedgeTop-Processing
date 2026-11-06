"""New private synthetic paths only; no host/service/engine/NAS installation."""
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tarfile
import tempfile
import unittest
import uuid
from unittest.mock import patch

from ops import enroll_reference_worker as install


def hashed(raw):return hashlib.sha256(raw).hexdigest()


@unittest.skipUnless(os.name=='posix' and os.geteuid()==0,'synthetic root-owned Linux installation')
class FirstWorkerEnrollmentTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.base=Path(self.temp.name);os.chown(self.base,0,10001);self.base.chmod(0o750)
        self.home=self.base/'home';self.home.mkdir(mode=0o700);os.chown(self.home,10001,10001)
        self.input=self.base/'inputs';self.input.mkdir(mode=0o700)
        self.parent=self.base/'opt';self.parent.mkdir(mode=0o750);os.chown(self.parent,0,10001)
        guard=patch.object(install,'INSTALL_BASE',self.parent);guard.start();self.addCleanup(guard.stop)
        self.operation=str(uuid.uuid4());self.root=self.parent/self.operation
        self.source_files={'processing/__init__.py':b'', 'processing/reference_driver.py':b'# reviewed synthetic driver\n',
                           'ops/reference_worker_daemon.py':b'# reviewed synthetic daemon\n',
                           'requirements.txt':b'cryptography==50.0.2\n'}
        code={name:hashed(raw) for name,raw in self.source_files.items() if name.endswith('.py')}
        footprint=hashed(json.dumps(code,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode())
        self.python=b'reviewed synthetic interpreter (never executed)\n'
        self.dependency_files={'bin/python':self.python,'pyvenv.cfg':b'include-system-site-packages = false\n'}
        self.manifest=dict(schemaVersion=1,protocol=install.PROTOCOL,operationId=self.operation,
            host=dict(hostname='synthetic',bootId=str(uuid.uuid4()),dockerEngineId='synthetic-engine'),
            service=dict(name='synthetic',uid=10001,gid=10001,home=str(self.home)),installationRoot=str(self.root),
            source=self.archive('source',self.source_files),dependencies=self.archive('dependencies',self.dependency_files),
            config={},unitSha256='0'*64,gate={},
            engine=dict(containerId='a'*64,imageId='sha256:'+'b'*64,version='3.8.5',loopbackPort=13004,catalogSha256='c'*64))
        self.manifest['source'].update(commit='d'*40,footprintSha256=footprint,
                                       requirementsSha256=hashed(self.source_files['requirements.txt']))
        self.manifest['dependencies'].update(pythonSha256=hashed(self.python),pythonVersion='3.13.17',
            versions=dict(cryptography='50.0.2',cffi='2.1.1',pycparser='3.12',rfc8785='0.1.4'))
        gate=self.home/'.local/state/LedgeTop-Processing/node-gates'/hashed(b'synthetic-engine')/'gate.json'
        gate.parent.mkdir(parents=True,mode=0o700)
        state=dict(schema=1,engineId='synthetic-engine',nodeId='main',authorityId=str(uuid.uuid4()),generation=1,
                   coordinatorOrigin='http://127.0.0.1:18080',reservation=None)
        raw=json.dumps(state).encode();gate.write_bytes(raw);gate.chmod(0o600);os.chown(gate,10001,10001)
        self.manifest['gate']=dict(path=str(gate),sha256=hashed(raw),**{k:state[k] for k in
            ('nodeId','authorityId','generation','coordinatorOrigin')})
        config=dict(nodeId='main',authorityId=state['authorityId'],engineId='synthetic-engine',gateGeneration=1,
            gateCoordinatorOrigin=state['coordinatorOrigin'],engineVersion='3.8.5',engineOrigin='http://127.0.0.1:13004',
            engineToken='synthetic-private-token-not-printed',
            maintenanceReadiness=dict(sourceRoot=str(self.root/'source'),sourceSha256=footprint,activation=None),
            stateRoot=str(self.home/'.local/share/ledgetop-processing-reference/worker-state'/self.operation),
            scratchRoot=str(self.home/'.local/share/ledgetop-processing-reference/worker-scratch'/self.operation))
        tls=self.home/'synthetic-tls';tls.mkdir(mode=0o700);os.chown(tls,10001,10001)
        for name in ('caFile','serverCertFile','serverKeyFile'):
            value=tls/name;value.write_bytes(('synthetic-'+name).encode());value.chmod(0o600);os.chown(value,10001,10001)
            config[name]=str(value)
        self.config_data=json.dumps(config).encode();path=self.input/'worker-config.json';path.write_bytes(self.config_data);path.chmod(0o600)
        self.manifest['config']=dict(path=str(path),sha256=hashed(self.config_data),bytes=len(self.config_data),ownerUid=0)
        self.manifest['unitSha256']=hashed(install.render_unit(self.manifest).encode())
        self.original_gate=raw
        testcase=self
        class Boundary:
            calls=[]
            def check_host(self,m):
                self.calls.append('host');install.require(os.geteuid()==0,'root_required')
                install.require(m['host']==testcase.manifest['host'],'original_host_changed')
            def local_filesystem(self,path):self.calls.append('local')
            def no_unit(self,m,*,owned=False):self.calls.append('unit-status')
            def engine(self,m,c):self.calls.append('engine-read')
            def ensure_user_directory(self,m,path):
                process=subprocess.Popen([sys.executable,'-I','-B','-'],stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,stderr=subprocess.PIPE,user=10001,group=10001,extra_groups=[],start_new_session=True)
                try:
                    raw,error=process.communicate(install.user_directory_script(testcase.home,path,10001),timeout=5)
                    testcase.assertEqual(process.returncode,0,error.decode());return json.loads(raw)
                finally:
                    from ops.enrolled_coordinator_transport import _terminate_owned
                    _terminate_owned(process)
            def validate_prepared(self,m,path):
                self.calls.append('validate-readonly')
                testcase.assertEqual(install.source_footprint(testcase.root/'source'),m['source']['footprintSha256'])
                for current in (testcase.root/'source',testcase.root/'venv'):
                    for directory,dirs,files in os.walk(current):
                        for leaf in (Path(directory),*(Path(directory)/x for x in files)):
                            info=leaf.stat();testcase.assertEqual(info.st_uid,0)
                            testcase.assertEqual(info.st_gid,10001);testcase.assertFalse(info.st_mode&0o222)
                testcase.assertEqual(hashed(path.read_bytes()),m['config']['sha256'])
        self.boundary=Boundary();self.installer=install.Installer(self.manifest,boundary=self.boundary)

    def archive(self,name,files,*,extra=None):
        buffer=io.BytesIO()
        with tarfile.open(fileobj=buffer,mode='w') as archive:
            for path,raw in files.items():
                member=tarfile.TarInfo(path);member.size=len(raw);member.mode=0o755 if path=='bin/python' else 0o644
                archive.addfile(member,io.BytesIO(raw))
            if extra is not None:archive.addfile(extra)
        raw=buffer.getvalue();path=self.input/(name+'.tar');path.write_bytes(raw);path.chmod(0o600)
        return dict(path=str(path),sha256=hashed(raw),bytes=len(raw),ownerUid=0)

    def test_prepare_inspect_enroll_exact_replays_are_install_only(self):
        prepared=self.installer.prepare();self.assertEqual(self.installer.prepare(),prepared)
        self.assertEqual(self.installer.inspect(),prepared)
        result=self.installer.enroll();before=(self.installer.config.stat().st_ino,self.installer.unit.stat().st_ino)
        self.assertEqual(self.installer.enroll(),result);self.assertEqual(self.installer.inspect(),result)
        self.assertEqual(before,(self.installer.config.stat().st_ino,self.installer.unit.stat().st_ino))
        self.assertFalse(result['serviceStartIssued']);self.assertFalse(result['serviceEnableIssued']);self.assertFalse(result['engineMutationIssued'])
        self.assertEqual(self.installer.config.read_bytes(),self.config_data)
        self.assertEqual(self.installer.unit.read_bytes(),install.render_unit(self.manifest).encode())
        self.assertEqual(Path(self.manifest['gate']['path']).read_bytes(),self.original_gate)
        self.assertFalse(any(call in self.boundary.calls for call in ('start','enable','stop','update')))

    def test_inspection_of_absent_installation_creates_nothing(self):
        self.assertEqual(self.installer.inspect()['state'],'absent');self.assertFalse(self.root.exists())

    def test_existing_worker_config_and_foreign_installation_are_never_adopted(self):
        self.installer.config.parent.mkdir(parents=True);self.installer.config.write_bytes(b'old-private-config')
        with self.assertRaises(Exception):self.installer.prepare()
        self.assertEqual(self.installer.config.read_bytes(),b'old-private-config')
        self.assertFalse((self.root/'source').exists())

    def test_preexisting_target_directory_without_owned_journal_is_held(self):
        self.root.mkdir(mode=0o750);os.chown(self.root,0,10001)
        with self.assertRaises(Exception):self.installer.prepare()
        self.assertEqual(list(self.root.iterdir()),[])

    def test_source_tamper_and_changed_manifest_are_refused_without_repair(self):
        self.installer.prepare();path=self.root/'source/processing/reference_driver.py';path.chmod(0o640)
        before=self.installer.journal.read_bytes()
        with self.assertRaises(Exception):self.installer.inspect()
        self.assertEqual(self.installer.journal.read_bytes(),before);self.assertEqual(stat.S_IMODE(path.stat().st_mode),0o640)
        altered=copy.deepcopy(self.manifest);altered['source']['footprintSha256']='e'*64
        with self.assertRaises(Exception):install.Installer(altered,boundary=self.boundary).prepare()

    def test_unsafe_tar_and_symlinked_dependency_do_not_escape_new_stage(self):
        old=self.manifest['source'];bad=self.archive('source',{'../outside':b'foreign'})
        self.manifest['source']=dict(old,**bad);installer=install.Installer(self.manifest,boundary=self.boundary)
        with self.assertRaises(Exception):installer.prepare()
        self.assertFalse((self.parent/'outside').exists());self.assertFalse(installer.config.exists());self.assertFalse(installer.unit.exists())

    def test_partial_unrecorded_copy_is_held_not_replaced_or_adopted(self):
        self.installer.prepare();original=self.installer._save
        def lost(state,**kwargs):
            if 'config' in state['steps']:raise OSError('synthetic lost file-effect checkpoint')
            return original(state,**kwargs)
        with patch.object(self.installer,'_save',side_effect=lost):
            with self.assertRaises(OSError):self.installer.enroll()
        inode=self.installer.config.stat().st_ino
        with self.assertRaises(Exception):self.installer.enroll()
        self.assertEqual(self.installer.config.stat().st_ino,inode);self.assertFalse(self.installer.unit.exists())

    def test_lost_durable_config_checkpoint_reply_recovers_without_replacing(self):
        self.installer.prepare();original=self.installer._save;failed=[False]
        def lost(state,**kwargs):
            result=original(state,**kwargs)
            if 'config' in state['steps'] and not failed[0]:failed[0]=True;raise OSError('synthetic reply lost')
            return result
        with patch.object(self.installer,'_save',side_effect=lost):
            with self.assertRaises(OSError):self.installer.enroll()
        inode=self.installer.config.stat().st_ino;result=self.installer.enroll()
        self.assertEqual(inode,self.installer.config.stat().st_ino);self.assertFalse(result['serviceStartIssued'])

    def test_original_gate_change_and_existing_worker_state_refuse(self):
        gate=Path(self.manifest['gate']['path']);gate.write_bytes(b'foreign')
        with self.assertRaises(Exception):self.installer.prepare()
        self.assertFalse(self.root.exists())

    def test_existing_state_root_is_not_reused(self):
        path=Path(json.loads(self.config_data)['stateRoot']);path.mkdir(parents=True)
        with self.assertRaises(Exception):self.installer.prepare()
        self.assertFalse((self.root/'source').exists())

    def test_actual_service_uid_can_read_but_cannot_write_source_or_dependencies(self):
        self.installer.prepare()
        script=('import json,os,pathlib\nroot=pathlib.Path('+repr(str(self.root))+')\n'
            'paths=[p for base in (root/"source",root/"venv") for p in [base,*base.rglob("*")]]\n'
            'print(json.dumps(dict(uid=os.geteuid(),allReadable=all(os.access(p,os.R_OK) for p in paths),'
            'anyWritable=any(os.access(p,os.W_OK) for p in paths))))\n')
        process=subprocess.Popen([sys.executable,'-I','-B','-c',script],stdout=subprocess.PIPE,stderr=subprocess.PIPE,
            user=10001,group=10001,extra_groups=[],start_new_session=True)
        try:
            out,err=process.communicate(timeout=5)
            self.assertEqual(process.returncode,0,err.decode())
            self.assertEqual(json.loads(out),dict(uid=10001,allReadable=True,anyWritable=False))
        finally:
            from ops.enrolled_coordinator_transport import _terminate_owned
            _terminate_owned(process)

    def test_dependency_symlink_and_foreign_source_namespace_are_refused(self):
        entry=tarfile.TarInfo('bin/foreign');entry.type=tarfile.SYMTYPE;entry.linkname='/outside'
        old=self.manifest['dependencies'];replacement=self.archive('dependencies',self.dependency_files,extra=entry)
        self.manifest['dependencies']=dict(old,**replacement)
        installer=install.Installer(self.manifest,boundary=self.boundary)
        with self.assertRaises(Exception):installer.prepare()
        self.assertFalse(installer.config.exists());self.assertFalse(installer.unit.exists())

    def test_readonly_dependency_validation_does_not_call_factory_or_service(self):
        boundary=install.ActualBoundary();observed=[]
        expected=dict(pythonVersion=self.manifest['dependencies']['pythonVersion'],
            versions=self.manifest['dependencies']['versions'],sourceSha256=self.manifest['source']['footprintSha256'],
            configSha256=self.manifest['config']['sha256'])
        def run(m,args,**options):observed.append((args,options));return json.dumps(expected).encode()
        boundary.as_user=run;boundary.validate_prepared(self.manifest,self.root/'worker-config.json')
        self.assertEqual(observed[0][0][-3:],['-I','-B','-'])
        body=observed[0][1]['input_bytes']
        self.assertIn(b'load_config(',body);self.assertIn(b'immutable_source_paths(',body)
        self.assertNotIn(b'build_worker(',body);self.assertNotIn(b'systemctl',body)

    def test_ancestor_swap_before_parent_open_writes_no_foreign_file(self):
        self.installer.prepare();foreign=self.base/'foreign';foreign.mkdir();(foreign/'ledgetop-processing').mkdir()
        original=self.installer._parent
        def swapped(path,state):
            proof=original(path,state)
            (self.home/'.config').rename(self.home/'original-config')
            (self.home/'.config').symlink_to(foreign,target_is_directory=True)
            return proof
        with patch.object(self.installer,'_parent',side_effect=swapped):
            with self.assertRaises(Exception):self.installer.enroll()
        self.assertEqual(list((foreign/'ledgetop-processing').iterdir()),[])
        self.assertFalse((self.home/'original-config/ledgetop-processing/reference-worker.json').exists())

    def test_ancestor_swap_after_pinned_parent_never_redirects_file_creation(self):
        self.installer.prepare();foreign=self.base/'foreign';foreign.mkdir();(foreign/'ledgetop-processing').mkdir()
        original=os.open;swapped=[False]
        def opened(path,*args,**kwargs):
            if path=='reference-worker.json' and kwargs.get('dir_fd') is not None and not swapped[0]:
                swapped[0]=True
                (self.home/'.config').rename(self.home/'original-config')
                (self.home/'.config').symlink_to(foreign,target_is_directory=True)
            return original(path,*args,**kwargs)
        with patch.object(install.os,'open',side_effect=opened):
            with self.assertRaises(Exception):self.installer.enroll()
        self.assertTrue(swapped[0]);self.assertEqual(list((foreign/'ledgetop-processing').iterdir()),[])
        self.assertTrue((self.home/'original-config/ledgetop-processing/reference-worker.json').exists())

    def test_launch_parity_and_unexpected_manager_overrides_are_explicit(self):
        unit=install.render_unit(self.manifest)
        self.assertIn('XDG_STATE_HOME='+str(self.home/'.local/state'),unit)
        self.assertIn('Environment=PYTHONNOUSERSITE=1 HOME=',unit)
        self.assertNotIn('*',unit)
        for name in install.UNSET_STARTUP_NAMES:self.assertIn(name,unit)
        boundary=install.ActualBoundary()
        boundary.as_user=lambda *args,**kwargs:b'HOME=/safe\nLD_AUDIT=/removed-by-unit\n'
        self.assertTrue(boundary.assert_clean_manager_environment(self.manifest)['unexpectedSourceOverridesAbsent'])
        for text in (b'LD_FUTURE_STARTUP=/unreviewed\n',b'PYTHON_FUTURE_STARTUP=unreviewed\n'):
            boundary.as_user=lambda *args,**kwargs:text
            with self.assertRaises(Exception):boundary.assert_clean_manager_environment(self.manifest)

    def test_system_site_packages_cannot_differ_from_isolated_preflight(self):
        files=dict(self.dependency_files,**{'pyvenv.cfg':b'include-system-site-packages = true\n'})
        artifact=self.archive('dependencies',files)
        self.manifest['dependencies'].update(artifact)
        installer=install.Installer(self.manifest,boundary=self.boundary)
        with self.assertRaises(Exception):installer.prepare()
        self.assertFalse(installer.config.exists());self.assertFalse(installer.unit.exists())


if __name__=='__main__':unittest.main()
