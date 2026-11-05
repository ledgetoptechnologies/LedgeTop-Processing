"""Actual exclusive local writer; no live host/container/NAS preparation."""
from contextlib import redirect_stdout
from contextlib import nullcontext
import copy
import hashlib
import io
import os
from pathlib import Path
import tempfile
import unittest
import uuid
from unittest.mock import patch

from ops import enrolled_config_stage as s
from ops import enrolled_coordinator_driver as d


class PreparationLifecycleTests(unittest.TestCase):
    """Real phase methods with synthetic Docker boundaries, no live effects."""
    def setUp(self):
        self.stage=s.ConfigPreparation.__new__(s.ConfigPreparation)
        operation=str(uuid.uuid4());self.identity='a'*64
        self.payloads={'bootstrap.json':b'bootstrap','final.json':b'final'}
        root='/private/config';directory='first-cutover-'+operation
        self.stage.plan=dict(operationId=operation,candidate={'imageId':'sha256:'+'b'*64},
                             predecessor={'containerId':'c'*64})
        self.stage.bundle=dict(sourceHashes={name:'d'*64 for name in s.SOURCES},artifacts={
            role+'Config':dict(path=root+'/'+directory+'/'+role+'.json',sha256=hashlib.sha256(self.payloads[role+'.json']).hexdigest())
            for role in ('bootstrap','final')})
        self.stage.name='synthetic-config-stage';self.commands=[];self.exists=False;self.record=None;self.fail=None
        from types import SimpleNamespace
        self.stage.lock=SimpleNamespace(locked=lambda plan:nullcontext())
        def write(record):self.record=copy.deepcopy(record)
        self.stage.receipt=SimpleNamespace(path=SimpleNamespace(exists=lambda:self.record is not None),
            locked=lambda:nullcontext(),write=write,read=lambda:copy.deepcopy(self.record))
        self.state=dict(Running=False,Pid=0,ExitCode=0,StartedAt='0001-01-01T00:00:00Z',
                        FinishedAt='0001-01-01T00:00:00Z',OOMKilled=False,Error='')
        def command(*args,**kwargs):
            self.commands.append(args)
            if args[:2]==('container','create'):self.exists=True
            elif args[:2]==('container','start'):
                self.state.update(StartedAt='2026-11-05T00:00:00Z',FinishedAt='2026-11-05T00:00:01Z')
            elif args[:2]==('container','rm'):self.exists=False
            if self.fail==args[1]:raise TimeoutError('synthetic lost reply after effect')
            return (self.identity+'\n').encode()
        self.reads=[]
        self.stage.docker=SimpleNamespace(command=command,matching=lambda filters:[self.identity] if self.exists else [],
            read_private_config_set=lambda *args:self.reads.append(args))
        self.stage._inputs=lambda *args:self.payloads
        self.stage._root_info=lambda:(root,[1,2,0o40700,10001,10001])
        self.stage._original=lambda:({'State':{'Running':False}},root)
        self.stage._owned=lambda record:dict(Id=self.identity,State=copy.deepcopy(self.state))

    def test_lost_create_is_inspected_without_another_create(self):
        self.fail='create'
        with self.assertRaises(TimeoutError):self.stage.create('bootstrap','final')
        with self.assertRaisesRegex(RuntimeError,'inspection_only'):self.stage.create('bootstrap','final')
        self.assertEqual(self.stage.inspect()['phase'],'created')
        self.assertEqual([command[1] for command in self.commands],['create'])

    def test_lost_start_recovers_by_readback_without_repeating_writer(self):
        self.stage.create('bootstrap','final');self.fail='start'
        with self.assertRaises(TimeoutError):self.stage.start('bootstrap','final')
        with self.assertRaisesRegex(RuntimeError,'inspection_only'):self.stage.start('bootstrap','final')
        self.assertEqual(self.stage.inspect()['phase'],'verified');self.assertEqual(len(self.reads),1)
        self.assertEqual([command[1] for command in self.commands],['create','start'])

    def test_never_started_helper_is_not_promoted_by_exact_readback(self):
        self.stage.create('bootstrap','final');self.record['phase']='start-intent'
        with self.assertRaisesRegex(RuntimeError,'not_verified_terminal'):self.stage.inspect()
        self.assertEqual(self.reads,[]);self.assertEqual(self.record['phase'],'start-intent')

    def test_nonzero_writer_retains_unknown_preparation_without_cleanup(self):
        self.stage.create('bootstrap','final');self.record['phase']='start-intent';self.state['ExitCode']=2
        with self.assertRaisesRegex(RuntimeError,'not_verified_terminal'):self.stage.inspect()
        with self.assertRaisesRegex(RuntimeError,'before_helper_cleanup'):self.stage.cleanup()
        self.assertTrue(self.exists);self.assertEqual(len(self.commands),1)

    def test_lost_cleanup_is_inspected_without_second_remove(self):
        self.stage.create('bootstrap','final');self.stage.start('bootstrap','final');self.fail='rm'
        with self.assertRaises(TimeoutError):self.stage.cleanup()
        self.assertEqual(self.stage.inspect()['phase'],'cleaned')
        self.assertEqual(self.stage.inspect()['phase'],'cleaned')
        self.assertEqual([command[1] for command in self.commands],['create','start','rm'])


class ExplicitEntrypointTests(unittest.TestCase):
    def test_staging_recipe_has_no_state_lease_network_or_private_payload_argv(self):
        stage=s.ConfigPreparation.__new__(s.ConfigPreparation)
        stage.plan=dict(operationId=str(uuid.uuid4()),candidate={'imageId':'sha256:'+'a'*64});stage.name='owned-stage'
        args=stage._args(dict(createIntent=str(uuid.uuid4()),configRoot='/private/certificates'))
        self.assertEqual(args[args.index('--network')+1],'none');self.assertIn('--init=false',args)
        self.assertEqual(args.count('--mount'),1)
        self.assertEqual(args[args.index('--mount')+1],'type=bind,src=/private/certificates,dst=/stage')
        self.assertNotIn('/prototype-state',' '.join(args));self.assertNotIn('synthetic-private-token',' '.join(args))
        self.assertEqual(args[-3:],['-I','-B','-'])
    def test_action_is_required_and_help_has_no_apply_default(self):
        with redirect_stdout(io.StringIO()) as output:
            with self.assertRaises(SystemExit) as result:d.main(['--help'])
        self.assertEqual(result.exception.code,0)
        self.assertIn('config-inspect',output.getvalue());self.assertIn('reconcile-rearm',output.getvalue())
        self.assertNotIn('--token',output.getvalue())

    def test_private_path_failure_is_sanitized_without_traceback_or_argv(self):
        with redirect_stdout(io.StringIO()) as output:
            code=d.main(['--plan','/private/synthetic-secret-key-name','--bundle','/private/bundle','--receipt','/private/receipt','--action','status'])
        self.assertEqual(code,2);self.assertNotIn('synthetic-secret',output.getvalue());self.assertNotIn('Traceback',output.getvalue())


@unittest.skipUnless(os.name=='posix','actual Linux private modes and UID10001 writer')
class ExclusiveWriterTests(unittest.TestCase):
    def setUp(self):
        self.assertEqual(os.geteuid(),10001)
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name);self.root.chmod(0o700)
        self.old=self.root/'existing.json';self.old.write_bytes(b'old-private-configuration');self.old.chmod(0o440)
        self.before=(self.old.read_bytes(),self.old.stat().st_mode,self.old.stat().st_uid,self.old.stat().st_gid,self.old.stat().st_ino)
        self.payloads={'bootstrap.json':b'{"synthetic":"bootstrap"}','final.json':b'{"synthetic":"final"}'}
        self.settings=dict(schemaVersion=1,operationId=str(uuid.uuid4()),planSha256='a'*64,imageId='sha256:'+'b'*64,
            sourceHashes={name:'c'*64 for name in s.SOURCES},
            rootIdentity=s.identity(self.root.stat()),hashes={name:hashlib.sha256(raw).hexdigest() for name,raw in self.payloads.items()})
        self.operation=self.root/('first-cutover-'+self.settings['operationId'])

    def unchanged_old(self):
        self.assertEqual((self.old.read_bytes(),self.old.stat().st_mode,self.old.stat().st_uid,self.old.stat().st_gid,self.old.stat().st_ino),self.before)

    def test_exact_new_private_files_and_fsync_readback_without_old_permission_changes(self):
        proof=s.write_new(self.settings,self.payloads,root=str(self.root))
        self.assertEqual(proof['status'],'staged');self.assertEqual(self.operation.stat().st_mode&0o777,0o700)
        for name,raw in self.payloads.items():
            path=self.operation/name
            self.assertEqual(path.read_bytes(),raw);self.assertEqual(path.stat().st_mode&0o777,0o600)
            self.assertEqual((path.stat().st_uid,path.stat().st_gid),(10001,10001))
        self.unchanged_old()
        with self.assertRaises(FileExistsError):s.write_new(self.settings,self.payloads,root=str(self.root))
        self.unchanged_old()

    def test_preexisting_symlink_operation_is_never_followed_or_overwritten(self):
        outside=self.root/'outside';outside.mkdir();self.operation.symlink_to(outside,target_is_directory=True)
        with self.assertRaises(FileExistsError):s.write_new(self.settings,self.payloads,root=str(self.root))
        self.assertEqual(list(outside.iterdir()),[]);self.unchanged_old()

    def test_fsync_failure_retains_partial_new_files_and_never_retries_or_promotes(self):
        with patch.object(s.os,'fsync',side_effect=OSError('synthetic durability failure')) as fault:
            with self.assertRaises(OSError):s.write_new(self.settings,self.payloads,root=str(self.root))
        self.assertEqual(fault.call_count,1);self.assertTrue((self.operation/'bootstrap.json').exists())
        self.assertFalse((self.operation/'final.json').exists());self.unchanged_old()
        with self.assertRaises(FileExistsError):s.write_new(self.settings,self.payloads,root=str(self.root))

    def test_unknown_directory_close_is_not_blindly_retried(self):
        original=s.close_verified;closed=[];system_close=os.close;attempts=[]
        def traced(fd):attempts.append(fd);return system_close(fd)
        def close(fd):
            mode=os.fstat(fd).st_mode;closed.append((fd,mode))
            original(fd)
            import stat
            if stat.S_ISDIR(mode):raise OSError('synthetic close reported error after actual close')
        with patch.object(s,'close_verified',side_effect=close),patch.object(s.os,'close',side_effect=traced):
            with self.assertRaises(OSError):s.write_new(self.settings,self.payloads,root=str(self.root))
        import stat
        directories=[fd for fd,mode in closed if stat.S_ISDIR(mode)]
        self.assertEqual(len(directories),1);self.assertEqual(attempts.count(directories[0]),1);self.unchanged_old()


if __name__=='__main__':unittest.main()
