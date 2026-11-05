"""Systemd transport is synthetic; unit/process validation executes real reads."""
import hashlib
import os
from pathlib import Path
import shlex
import sys
import tempfile
from types import SimpleNamespace
import unittest

from ops.reference_worker_handover import SystemdWorkerControl,WorkerHandoverHeld


@unittest.skipUnless(sys.platform.startswith('linux'),'Linux proc/unit contract')
class WorkerServiceControlTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.source=self.root/'source';self.source.mkdir()
        self.unit=self.root/'worker.service';self.config=self.root/'worker.json';self.config.write_text('{}')
        self.raw=('''[Service]
Type=simple
WorkingDirectory=%s
ExecStart=%s -B -m ops.reference_worker_daemon --config %s
KillMode=mixed
SendSIGKILL=no
UMask=0077
NoNewPrivileges=yes
'''%(self.source,shlex.quote(sys.executable),shlex.quote(str(self.config))))
        self.unit.write_text(self.raw);self.unit.chmod(0o600)
        self.settings=dict(unitName='worker.service',unitFile=str(self.unit),unitSha256=hashlib.sha256(self.unit.read_bytes()).hexdigest(),
            configPath=str(self.config),sourceRoot=str(self.source),pythonExecutable=sys.executable)
        self.calls=[];self.pid=0
        self.loaded=dict(FragmentPath=str(self.unit),DropInPaths='',NeedDaemonReload='no',Type='simple',
            KillMode='mixed',KillSignal='15',SendSIGKILL='no',UMask='0077',NoNewPrivileges='yes',
            ExecStartPre='',ExecStartPost='',ExecStop='',ExecStopPost='',ExecReload='',FailureAction='none',SuccessAction='none',
            ExecStart='{ path=%s ; argv[]=%s -B -m ops.reference_worker_daemon --config %s ; ignore_errors=no ; start_time=[n/a] ; stop_time=[n/a] ; pid=0 ; code=(null) ; status=0/0 }'%(
                sys.executable,shlex.quote(sys.executable),shlex.quote(str(self.config))))
        def runner(command,**kwargs):
            self.calls.append(command)
            if any('DropInPaths' in arg for arg in command):
                return SimpleNamespace(returncode=0,stdout='\n'.join(key+'='+value for key,value in self.loaded.items())+'\n')
            return SimpleNamespace(returncode=0,stdout='MainPID=%s\nActiveState=%s\nSubState=dead\nControlGroup=\nInvocationID=\nFragmentPath=%s\n'%(
                self.pid,'active' if self.pid else 'inactive',self.unit))
        self.runner=runner

    def test_commands_are_fixed_user_unit_actions_without_force_kill(self):
        control=SystemdWorkerControl(self.settings,runner=self.runner)
        control.command('stop');control.command('start')
        self.assertEqual([call for call in self.calls if '--no-block' in call],[['/usr/bin/systemctl','--user','--no-block','stop','worker.service'],
            ['/usr/bin/systemctl','--user','--no-block','start','worker.service']])
        with self.assertRaises(WorkerHandoverHeld):control.command('kill')

    def test_effective_loaded_overrides_refuse_stop_or_start_before_side_effect(self):
        for key,value in (('DropInPaths','/tmp/unreviewed.conf'),('NeedDaemonReload','yes'),('SendSIGKILL','yes'),
                ('KillSignal','9'),('KillMode','control-group'),('UMask','0022'),('NoNewPrivileges','no'),
                ('ExecStartPre','{ path=/tmp/hook ; }'),('ExecStop','{ path=/tmp/stop ; }')):
            with self.subTest(key=key):
                original=self.loaded[key];self.loaded[key]=value;self.calls.clear()
                with self.assertRaises(WorkerHandoverHeld):SystemdWorkerControl(self.settings,runner=self.runner).command('stop')
                self.assertFalse(any('--no-block' in call for call in self.calls))
                self.loaded[key]=original

    def test_effective_command_is_checked_before_start_not_only_after_process_exists(self):
        self.loaded['ExecStart']=self.loaded['ExecStart'].replace(' -B -m ',' -I -m ')
        with self.assertRaisesRegex(WorkerHandoverHeld,'loaded_worker_command_changed'):
            SystemdWorkerControl(self.settings,runner=self.runner).command('start')
        self.assertFalse(any('--no-block' in call for call in self.calls))

    def test_loaded_command_private_path_quoting_round_trips_exact_argv(self):
        old=self.config;self.config=self.root/"worker's config.json"
        self.settings['configPath']=str(self.config)
        self.unit.write_text(self.raw.replace(shlex.quote(str(old)),shlex.quote(str(self.config))))
        self.settings['unitSha256']=hashlib.sha256(self.unit.read_bytes()).hexdigest()
        self.loaded['ExecStart']=self.loaded['ExecStart'].replace(shlex.quote(str(old)),shlex.quote(str(self.config)))
        SystemdWorkerControl(self.settings,runner=self.runner).command('start')
        self.assertTrue(any('--no-block' in call for call in self.calls))

    def test_unsupported_escaped_or_multiple_loaded_commands_hold_before_start(self):
        original=self.loaded['ExecStart']
        cases=(original.replace(str(self.config),str(self.config)+r'\x20extra'),
            original+' '+original,original.replace('ignore_errors=no','ignore_errors=yes'))
        for value in cases:
            with self.subTest(commandEncoding=value):
                self.loaded['ExecStart']=value;self.calls.clear()
                with self.assertRaises(WorkerHandoverHeld):SystemdWorkerControl(self.settings,runner=self.runner).command('start')
                self.assertFalse(any('--no-block' in call for call in self.calls))

    def test_reviewed_hash_does_not_override_no_forced_kill_policy(self):
        self.unit.write_text(self.raw+'KillSignal=SIGKILL\n')
        self.settings['unitSha256']=hashlib.sha256(self.unit.read_bytes()).hexdigest()
        with self.assertRaises(WorkerHandoverHeld):SystemdWorkerControl(self.settings,runner=self.runner)
        self.assertEqual(self.calls,[])

    def test_systemd_active_does_not_substitute_for_actual_process_identity(self):
        self.pid=os.getpid()
        control=SystemdWorkerControl(self.settings,runner=self.runner)
        with self.assertRaisesRegex(WorkerHandoverHeld,'worker_process_identity_changed'):control.probe()

    def test_missing_cgroup_does_not_authorize_a_still_live_old_process(self):
        control=SystemdWorkerControl(self.settings,runner=self.runner)
        start=(Path('/proc')/str(os.getpid())/'stat').read_text().rsplit(')',1)[1].split()[19]
        self.assertFalse(control.stopped(dict(pid=os.getpid(),startTime=start,cgroup='/synthetic-absent-cgroup')))


if __name__=='__main__':unittest.main()
