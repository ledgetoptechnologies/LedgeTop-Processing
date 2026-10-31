import os
from pathlib import Path
import signal
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from ops.authority_handoff import DockerProcessAttestor
from ops.outer_authority_guard import OuterAuthorityGuard,OuterHeld
from ops.pr197_operation_fence import OperationFence
from processing.authority_lease import AuthorityLease,LeaseUnavailable,acquire
from processing.prototype import Queue


@unittest.skipUnless(os.name=='posix' and hasattr(os,'fork') and hasattr(os,'pidfd_open'),'Linux process fixture required')
class OuterAuthorityTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
        self.old=self.root/'old';self.old.mkdir(mode=0o700);(self.old/'coordinator.lock').touch(mode=0o600)
        self.leases=self.root/'leases';self.leases.mkdir(mode=0o700)
        self.outer=self.root/'outer';self.outer.mkdir(mode=0o700)
        self.authority='12345678-1234-1234-1234-123456789abc';self.image='sha256:'+'a'*64
        (self.leases/(self.authority+'.lease')).touch(mode=0o600)
        (self.outer/(self.authority+'.outer-lock')).touch(mode=0o600)
        self.candidate=self.root/'candidate';self.candidate.mkdir(mode=0o700)
        q=Queue(self.candidate,node_ids=('cluster','main'),viewer_enabled=True)
        with q._db() as db:db.execute('UPDATE coordinator_identity SET authority_id=? WHERE singleton=1',(self.authority,))
        q.close()
        self.mount=patch('ops.pr197_stopped_census._mount');self.mount.start();self.addCleanup(self.mount.stop)

    def test_candidate_owns_independent_inner_and_state_locks(self):
        attestor=DockerProcessAttestor('b'*64,self.image,self.root)
        ready_r,ready_w=os.pipe();release_r,release_w=os.pipe()
        with OuterAuthorityGuard(self.outer,self.authority,enabled=True) as outer, \
             OperationFence(self.old,self.leases,self.authority,enabled=True) as fence:
            result=outer.prepare_candidate(fence,self.candidate,self.image,operation_id='c'*32)
            self.assertEqual(result['state'],'candidate_start_not_authorized')
            child=os.fork()
            if child==0:
                signal.alarm(10);os.close(ready_r);os.close(release_w)
                try:
                    os.close(outer.outer);os.close(fence.fds[1])
                    lease=acquire(self.leases,self.candidate)
                    queue=Queue(self.candidate,node_ids=('cluster','main'),viewer_enabled=True)
                    os.write(ready_w,b'1');os.read(release_r,1);queue.close();lease.close();os._exit(0)
                except BaseException:os._exit(2)
            os.close(ready_w);os.close(release_r);reaped=False
            try:
                self.assertEqual(os.read(ready_r,1),b'1')
                with patch.object(attestor,'inspect',return_value=123):
                    observed=outer.observe_candidate(child,attestor,self.candidate)
                self.assertTrue(observed['candidateLocksObserved']);self.assertFalse(observed['admissionAuthorized'])
                outer.pending.write_bytes(b'synthetic-tamper')
                with patch.object(attestor,'inspect',return_value=123):
                    with self.assertRaisesRegex(OuterHeld,'outer_marker_changed'):
                        outer.observe_candidate(child,attestor,self.candidate)
                with self.assertRaises(LeaseUnavailable):AuthorityLease(self.leases,self.authority)
                os.write(release_w,b'1');_,status=os.waitpid(child,0);reaped=True
                self.assertEqual(os.waitstatus_to_exitcode(status),0)
            finally:
                os.close(ready_r);os.close(release_w)
                if not reaped:
                    try:os.kill(child,signal.SIGKILL)
                    except ProcessLookupError:pass
                    os.waitpid(child,0)
        with self.assertRaisesRegex(OuterHeld,'outer_handoff_requires_reconciliation'):
            with OuterAuthorityGuard(self.outer,self.authority,enabled=True):pass

    def test_failure_after_marker_never_automatically_reopens(self):
        with OuterAuthorityGuard(self.outer,self.authority,enabled=True) as outer, \
             OperationFence(self.old,self.leases,self.authority,enabled=True) as fence:
            outer.prepare_candidate(fence,self.candidate,self.image,operation_id='c'*32)
        self.assertTrue((self.outer/(self.authority+'.outer-handoff')).exists())
        with self.assertRaises(OuterHeld):
            with OuterAuthorityGuard(self.outer,self.authority,enabled=True):pass

    def test_supervisor_death_leaves_marker_but_legacy_bypass_remains(self):
        child=os.fork()
        if child==0:
            signal.alarm(10)
            try:
                with OuterAuthorityGuard(self.outer,self.authority,enabled=True) as outer, \
                     OperationFence(self.old,self.leases,self.authority,enabled=True) as fence:
                    outer.prepare_candidate(fence,self.candidate,self.image,operation_id='c'*32)
                    os.kill(os.getpid(),signal.SIGKILL)
            finally:os._exit(3)
        _,status=os.waitpid(child,0);self.assertTrue(os.WIFSIGNALED(status))
        with self.assertRaisesRegex(OuterHeld,'outer_handoff_requires_reconciliation'):
            with OuterAuthorityGuard(self.outer,self.authority,enabled=True):pass
        # Legacy direct launchers ignore this marker; this is precisely why an
        # external host-enforced launcher boundary remains mandatory.
        lease=AuthorityLease(self.leases,self.authority);lease.close()
