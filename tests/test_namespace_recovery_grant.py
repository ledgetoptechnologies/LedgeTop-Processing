import os, subprocess, sys, tempfile, unittest
from pathlib import Path
from ops.previous_init_witness import InitWitness, Held, process_identity
from ops.recovery_grant import RecoveryGrant
from tests.test_namespace_owner_restart import OwnedFixture

class GrantFixture(OwnedFixture):
    def __init__(self,journal,previous=None):
        self.init_witness=self.wrapper_witness=None
        if previous is not None:
            fds=previous.grant.duplicate_dead_fds()
            try: super().__init__(journal,fds)
            finally:
                for fd in fds: os.close(fd)
        else: super().__init__(journal)
        try:
            init=self.ready['initHostPid']; initparent,initbirth=process_identity(init)
            wrapperparent,wrapperbirth=process_identity(self.child.pid)
            self.init_witness=InitWitness(journal+'.init.'+str(init),fixture=True)
            self.init_witness.bind_live(init,expected_parent=initparent,expected_birth=initbirth)
            self.wrapper_witness=InitWitness(journal+'.wrapper.'+str(self.child.pid),fixture=True)
            self.wrapper_witness.bind_live(self.child.pid,expected_parent=wrapperparent,expected_birth=wrapperbirth)
            self.grant=RecoveryGrant(journal,self.init_witness,self.wrapper_witness,fixture=True)
        except BaseException:
            self.cleanup(); raise
    def cleanup(self):
        try:
            if self.init_witness is not None: self.init_witness.close(); self.init_witness=None
        finally:
            try:
                if self.wrapper_witness is not None: self.wrapper_witness.close(); self.wrapper_witness=None
            finally: super().cleanup()

class Tests(unittest.TestCase):
    def test_exact_witnesses_allow_fresh_owner(self):
        with tempfile.TemporaryDirectory() as root:
            journal=str(Path(root)/'gate.sqlite'); first=second=None
            try:
                first=GrantFixture(journal); first.cmd('open'); first.cmd('drain'); first.cmd('stop'); first.kill()
                second=GrantFixture(journal,first)
                self.assertTrue(second.ready['recoverable'])
                self.assertTrue(second.cmd('open')['gate']['open'])
            finally:
                try:
                    if second is not None: second.cleanup()
                finally:
                    if first is not None: first.cleanup()
    def test_unrelated_dead_init_witness_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            journal=str(Path(root)/'gate.sqlite'); first=other=None; witness=None
            try:
                first=GrantFixture(journal); first.cmd('open'); first.cmd('drain'); first.cmd('stop'); first.kill()
                other=subprocess.Popen([sys.executable,'-c','import time; time.sleep(300)'])
                parent,birth=process_identity(other.pid)
                witness=InitWitness(str(Path(root)/'unrelated.sqlite'),fixture=True)
                witness.bind_live(other.pid,expected_parent=parent,expected_birth=birth)
                other.kill(); other.wait(timeout=3)
                grant=RecoveryGrant(journal,witness,first.wrapper_witness,fixture=True)
                with self.assertRaises(Held): grant.duplicate_dead_fds()
            finally:
                try:
                    if other is not None:
                        if other.poll() is None: other.kill()
                        other.wait(timeout=3)
                finally:
                    try:
                        if witness is not None: witness.close()
                    finally:
                        if first is not None: first.cleanup()
    def test_supervisor_witness_loss_holds(self):
        with tempfile.TemporaryDirectory() as root:
            journal=str(Path(root)/'gate.sqlite'); first=None
            try:
                first=GrantFixture(journal); first.cmd('open'); first.cmd('drain'); first.cmd('stop'); first.kill()
                first.init_witness.close(); first.init_witness=None
                with self.assertRaises(Held): first.grant.duplicate_dead_fds()
            finally:
                if first is not None: first.cleanup()

if __name__=='__main__': unittest.main()
