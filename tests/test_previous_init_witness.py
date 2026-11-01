import os, select, signal, sqlite3, subprocess, sys, tempfile, unittest
from unittest.mock import patch
from pathlib import Path
from ops.previous_init_witness import InitWitness, Held, process_identity

class Tests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.journal=Path(self.temp.name)/'witness.sqlite'
        self.witness=InitWitness(self.journal,fixture=True); self.child=None
    def tearDown(self):
        try:
            if self.child is not None:
                if self.child.poll() is None: self.child.kill()
                self.child.wait(timeout=3)
        finally:
            try: self.witness.close()
            finally: self.temp.cleanup()
    def spawn(self):
        self.child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(300)'],
                                    stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        return process_identity(self.child.pid)
    def test_exact_retained_pidfd_after_exit_and_reopen_holds(self):
        parent,birth=self.spawn()
        self.witness.bind_live(self.child.pid,expected_parent=parent,expected_birth=birth)
        with self.assertRaises(Held): self.witness.duplicate_after_exit()
        self.child.kill(); self.child.wait(timeout=3)
        fd=self.witness.duplicate_after_exit()
        try: self.assertTrue(select.select([fd],[],[],0)[0])
        finally: os.close(fd)
        self.witness.close(); self.witness=InitWitness(self.journal,fixture=True)
        with self.assertRaises(Held): self.witness.duplicate_after_exit()
        with self.assertRaises(Held): self.witness.bind_live(os.getpid(),expected_parent=os.getppid(),expected_birth=process_identity(os.getpid())[1])
    def test_wrong_parent_or_birth_never_binds(self):
        parent,birth=self.spawn()
        with self.assertRaises(Held): self.witness.bind_live(self.child.pid,expected_parent=parent,expected_birth=birth+1)
        with self.assertRaises(Held): self.witness.bind_live(self.child.pid,expected_parent=parent+1,expected_birth=birth)
        with self.assertRaises(Held): self.witness.duplicate_after_exit()
    def test_pidfd_close_error_still_closes_database(self):
        parent,birth=self.spawn()
        self.witness.bind_live(self.child.pid,expected_parent=parent,expected_birth=birth)
        original=os.close
        def close_then_fail(fd):
            original(fd)
            raise OSError('synthetic close error')
        with patch('ops.previous_init_witness.os.close',side_effect=close_then_fail):
            with self.assertRaises(OSError): self.witness.close()
        with self.assertRaises(sqlite3.ProgrammingError): self.witness.db.execute('SELECT 1')
    def test_default_disabled_before_journal(self):
        path=Path(self.temp.name)/'denied.sqlite'
        with self.assertRaises(Held): InitWitness(path)
        self.assertFalse(path.exists())

if __name__=='__main__': unittest.main()
