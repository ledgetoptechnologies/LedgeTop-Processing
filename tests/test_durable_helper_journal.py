"""Isolated journal process-kill tests; no Docker host is touched."""
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

if os.name == 'posix':
    from ops.durable_helper_journal import HelperJournal, JournalError

OP = '1' * 32
CID = 'a' * 64


class FakeDaemon:
    def __init__(self, *, mismatch=False):
        self.container = True
        self.mismatch = mismatch
        self.removals = []

    def docker(self, *args, timeout=30):
        if args[0] == 'ps': return CID + '\n' if self.container else ''
        if args[0] == 'rm':
            self.removals.append(args[-1]); self.container = False; return ''
        raise AssertionError(args)

    def inspected(self, cid, *, timeout=30):
        return {'Id': cid, 'Name': '/somebody-else' if self.mismatch else '/ledgetop-update-helper-' + OP,
                'Config': {'Labels': {'io.ledgetop.update-helper': OP}}}


@unittest.skipUnless(os.name == 'posix', 'POSIX flock and SIGKILL required')
class DurableHelperJournalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.root = Path(self.temp.name)

    def tearDown(self): self.temp.cleanup()

    def test_killed_owner_releases_lock_and_exact_helper_can_be_recovered(self):
        code = '''import sys,time
from ops.durable_helper_journal import HelperJournal
with HelperJournal(sys.argv[1]) as journal:
 journal.write("1"*32,"create_pending")
 journal.write("1"*32,"created","a"*64)
 journal.write("1"*32,"start_pending","a"*64)
 print("ready",flush=True)
 time.sleep(120)
'''
        child = subprocess.Popen([sys.executable, '-c', code, str(self.root)], stdout=subprocess.PIPE, text=True)
        try:
            self.assertEqual(child.stdout.readline().strip(), 'ready')
            with self.assertRaisesRegex(JournalError, 'helper_journal_busy'):
                with HelperJournal(self.root): pass
            os.kill(child.pid, signal.SIGKILL); child.wait(timeout=5)
            fixture = FakeDaemon()
            with HelperJournal(self.root) as journal:
                journal.recover(fixture)
                self.assertEqual(journal.read()['phase'], 'resolved')
            self.assertEqual(fixture.removals, [CID])
        finally:
            if child.poll() is None: child.kill(); child.wait(timeout=5)
            child.stdout.close()

    def test_unknown_create_outcome_is_held_even_if_listing_is_empty(self):
        fixture = FakeDaemon(); fixture.container = False
        with HelperJournal(self.root) as journal:
            journal.write(OP, 'create_pending')
            with self.assertRaisesRegex(JournalError, 'create_outcome_requires_inspection'):
                journal.recover(fixture)
        self.assertEqual(fixture.removals, [])

    def test_mismatched_identity_is_not_removed(self):
        fixture = FakeDaemon(mismatch=True)
        with HelperJournal(self.root) as journal:
            journal.write(OP, 'create_pending')
            journal.write(OP, 'created', CID)
            with self.assertRaisesRegex(JournalError, 'helper_identity_ambiguous'):
                journal.recover(fixture)
        self.assertEqual(fixture.removals, [])

    def test_unresolved_record_cannot_be_overwritten_or_rebound(self):
        with HelperJournal(self.root) as journal:
            journal.write(OP, 'create_pending')
            with self.assertRaisesRegex(JournalError, 'invalid_helper_transition'):
                journal.write('2' * 32, 'create_pending')
            journal.write(OP, 'created', CID)
            with self.assertRaisesRegex(JournalError, 'invalid_helper_transition'):
                journal.write(OP, 'start_pending', 'b' * 64)


if __name__ == '__main__': unittest.main()
