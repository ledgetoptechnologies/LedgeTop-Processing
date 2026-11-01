"""Owned-process synthetic crash fixture. Do not run before lifecycle review.

Only this test's Popen child is ever killed after a 15-second timeout; no
process-group signals, shell, or machine-wide process enumeration. Each child
normally self-exits via os._exit at the exact fixture boundary. Input/state are
new random /tmp directories, deliberately retained. The guard is test-qualified
only through in-process mocks, never by environment or deployed configuration.
"""
import io
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import uuid
from unittest.mock import patch

from processing import intake_offline_migration as migration, viewer_tasks
from processing.prototype import Queue
from processing.viewer_tasks import TaskStore, InputRecoveryRequired

CHILD=r'''
import io,os,sys
from pathlib import Path
from unittest.mock import patch
from processing import intake_offline_migration as migration, viewer_tasks
from processing.prototype import Queue
from processing.viewer_tasks import TaskStore
root=Path(sys.argv[1]);identifier=sys.argv[2];mode=sys.argv[3]
with patch.object(migration,'QUALIFIED',True),patch.object(viewer_tasks,'INTAKE_JOURNAL_QUALIFIED',True):
    q=Queue(root/'queue');q._intake_guard_enabled=True
    store=TaskStore(q,root/'queue'/'viewer-tasks',input_free_floor=0,intake_journal=True)
    if mode=='post_link':
        original=viewer_tasks.sync_directory
        def exit_after_link(directory):
            if any(Path(directory).glob('input-*')):os._exit(73)
            return original(directory)
        with patch.object(viewer_tasks,'sync_directory',side_effect=exit_after_link):
            store.upload(identifier,'a.jpg',io.BytesIO(b'synthetic-child'),15)
    elif mode=='post_commit':
        original=Path.unlink
        def exit_before_unlink(path,*args,**kwargs):
            if path.name.startswith('.incoming-'):os._exit(74)
            return original(path,*args,**kwargs)
        with patch.object(Path,'unlink',exit_before_unlink):
            store.upload(identifier,'a.jpg',io.BytesIO(b'synthetic-child'),15)
    elif mode=='lost_response':
        store.upload(identifier,'a.jpg',io.BytesIO(b'synthetic-child'),15)
        os._exit(75)
    os._exit(91)
'''

@unittest.skipUnless(os.name=='posix' and hasattr(os,'posix_fallocate'),'POSIX required')
class IntakeOwnedProcessCrashTests(unittest.TestCase):
    def test_child_crash_and_reopen(self):
        for mode,exitcode,state,totals in (
            ('post_link',73,'sealed',(0,0)),('post_commit',74,'cleanup',(1,15)),
            ('lost_response',75,'committed',(1,15))):
            with self.subTest(mode=mode):
                root=Path(tempfile.mkdtemp(prefix='lt-owned-intake-crash-')).resolve()
                print('retained owned synthetic crash root:',root,flush=True)
                identifier=str(uuid.uuid4())
                with patch.object(migration,'QUALIFIED',True),patch.object(viewer_tasks,'INTAKE_JOURNAL_QUALIFIED',True):
                    q=Queue(root/'queue')
                    try:
                        TaskStore(q,root/'queue'/'viewer-tasks',input_free_floor=0)
                        with q._db() as db:migration.install_empty_offline(db)
                        q._intake_guard_enabled=True
                        store=TaskStore(q,root/'queue'/'viewer-tasks',input_free_floor=0,intake_journal=True)
                        store.initialize(identifier,'synthetic',[],None,'production')
                    finally:q.close()
                    child=subprocess.Popen([sys.executable,'-c',CHILD,str(root),identifier,mode],
                        stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
                    print('owned synthetic child:',mode,'pid:',child.pid,flush=True)
                    try:
                        _,stderr=child.communicate(timeout=15)
                    except subprocess.TimeoutExpired:
                        self.fail('owned synthetic child exceeded deadline')
                    finally:
                        # Only the freshly created Popen child, no process group.
                        if child.poll() is None:
                            child.kill();child.communicate(timeout=5)
                    self.assertEqual(child.returncode,exitcode,stderr.decode(errors='replace'))
                    self.assertIsNotNone(child.poll())
                    print('owned synthetic child exited:',child.pid,'code:',child.returncode,flush=True)
                    q=Queue(root/'queue');q._intake_guard_enabled=True
                    try:
                        store=TaskStore(q,root/'queue'/'viewer-tasks',input_free_floor=0,intake_journal=True)
                        with q._db() as db:
                            self.assertEqual(db.execute('SELECT state FROM viewer_intake_journal_v1').fetchone()[0],state)
                            self.assertEqual(tuple(db.execute('SELECT files,bytes FROM viewer_intake_totals_v1').fetchone()),totals)
                        final=list(root.rglob('input-*'));self.assertEqual(len(final),1)
                        self.assertEqual(final[0].read_bytes(),b'synthetic-child')
                        if mode!='lost_response':
                            with self.assertRaises(InputRecoveryRequired):store.startup_intake_census()
                        else:
                            store.startup_intake_census()
                            store.upload(identifier,'a.jpg',io.BytesIO(b'synthetic-child'),15)
                            with q._db() as db:self.assertEqual(tuple(db.execute('SELECT files,bytes FROM viewer_intake_totals_v1').fetchone()),(1,15))
                    finally:q.close()

if __name__=='__main__':unittest.main()
