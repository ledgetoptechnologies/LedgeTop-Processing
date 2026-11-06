"""Synthetic isolated supervisor proof, NOT a production recovery facility.

The test container, kernel, supervisor and writer image are trusted. The
writer begins with only stdio pipes and installs restrictive seccomp before
opening scratch or receiving bytes. The supervisor waits for that exact child;
it does not infer descriptor retirement from a PID, lease, or elapsed time.
"""
import hashlib
import json
import os
from pathlib import Path
import select
import stat
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import uuid
from contextlib import closing


@unittest.skipUnless(os.name=='posix','isolated Linux supervisor required')
class IsolatedWriterRecoveryTests(unittest.TestCase):
    def test_partial_death_verified_retirement_and_exact_replay(self):
        with tempfile.TemporaryDirectory(prefix='v2-supervisor-') as temp:
            root=Path(temp);root.chmod(0o700)
            database=root/'supervisor.sqlite';partial=root/'object.part';final=root/'object.bin'
            op=str(uuid.uuid4());pin=uuid.uuid4().hex;expected=hashlib.sha256(b'abc').hexdigest()
            fixture=Path(__file__).parent/'fixtures'/'direct_v2_confined_writer.py'
            def connect():
                db=sqlite3.connect(database,isolation_level=None);db.row_factory=sqlite3.Row
                db.execute('PRAGMA synchronous=FULL');return db
            with closing(connect()) as db:
                db.execute('CREATE TABLE operation(id TEXT PRIMARY KEY,pin TEXT NOT NULL,sha TEXT NOT NULL,state TEXT NOT NULL,dev INTEGER,ino INTEGER,receipt TEXT)')
                db.execute("INSERT INTO operation(id,pin,sha,state) VALUES(?,?,?,'running')",(op,pin,expected))
            def start(mode):
                return subprocess.Popen([sys.executable,str(fixture),str(partial),mode],
                    stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,close_fds=True)
            def line(child):
                ready,_,_=select.select([child.stdout],[],[],5)
                if not ready:self.fail('confined synthetic writer failed to signal')
                return child.stdout.readline()
            child=None
            try:
                child=start('partial');self.assertEqual(line(child),b'READY\n')
                child.stdin.write(b'a');child.stdin.flush();self.assertEqual(line(child),b'PARTIAL\n')
                before=partial.stat();self.assertEqual(before.st_size,1)
                with closing(connect()) as db:
                    db.execute('BEGIN IMMEDIATE')
                    db.execute('UPDATE operation SET dev=?,ino=? WHERE id=? AND state=\'running\'',
                               (before.st_dev,before.st_ino,op));db.commit()
                # The confined child is still alive: no recovery or pin release.
                self.assertIsNone(child.poll())
                with closing(connect()) as db:self.assertEqual(db.execute('SELECT state FROM operation').fetchone()[0],'running')
                retired_child=child
                child.kill();child.communicate(timeout=3);self.assertIsNotNone(child.returncode);child=None

                def recover(operation_id,pin_id,retired):
                    # Caller is this trusted supervisor after exact child wait;
                    # seccomp denied fork, Unix socket creation/FD passing,
                    # ptrace, pidfd extraction and io_uring in that child.
                    if retired is not retired_child or retired.poll() is None:
                        raise ValueError('confined writer quiescence not established')
                    with closing(connect()) as db:
                        db.execute('BEGIN IMMEDIATE');row=db.execute('SELECT * FROM operation WHERE id=?',(operation_id,)).fetchone()
                        if row is None or row['pin']!=pin_id:raise ValueError('operation binding changed')
                        if row['state']=='recovered':db.commit();return json.loads(row['receipt'])
                        if row['state'] not in ('running','recovering'):raise ValueError('operation held')
                        db.execute("UPDATE operation SET state='recovering' WHERE id=?",(operation_id,));db.commit()
                    directory=os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
                    try:
                        try:current=os.stat(partial.name,dir_fd=directory,follow_symlinks=False)
                        except FileNotFoundError:current=None
                        if current is not None:
                            if ((current.st_dev,current.st_ino)!=(row['dev'],row['ino'])
                                    or not stat.S_ISREG(current.st_mode) or current.st_nlink!=1
                                    or current.st_uid!=os.geteuid()):raise ValueError('partial identity changed')
                            os.unlink(partial.name,dir_fd=directory)
                        elif row['state']!='recovering':raise ValueError('partial disappearance uncertain')
                        os.fsync(directory)
                    finally:os.close(directory)
                    receipt=dict(operationId=operation_id,pin=pin_id,partialRetired=True,sourceCustodyAccepted=False)
                    with closing(connect()) as db:
                        db.execute('BEGIN IMMEDIATE')
                        changed=db.execute("UPDATE operation SET state='recovered',receipt=? WHERE id=? AND pin=? AND state='recovering'",
                                           (json.dumps(receipt,sort_keys=True),operation_id,pin_id)).rowcount
                        if changed!=1:raise ValueError('recovery outcome uncertain')
                        db.commit()
                    return receipt
                receipt=recover(op,pin,retired_child);self.assertFalse(receipt['sourceCustodyAccepted'])
                self.assertFalse(partial.exists())
                # Reopen the journal after the committed recovery: exact replay
                # must return the same outcome without a second deletion.
                self.assertEqual(recover(op,pin,retired_child),receipt)
                with self.assertRaises(ValueError):recover(op,uuid.uuid4().hex,retired_child)
                with self.assertRaises(ValueError):recover(op,pin,None)
                child=start('complete');self.assertEqual(line(child),b'READY\n')
                child.stdin.write(b'abc');child.stdin.flush();self.assertEqual(line(child),b'COMPLETE\n')
                child.stdin.close();child.stdin=None;_,error=child.communicate(timeout=3)
                self.assertEqual(child.returncode,0,error);child=None
                self.assertEqual(hashlib.sha256(partial.read_bytes()).hexdigest(),expected)
                directory=os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
                try:os.rename(partial.name,final.name,src_dir_fd=directory,dst_dir_fd=directory);os.fsync(directory)
                finally:os.close(directory)
                self.assertEqual(final.read_bytes(),b'abc')
            finally:
                if child is not None and child.poll() is None:
                    child.kill();child.communicate(timeout=3)
