"""Synthetic coordinator database integration; no transport route."""
import unittest
import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from processing.viewer_archive_spool import SpoolHeld
from processing.viewer_chunk_spool import SyntheticChunkSpool
from tests import test_viewer_archive_spool as spool_tests

class ChunkSpoolTests(unittest.TestCase):
    def setUp(self):
        self.base=spool_tests.ViewerArchiveSpoolTests('test_authenticated_exact_spool_and_replay')
        self.addCleanup(self.base.doCleanups);self.base.setUp()
        self.chunk=SyntheticChunkSpool(self.base.spool,synthetic_fixture=True)
        self.job=self.base.fixture.job;self.identity=self.base.identity()
        with self.base.fixture.queue._db() as db:self.token=db.execute('SELECT token FROM viewer_worker_claims WHERE job_id=?',(self.job,)).fetchone()['token']
        self.archive=self.base.base.base.archive
    def test_exact_coordinator_chunk_replay_and_final_receipt(self):
        self.assertEqual(self.chunk.begin(self.job,self.identity,self.token),{'state':'ready'})
        self.assertEqual(self.chunk.begin(self.job,self.identity,self.token),{'state':'ready'})
        midpoint=len(self.archive)//2;a,b=self.archive[:midpoint],self.archive[midpoint:]
        first=self.chunk.write(self.job,self.identity,self.token,0,0,a)
        self.assertEqual(first,self.chunk.write(self.job,self.identity,self.token,0,0,a))
        with self.assertRaises(Exception):self.chunk.write(self.job,self.identity,self.token,1,midpoint-1,b)
        self.chunk.write(self.job,self.identity,self.token,1,midpoint,b)
        receipt=self.chunk.finish(self.job,self.identity,self.token,2)
        self.assertEqual(receipt['archiveSha256'],self.base.base.record['archiveSha256'])
        self.assertEqual(self.chunk.finish(self.job,self.identity,self.token,2),receipt)
        self.assertEqual(self.base.spool.lookup(self.job,self.identity,self.token),{'state':'held'})
        self.assertEqual(self.base.fixture.queue.get(self.job)['status'],'running')
    def test_creation_failure_is_held_without_recreate(self):
        with patch('processing.viewer_chunk_spool.open_slot',side_effect=OSError('synthetic crash')):
            with self.assertRaises(OSError):self.chunk.begin(self.job,self.identity,self.token)
        with self.assertRaisesRegex(SpoolHeld,'chunk_session_requires_inspection'):
            self.chunk.begin(self.job,self.identity,self.token)
        with self.base.fixture.queue._db() as db:
            self.assertEqual(db.execute('SELECT state FROM viewer_worker_chunk_sessions WHERE job_id=?',(self.job,)).fetchone()['state'],'creating')
        self.assertEqual(list(self.base.root.glob('*.alloc')),[])

    def test_hold_blocks_later_chunk_but_admitted_chunk_finishes(self):
        from processing.viewer_chunk_journal import SyntheticChunkJournal
        self.chunk.begin(self.job,self.identity,self.token)
        entered=threading.Event();release=threading.Event();original=SyntheticChunkJournal.write
        def paused(journal,*args):
            entered.set()
            if not release.wait(5):raise RuntimeError('chunk barrier timeout')
            return original(journal,*args)
        with patch.object(SyntheticChunkJournal,'write',paused):
            with ThreadPoolExecutor(max_workers=1) as pool:
                future=pool.submit(self.chunk.write,self.job,self.identity,self.token,0,0,self.archive[:4])
                try:
                    self.assertTrue(entered.wait(5))
                    self.assertEqual(self.base.fixture.claims.mutation_hold(self.job,self.identity,self.token)['state'],'held')
                finally:release.set()
                self.assertEqual(future.result(timeout=5)['schemaVersion'],1)
        with self.assertRaises(SpoolHeld):self.chunk.write(self.job,self.identity,self.token,1,4,self.archive[4:])
        with self.assertRaises(SpoolHeld):self.chunk.finish(self.job,self.identity,self.token,1)
        self.assertEqual(len(list(self.base.root.glob('*.alloc'))),1)

    def test_durable_admission_survives_hold_only_in_own_process(self):
        self.chunk.begin(self.job,self.identity,self.token)
        data=self.archive[:8];digest=hashlib.sha256(data).hexdigest()
        permit=self.chunk.admit_chunk(self.job,self.identity,self.token,'chunk-0',0,0,len(data),digest)
        self.assertEqual(permit['state'],'active')
        other=SyntheticChunkSpool(self.base.spool,synthetic_fixture=True)
        with self.assertRaisesRegex(SpoolHeld,'chunk_permit_requires_inspection'):
            other.write_admitted(self.job,self.identity,self.token,'chunk-0',permit['permit'],0,0,data)
        self.assertEqual(self.base.fixture.claims.mutation_hold(self.job,self.identity,self.token)['state'],'held')
        receipt=self.chunk.write_admitted(self.job,self.identity,self.token,'chunk-0',permit['permit'],0,0,data)
        self.assertEqual(receipt['schemaVersion'],1)
        with self.assertRaises(SpoolHeld):self.chunk.admit_chunk(self.job,self.identity,self.token,'chunk-1',1,8,1,hashlib.sha256(b'x').hexdigest())
        self.assertEqual(len(list(self.base.root.glob('*.alloc'))),1)

    def test_finished_permit_replay_rechecks_receipt_and_slot_bytes(self):
        self.chunk.begin(self.job,self.identity,self.token)
        data=self.archive[:8];digest=hashlib.sha256(data).hexdigest()
        permit=self.chunk.admit_chunk(self.job,self.identity,self.token,'chunk-0',0,0,len(data),digest)
        with self.assertRaises(SpoolHeld):
            self.chunk.write_admitted(self.job,self.identity,self.token,'chunk-0','wrong',0,0,data)
        self.chunk.write_admitted(self.job,self.identity,self.token,'chunk-0',permit['permit'],0,0,data)
        self.assertEqual(self.chunk.admit_chunk(self.job,self.identity,self.token,'chunk-0',0,0,len(data),digest),{'state':'finished'})
        allocation=next(self.base.root.glob('*.alloc'))
        with allocation.open('r+b') as stream:stream.write(b'X');stream.flush()
        with self.assertRaises(Exception):self.chunk.admit_chunk(self.job,self.identity,self.token,'chunk-0',0,0,len(data),digest)

    def test_finished_permit_replay_rejects_missing_receipt(self):
        self.chunk.begin(self.job,self.identity,self.token)
        data=self.archive[:8];digest=hashlib.sha256(data).hexdigest()
        permit=self.chunk.admit_chunk(self.job,self.identity,self.token,'chunk-0',0,0,len(data),digest)
        self.chunk.write_admitted(self.job,self.identity,self.token,'chunk-0',permit['permit'],0,0,data)
        receipt=next(self.base.root.glob('*.chunks-v1.*.receipt'));receipt.unlink()
        with self.assertRaises(Exception):self.chunk.admit_chunk(self.job,self.identity,self.token,'chunk-0',0,0,len(data),digest)
        self.assertFalse(receipt.exists())

    def test_interrupted_admission_does_not_auto_resume(self):
        self.chunk.begin(self.job,self.identity,self.token)
        data=self.archive[:8];digest=hashlib.sha256(data).hexdigest()
        self.chunk.admit_chunk(self.job,self.identity,self.token,'chunk-0',0,0,len(data),digest)
        restarted=SyntheticChunkSpool(self.base.spool,synthetic_fixture=True)
        with self.assertRaisesRegex(SpoolHeld,'chunk_permit_requires_inspection'):
            restarted.write(self.job,self.identity,self.token,0,0,data)
        with self.assertRaisesRegex(SpoolHeld,'chunk_permit_requires_inspection'):
            restarted.admit_chunk(self.job,self.identity,self.token,'chunk-0',0,0,len(data),digest)
        with self.base.fixture.queue._db() as db:
            self.assertEqual(db.execute('SELECT state FROM viewer_worker_chunk_permits WHERE job_id=?',(self.job,)).fetchone()['state'],'active')

    def test_legacy_receiving_is_never_adopted(self):
        def fail():yield b'x';raise OSError('interrupted')
        with self.assertRaises(Exception):self.base.spool.receive(self.job,self.identity,self.token,fail())
        with self.assertRaisesRegex(SpoolHeld,'legacy_spool_requires_inspection'):self.chunk.begin(self.job,self.identity,self.token)
    def test_wrong_principal_and_missing_header_hold(self):
        with self.assertRaises(Exception):self.chunk.begin(self.job,dict(self.identity,node_id='main'),self.token)
        self.chunk.begin(self.job,self.identity,self.token)
        header=next(self.base.root.glob('*.chunks-v1.header'));header.unlink()
        with self.assertRaises(Exception):self.chunk.write(self.job,self.identity,self.token,0,0,b'bytes')
        self.assertFalse(header.exists())
