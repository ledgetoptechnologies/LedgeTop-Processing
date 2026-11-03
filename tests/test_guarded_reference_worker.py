import os
import unittest
from unittest.mock import patch

from processing.host_mutation_journal import HostMutationJournal, MutationHeld
from processing.node_gate import GateError, NodeGate
from processing.reference_engine_worker import WorkerHeld
from processing import reference_protocol as protocol
from tests.test_reference_engine_worker import ReferenceEngineWorkerTests


class GuardedWorkerTests(ReferenceEngineWorkerTests):
    # Do not rerun the parent class's full suite under guarded mode.
    def make_guarded(self):
        return self.make_worker(host_mutation_journal=True)

    def test_guarded_real_worker_records_exact_identity_and_blocks_completion(self):
        self.worker = self.make_guarded()
        self.engine.action = lambda action, identifier: {'success': True, 'uuid': identifier}
        self.assertEqual(self.start(), 'processing')
        row = self.worker._row(self.job['jobId'])
        with NodeGate('physical-engine') as gate:
            journal = HostMutationJournal(gate, row['receipt']).open()
            rows = journal.db.execute('SELECT kind,request_digest,state FROM operations ORDER BY seq').fetchall()
            self.assertEqual([r[0] for r in rows], ['init', 'upload', 'commit'])
            self.assertTrue(all(r[2] == 'done' for r in rows))
            task = dict(name=self.job['jobId'], engine_uuid=self.job['jobId'], outputs=None, options=[])
            self.assertEqual(rows[0][1], protocol.digest(task))
            self.assertEqual(rows[1][1], protocol.digest(dict(task=self.job['jobId'], name=self.job['submission']['manifest']['objects'][0]['name'], bytes=3,
                sha256=self.job['submission']['manifest']['objects'][0]['sha256'])))
            self.assertEqual(rows[2][1], protocol.digest(dict(task=self.job['jobId'])))
            journal.check_reconciliation_ready()
            self.assertEqual(journal.job['id'], self.job['jobId'])
            with self.assertRaises(GateError):
                gate.prepare_completion(row['receipt'], journal.job)
            journal.close()
        # Restart with the default disabled does not unguard prior executions.
        self.worker = self.make_worker()
        self.assertEqual(self.start(), 'processing')
        with self.assertRaises(WorkerHeld): self.worker.poll(self.job['jobId'])
        self.assertEqual(self.engine.calls, ['initialize', 'upload'])

    @unittest.skipUnless(hasattr(os, 'fork'), 'requires actual process death')
    def test_crash_during_actual_upload_leaves_pending_no_replay(self):
        self.worker = self.make_guarded()
        self.engine.upload = lambda *args: os._exit(17)
        child = os.fork()
        if child == 0:
            try:
                self.start()
            finally:
                os._exit(99)
        _, status = os.waitpid(child, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 17)
        row = self.worker._row(self.job['jobId'])
        with NodeGate('physical-engine') as gate:
            journal = HostMutationJournal(gate, row['receipt']).open()
            rows = journal.db.execute('SELECT op_id,kind,request_digest,state FROM operations ORDER BY seq').fetchall()
            self.assertEqual([(r[1], r[3]) for r in rows], [('init','done'), ('upload','pending')])
            with self.assertRaises(MutationHeld): journal.begin(rows[1][0], rows[1][1], rows[1][2])
            journal.close()
        self.worker = self.make_worker()
        self.assertEqual(self.start(), 'upload_pending')
        with self.assertRaises(WorkerHeld): self.worker.poll(self.job['jobId'])
        self.assertEqual(self.engine.calls, [])

    def test_invalid_commit_response_remains_pending(self):
        self.worker = self.make_guarded()
        self.engine.action = lambda action, identifier: {'uuid': 'wrong'}
        with self.assertRaises(WorkerHeld): self.start()
        row = self.worker._row(self.job['jobId'])
        with NodeGate('physical-engine') as gate:
            journal = HostMutationJournal(gate, row['receipt']).open()
            self.assertEqual(journal.db.execute('SELECT kind,state FROM operations ORDER BY seq DESC LIMIT 1').fetchone(), ('commit','pending'))
            journal.close()
        self.worker = self.make_worker()
        self.assertEqual(self.start(), 'commit_pending')
        with self.assertRaises(WorkerHeld): self.worker.poll(self.job['jobId'])

