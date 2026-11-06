"""Start witness across real SIGKILL, over the real listener and driver request path.

Harness and processes come from test_reference_source_fetch_process_restart. Here
FileEngine's status and output are scripted, since its default is always 20.
"""
import json
import os
import unittest

from processing import reference_presentation as presentation
from processing import reference_protocol as protocol
from tests import test_reference_source_fetch_process_restart as restart
from tests.source_fetch_process_fixture import ENGINE_SCRIPT

Base = restart.ProcessRestartTests


@unittest.skipIf(os.name == 'nt', 'SIGKILL and the private ledger paths are POSIX')
class WitnessProcessTests(unittest.TestCase):
    setUp, spawn, kill, step, advance, read = Base.setUp, Base.spawn, staticmethod(Base.kill), Base.step, Base.advance, Base.read
    status, receipts, journal, to_first_object = Base.status, Base.receipts, Base.journal, Base.to_first_object
    engine_calls, fetched, restart_both = Base.engine_calls, Base.fetched, Base.restart_both

    def script(self, **values):
        (self.root / ENGINE_SCRIPT).write_text(json.dumps(values))

    def rows(self):
        return self.read(self.database, 'SELECT kind,acknowledged,payload_json FROM reference_outbox WHERE identity=?',
            presentation.IDENTITY_PREFIX + self.job_id)

    def latch(self):
        return self.journal()[1].get('startWitness')

    def executing(self, worker):
        self.fetched(worker)
        self.advance(worker, 'execution_pending', 'executing')

    def test_killed_between_latch_and_post_resends_the_exact_bytes(self):
        self.spawn('coordinator')
        dying = self.spawn('worker', SOURCE_FETCH_DIE='before_witness_post')
        self.executing(dying)
        self.assertEqual(self.step(dying)['state'], 'executing')
        latched = self.latch()
        self.assertIsNone(latched['ack'])
        self.assertIsNone(self.step(dying))
        self.assertEqual(dying.wait(), -9)
        self.assertEqual(self.rows(), [])
        worker = self.spawn('worker')
        self.assertEqual(self.step(worker)['state'], 'executing')
        self.assertEqual(self.latch()['request'], latched['request'])
        self.assertEqual(self.latch()['ack']['requestSha256'], protocol.digest(latched['request']))
        self.assertEqual(len(self.rows()), 1)

    def test_killed_after_post_replays_to_the_stored_receipt(self):
        self.spawn('coordinator')
        dying = self.spawn('worker', SOURCE_FETCH_DIE='after_witness_post')
        self.executing(dying)
        self.assertEqual(self.step(dying)['state'], 'executing')
        self.assertIsNone(self.step(dying))
        self.assertEqual(dying.wait(), -9)
        self.assertIsNone(self.latch()['ack'])
        stored = self.rows()
        self.assertEqual([row[:2] for row in stored], [(presentation.KIND, 1)])
        worker = self.spawn('worker')
        self.assertEqual(self.step(worker)['state'], 'executing')
        record = json.loads(stored[0][2])
        ack = self.latch()['ack']
        self.assertEqual((ack['acceptedAt'], ack['workerObservedAt']), (record['acceptedAt'], record['workerObservedAt']))
        self.assertEqual(self.rows(), stored)

    def test_status_10_with_old_output_stays_unlatched_across_a_worker_kill(self):
        self.spawn('coordinator')
        worker = self.spawn('worker')
        self.script(code=10, output=['restored output'])
        self.executing(worker)
        for _ in range(2):
            self.assertEqual(self.step(worker)['state'], 'executing')
        self.assertIsNone(self.latch())
        self.kill(worker)
        worker = self.spawn('worker')
        for _ in range(2):
            self.assertEqual(self.step(worker)['state'], 'executing')
        self.assertIsNone(self.latch())
        self.assertEqual(self.rows(), [])
        self.script(code=20, output=['restored output'])
        self.assertEqual(self.step(worker)['state'], 'executing')
        self.assertIsNotNone(self.latch())

    def test_status_20_with_empty_output_stays_unlatched(self):
        self.spawn('coordinator')
        worker = self.spawn('worker')
        self.script(code=20, output=['', '  '])
        self.executing(worker)
        for _ in range(3):
            self.assertEqual(self.step(worker)['state'], 'executing')
        self.assertIsNone(self.latch())

    def test_both_processes_killed_keep_one_row_and_never_charge_recovery(self):
        coordinator = self.spawn('coordinator')
        worker = self.spawn('worker')
        self.executing(worker)
        for _ in range(2):
            self.assertEqual(self.step(worker)['state'], 'executing')
        stored, ack = self.rows(), self.latch()['ack']
        self.assertEqual(len(stored), 1)
        self.assertIsNotNone(ack)
        self.restart_both(worker, coordinator)
        worker = self.spawn('worker')
        self.advance(worker, 'executing')
        self.assertEqual(self.status(), ('running', None))
        self.assertEqual(self.receipts()[0]['engineClass'], 'started')
        self.assertEqual(self.rows(), stored)
        self.assertEqual(self.latch()['ack'], ack)
        self.assertEqual(self.journal()[1]['sourceRecovery'], {'attempts': 0, 'pending': None})
        self.assertEqual(self.engine_calls().count('initialize'), 1)


del Base  # Otherwise unittest also collects the borrowed class here.

if __name__ == '__main__':
    unittest.main()
