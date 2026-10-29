"""Actual driver/ledger/broker input copies; transport fixtures, no live services."""
from contextlib import closing
import copy
import io
import json
from pathlib import Path
import sqlite3
import unittest
from unittest.mock import patch

from processing.reference_driver import ReferenceDriver
from processing.reference_transfer import validate_grant, TransferError
from processing.scratch_policy import ScratchLedger
from tests import test_reference_driver as driver_fixture


class InputCopyDriverTests(unittest.TestCase):
    def setUp(self):
        self.f = driver_fixture.ReferenceDriverTests()
        self.f.setUp()
        self.addCleanup(self.f.tearDown)
        self.f.patches.pop().stop()  # Use the actual BrokerClient download implementation.
        self.downloads = []
        self.f.broker._open = self.broker_open
        self.f.ledger = ScratchLedger(self.f.root / 'ledger.sqlite', self.f.scratch,
            capacity_probe=lambda _: 10**9, safety_floor_bytes=1024, cleanup_grace_seconds=0,
            input_copy_accounting=True)
        self.f.ledger.enable_input_copy_accounting()
        self.f.worker.ledger = self.f.ledger
        self.f.driver = self.make_driver()

    def make_driver(self, **changes):
        values = dict(state_path=self.f.root / 'input-driver.sqlite', ssl_context=self.f.context,
            output_budget=1024*1024, transfer_ttl=240, safety_floor=1024,
            failure_outbox=self.f.failures, worker_instance_id='instance', input_copy_accounting=True)
        values.update(changes)
        driver = ReferenceDriver(self.f.worker, self.f.ledger, self.f.outbox, self.f.broker,
            'https://coordinator.invalid:9443', driver_fixture.TOKEN, **values)
        driver._request = self.f.request
        driver.sender._open = self.f.sender_open
        return driver

    def broker_open(self, request, timeout):
        object_id = request.full_url.rsplit('/',1)[1]
        self.downloads.append(object_id)
        content = self.f.contents[object_id]
        response = io.BytesIO(content)
        response.status = 200
        response.headers = {'Content-Length': str(len(content))}
        response.geturl = lambda: request.full_url
        return response

    def row(self, job):
        with self.f.driver._db() as db:
            row = db.execute('SELECT stage,data FROM driver_jobs WHERE job=?',(job['jobId'],)).fetchone()
        return row['stage'], json.loads(row['data'])

    def copies(self):
        with self.f.ledger._transaction() as db:
            return [dict(row) for row in db.execute('SELECT * FROM input_copies ORDER BY id')]

    def fetching(self, contents=(b'first-bytes',b'second-bytes')):
        job = self.f.submit(contents=contents)
        self.f.advance(job,'fetching')
        return job

    def test_full_original_closure_precedes_first_real_broker_download(self):
        job = self.fetching()
        stage,data = self.row(job)
        self.assertEqual(stage,'fetching');self.assertEqual(self.downloads,[])
        rows = self.copies();self.assertEqual(len(rows),2)
        self.assertTrue(all(row['fetched'] is None for row in rows))
        identity = data['inputAccounting']['binding']
        self.assertEqual((identity['jobId'],identity['environment'],identity['generation'],identity['inputManifestSha256'],identity['pipelineDigest']),
                         (job['jobId'],'staging',1,job['manifestSha256'],job['pipelineDigest']))
        for row in rows:
            self.assertEqual(row['path'],'jobs/'+data['reservation']['reservationId']+'/'+row['id']+'.input')
        self.assertEqual(self.f.driver.step()['state'],'fetching')
        self.assertEqual(len(self.downloads),1)
        self.assertEqual(sum(row['fetched'] is not None for row in self.copies()),1)
        self.assertEqual(self.f.driver.step()['state'],'fetching')
        self.assertEqual(len(self.downloads),2)
        self.assertEqual(self.f.driver.step()['state'],'processing_pending')
        with self.f.ledger._transaction() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM input_releases').fetchone()[0],0)
        self.assertTrue(all((self.f.scratch/row['path']).exists() for row in self.copies()))

    def test_real_broker_integrity_failure_never_confirms_a_copy(self):
        self.fetching();self.f.broker._open = lambda request,timeout: self.bad_response(request)
        self.assertEqual(self.f.driver.step()['state'],'retry_wait')
        self.assertTrue(all(row['fetched'] is None for row in self.copies()))
        self.assertFalse(list(self.f.scratch.glob('jobs/*/*.input')))

    def test_driver_rechecks_bytes_even_when_downloader_claims_success(self):
        self.fetching()
        def wrong(grant,object_id,target):
            Path(target).write_bytes(b'wrong');return target
        with patch.object(self.f.broker,'download_object',side_effect=wrong):
            self.assertEqual(self.f.driver.step(),{'state':'held','reason':'input_copy_integrity_changed'})
        self.assertTrue(all(row['fetched'] is None for row in self.copies()))

    def test_registration_commit_loss_replays_only_its_persisted_intent(self):
        job=self.f.submit();self.f.advance(job,'claimed');original=self.f.ledger.register_input_binding
        def lost(*args,**kwargs):
            original(*args,**kwargs);raise RuntimeError('synthetic lost registration return')
        with patch.object(self.f.ledger,'register_input_binding',side_effect=lost):
            self.assertEqual(self.f.driver.step()['state'],'retry_wait')
        self.assertEqual(self.row(job)[1]['inputAccounting']['state'],'binding_pending')
        self.assertEqual(self.downloads,[]);self.assertEqual(len(self.copies()),1)
        self.f.driver=self.make_driver();self.f.advance(job,'fetching')
        self.assertEqual(self.downloads,[]);self.assertEqual(self.row(job)[1]['inputAccounting']['state'],'bound')

    @staticmethod
    def bad_response(request):
        response=io.BytesIO(b'bad');response.status=200;response.headers={'Content-Length':'3'}
        response.geturl=lambda:request.full_url
        return response

    def test_crash_after_publication_before_confirmation_rehashes_without_redownload(self):
        job=self.fetching(contents=(b'published-object',));original=self.f.broker.download_object
        def lost(*args):
            original(*args);raise TransferError('synthetic lost publication return')
        with patch.object(self.f.broker,'download_object',side_effect=lost):
            self.assertEqual(self.f.driver.step()['state'],'retry_wait')
        self.assertIsNone(self.copies()[0]['fetched'])
        self.f.driver=self.make_driver()
        with patch.object(self.f.worker,'_file',wraps=self.f.worker._file) as verify:
            self.assertEqual(self.f.driver.step()['state'],'fetching')
            self.assertEqual(verify.call_count,1)
        self.assertEqual(len(self.downloads),1)
        self.assertIsNotNone(self.copies()[0]['fetched'])
        self.assertEqual(self.row(job)[1]['inputAccounting']['next'],1)

    def test_crash_after_confirm_before_journal_replays_exact_verification(self):
        job=self.fetching(contents=(b'confirmed-object',));original=self.f.ledger.confirm_input_copy
        def lost(*args,**kwargs):
            original(*args,**kwargs);raise RuntimeError('synthetic lost confirmation return')
        with patch.object(self.f.ledger,'confirm_input_copy',side_effect=lost):
            self.assertEqual(self.f.driver.step()['state'],'retry_wait')
        self.assertIsNotNone(self.copies()[0]['fetched']);self.assertEqual(self.row(job)[1]['inputAccounting']['next'],0)
        self.f.driver=self.make_driver()
        self.assertEqual(self.f.driver.step()['state'],'fetching')
        self.assertEqual(len(self.downloads),1)

    def test_crash_after_progress_commit_reconciles_prefix_once(self):
        job=self.fetching();original=self.f.driver._save
        def lost(job_id,stage,data):
            original(job_id,stage,data)
            if data.get('inputPaths'):raise RuntimeError('synthetic lost journal commit return')
        with patch.object(self.f.driver,'_save',side_effect=lost):
            self.assertEqual(self.f.driver.step()['state'],'retry_wait')
        self.assertEqual(self.row(job)[1]['inputAccounting']['next'],1)
        self.f.driver=self.make_driver()
        with patch.object(self.f.worker,'_file',wraps=self.f.worker._file) as verify:
            self.assertEqual(self.f.driver.step()['state'],'fetching')
            self.assertEqual(verify.call_count,2)  # Cold prefix plus the next object, exactly once each.
            self.assertEqual(self.f.driver.step()['state'],'processing_pending')
            self.assertEqual(verify.call_count,2)
        self.assertEqual(len(self.downloads),2)

    def test_corrupted_recovered_prefix_holds_before_any_more_downloads(self):
        self.fetching();self.f.driver.step();row=next(row for row in self.copies() if row['fetched'] is not None)
        (self.f.scratch/row['path']).write_bytes(b'changed')
        self.f.driver=self.make_driver()
        self.assertEqual(self.f.driver.step(),{'state':'held','reason':'input_copy_integrity_changed'})
        self.assertEqual(len(self.downloads),1)

    def test_cold_journal_path_drift_does_not_confirm_an_unlisted_path(self):
        job=self.fetching();self.f.driver.step();stage,data=self.row(job)
        key=next(iter(data['inputPaths']));data['inputPaths'][key]='jobs/'+data['reservation']['reservationId']+'/unlisted.input'
        self.f.driver._save(job['jobId'],stage,data);self.f.driver=self.make_driver()
        self.assertEqual(self.f.driver.step()['state'],'held');self.assertEqual(len(self.downloads),1)

    def test_hot_prefix_path_drift_cannot_reach_execution(self):
        job=self.fetching();self.f.driver.step();stage,data=self.row(job)
        key=next(iter(data['inputPaths']));data['inputPaths'][key]='jobs/'+data['reservation']['reservationId']+'/unlisted.input'
        self.f.driver._save(job['jobId'],stage,data)
        self.assertEqual(self.f.driver.step()['state'],'fetching')
        self.assertEqual(self.f.driver.step()['state'],'held')
        self.assertFalse(any(call[0]=='initialize' for call in self.f.transport.calls))

    def test_manifest_or_generation_drift_holds_before_download(self):
        job=self.fetching();stage,data=self.row(job);data['claim']['generation']=2
        self.f.driver._save(job['jobId'],stage,data);self.f.driver=self.make_driver()
        self.assertEqual(self.f.driver.step(),{'state':'held','reason':'input_copy_grant_changed'})
        self.assertEqual(self.downloads,[])

    def test_pipeline_body_drift_holds_before_download(self):
        job=self.fetching();stage,data=self.row(job)
        data['claim']['submission']['pipeline']['odm']['options']['unexpected-option']=True
        self.f.driver._save(job['jobId'],stage,data)
        self.assertEqual(self.f.driver.step(),{'state':'held','reason':'input_copy_manifest_changed'})
        self.assertEqual(self.downloads,[])

    def test_success_retains_all_inputs_without_inventing_release_authority(self):
        job=self.f.submit(contents=(b'production-one',b'production-two'),environment='production')
        self.f.advance(job,'awaiting_acceptance')
        self.assertEqual(self.f.queue.get(job['jobId'])['status'],'succeeded')
        self.assertTrue(all(row['fetched'] is not None and (self.f.scratch/row['path']).exists() for row in self.copies()))
        self.assertIsNone(self.f.ledger.release_authority)
        self.assertEqual(self.f.ledger.input_cleanup_candidates(),[])

    def test_failed_engine_retains_fetched_inputs_without_release(self):
        self.f.transport.code=30;job=self.f.submit()
        self.f.advance(job,'awaiting_failure_acceptance')
        self.assertEqual(self.f.queue.get(job['jobId'])['status'],'failed')
        self.assertTrue(all((self.f.scratch/row['path']).exists() for row in self.copies()))
        self.assertEqual(self.f.ledger.input_cleanup_candidates(),[])

    def test_missing_committed_input_binding_is_not_backfilled(self):
        self.fetching()
        with self.f.ledger._transaction() as db:db.execute('DELETE FROM input_bindings')
        self.f.driver=self.make_driver()
        self.assertEqual(self.f.driver.step()['state'],'held');self.assertEqual(self.downloads,[])
        with self.f.ledger._transaction() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM input_bindings').fetchone()[0],0)

    def test_unowned_existing_file_without_attempt_is_not_adopted(self):
        self.fetching();row=self.copies()[0];target=self.f.scratch/row['path'];target.parent.mkdir(parents=True,exist_ok=True)
        target.write_bytes(self.f.contents[row['id']])
        self.assertEqual(self.f.driver.step()['state'],'held');self.assertEqual(self.downloads,[])
        self.assertIsNone(self.copies()[0]['fetched'])

    def test_late_profile_flip_and_default_marked_profile_are_refused(self):
        with self.assertRaises(ValueError):self.make_driver(input_copy_accounting=False)
        with self.assertRaises(ValueError):self.make_driver(input_copy_accounting=1)
        self.f.driver.input_copy_accounting=False
        self.assertEqual(self.f.driver.step(),{'state':'held','reason':'input_copy_profile_changed'})
        self.f.driver.input_copy_accounting=True;self.f.ledger.input_copy_accounting=False
        self.assertEqual(self.f.driver.step(),{'state':'held','reason':'input_copy_profile_changed'})

    def test_legacy_driver_binding_is_not_implicitly_adopted(self):
        legacy=self.f.root/'driver.sqlite';before=legacy.read_bytes()
        with self.assertRaises(ValueError):self.make_driver(state_path=legacy)
        self.assertEqual(legacy.read_bytes(),before)

    def test_pre_enabled_ledger_is_required_without_implicit_enable(self):
        root=self.f.root/'unlatched';root.mkdir()
        ledger=ScratchLedger(root/'ledger.sqlite',root,capacity_probe=lambda _:10**9,safety_floor_bytes=1024,input_copy_accounting=True)
        original=self.f.ledger;self.f.ledger=ledger;self.f.worker.ledger=ledger
        try:
            with self.assertRaises(ValueError):self.make_driver(state_path=root/'driver.sqlite')
            with ledger._transaction() as db:
                self.assertNotIn('inputCopyAccountingVersion',ledger.validate_accounting(db))
                self.assertFalse(db.execute("SELECT 1 FROM sqlite_master WHERE name='input_copies'").fetchone())
        finally:self.f.ledger=original;self.f.worker.ledger=original

    def test_new_bookkeeping_has_bounded_full_scans_for_10000_one_object_advances(self):
        # Isolate NEW bookkeeping from the existing quadratic full-claim/grant/JSON path.
        # Actual driver helper, actual ledger, actual BrokerClient and actual 10k tiny files.
        self.f.store.local_max_photos = 10000  # Synthetic one-node control fixture only; no physical routing claim.
        job=self.fetching(contents=(b'z',)*10000);_,data=self.row(job)
        grant=validate_grant(data['grant'],job=data['claim'])
        with patch.object(self.f.ledger,'_closure_matches',wraps=self.f.ledger._closure_matches) as scans, \
                patch.object(self.f.ledger,'register_input_binding',wraps=self.f.ledger.register_input_binding) as register, \
                patch.object(self.f.ledger,'confirm_input_copy',wraps=self.f.ledger.confirm_input_copy) as confirm, \
                patch.object(self.f.worker,'_file',wraps=self.f.worker._file) as verify, \
                patch.object(self.f.driver,'_save'):
            for _ in range(10000):
                self.assertEqual(self.f.driver._fetch_input_copy(job['jobId'],data['claim'],data,grant)['state'],'fetching')
            self.assertEqual(self.f.driver._fetch_input_copy(job['jobId'],data['claim'],data,grant)['state'],'processing_pending')
            self.assertEqual(register.call_count,0)  # Original registration already durably preceded fetching.
            self.assertEqual(scans.call_count,2)  # Cold session admission and final reconciliation only.
            self.assertEqual(confirm.call_count,10000);self.assertEqual(verify.call_count,10000)
        self.assertEqual(len(self.downloads),10000)
        self.assertEqual(data['inputAccounting']['next'],10000)
        self.assertEqual(data['inputAccounting']['byteCount'],10000)


if __name__=='__main__':unittest.main()
