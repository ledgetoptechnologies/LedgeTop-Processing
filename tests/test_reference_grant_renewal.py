"""Source-only candidate renewal journal; no issuer, sockets or native engine."""
import copy
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
import uuid
from unittest.mock import patch

from processing import reference_grant_renewal as r


def stamp(ms):
    return datetime.fromtimestamp(ms/1000, timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')


class RenewalJournalTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup)
        self.path = Path(temp.name)/'candidate.sqlite'
        fixture = json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())
        self.grant = fixture['transferGrant']
        self.grant['expiresAt'] = stamp(200000)
        manifest = fixture['submission']['manifest']
        self.grant['byteBudget'] = 3*manifest['totalBytes']
        self.job = dict(jobId=self.grant['jobId'], generation=1, environment='staging',
            manifestSha256=manifest['sha256'], submission=fixture['submission'],
            transferRequest={key:self.grant[key] for key in ('reservationId','generation','expiresAt','manifestSha256','byteBudget','nonce')})
        self.policy = dict(profile=r.PROFILE, maxEpochs=3, maxLifetimeMs=1000000,
            byteBudget=self.grant['byteBudget'], attemptBudget=min(3*len(manifest['objects']),len(manifest['objects'])+10))
        self.capabilities=dict(schemaVersion=2,profile=r.PROFILE,wholeObject200=True,maxResponseBytes=16*1024**2,maxConcurrentBytes=64*1024**2,maxRequestsPerEpoch=10010,maxEpochs=32,maxEpochTtlMs=1000000,maxLifetimeMs=1000000,maxTotalRequests=10010,maxTotalBytes=self.policy['byteBudget'])
        self.connections=[]; self.addCleanup(self.close_connections)

    def close_connections(self):
        for db in self.connections:db.close()

    def connection(self):
        db=sqlite3.connect(self.path);self.connections.append(db);return db

    def journal(self, db=None, **overrides):
        return r.RenewalJournal(db or self.connection(), enabled=True, job=self.job,
            original_grant=self.grant, node_id='main', worker_id='main-worker',
            policy=overrides.get('policy',self.policy), started_ms=100000, capabilities=overrides.get('capabilities',self.capabilities))

    def request(self, journal, *, epoch=1, expiry=300000):
        last=journal.db.execute('SELECT epoch,sha,expires FROM renewal_epochs ORDER BY epoch DESC LIMIT 1').fetchone()
        return dict(schemaVersion=2,profile=r.PROFILE,requestId=str(uuid.uuid4()),
            jobId=self.job['jobId'],generation=1,environment='staging',
            reservationId=self.grant['reservationId'],manifestSha256=self.grant['manifestSha256'],
            nodeId='main',workerId='main-worker',epoch=epoch,predecessorSha256=last[1],
            expiresAt=stamp(expiry),nonce='renewal-nonce-'+str(epoch))

    def envelope(self, request):
        grant=copy.deepcopy(self.grant)
        grant.update(expiresAt=request['expiresAt'],nonce=request['nonce'],token='private-renewal-token')
        return dict(schemaVersion=2,profile=r.PROFILE,requestId=request['requestId'],
            epoch=request['epoch'],predecessorSha256=request['predecessorSha256'],grant=grant)

    def renew(self,j,epoch=1,expiry=300000,now=150000):
        request=self.request(j,epoch=epoch,expiry=expiry)
        j.request(request,now_ms=now,inflight_drained=True)
        envelope=self.envelope(request);j.accept(envelope,now_ms=now,inflight_drained=True)
        return request,envelope

    def test_disabled_before_schema_and_existing_v1_shape_untouched(self):
        db=self.connection();before=copy.deepcopy(self.grant)
        with self.assertRaisesRegex(r.RenewalHeld,'disabled'):
            r.RenewalJournal(db,job=self.job,original_grant=self.grant,node_id='main',worker_id='w',policy=self.policy,started_ms=100000)
        self.assertEqual(db.execute('SELECT count(*) FROM sqlite_master').fetchone()[0],0)
        self.journal(db);self.assertEqual(self.grant,before)

    def test_request_and_accept_exact_replay_across_reopen_redacts_receipt(self):
        j=self.journal();request=self.request(j)
        receipt=j.request(request,now_ms=150000,inflight_drained=True)
        self.assertEqual(receipt,j.request(copy.deepcopy(request),now_ms=150000,inflight_drained=True))
        envelope=self.envelope(request);ack=j.accept(envelope,now_ms=150000,inflight_drained=True)
        fresh=self.journal();self.assertEqual(ack,fresh.accept(envelope,now_ms=250000,inflight_drained=True))
        self.assertNotIn('token',json.dumps(ack));self.assertNotIn('private',json.dumps(ack))
        self.assertEqual(fresh.db.execute('SELECT count(*) FROM renewal_epochs').fetchone()[0],2)

    def test_pending_request_survives_restart_and_conflicting_replay_holds(self):
        j=self.journal();request=self.request(j);j.request(request,now_ms=150000,inflight_drained=True)
        fresh=self.journal();fresh.accept(self.envelope(request),now_ms=150000,inflight_drained=True)
        other=copy.deepcopy(request);other['nonce']='different'
        with self.assertRaises(r.RenewalHeld):fresh.request(other,now_ms=150000,inflight_drained=True)
        bad=self.envelope(request);bad['grant']['token']='a-different-private-token'
        with self.assertRaisesRegex(r.RenewalHeld,'conflict'):fresh.accept(bad,now_ms=150000,inflight_drained=True)

    def test_all_identity_and_chain_substitutions_hold_without_request_insert(self):
        j=self.journal();base=self.request(j)
        for key,value in [('jobId','other'),('generation',True),('environment','production'),
                          ('nodeId','cluster'),('workerId','other'),('reservationId','other'),
                          ('manifestSha256','1'*64),('epoch',2),('predecessorSha256','2'*64),
                          ('schemaVersion',True),('extra',1)]:
            bad=copy.deepcopy(base);bad[key]=value
            with self.subTest(key=key),self.assertRaises(r.RenewalHeld):
                j.request(bad,now_ms=150000,inflight_drained=True)
        self.assertEqual(j.db.execute('SELECT count(*) FROM renewal_requests').fetchone()[0],0)

    def test_drain_gate_and_expiry_lifetime_limits(self):
        j=self.journal();request=self.request(j)
        with self.assertRaisesRegex(r.RenewalHeld,'drain'):j.request(request,now_ms=150000)
        for expiry in (150000,200000,1100001):
            bad=self.request(j,expiry=expiry)
            with self.assertRaises(r.RenewalHeld):j.request(bad,now_ms=150000,inflight_drained=True)
        j.request(request,now_ms=150000,inflight_drained=True)
        with self.assertRaises(r.RenewalHeld):j.accept(self.envelope(request),now_ms=300000,inflight_drained=True)

    def test_full_object_charges_persist_across_epochs_and_unknown_restart(self):
        j=self.journal();obj=next(iter(j.objects));size=j.objects[obj]
        attempt=str(uuid.uuid4());one=j.charge_attempt(attempt,obj,epoch=0,now_ms=110000)
        self.assertEqual(one['chargedBytes'],size)
        fresh=self.journal();self.assertTrue(fresh.charge_attempt(attempt,obj,epoch=0,now_ms=110000)['replay'])
        self.renew(fresh)
        fresh.charge_attempt(str(uuid.uuid4()),obj,epoch=1,now_ms=150000)
        self.renew(fresh,epoch=2,expiry=400000,now=250000)
        fresh.charge_attempt(str(uuid.uuid4()),obj,epoch=2,now_ms=250000)
        with self.assertRaises(r.RenewalHeld):fresh.charge_attempt(str(uuid.uuid4()),obj,epoch=2,now_ms=250000)
        self.assertEqual(fresh.db.execute('SELECT count(*) FROM renewal_attempts WHERE object_id=?',(obj,)).fetchone()[0],3)
        self.assertEqual(fresh.db.execute('SELECT sum(bytes) FROM renewal_attempts WHERE object_id=?',(obj,)).fetchone()[0],3*size)

    def test_byte_budget_not_reset_by_new_epoch(self):
        self.policy['byteBudget']=self.grant['byteBudget']=self.job['transferRequest']['byteBudget']=self.job['submission']['manifest']['totalBytes']
        j=self.journal()
        for obj in j.objects:j.charge_attempt(str(uuid.uuid4()),obj,epoch=0,now_ms=110000)
        self.renew(j)
        with self.assertRaises(r.RenewalHeld):j.charge_attempt(str(uuid.uuid4()),next(iter(j.objects)),epoch=1,now_ms=150000)

    def test_old_epoch_and_expired_epoch_refuse_new_attempt(self):
        j=self.journal();obj=next(iter(j.objects));self.renew(j)
        for epoch,now in ((0,150000),(1,300000)):
            with self.assertRaises(r.RenewalHeld):j.charge_attempt(str(uuid.uuid4()),obj,epoch=epoch,now_ms=now)

    def test_clock_rollback_holds_after_reopen(self):
        j=self.journal();obj=next(iter(j.objects));j.charge_attempt(str(uuid.uuid4()),obj,epoch=0,now_ms=150000)
        fresh=self.journal()
        with self.assertRaisesRegex(r.RenewalHeld,'clock'):fresh.charge_attempt(str(uuid.uuid4()),obj,epoch=0,now_ms=149999)

    def test_original_latch_and_unknown_schema_hold(self):
        j=self.journal();other=copy.deepcopy(self.policy);other['maxEpochs']=2
        with self.assertRaisesRegex(r.RenewalHeld,'latch'):self.journal(policy=other)
        j.db.execute('CREATE TABLE unexpected (value TEXT)');j.db.commit()
        with self.assertRaisesRegex(r.RenewalHeld,'schema'):self.journal()

    def test_temp_and_attached_schema_hold(self):
        j=self.journal();j.db.execute('CREATE TEMP VIEW renewal_epochs AS SELECT 0 AS epoch');j.db.commit()
        with self.assertRaises(r.RenewalHeld):j.request(self.request(self.journal()),now_ms=150000,inflight_drained=True)
        j.db.execute('DROP VIEW temp.renewal_epochs');j.db.execute("ATTACH DATABASE ':memory:' AS extra")
        with self.assertRaises(r.RenewalHeld):j.charge_attempt(str(uuid.uuid4()),next(iter(j.objects)),epoch=0,now_ms=150000)

    def test_immutable_history_triggers(self):
        j=self.journal();self.renew(j)
        for table in ('renewal_requests','renewal_epochs'):
            with self.assertRaises(sqlite3.IntegrityError):j.db.execute('DELETE FROM '+table)
            j.db.rollback()

    def test_duplicate_json_every_depth_and_nonfinite_rejected(self):
        for raw in (b'{"a":1,"a":2}',b'{"a":{"x":1,"x":2}}',b'{"a":NaN}',b'[]'+b' '*r.MAX_GRANT_BYTES):
            with self.assertRaises(r.RenewalHeld):r.decode(raw)

    def test_foreign_grant_and_malformed_wrapper_hold(self):
        j=self.journal();request=self.request(j);j.request(request,now_ms=150000,inflight_drained=True)
        for key,value in [('jobId','foreign'),('byteBudget',self.grant['byteBudget']+1),('nonce','foreign'),('objectIds',[])]:
            env=self.envelope(request);env['grant'][key]=value
            with self.subTest(key=key),self.assertRaises(r.RenewalHeld):j.accept(env,now_ms=150000,inflight_drained=True)
        self.assertEqual(j.db.execute('SELECT count(*) FROM renewal_epochs').fetchone()[0],1)

    def test_server_capabilities_hold_before_any_schema_effect(self):
        variants=[('wholeObject200',False),('maxResponseBytes',2),('maxConcurrentBytes',2),
                  ('maxRequestsPerEpoch',0),('maxTotalBytes',2),('maxTotalRequests',1),
                  ('maxEpochTtlMs',99999),('maxLifetimeMs',999999),('maxEpochs',2)]
        for key,value in variants:
            capabilities=copy.deepcopy(self.capabilities);capabilities[key]=value
            db=self.connection()
            with self.subTest(key=key),self.assertRaises(r.RenewalHeld):
                self.journal(db,capabilities=capabilities)
            self.assertEqual(db.execute('SELECT count(*) FROM sqlite_master').fetchone()[0],0)

    def test_100_gets_times_33_epochs_cannot_negotiate_10000_objects(self):
        caps=copy.deepcopy(self.capabilities);caps.update(maxRequestsPerEpoch=100,maxEpochs=32,maxTotalRequests=10010,maxTotalBytes=150_000_000_000)
        policy=dict(profile=r.PROFILE,maxEpochs=32,maxLifetimeMs=1000000,byteBudget=150_000_000_000,attemptBudget=10010)
        manifest=dict(objects=[{'byteSize':5_000_000}]*10000)
        with self.assertRaisesRegex(r.RenewalHeld,'aggregate'):
            r.validate_compatibility(caps,policy=policy,manifest=manifest,started_ms=100000,original_expiry_ms=200000)
        caps['maxRequestsPerEpoch']=10010
        checked=r.validate_compatibility(caps,policy=policy,manifest=manifest,started_ms=100000,original_expiry_ms=200000)
        self.assertEqual(checked,caps)
        self.assertFalse(self.path.exists())

    def test_nonce_cannot_repeat_original_prior_or_pending(self):
        j=self.journal();request=self.request(j);request['nonce']=self.grant['nonce']
        with self.assertRaisesRegex(r.RenewalHeld,'nonce'):j.request(request,now_ms=150000,inflight_drained=True)
        first,_=self.renew(j)
        second=self.request(j,epoch=2,expiry=400000);second['nonce']=first['nonce']
        with self.assertRaisesRegex(r.RenewalHeld,'nonce'):j.request(second,now_ms=250000,inflight_drained=True)
        pending=self.request(j,epoch=2,expiry=400000);j.request(pending,now_ms=250000,inflight_drained=True)
        competing=copy.deepcopy(pending);competing['requestId']=str(uuid.uuid4())
        with self.assertRaises(r.RenewalHeld):j.request(competing,now_ms=250000,inflight_drained=True)

    def test_unknown_attempt_replay_cannot_authorize_second_get(self):
        j=self.journal();obj=next(iter(j.objects));attempt=str(uuid.uuid4())
        self.assertTrue(j.charge_attempt(attempt,obj,epoch=0,now_ms=110000)['mayStartGet'])
        fresh=self.journal()
        replay=fresh.charge_attempt(attempt,obj,epoch=0,now_ms=110000)
        self.assertFalse(replay['mayStartGet'])
        self.assertTrue(fresh.charge_attempt(str(uuid.uuid4()),obj,epoch=0,now_ms=110000)['mayStartGet'])
        self.assertEqual(fresh.db.execute('SELECT count(*) FROM renewal_attempts').fetchone()[0],2)

    def test_pending_renewal_fences_new_reads_and_accept_requires_drain(self):
        j=self.journal();request=self.request(j);j.request(request,now_ms=150000,inflight_drained=True)
        with self.assertRaisesRegex(r.RenewalHeld,'pending'):
            j.charge_attempt(str(uuid.uuid4()),next(iter(j.objects)),epoch=0,now_ms=150000)
        with self.assertRaisesRegex(r.RenewalHeld,'drain'):j.accept(self.envelope(request),now_ms=150000)
        receipt=j.reconcile(request['requestId']);self.assertFalse(receipt['accepted'])
        self.assertIsNone(receipt['grantSha256'])
        j.accept(self.envelope(request),now_ms=150000,inflight_drained=True)
        receipt=self.journal().reconcile(request['requestId']);self.assertTrue(receipt['accepted'])
        self.assertNotIn('private-renewal-token',json.dumps(receipt))

    def test_actual_per_epoch_request_ceiling_and_total_attempt_budget(self):
        caps=copy.deepcopy(self.capabilities);caps['maxRequestsPerEpoch']=1
        j=self.journal(capabilities=caps);obj=next(iter(j.objects))
        j.charge_attempt(str(uuid.uuid4()),obj,epoch=0,now_ms=110000)
        with self.assertRaises(r.RenewalHeld):j.charge_attempt(str(uuid.uuid4()),obj,epoch=0,now_ms=110000)
        self.renew(j)
        self.assertTrue(j.charge_attempt(str(uuid.uuid4()),obj,epoch=1,now_ms=150000)['mayStartGet'])

    def test_unknown_request_reconciliation_does_not_create_epoch(self):
        j=self.journal();before=j.db.total_changes
        self.assertIsNone(j.reconcile(str(uuid.uuid4())))
        self.assertEqual(j.db.total_changes,before)

    def tamper_history(self,j,sql,parameters=()):
        """Simulate a foreign preexisting dirty snapshot; restore EXACT DDL."""
        for statement in r.TRIGGERS:j.db.execute('DROP TRIGGER '+statement.split()[2])
        j.db.execute('PRAGMA ignore_check_constraints=ON')
        j.db.execute(sql,parameters);j.db.commit()
        j.db.execute('PRAGMA ignore_check_constraints=OFF')
        for statement in r.TRIGGERS:j.db.execute(statement)
        j.db.commit()

    def test_insert_or_replace_cannot_bypass_history_with_recursive_triggers_off(self):
        j=self.journal();j.db.execute('PRAGMA recursive_triggers=OFF');self.renew(j)
        obj=next(iter(j.objects));j.charge_attempt(str(uuid.uuid4()),obj,epoch=1,now_ms=150000)
        for table in ('renewal_epochs','renewal_requests','renewal_attempts','renewal_meta'):
            before=j.db.execute('SELECT * FROM '+table).fetchall()
            with self.subTest(table=table),self.assertRaisesRegex(sqlite3.IntegrityError,'collision'):
                j.db.execute('INSERT OR REPLACE INTO '+table+' SELECT * FROM '+table+' LIMIT 1')
            j.db.rollback()
            self.assertEqual(j.db.execute('SELECT * FROM '+table).fetchall(),before)
        with self.assertRaises(sqlite3.IntegrityError):
            j.db.execute('INSERT OR REPLACE INTO renewal_epochs SELECT epoch,request_id,record,sha,900000 FROM renewal_epochs WHERE epoch=0')
        j.db.rollback();j.audit()

    def test_dirty_original_expiry_cannot_authorize_get_or_survive_reopen(self):
        j=self.journal();self.tamper_history(j,'UPDATE renewal_epochs SET expires=900000 WHERE epoch=0')
        with self.assertRaisesRegex(r.RenewalHeld,'original_expiry'):
            j.charge_attempt(str(uuid.uuid4()),next(iter(j.objects)),epoch=0,now_ms=250000)
        with self.assertRaisesRegex(r.RenewalHeld,'original_expiry'):self.journal()
        self.assertEqual(j.db.execute('SELECT count(*) FROM renewal_attempts').fetchone()[0],0)

    def test_dirty_oversized_records_refused_by_sql_probe_before_decode(self):
        cases=[('renewal_meta','policy',4097),('renewal_epochs','record',r.MAX_GRANT_BYTES+1)]
        for table,column,size in cases:
            with self.subTest(table=table):
                old=self.path;self.path=old.parent/('oversized-'+table+'.sqlite')
                j=self.journal();self.path=old
                self.tamper_history(j,'UPDATE '+table+' SET '+column+'=?',('x'*size,))
                traced=[];j.db.set_trace_callback(traced.append)
                with patch.object(r,'decode',side_effect=AssertionError('unbounded decode')):
                    with self.assertRaisesRegex(r.RenewalHeld,'cell_limit'):j.audit()
                self.assertFalse(any(s.startswith('SELECT binding,policy') or s.startswith('SELECT epoch,request_id,record') for s in traced))
                j.db.set_trace_callback(None)

    def test_dirty_excess_attempts_refused_before_materialization(self):
        j=self.journal();obj=next(iter(j.objects))
        for _ in range(4):
            j.db.execute('INSERT INTO renewal_attempts VALUES(?,?,0,?)',(str(uuid.uuid4()),obj,j.objects[obj]))
        j.db.commit();traced=[];j.db.set_trace_callback(traced.append)
        with self.assertRaisesRegex(r.RenewalHeld,'row_limit'):j.audit()
        self.assertFalse(any(s.startswith('SELECT attempt_id,object_id') or s.startswith('SELECT binding,policy') for s in traced))
        j.db.set_trace_callback(None)
        with self.assertRaisesRegex(r.RenewalHeld,'row_limit'):self.journal()

    def test_dirty_epoch_count_limit_before_read_and_typed_cell_guard(self):
        j=self.journal()
        for epoch in range(1,5):j.db.execute('INSERT INTO renewal_epochs VALUES(?,NULL,?,?,?)',(epoch,'{}','0'*64,200000))
        j.db.commit()
        with self.assertRaisesRegex(r.RenewalHeld,'row_limit'):j.audit()

    def test_original_record_ordinary_oversized_write_and_clock_reset_refused(self):
        j=self.journal()
        with self.assertRaises(sqlite3.IntegrityError):
            j.db.execute('INSERT INTO renewal_requests VALUES(?,?,?,?)',(str(uuid.uuid4()),1,'x'*4097,'0'*64))
        j.db.rollback()
        j.charge_attempt(str(uuid.uuid4()),next(iter(j.objects)),epoch=0,now_ms=150000)
        with self.assertRaises(sqlite3.IntegrityError):j.db.execute('UPDATE renewal_meta SET clock=100000')
        j.db.rollback()

    def test_original_object_count_bound_before_grant_scan_or_schema(self):
        db=self.connection();self.job['submission']['manifest']['objects'] *= 10001
        with patch.object(r,'validate_grant',side_effect=AssertionError('unbounded scan')):
            with self.assertRaisesRegex(r.RenewalHeld,'object_count'):self.journal(db)
        self.assertEqual(db.execute('SELECT count(*) FROM sqlite_master').fetchone()[0],0)


if __name__ == '__main__':unittest.main()
