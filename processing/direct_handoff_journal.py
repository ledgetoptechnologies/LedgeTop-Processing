"""Unwired coordinator journal around the actual TLS worker sender.

Transport trust is supplied by a future authenticated listener. This module
never issues a Viewer custody receipt or releases a reservation; unknown
outcomes remain held for a future independently proven reconciliation.
"""
import hashlib
import hmac
import json

from . import reference_protocol as p
from .direct_worker_sender import DirectWorkerSender
from .direct_worker_receipt import verify as verify_worker_closure
from .direct_handoff_census import SESSION_ALLOWANCE, ROW_ALLOWANCE, TRIGGERS, verify_triggers, verify_census
from .maintenance import Conflict


class DirectHandoff:
    def __init__(self, store, sender, *, principal, bearer):
        if not isinstance(sender, DirectWorkerSender):raise ValueError('verified worker sender required')
        p._slug(principal,'Viewer principal')
        if (type(bearer) is not str or not 32<=len(bearer)<=4096
                or not all(33<=ord(c)<=126 for c in bearer)):
            raise ValueError('scoped Viewer bearer required')
        self.store,self.queue,self.sender=store,store.queue,sender
        self.principal,self._bearer=principal,bearer
        self.item=dict(sender._item)
        self.manifest,self.derived=sender._prepared.snapshot(self.item)
        self.binding=hashlib.sha256(p.canonical_json(self.item)).hexdigest()
        self._validated=False

    def _authorize(self, token, protected_transport):
        if (protected_transport is not True or type(token) is not str or not token.isascii()
                or not hmac.compare_digest(token,self._bearer)):
            raise Conflict('direct handoff authorization required')

    def _check(self,db,*,full=False):
        r=db.execute('SELECT * FROM direct_intake_reservations WHERE reservation_id=?',
                     (self.item['reservationId'],)).fetchone()
        if r is None:raise Conflict('direct node reservation required')
        expected=dict(jobId='job_id',attemptId='attempt_id',generation='generation',environment='environment',
                      authorityId='authority_id',nodeId='node_id',workerId='worker_id',
                      workerInstanceId='instance_id',manifestSha256='manifest_sha',
                      derivedManifestSha256='derived_manifest_sha')
        if r['state']!='receiving' or any(self.item[k]!=r[v] or type(self.item[k]) is not type(r[v]) for k,v in expected.items()):
            raise Conflict('direct node reservation binding changed')
        profile=db.execute('SELECT principal,profile FROM direct_intake_profile_bindings WHERE reservation_id=?',
                           (r['reservation_id'],)).fetchone()
        if (profile is None or profile['principal']!=self.principal
                or profile['profile']!=self.item.get('limitProfile')):
            raise Conflict('direct principal or profile binding changed')
        row=db.execute('''SELECT r.node_id,r.manifest_sha,r.environment,r.generation,j.status,j.held_reason,
            EXISTS(SELECT 1 FROM reference_source_intakes s WHERE s.job_id=r.job_id) AS has_source
            FROM reference_jobs r JOIN jobs j ON j.id=r.job_id WHERE r.job_id=?''',(r['job_id'],)).fetchone()
        if (row is None or row['status']!='held' or row['held_reason']!='direct_intake_receiving_profile'
                or row['node_id']!=r['node_id'] or row['has_source']
                or row['manifest_sha']!=self.item['manifestSha256']
                or row['environment']!=self.item['environment'] or row['generation']!=self.item['generation']):
            raise Conflict('direct source reservation changed')
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='direct_handoff_sessions'").fetchone():
            verify_triggers(db)
        else:
            full=True
        if full or not self._validated:
            verify_census(db,full=True)
            record=self.store._row(db,r['job_id']);submission=json.loads(record['input_json'])
            if (type(submission) is not dict or type(submission.get('directIntakeVersion')) is not int
                    or submission['directIntakeVersion']!=1
                    or submission.get('directDerivedManifestSha256')!=self.item['derivedManifestSha256']
                    or submission.get('submissionId')!=self.item['attemptId'] or 'sourceIntakeVersion' in submission
                    or self.store._reference_input(db,dict(id=r['job_id'],environment=r['environment'])) is None
                    or p.canonical_json(submission['manifest'])!=p.canonical_json(self.manifest)
                    or self.item['inputBytes']!=self.manifest['totalBytes']):
                raise Conflict('direct source reservation changed')
            self._validated=True
        return r

    def _tables(self,db):
        db.execute('''CREATE TABLE IF NOT EXISTS direct_handoff_sessions (
            reservation TEXT PRIMARY KEY, principal TEXT NOT NULL, binding TEXT NOT NULL,
            allowance INTEGER NOT NULL, state TEXT NOT NULL, closure BLOB, object_count INTEGER NOT NULL)''')
        db.execute('''CREATE TABLE IF NOT EXISTS direct_handoff_objects (
            reservation TEXT NOT NULL, object_id TEXT NOT NULL, sha TEXT NOT NULL,
            size INTEGER NOT NULL, state TEXT NOT NULL, response BLOB,
            PRIMARY KEY(reservation,object_id))''')
        for name,sql in TRIGGERS.items():
            if not db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND name=?",(name,)).fetchone():
                db.execute(sql)
        verify_triggers(db)

    def _session(self,db):
        self._check(db);self._tables(db)
        row=db.execute('SELECT * FROM direct_handoff_sessions WHERE reservation=?',
                       (self.item['reservationId'],)).fetchone()
        if row is None:
            self._check(db,full=True)
            allowance=SESSION_ALLOWANCE+ROW_ALLOWANCE*len(self.manifest['objects'])
            self.store._quota(db,allowance)
            db.execute('INSERT INTO direct_handoff_sessions VALUES(?,?,?,?,?,NULL,0)',
                       (self.item['reservationId'],self.principal,self.binding,allowance,'receiving'))
            return 'receiving'
        if (row['principal']!=self.principal or row['binding']!=self.binding
                or row['allowance']!=SESSION_ALLOWANCE+ROW_ALLOWANCE*len(self.manifest['objects'])
                or type(row['object_count']) is not int or not 0<=row['object_count']<=len(self.manifest['objects'])
                or row['state'] not in ('receiving','closure_pending','closure_observed')):
            raise Conflict('direct handoff session changed')
        return row['state']

    def _object_observation(self, result, obj):
        expected=dict(protocol='ltds-direct-local-object-v1',reservationId=self.item['reservationId'],
            scratchBindingSha256=self.sender._sha,jobId=self.item['jobId'],attemptId=self.item['attemptId'],
            generation=self.item['generation'],nodeId=self.item['nodeId'],objectId=obj['id'],
            byteSize=obj['byteSize'],sha256=obj['sha256'],localObjectDurable=True,
            sourceCustodyAccepted=False,transferComplete=False)
        if (type(result) is not dict or set(result)!=set(expected)|{'replayed'}
                or type(result['replayed']) is not bool
                or p.canonical_json({k:v for k,v in result.items() if k!='replayed'})!=p.canonical_json(expected)):
            raise Conflict('direct worker observation changed')
        return result

    def send_object(self, token, *, protected_transport, object_id, byte_size, sha256, reader):
        self._authorize(token,protected_transport)
        obj=self.sender._prepared.object(self.item,object_id)
        if type(byte_size) is not int or byte_size!=obj['byteSize'] or sha256!=obj['sha256']:
            raise Conflict('direct source object binding changed')
        with self.queue._coordinator_freeze.operation('source-io'):
            with self.queue._db() as db:
                db.execute('BEGIN IMMEDIATE')
                if self._session(db)!='receiving':raise Conflict('direct source input closed')
                prior=db.execute('SELECT * FROM direct_handoff_objects WHERE reservation=? AND object_id=?',
                                 (self.item['reservationId'],object_id)).fetchone()
                if prior:
                    if (prior['sha'],prior['size'])!=(sha256,byte_size):raise Conflict('direct object binding changed')
                    if prior['state']=='observed' and prior['response'] is not None:
                        return self._object_observation(json.loads(prior['response']),obj)
                    raise Conflict('direct object outcome uncertain; reconciliation required')
                db.execute('INSERT INTO direct_handoff_objects VALUES(?,?,?,?,?,NULL)',
                           (self.item['reservationId'],object_id,sha256,byte_size,'intent'))
            # No DB lock is held during network I/O. Intent remains durable on
            # any failure or process death; no automatic repeat or release.
            result=self._object_observation(self.sender.put(object_id,reader),obj)
            wire=p.canonical_json(result)
            if len(wire)>768:raise Conflict('direct observation too large')
            with self.queue._db() as db:
                db.execute('BEGIN IMMEDIATE');self._check(db)
                changed=db.execute('''UPDATE direct_handoff_objects SET state='observed',response=?
                    WHERE reservation=? AND object_id=? AND sha=? AND size=? AND state='intent' AND response IS NULL''',
                    (wire,self.item['reservationId'],object_id,sha256,byte_size)).rowcount
                if changed!=1:raise Conflict('direct observation outcome uncertain')
            return result

    def _expected_closure(self):
        result=dict(protocol='ltds-direct-local-closure-v1',reservationId=self.item['reservationId'],
            scratchBindingSha256=self.sender._sha,authorityId=self.item['authorityId'],nodeId=self.item['nodeId'],
            workerId=self.item['workerId'],workerInstanceId=self.item['workerInstanceId'],jobId=self.item['jobId'],
            attemptId=self.item['attemptId'],generation=self.item['generation'],environment=self.item['environment'],
            manifestSha256=self.item['manifestSha256'],derivedManifestSha256=self.item['derivedManifestSha256'],
            sourceObjectCount=len(self.manifest['objects'])-len(self.derived),derivedObjectCount=len(self.derived),
            localClosureComplete=True,sourceCustodyAccepted=False,processingAuthorized=False)
        if 'limitProfile' in self.item:result['limitProfile']=self.item['limitProfile']
        return result

    def _verify_closure(self, result):
        return verify_worker_closure(result,expected_closure=self._expected_closure(),
                                     issuer_id=self.sender._issuer,key_id=self.sender._key,
                                     public_key=self.sender._public)

    def complete(self, token, *, protected_transport):
        """Persist worker-local closure, never issue a Viewer custody receipt."""
        self._authorize(token,protected_transport)
        with self.queue._coordinator_freeze.operation('source-io'):
            with self.queue._db() as db:
                db.execute('BEGIN IMMEDIATE');state=self._session(db)
                # Completion must revalidate the full pinned source inventory,
                # even though ordinary object transfers use scalar checks.
                self._check(db,full=True)
                if state=='closure_observed':
                    saved=db.execute('SELECT closure FROM direct_handoff_sessions WHERE reservation=?',
                                     (self.item['reservationId'],)).fetchone()['closure']
                    if saved is None:raise Conflict('direct closure record missing')
                    return self._verify_closure(json.loads(saved))
                if state!='receiving':raise Conflict('direct closure outcome uncertain')
                rows=db.execute('SELECT * FROM direct_handoff_objects WHERE reservation=?',
                                (self.item['reservationId'],)).fetchall()
                if len(rows)!=len(self.manifest['objects']):
                    raise Conflict('direct source objects incomplete or uncertain')
                for row in rows:
                    if row['state']!='observed' or row['response'] is None:
                        raise Conflict('direct source objects incomplete or uncertain')
                    obj=self.sender._prepared.object(self.item,row['object_id'])
                    if (row['sha'],row['size'])!=(obj['sha256'],obj['byteSize']):
                        raise Conflict('direct source observation binding changed')
                    self._object_observation(json.loads(row['response']),obj)
                db.execute("UPDATE direct_handoff_sessions SET state='closure_pending' WHERE reservation=?",
                           (self.item['reservationId'],))
            result=self._verify_closure(self.sender.complete())
            wire=p.canonical_json(result)
            if len(wire)>65536:raise Conflict('direct closure observation too large')
            with self.queue._db() as db:
                db.execute('BEGIN IMMEDIATE');self._check(db)
                changed=db.execute("UPDATE direct_handoff_sessions SET state='closure_observed',closure=? WHERE reservation=? AND state='closure_pending' AND closure IS NULL",
                                   (wire,self.item['reservationId'])).rowcount
                if changed!=1:raise Conflict('direct closure outcome uncertain')
            return result

    def status(self, token, *, protected_transport):
        self._authorize(token,protected_transport)
        with self.queue._db() as db:
            self._check(db)
            if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='direct_handoff_sessions'").fetchone():
                return dict(state='not_started',sourceCustodyAccepted=False)
            row=db.execute('SELECT * FROM direct_handoff_sessions WHERE reservation=?',
                           (self.item['reservationId'],)).fetchone()
            if row is None:return dict(state='not_started',sourceCustodyAccepted=False)
            if row['principal']!=self.principal or row['binding']!=self.binding:raise Conflict('direct handoff session changed')
            counts={r['state']:r['n'] for r in db.execute('SELECT state,count(*) AS n FROM direct_handoff_objects WHERE reservation=? GROUP BY state',
                    (self.item['reservationId'],))}
            return dict(state=row['state'],observedObjects=counts.get('observed',0),
                        uncertainObjects=counts.get('intent',0),sourceCustodyAccepted=False)
