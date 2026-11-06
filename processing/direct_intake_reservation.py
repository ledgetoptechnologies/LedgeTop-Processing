"""Unwired direct-intake reservation prototype; no transfer or acceptance API.

The caller must authenticate a future direct-only submission and pin its worker
identities. Reserving a node does not establish worker scratch custody, accept
source bytes, authorize execution, or release the reservation. Never wire this
prototype into runtime until freeze, quota, recovery and worker-proof contracts
are integrated and independently qualified.
"""
import json

from . import reference_protocol as protocol
from .maintenance import Conflict


def verify_profile_schema(db):
    rows=db.execute('PRAGMA table_info(direct_intake_profile_bindings)').fetchall()
    expected=[('reservation_id','TEXT',1,1),('principal','TEXT',1,0),('operation','TEXT',1,0),('profile','TEXT',1,0)]
    if [(r[1],r[2].upper(),r[3],r[5]) for r in rows]!=expected:
        raise Conflict('direct profile binding schema requires inspection')


class DirectIntakeReservations:
    def __init__(self, store, *, worker_bindings):
        self.store = store
        self.queue = store.queue
        if type(worker_bindings) is not dict or set(worker_bindings) != set(self.queue.node_ids):
            raise ValueError('complete trusted worker inventory required')
        self.workers = {}
        for node, worker in worker_bindings.items():
            if type(worker) is not dict or set(worker) != {'workerId', 'workerInstanceId'}:
                raise ValueError('closed worker binding required')
            for value in worker.values():
                protocol._slug(value, 'worker binding')
            self.workers[node] = dict(worker)
        # This prototype is not mounted by the runtime. Table creation itself
        # participates in the freeze gate, as does every reservation.
        with self.queue._coordinator_freeze.operation('source-io-admission'), self.queue._db() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS direct_intake_reservations (
                reservation_id TEXT PRIMARY KEY, job_id TEXT UNIQUE NOT NULL,
                attempt_id TEXT NOT NULL, generation INTEGER NOT NULL,
                environment TEXT NOT NULL, authority_id TEXT NOT NULL, manifest_sha TEXT NOT NULL, derived_manifest_sha TEXT NOT NULL,
                node_id TEXT UNIQUE NOT NULL, worker_id TEXT NOT NULL,
                instance_id TEXT NOT NULL, state TEXT NOT NULL CHECK(state='receiving'),
                FOREIGN KEY(job_id) REFERENCES reference_jobs(job_id))''')
            columns={row['name'] for row in db.execute('PRAGMA table_info(direct_intake_reservations)')}
            if columns!={'reservation_id','job_id','attempt_id','generation','environment','authority_id',
                         'manifest_sha','derived_manifest_sha','node_id','worker_id','instance_id','state'}:
                raise Conflict('direct reservation schema requires inspection')

    def _select_profile(self,db,selection,*,reservation_id,job_id,attempt_id,generation,environment,node_id,manifest_sha,derived_manifest_sha):
        from . import direct_negotiation_candidate as n
        if type(selection) is not dict or set(selection)!={'request','capabilities','principal','worker','guard'}:
            raise Conflict('trusted profile selection required')
        req=n._decode(selection['request'],n.MAX_REQUEST_BYTES)
        expected=dict(reservationId=reservation_id,jobId=job_id,attemptId=attempt_id,generation=generation,
            environment=environment,authorityId=self.queue.authority_id,nodeId=node_id,
            workerId=self.workers[node_id]['workerId'],workerInstanceId=self.workers[node_id]['workerInstanceId'],
            manifestSha256=manifest_sha,derivedManifestSha256=derived_manifest_sha)
        if any(req.get(key)!=value or type(req.get(key)) is not type(value) for key,value in expected.items()):
            raise Conflict('profile reservation binding changed')
        response=n.negotiate(db,selection['request'],selection['capabilities'],
            authenticated_principal=selection['principal'],authenticated_worker=selection['worker'],
            admission_guard=selection['guard'],_store=self.store,_inside_transaction=True)
        db.execute('''CREATE TABLE IF NOT EXISTS direct_intake_profile_bindings (
            reservation_id TEXT NOT NULL PRIMARY KEY,principal TEXT NOT NULL,operation TEXT NOT NULL,profile TEXT NOT NULL)''')
        verify_profile_schema(db)
        values=(reservation_id,req['principalId'],req['operationId'],req['profile'])
        prior=db.execute('SELECT reservation_id,principal,operation,profile FROM direct_intake_profile_bindings WHERE reservation_id=?',
                         (reservation_id,)).fetchone()
        if prior is None:
            self.store._quota(db,sum(len(v.encode()) for v in values))
            db.execute('INSERT INTO direct_intake_profile_bindings VALUES(?,?,?,?)',values)
        elif tuple(prior)!=values:
            raise Conflict('direct profile reservation changed')
        return response

    def reserve(self, *, job_id, attempt_id, generation, reservation_id, manifest_sha, derived_manifest_sha, profile_selection=None):
        """Reserve one eligible idle node, or return None without mutation.

        Exact retries return the persisted identity. A reservation holds the node
        across coordinator restarts; there is deliberately no timeout or release.
        """
        if (type(profile_selection) is not dict or
                set(profile_selection)!={'request','capabilities','principal','worker','guard'} or
                type(profile_selection['worker']) is not dict):
            raise Conflict('trusted profile selection required; older direct records require inspection')
        profile_selection=dict(profile_selection,worker=dict(profile_selection['worker']))
        for value, label in ((job_id, 'job'), (attempt_id, 'attempt'),
                             (reservation_id, 'reservation')):
            protocol._uuid(value, label)
        protocol._hash(manifest_sha, 'manifest digest')
        protocol._hash(derived_manifest_sha, 'derived manifest digest')
        if type(generation) is not int or generation < 1:
            raise ValueError('positive generation required')
        with self.queue._coordinator_freeze.operation('source-io-admission'), self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            prior = db.execute('SELECT * FROM direct_intake_reservations WHERE reservation_id=? OR job_id=?',
                               (reservation_id, job_id)).fetchall()
            if prior:
                if len(prior) != 1:
                    raise Conflict('direct reservation identity collision')
                row = prior[0]
                job = self.store._row(db, job_id)
                saved_submission = json.loads(job['input_json'])
                if ((row['reservation_id'], row['job_id'], row['attempt_id'], row['generation'], row['manifest_sha'], row['derived_manifest_sha'], row['authority_id']) !=
                        (reservation_id, job_id, attempt_id, generation, manifest_sha, derived_manifest_sha, self.queue.authority_id)
                        or job is None or (job['status'], job['node_id']) != ('held', row['node_id'])
                        or job['held_reason'] not in ('direct_intake_receiving','direct_intake_receiving_profile')
                        or job['generation'] != generation or job['environment'] != row['environment']
                        or job['manifest_sha'] != manifest_sha or job['source_state'] is not None
                        or type(saved_submission) is not dict
                        or type(saved_submission.get('directIntakeVersion')) is not int
                        or saved_submission.get('directIntakeVersion') != 1
                        or saved_submission.get('directDerivedManifestSha256') != derived_manifest_sha
                        or 'sourceIntakeVersion' in saved_submission
                        or saved_submission.get('submissionId') != attempt_id
                        or self.store._reference_input(db, dict(id=job_id, environment=job['environment'])) is None
                        or row['node_id'] not in self.workers
                        or (row['worker_id'], row['instance_id']) !=
                        (self.workers[row['node_id']]['workerId'], self.workers[row['node_id']]['workerInstanceId'])):
                    raise Conflict('direct reservation binding changed')
                if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='direct_intake_profile_bindings'").fetchone():
                    verify_profile_schema(db)
                    bound=db.execute('SELECT 1 FROM direct_intake_profile_bindings WHERE reservation_id=?',(reservation_id,)).fetchone()
                else:bound=None
                requires_profile=job['held_reason']=='direct_intake_receiving_profile'
                if requires_profile and (not bound or profile_selection is None):raise Conflict('direct profile replay required')
                if bound and not requires_profile:raise Conflict('direct profile reservation changed')
                if profile_selection is not None:
                    if not bound:raise Conflict('cannot change direct reservation profile')
                    self._select_profile(db,profile_selection,reservation_id=reservation_id,job_id=job_id,attempt_id=attempt_id,
                        generation=generation,environment=row['environment'],node_id=row['node_id'],manifest_sha=manifest_sha,
                        derived_manifest_sha=derived_manifest_sha)
                return dict(reservationId=reservation_id, jobId=job_id, attemptId=attempt_id,
                            generation=generation, environment=row['environment'], authorityId=row['authority_id'],
                            manifestSha256=manifest_sha, derivedManifestSha256=derived_manifest_sha,
                            nodeId=row['node_id'], workerId=row['worker_id'],
                            workerInstanceId=row['instance_id'], state='receiving', replayed=True,
                            **({'limitProfile':json.loads(profile_selection['request'])['profile']} if profile_selection is not None else {}))
            row = self.store._row(db, job_id)
            submission = json.loads(row['input_json'])
            if (row['status'] != 'queued' or row['phase'] not in ('queued', 'waiting_capacity')
                    or row['source_state'] is not None
                    or type(submission) is not dict or submission.get('directIntakeVersion') != 1
                    or type(submission.get('directIntakeVersion')) is not int
                    or submission.get('directDerivedManifestSha256') != derived_manifest_sha
                    or submission.get('submissionId') != attempt_id or row['generation'] != generation
                    or row['manifest_sha'] != manifest_sha
                    or 'sourceIntakeVersion' in submission):
                raise Conflict('direct-only queued job binding required')
            if self.store._reference_input(db, dict(id=job_id, environment=row['environment'])) is None:
                raise Conflict('direct manifest integrity required')
            # Check the real existing FIFO/production and per-node eligibility
            # selector while holding the same SQLite write lock as all claims.
            for node in self.queue.node_ids:
                candidate = self.store._candidate_any_row(db, node)
                if candidate is None or candidate['id'] != job_id:
                    continue
                worker = self.workers[node]
                if profile_selection is not None:
                    self._select_profile(db,profile_selection,reservation_id=reservation_id,job_id=job_id,attempt_id=attempt_id,
                        generation=generation,environment=row['environment'],node_id=node,manifest_sha=manifest_sha,
                        derived_manifest_sha=derived_manifest_sha)
                self.store._quota(db, sum(len(x.encode()) for x in
                    (reservation_id, job_id, attempt_id, str(generation), row['environment'], self.queue.authority_id, manifest_sha, derived_manifest_sha, node,
                     worker['workerId'], worker['workerInstanceId'], 'receiving')))
                db.execute('''INSERT INTO direct_intake_reservations
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,'receiving')''',
                    (reservation_id, job_id, attempt_id, generation, row['environment'], self.queue.authority_id, manifest_sha, derived_manifest_sha, node,
                     worker['workerId'], worker['workerInstanceId']))
                reason='direct_intake_receiving_profile' if profile_selection is not None else 'direct_intake_receiving'
                changed = db.execute('''UPDATE jobs SET status='held',node_id=?,
                    held_reason=?,updated_at=CURRENT_TIMESTAMP
                    WHERE id=? AND status='queued' ''', (node,reason,job_id)).rowcount
                if changed != 1:
                    raise Conflict('direct reservation queue changed')
                db.execute('UPDATE reference_jobs SET node_id=?,updated_at=CURRENT_TIMESTAMP WHERE job_id=?',
                           (node, job_id))
                return dict(reservationId=reservation_id, jobId=job_id, attemptId=attempt_id,
                            generation=generation, environment=row['environment'], authorityId=self.queue.authority_id,
                            manifestSha256=manifest_sha, derivedManifestSha256=derived_manifest_sha,
                            nodeId=node, workerId=worker['workerId'],
                            workerInstanceId=worker['workerInstanceId'], state='receiving', replayed=False,
                            **({'limitProfile':json.loads(profile_selection['request'])['profile']} if profile_selection is not None else {}))
            return None
