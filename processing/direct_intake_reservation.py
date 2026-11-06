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
        # Prototype only: production schema integration must also extend the
        # coordinator freeze/custody table inventory and metadata accounting.
        with self.queue._db() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS direct_intake_reservations (
                reservation_id TEXT PRIMARY KEY, job_id TEXT UNIQUE NOT NULL,
                attempt_id TEXT NOT NULL, generation INTEGER NOT NULL,
                environment TEXT NOT NULL, authority_id TEXT NOT NULL, manifest_sha TEXT NOT NULL,
                node_id TEXT UNIQUE NOT NULL, worker_id TEXT NOT NULL,
                instance_id TEXT NOT NULL, state TEXT NOT NULL CHECK(state='receiving'),
                FOREIGN KEY(job_id) REFERENCES reference_jobs(job_id))''')

    def reserve(self, *, job_id, attempt_id, generation, reservation_id, manifest_sha):
        """Reserve one eligible idle node, or return None without mutation.

        Exact retries return the persisted identity. A reservation holds the node
        across coordinator restarts; there is deliberately no timeout or release.
        """
        for value, label in ((job_id, 'job'), (attempt_id, 'attempt'),
                             (reservation_id, 'reservation')):
            protocol._uuid(value, label)
        protocol._hash(manifest_sha, 'manifest digest')
        if type(generation) is not int or generation < 1:
            raise ValueError('positive generation required')
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            prior = db.execute('SELECT * FROM direct_intake_reservations WHERE reservation_id=? OR job_id=?',
                               (reservation_id, job_id)).fetchall()
            if prior:
                if len(prior) != 1:
                    raise Conflict('direct reservation identity collision')
                row = prior[0]
                job = self.store._row(db, job_id)
                saved_submission = json.loads(job['input_json'])
                if ((row['reservation_id'], row['job_id'], row['attempt_id'], row['generation'], row['manifest_sha'], row['authority_id']) !=
                        (reservation_id, job_id, attempt_id, generation, manifest_sha, self.queue.authority_id)
                        or job is None or (job['status'], job['node_id'], job['held_reason']) !=
                        ('held', row['node_id'], 'direct_intake_receiving')
                        or job['generation'] != generation or job['environment'] != row['environment']
                        or job['manifest_sha'] != manifest_sha or job['source_state'] is not None
                        or type(saved_submission) is not dict
                        or type(saved_submission.get('directIntakeVersion')) is not int
                        or saved_submission.get('directIntakeVersion') != 1
                        or 'sourceIntakeVersion' in saved_submission
                        or saved_submission.get('submissionId') != attempt_id
                        or self.store._reference_input(db, dict(id=job_id, environment=job['environment'])) is None
                        or row['node_id'] not in self.workers
                        or (row['worker_id'], row['instance_id']) !=
                        (self.workers[row['node_id']]['workerId'], self.workers[row['node_id']]['workerInstanceId'])):
                    raise Conflict('direct reservation binding changed')
                return dict(reservationId=reservation_id, jobId=job_id, attemptId=attempt_id,
                            generation=generation, environment=row['environment'], authorityId=row['authority_id'],
                            nodeId=row['node_id'], workerId=row['worker_id'],
                            workerInstanceId=row['instance_id'], state='receiving', replayed=True)
            row = self.store._row(db, job_id)
            submission = json.loads(row['input_json'])
            if (row['status'] != 'queued' or row['phase'] not in ('queued', 'waiting_capacity')
                    or row['source_state'] is not None
                    or type(submission) is not dict or submission.get('directIntakeVersion') != 1
                    or type(submission.get('directIntakeVersion')) is not int
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
                self.store._quota(db, sum(len(x.encode()) for x in
                    (reservation_id, job_id, attempt_id, str(generation), row['environment'], self.queue.authority_id, manifest_sha, node,
                     worker['workerId'], worker['workerInstanceId'], 'receiving')))
                db.execute('''INSERT INTO direct_intake_reservations
                    VALUES(?,?,?,?,?,?,?,?,?,?,'receiving')''',
                    (reservation_id, job_id, attempt_id, generation, row['environment'], self.queue.authority_id, manifest_sha, node,
                     worker['workerId'], worker['workerInstanceId']))
                changed = db.execute('''UPDATE jobs SET status='held',node_id=?,
                    held_reason='direct_intake_receiving',updated_at=CURRENT_TIMESTAMP
                    WHERE id=? AND status='queued' ''', (node, job_id)).rowcount
                if changed != 1:
                    raise Conflict('direct reservation queue changed')
                db.execute('UPDATE reference_jobs SET node_id=?,updated_at=CURRENT_TIMESTAMP WHERE job_id=?',
                           (node, job_id))
                return dict(reservationId=reservation_id, jobId=job_id, attemptId=attempt_id,
                            generation=generation, environment=row['environment'], authorityId=self.queue.authority_id,
                            nodeId=node, workerId=worker['workerId'],
                            workerInstanceId=worker['workerInstanceId'], state='receiving', replayed=False)
            return None
