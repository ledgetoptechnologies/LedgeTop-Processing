"""Unwired synthetic read-only restart evidence; never resumes a job."""
from .viewer_archive_spool import ViewerArchiveSpool,SpoolHeld

class SyntheticRestartCensus:
    def __init__(self,spool,*,synthetic_fixture=False):
        if synthetic_fixture is not True or type(spool) is not ViewerArchiveSpool:raise SpoolHeld('synthetic_fixture_required')
        self.spool=spool

    def report(self,job,identity,token):
        claims=self.spool.claims
        with claims.queue._db() as db:
            db.execute('BEGIN');claims._authorize_inventory(db,job,identity,token)
            row=db.execute('SELECT status,held_reason FROM jobs WHERE id=?',(job,)).fetchone()
            if not row or (row['status'],row['held_reason'])!=('held','coordinator_restart'):raise SpoolHeld('restart_hold_required')
            taskrow=db.execute('SELECT * FROM viewer_tasks WHERE job_id=? AND removed=0',(job,)).fetchone()
            if not taskrow:raise SpoolHeld('task_unavailable')
            task=claims.store._task(db,taskrow)
        reader,_=self.spool.open_for_viewer(task)
        try:
            with claims.queue._db() as db:
                db.execute('BEGIN');claims._authorize_inventory(db,job,identity,token)
                row=db.execute('SELECT status,held_reason FROM jobs WHERE id=?',(job,)).fetchone()
                active=db.execute("SELECT 1 FROM viewer_worker_chunk_permits WHERE job_id=? AND state='active'",(job,)).fetchone()
                if not row or (row['status'],row['held_reason'])!=('held','coordinator_restart') or active:
                    raise SpoolHeld('restart_state_changed')
                if not self.spool._completion_evidence(db,task,reader,allow_restart_hold=True):raise SpoolHeld('completion_evidence_unavailable')
                reader.slot._check()
            return dict(schemaVersion=1,state='held',reason='coordinator_restart',
                        coordinatorArchiveVerified=True,completionAttestationVerified=True,
                        blockers=['current_node_source_unproven','physical_engine_fence_unproven'],
                        automaticResumeAuthorized=False)
        finally:reader.close()
