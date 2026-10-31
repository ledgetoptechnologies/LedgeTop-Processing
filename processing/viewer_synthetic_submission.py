"""Disconnected loopback-only submission experiment; never enable in production.

The authenticated coordinator grant is self-reported, not a physical fence or
live lease. The caller owns a daemon lock. Mutations are journaled once and an
unacknowledged request is held forever for inspection; files are never removed.
"""
import json
from urllib.parse import urlsplit

from . import reference_protocol as p
from .direct_output_slot import _identity,open_slot
from .viewer_locked_upload import upload_locked
from .viewer_node_inputs import ViewerNodeInputs
from .viewer_service import NativeEngine
from .viewer_strict_mutations import initialize,commit


class SubmissionHeld(RuntimeError):pass


def submit(inputs,engine,*,synthetic_fixture=False):
    """Run synthetic-only submission; pending operations never get replayed."""
    if type(inputs) is not ViewerNodeInputs or type(engine) is not NativeEngine or synthetic_fixture is not True:
        raise SubmissionHeld('synthetic_fixture_required')
    origin=urlsplit(engine.origin)
    if origin.scheme!='http' or origin.hostname!='127.0.0.1' or origin.port is None:
        raise SubmissionHeld('loopback_fixture_required')
    with inputs._lock:
        plan=inputs.fetch_plan();grant=inputs.request_execution_grant()
        encoded=p.canonical_json(grant).decode()
        with inputs._gate():
            with inputs._db() as db:
                manifest=db.execute('SELECT phase,digest,count,total FROM manifest WHERE singleton=1').fetchone()
                inputs._verify_inventory_rows(db,manifest)
                row_count=db.execute('SELECT COUNT(*) FROM objects').fetchone()[0]
                not_verified=db.execute("SELECT 1 FROM objects WHERE phase!='verified' LIMIT 1").fetchone()
                current=db.execute('SELECT grant_value,origin,phase,ordinal,object_id FROM synthetic_submission WHERE singleton=1').fetchone()
                if current is None:
                    db.execute('INSERT INTO synthetic_submission VALUES(1,?,?,?,?,NULL)',(encoded,engine.origin,'ready',0))
                    current=(encoded,engine.origin,'ready',0,None)
                if current[0]!=encoded or current[1]!=engine.origin:raise SubmissionHeld('submission_binding_changed')
            if row_count!=grant['inputCount'] or not_verified:raise SubmissionHeld('input_not_verified')
            phase,ordinal,object_id=current[2:]
            if phase.endswith('_pending'):raise SubmissionHeld('mutation_requires_inspection')
            if phase=='committed':
                if ordinal!=row_count or object_id is not None:raise SubmissionHeld('submission_state_invalid')
                return {'state':'synthetic_submitted','engineTaskId':grant['engineTaskId']}
            if phase not in ('ready','initialized','uploaded') or type(ordinal) is not int or not 0<=ordinal<=row_count:
                raise SubmissionHeld('submission_state_invalid')
            if phase in ('ready','initialized') and (ordinal!=0 or object_id is not None):raise SubmissionHeld('submission_state_invalid')
            if phase=='uploaded':
                if ordinal<1:raise SubmissionHeld('submission_state_invalid')
                previous=next(inputs._verified_input_rows(ordinal-1,ordinal),None)
                if previous is None or object_id!=previous[0]['objectId']:raise SubmissionHeld('submission_state_invalid')

            def transition(before,after,index,expected_object=None,new_object=None):
                with inputs._db() as db:
                    changed=db.execute('UPDATE synthetic_submission SET phase=?,ordinal=?,object_id=? WHERE singleton=1 AND phase=? AND ordinal=? AND object_id IS ?',
                        (after,index,new_object,before,index,expected_object)).rowcount
                    if changed!=1:raise SubmissionHeld('submission_state_changed')

            def mutation(operation,operation_id,action):
                payload=dict(operationId=operation_id,operation=operation)
                expected=dict(jobId=grant['jobId'],**payload,state='active')
                permit=inputs.worker._request(payload,job_id=grant['jobId'],claim_token=inputs.claim['claimToken'],kind='mutation-permit')
                if permit!=expected:raise SubmissionHeld('mutation_permit_changed')
                action()
                finished=inputs.worker._request(payload,job_id=grant['jobId'],claim_token=inputs.claim['claimToken'],kind='mutation-finish')
                if finished!={**expected,'state':'finished'}:raise SubmissionHeld('mutation_finish_changed')

            if phase=='ready':
                transition('ready','init_pending',0)
                try:mutation('init','init',lambda:initialize(engine,grant['engineTaskId'],plan,grant['pipelineSha256'],grant['taskName'],grant['taskNameSha256']))
                except Exception:raise SubmissionHeld('initialization_uncertain') from None
                transition('init_pending','initialized',0);phase='initialized'
            for entry,input_phase,key,recorded in inputs._verified_input_rows(ordinal,row_count):
                if input_phase!='verified':raise SubmissionHeld('input_not_verified')
                expected_key=p.digest(dict(purpose='viewer-input-slot-v1',bindingSha256=inputs.binding_sha,objectId=entry['objectId']))
                if key!=expected_key or not isinstance(recorded,str):raise SubmissionHeld('input_identity_changed')
                slot=inputs._input_slot(entry['objectId'],key,entry['bytes'],create=False,recorded=recorded)
                try:
                    identity=dict(root=_identity(slot._root),allocation=_identity(slot._fd),metadata=_identity(slot._meta))
                    if p.canonical_json(identity).decode()!=recorded or inputs._hash_slot(slot,entry['bytes'])!=entry['sha256']:
                        raise SubmissionHeld('input_identity_changed')
                    previous_object=None if phase=='initialized' else object_id
                    transition(phase,'upload_pending',ordinal,previous_object,entry['objectId'])
                    try:mutation('upload','upload-'+str(ordinal),lambda:upload_locked(engine,grant['engineTaskId'],{k:entry[k] for k in ('name','bytes','sha256')},slot,identity))
                    except Exception:raise SubmissionHeld('upload_uncertain') from None
                    with inputs._db() as db:
                        changed=db.execute("UPDATE synthetic_submission SET phase='uploaded',ordinal=? WHERE singleton=1 AND phase='upload_pending' AND ordinal=? AND object_id=?",
                                           (ordinal+1,ordinal,entry['objectId'])).rowcount
                        if changed!=1:raise SubmissionHeld('submission_state_changed')
                    ordinal+=1;phase='uploaded';object_id=entry['objectId']
                finally:slot.close()
            transition(phase,'commit_pending',ordinal,object_id,None)
            try:mutation('commit','commit',lambda:commit(engine,grant['engineTaskId']))
            except Exception:raise SubmissionHeld('commit_uncertain') from None
            transition('commit_pending','committed',ordinal)
            return {'state':'synthetic_submitted','engineTaskId':grant['engineTaskId']}
