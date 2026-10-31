"""Disabled synthetic-only direct Viewer worker supervisor.

This fixture driver has no daemon entry point and never authorizes production.
Its authoritative progress is the existing durable enrollment, input,
submission and output journals; ambiguous operations remain held there.
"""
from pathlib import Path
import time

from .viewer_local_result import LocalResultHeld,ArchiveStatusTransportUnavailable,capture,resume_capture_status,send_metadata,send_completion
from .viewer_node_inputs import ViewerNodeInputs
from .viewer_node_enrollment import ViewerNodeEnrollment,EnrollmentTransportUnavailable
from .viewer_service import NativeEngine
from .viewer_synthetic_submission import submit


class SupervisorHeld(RuntimeError):pass


class TransientTransport(RuntimeError):
    """Reserved for a verified transient transport classification."""


def _deliver(worker):
    # Only post-capture metadata/receipt operations are safe to retry; their
    # immutable identities and durable journals perform the actual replay.
    try:
        send_metadata(worker,synthetic_fixture=True)
        return send_completion(worker,synthetic_fixture=True)
    except (EnrollmentTransportUnavailable,ArchiveStatusTransportUnavailable) as error:
        raise TransientTransport('delivery_transport_unavailable') from error


def tick(worker,engine,job_id,request_id,environment,*,input_root,input_root_identity,
         max_total,max_object,synthetic_fixture=False,max_objects=64):
    """One bounded turn; no deletion, remote custody claim or live execution."""
    if synthetic_fixture is not True or type(worker) is not ViewerNodeEnrollment or type(engine) is not NativeEngine:
        raise SupervisorHeld('synthetic_fixture_required')
    if type(max_objects) is not int or not 1<=max_objects<=64:raise SupervisorHeld('object_limit_invalid')
    with worker._db() as db:
        intent=db.execute('SELECT request,slot_phase FROM intent WHERE singleton=1').fetchone()
    input_journal=worker.owner.root/'viewer-inputs.sqlite'
    if intent is not None:
        if not input_journal.exists() or input_journal.is_symlink():raise SupervisorHeld('input_journal_requires_inspection')
        try:status=resume_capture_status(worker,synthetic_fixture=True)
        except LocalResultHeld as error:
            if str(error)!='synthetic_submission_unavailable':raise
        else:
            if status['state']=='absent_no_dispatch':capture(worker,engine,synthetic_fixture=True)
            elif status['state']!='sealed_no_dispatch':raise SupervisorHeld('capture_state_unknown')
            return _deliver(worker)
    inputs=ViewerNodeInputs(worker,job_id,request_id,environment,input_root=input_root,
        input_root_identity=input_root_identity,max_total=max_total,max_object=max_object)
    count=inputs.claim.get('objectCount')
    if type(count) is not int or not 1<=count<=max_objects:raise SupervisorHeld('synthetic_object_limit')
    inputs.fetch_plan();inventory=inputs.freeze_inventory()
    if inventory['count']>max_objects:raise SupervisorHeld('synthetic_object_limit')
    with inputs._db() as db:
        rows=db.execute('SELECT object_id FROM objects ORDER BY ordinal').fetchall()
    if len(rows)!=inventory['count']:raise SupervisorHeld('inventory_changed')
    for (object_id,) in rows:inputs.transfer(object_id)
    submit(inputs,engine,synthetic_fixture=True)
    capture(worker,engine,synthetic_fixture=True)
    return _deliver(worker)


def run(worker,engine,job_id,request_id,environment,*,max_attempts=5,initial_delay=.1,max_delay=2,
        synthetic_fixture=False,**kwargs):
    """Bounded retries; callers inspect the final exception, no background loop."""
    if synthetic_fixture is not True or type(max_attempts) is not int or not 1<=max_attempts<=8:
        raise SupervisorHeld('synthetic_fixture_required')
    if type(initial_delay) not in (int,float) or type(max_delay) not in (int,float) or not 0<=initial_delay<=max_delay<=2:
        raise SupervisorHeld('retry_delay_invalid')
    delay=initial_delay
    for attempt in range(max_attempts):
        try:return tick(worker,engine,job_id,request_id,environment,synthetic_fixture=True,**kwargs)
        except TransientTransport:
            if attempt+1==max_attempts:raise
            time.sleep(delay);delay=min(max_delay,delay*2)
