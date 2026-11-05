"""Durable first enrolled coordinator cutover mode, separate from historical ABI.

The driver is the explicit Docker/lease/custody boundary. This module owns the
single update journal and ordering; it never controls a processing queue itself.
No operation runs on import. An ambiguous mutation is inspected, never replayed.
"""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import uuid

MODE = 'enrolled-693-first-v2-cutover-v1'
PREDECESSOR_ID = '525ca6e24ad3b9e57e2a3be36567471c10745a4aaa8ebc03c7f2d2f215dfb977'
PREDECESSOR_IMAGE = 'sha256:7fae9385fb5ca829783fe05eef2fdb1983abf4d491b2b9f795e55b617a5a0004'
PREDECESSOR_SOURCE = '693cb7eca32295da959a4f42399f317577ef8a8a'
AUTHORITY = '7131882e-38ce-4d3e-99cc-4ba5a94d2ee8'
NAME = 'ledgetop-processing-prototype-c3fcb3a'
HASH = re.compile(r'[0-9a-f]{64}\Z')
IMAGE = re.compile(r'sha256:[0-9a-f]{64}\Z')

# Each mutation is durable before the driver is invoked. Read-only observations
# between these steps verify the exact effect and bind the next transition.
STEPS = (
    ('disable-predecessor-restart','restart-disabled'),
    ('stop-predecessor','predecessor-stopped'),
    ('create-original-lease-holder','original-lease-holder-created'),
    ('acquire-original-leases','original-leases-held'),
    ('capture-full-custody','custody-verified'),
    ('copy-verified-backup','backup-verified'),
    ('release-original-leases','original-leases-released'),
    ('create-bootstrap','bootstrap-created'),
    ('start-bootstrap','bootstrap-running'),
    ('intent-bootstrap-gate','bootstrap-freeze-intent'),
    ('drain-bootstrap-gate','bootstrap-draining'),
    ('seal-bootstrap-gate','bootstrap-frozen'),
    ('stop-bootstrap','bootstrap-stopped'),
    ('acquire-frozen-original-leases','frozen-original-leases-held'),
    ('verify-frozen-transform','frozen-transform-verified'),
    ('release-frozen-original-leases','frozen-original-leases-released'),
    ('rename-predecessor','predecessor-retained'),
    ('create-final','final-created'),
    ('start-final','final-frozen'),
    ('verify-final-readiness','final-ready'),
    ('resume-final-gate','admission-open'),
    ('enable-final-restart','complete'),
)
READ_ACTIONS = frozenset(('capture-full-custody','verify-frozen-transform','verify-final-readiness'))
ROLLBACK_STEPS = (
    ('stop-exact-candidates','rollback-candidates-stopped'),
    ('create-rollback-lease-holder','rollback-lease-holder-created'),
    ('acquire-rollback-original-leases','rollback-original-leases-held'),
    ('verify-rollback-projection','rollback-projection-verified'),
    ('release-rollback-original-leases','rollback-original-leases-released'),
    ('retain-final-name','rollback-final-name-retained'),
    ('restore-predecessor-name','rollback-name-restored'),
    ('start-exact-predecessor','rollback-predecessor-running'),
    ('verify-predecessor-readiness','rollback-predecessor-ready'),
    ('restore-predecessor-restart','rolled-back-rebaseline-required'),
)
ROLLBACK_RESTART_ONLY = (('verify-predecessor-readiness','rollback-predecessor-ready'),
                         ('restore-predecessor-restart','rolled-back-rebaseline-required'))
ROLLBACK_ORIGINAL_ONLY = tuple(('verify-original-before-candidate',phase) if action=='verify-rollback-projection'
                               else (action,phase) for action,phase in ROLLBACK_STEPS)
ROLLBACK_READS = frozenset(('verify-rollback-projection','verify-original-before-candidate','verify-predecessor-readiness'))


class CutoverHeld(RuntimeError):
    """Keep original evidence and inspect; not permission to retry mutations."""


def require(condition, reason):
    if not condition: raise CutoverHeld(reason)


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def identifier(value):
    try:valid=type(value) is str and str(uuid.UUID(value)) == value
    except (ValueError,AttributeError):valid=False
    require(valid,'canonical_operation_required')
    return value


def validate_plan(plan):
    fields={'schemaVersion','mode','operationId','authorityId','predecessor','candidate','bindings','gates'}
    require(type(plan) is dict and set(plan)==fields and type(plan['schemaVersion']) is int and
            plan['schemaVersion']==1 and plan['mode']==MODE and plan['authorityId']==AUTHORITY,'closed_first_cutover_plan_required')
    identifier(plan['operationId'])
    old=plan['predecessor'];target=plan['candidate'];bindings=plan['bindings'];gates=plan['gates']
    require(type(old) is dict and set(old)=={'containerId','imageId','sourceCommit','restartPolicy'} and
            old['containerId']==PREDECESSOR_ID and old['imageId']==PREDECESSOR_IMAGE and
            old['sourceCommit']==PREDECESSOR_SOURCE,'reviewed_enrolled_predecessor_required')
    restart=old['restartPolicy']
    require(type(restart) is dict and set(restart)=={'Name','MaximumRetryCount'} and
            restart['Name'] in ('unless-stopped','always','no','on-failure') and
            type(restart['MaximumRetryCount']) is int and 0<=restart['MaximumRetryCount']<=100,
            'original_restart_policy_required')
    target_fields={'imageId','sourceCommit','bootstrapRecipeSha256','finalRecipeSha256',
                   'bootstrapConfigSha256','finalConfigSha256','declaredTransformSha256'}
    require(type(target) is dict and set(target)==target_fields and type(target['imageId']) is str and IMAGE.fullmatch(target['imageId']) and
            target['imageId']!=old['imageId'] and type(target['sourceCommit']) is str and re.fullmatch('[0-9a-f]{40}',target['sourceCommit']),
            'reviewed_immutable_target_required')
    for key in target_fields-{'imageId','sourceCommit'}:
        require(type(target[key]) is str and HASH.fullmatch(target[key]),'target_binding_digest_required')
    binding_fields={'stateSha256','authorityLeaseSha256','predecessorRecipeSha256','predecessorEnvironmentSha256',
                    'censusKeySha256','reviewedCountsSha256','deploymentSha256'}
    require(type(bindings) is dict and set(bindings)==binding_fields and
            all(type(value) is str and HASH.fullmatch(value) for value in bindings.values()),'closed_original_bindings_required')
    gate_fields={'nasQualificationSha256','pilotClosureSha256','legacyDeliverySha256','workerQualificationSha256'}
    require(type(gates) is dict and set(gates)==gate_fields and all(value is None or
            type(value) is str and HASH.fullmatch(value) for value in gates.values()),'closed_activation_gates_required')
    return json.loads(canonical(plan))


class CutoverJournal:
    """One private fsynced operation journal; no credentials or client rows."""
    def __init__(self,root):
        self.root=Path(root).absolute()
        require(self.root.resolve()==self.root and not any(p.is_symlink() for p in self.root.parents),
                'original_private_operation_required')
        info=self.root.lstat()
        require(stat.S_ISDIR(info.st_mode) and info.st_uid==os.geteuid() and stat.S_IMODE(info.st_mode)==0o700,
                'private_operation_required')
        self.path=self.root/'first-cutover.json'

    @contextmanager
    def locked(self):
        require(os.name=='posix','linux_first_cutover_required')
        import fcntl
        path=self.root/'first-cutover.lock'
        descriptor=os.open(path,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW|os.O_CLOEXEC,0o600)
        try:
            opened,current=os.fstat(descriptor),path.lstat()
            require(stat.S_ISREG(opened.st_mode) and opened.st_nlink==1 and opened.st_uid==os.geteuid() and
                    stat.S_IMODE(opened.st_mode)==0o600 and
                    (opened.st_dev,opened.st_ino)==(current.st_dev,current.st_ino),'private_operation_lock_required')
            try:fcntl.flock(descriptor,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except OSError:raise CutoverHeld('another_first_cutover_invocation_active') from None
            yield
        finally:os.close(descriptor)

    def read(self):
        info=self.path.lstat()
        require(stat.S_ISREG(info.st_mode) and info.st_nlink==1 and info.st_uid==os.geteuid() and
                stat.S_IMODE(info.st_mode)==0o600 and info.st_size<=1024*1024,'private_bounded_journal_required')
        fd=os.open(self.path,os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC)
        try:
            require((os.fstat(fd).st_dev,os.fstat(fd).st_ino)==(info.st_dev,info.st_ino),'journal_identity_changed')
            raw=os.read(fd,info.st_size+1)
            require(len(raw)==info.st_size,'journal_changed')
        finally:os.close(fd)
        after=self.path.lstat()
        require((after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns)==
                (info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns),'journal_changed')
        value=json.loads(raw)
        require(canonical(value)==raw,'canonical_journal_required')
        return value

    def write(self,value):
        raw=canonical(value)
        require(len(raw)<=1024*1024,'bounded_first_cutover_journal_required')
        if self.path.exists() or self.path.is_symlink():
            info=self.path.lstat()
            require(stat.S_ISREG(info.st_mode) and info.st_nlink==1 and info.st_uid==os.geteuid() and
                    stat.S_IMODE(info.st_mode)==0o600,'private_existing_journal_required')
        temporary=self.root/('first-cutover-'+uuid.uuid4().hex+'.tmp')
        fd=os.open(temporary,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW|os.O_CLOEXEC,0o600)
        try:
            with os.fdopen(fd,'wb',closefd=False) as stream:
                stream.write(raw);stream.flush();os.fsync(fd)
        finally:os.close(fd)
        os.replace(temporary,self.path)
        directory=os.open(self.root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:os.fsync(directory)
        finally:os.close(directory)


class FirstEnrolledCutover:
    """Serialized update phases, using one explicit reviewed boundary driver.

    Driver contract: global_lock(plan), preflight(plan), guard(action,intent,state,inspection=bool),
    execute(action,intent,state),
    observe(action,intent,state), verify(action,observation,intent,state).
    observe is read-only. verify must bind actual image/config/lease/custody and
    declared-transform evidence; a caller-provided ready boolean is insufficient.
    The concrete driver is a separate release gate, not supplied by this module.
    """
    def __init__(self,root,plan,driver):
        self.plan=validate_plan(plan);self.journal=CutoverJournal(root);self.driver=driver
        require(all(callable(getattr(driver,name,None)) for name in ('global_lock','preflight','guard','execute','observe','verify')),
                'reviewed_cutover_driver_required')
        self.plan_sha=digest(self.plan)

    @contextmanager
    def _locked(self):
        # The concrete boundary must use the existing host-wide update lock and
        # durable active-operation binding, not a new lock per receipt folder.
        with self.driver.global_lock(self.plan):
            with self.journal.locked():
                yield

    def prepare(self):
        with self._locked():
            require(not self.journal.path.exists() and not self.journal.path.is_symlink(),'new_first_cutover_operation_required')
            evidence=self.driver.preflight(self.plan)
            require(type(evidence) is dict and set(evidence)=={'deploymentSha256','predecessorVerified','readOnly'} and
                    evidence['deploymentSha256']==self.plan['bindings']['deploymentSha256'] and
                    evidence['predecessorVerified'] is True and evidence['readOnly'] is True,
                    'exact_readonly_preflight_required')
            state=dict(schemaVersion=1,mode=MODE,operationId=self.plan['operationId'],planSha256=self.plan_sha,
                       phase='prepared',direction='forward',rollbackScope='none',index=0,pending=None,events=[],proofs={},admissionOpened=False,noEffect=[],dispatch=None)
            self.journal.write(state)
            return state

    def _load(self):
        state=self.journal.read()
        fields={'schemaVersion','mode','operationId','planSha256','phase','direction','index','pending',
                'events','proofs','admissionOpened','rollbackScope','noEffect','dispatch'}
        require(type(state) is dict and set(state)==fields and type(state['schemaVersion']) is int and state['schemaVersion']==1 and state['mode']==MODE and
                state['operationId']==self.plan['operationId'] and state['planSha256']==self.plan_sha and
                state['direction'] in ('forward','rollback') and state['rollbackScope'] in ('none','restart-only','original-only','declared-v2')
                and type(state['index']) is int and
                type(state['admissionOpened']) is bool and type(state['events']) is list and len(state['events'])<=64 and
                type(state['proofs']) is dict and type(state['noEffect']) is list and len(state['noEffect'])<=64,
                'first_cutover_journal_binding_changed')
        steps=self._steps(state)
        require(0<=state['index']<=len(steps),'first_cutover_phase_invalid')
        expected='prepared' if state['index']==0 and state['direction']=='forward' else \
                 'rollback-prepared' if state['index']==0 else steps[state['index']-1][1]
        require(state['phase']==expected,'first_cutover_phase_invalid')
        forward_count=rollback_count=0;seen_rollback=False;proofs={};admission=False
        for event in state['events']:
            require(type(event) is dict and set(event)=={'intentSha256','action','observation','phase','direction','step'},
                    'cutover_history_shape_changed')
            direction=event['direction'];require(direction in ('forward','rollback'),'cutover_history_direction_changed')
            if direction=='rollback':seen_rollback=True
            require(not seen_rollback or direction=='rollback','cutover_history_order_changed')
            number=forward_count if direction=='forward' else rollback_count
            sequence=STEPS if direction=='forward' else self._steps(state)
            require(number<len(sequence) and type(event['step']) is int and event['step']==number and
                    (event['action'],event['phase'])==sequence[number],'cutover_history_order_changed')
            past=self._intent(dict(direction=direction,index=number),event['action'])
            require(event['intentSha256']==digest(past),'cutover_history_intent_changed')
            observation=event['observation']
            require(type(observation) is dict and set(observation)=={'intentId','bindingSha256','factsSha256'} and
                    observation['intentId']==past['intentId'] and all(type(observation[key]) is str and HASH.fullmatch(observation[key])
                    for key in ('bindingSha256','factsSha256')) and observation['bindingSha256']==digest(past),
                    'cutover_history_observation_changed')
            proofs[event['action']]=observation
            if event['action']=='resume-final-gate':admission=True
            if direction=='forward':forward_count+=1
            else:rollback_count+=1
        forward_actions={event['action'] for event in state['events'] if event['direction']=='forward'}
        expected_scope=('none' if state['direction']=='forward' else
                        'restart-only' if 'stop-predecessor' not in forward_actions else
                        'declared-v2' if 'start-bootstrap' in forward_actions else 'original-only')
        require(state['rollbackScope']==expected_scope and state['proofs']==proofs and state['admissionOpened']==admission and
                (state['index']==forward_count and not seen_rollback if state['direction']=='forward' else
                 state['index']==rollback_count and not admission),'cutover_history_projection_changed')
        for record in state['noEffect']:
            require(type(record) is dict and set(record)=={'intent','observation','disposition','afterEvents','dispatch'} and
                    record['disposition'] in ('rollback','rearm') and type(record['afterEvents']) is int and
                    0<=record['afterEvents']<=len(state['events']),'not_applied_history_changed')
            intent=record['intent'];direction=intent.get('direction') if type(intent) is dict else None
            require(direction in ('forward','rollback'),'not_applied_direction_changed')
            prior=[event for event in state['events'][:record['afterEvents']] if event['direction']==direction]
            sequence=STEPS if direction=='forward' else self._steps(dict(state,direction='rollback'))
            number=len(prior)
            require(number<len(sequence) and intent==self._intent(dict(direction=direction,index=number),sequence[number][0]) and
                    (record['disposition']!='rollback' or direction=='forward'),'not_applied_intent_changed')
            self._no_effect_shape(record['observation'],intent)
            require(record['dispatch'] in (None,digest(intent)) and
                    (record['observation']['proofKind']!='pre-dispatch' or record['dispatch'] is None),
                    'not_applied_dispatch_evidence_changed')
        if state['pending'] is not None:
            require(state['index']<len(steps) and state['pending']==self._intent(state,steps[state['index']][0]),
                    'first_cutover_pending_intent_changed')
        require(state['dispatch'] is None or state['pending'] is not None and state['dispatch']==digest(state['pending']),
                'dispatch_marker_changed')
        return state

    @staticmethod
    def _steps(state):
        if state['direction']=='forward':return STEPS
        return {'restart-only':ROLLBACK_RESTART_ONLY,'original-only':ROLLBACK_ORIGINAL_ONLY,
                'declared-v2':ROLLBACK_STEPS}.get(state['rollbackScope'],())

    def _intent(self,state,action):
        return dict(schemaVersion=1,operationId=self.plan['operationId'],planSha256=self.plan_sha,
                    direction=state['direction'],step=state['index'],action=action,
                    intentId=str(uuid.uuid5(uuid.UUID(self.plan['operationId']),state['direction']+':'+str(state['index'])+':'+action)))

    def _activation_gates(self):
        require(all(value is not None for value in self.plan['gates'].values()),'first_cutover_prerequisites_unqualified')

    def _finish(self,state,action,next_phase,intent,observation):
        require(self.driver.verify(action,observation,intent,state) is True,'cutover_effect_or_transform_not_verified')
        require(type(observation) is dict and set(observation)=={'intentId','bindingSha256','factsSha256'} and
                observation['intentId']==intent['intentId'] and
                all(type(observation[key]) is str and HASH.fullmatch(observation[key])
                    for key in ('bindingSha256','factsSha256')) and observation['bindingSha256']==digest(intent),
                    'bounded_verified_observation_required')
        event=dict(intentSha256=digest(intent),action=action,observation=observation,phase=next_phase,
                   direction=state['direction'],step=state['index'])
        state['events'].append(event);state['proofs'][action]=observation
        state.update(index=state['index']+1,phase=next_phase,pending=None,dispatch=None)
        if action=='resume-final-gate':state['admissionOpened']=True
        self.journal.write(state)
        return state

    def advance(self):
        """One bounded phase only. Pending mutation requires inspect(), never retry."""
        with self._locked():
            state=self._load()
            if state['direction']=='forward':self._activation_gates()
            steps=self._steps(state)
            if state['index']==len(steps):return state
            require(state['pending'] is None,'ambiguous_outcome_inspection_only')
            action,next_phase=steps[state['index']];intent=self._intent(state,action)
            require(not any(record['intent']==intent and record['disposition']=='rollback' for record in state['noEffect']),
                    'proven_not_applied_requires_rollback')
            require(self.driver.guard(action,intent,state,inspection=False) is True,
                    'current_cutover_boundary_not_qualified')
            state['pending']=intent;self.journal.write(state)  # Before any effect.
            if action in READ_ACTIONS or action in ROLLBACK_READS:
                observation=self.driver.observe(action,intent,state)
            else:
                state['dispatch']=digest(intent);self.journal.write(state)  # Before any possible external dispatch.
                try:self.driver.execute(action,intent,state)
                except Exception:raise CutoverHeld('mutation_outcome_uncertain_inspection_only') from None
                observation=self.driver.observe(action,intent,state)
            return self._finish(state,action,next_phase,intent,observation)

    def inspect(self):
        """Reconcile the exact prior effect using reads only; never reissue it."""
        with self._locked():
            state=self._load()
            if state['direction']=='forward':self._activation_gates()
            if state['pending'] is None:return state
            steps=self._steps(state)
            action,next_phase=steps[state['index']];intent=state['pending']
            require(self.driver.guard(action,intent,state,inspection=True) is True,
                    'current_inspection_boundary_not_qualified')
            observation=self.driver.observe(action,intent,state)
            return self._finish(state,action,next_phase,intent,observation)

    @staticmethod
    def _no_effect_shape(observation,intent):
        require(type(observation) is dict and set(observation)=={'intentId','bindingSha256','factsSha256','outcome','proofKind'} and
                observation['outcome']=='not-applied' and observation['proofKind'] in ('pre-dispatch','authenticated-rejection') and observation['intentId']==intent['intentId'] and
                observation['bindingSha256']==digest(intent) and
                all(type(observation[key]) is str and HASH.fullmatch(observation[key])
                    for key in ('bindingSha256','factsSha256')),'bounded_not_applied_evidence_required')

    def reconcile_not_applied(self, *, disposition):
        """Explicitly abort/rearm only an independently proven absent effect.

        The driver must prove the complete closed before-state, not merely the
        absence of an expected after-state. No mutation is executed here.
        """
        require(disposition in ('rollback','rearm'),'explicit_not_applied_disposition_required')
        with self._locked():
            state=self._load();intent=state['pending']
            require(intent is not None and not state['admissionOpened'] and
                    (disposition!='rollback' or state['direction']=='forward'),'pending_closed_intent_required')
            action=intent['action']
            observe=getattr(self.driver,'observe_not_applied',None)
            verify=getattr(self.driver,'verify_not_applied',None)
            require(callable(observe) and callable(verify),'not_applied_boundary_unsupported')
            require(self.driver.guard(action,intent,state,inspection=True) is True,'current_inspection_boundary_not_qualified')
            observation=observe(action,intent,state);self._no_effect_shape(observation,intent)
            require(observation['proofKind']!='pre-dispatch' or state['dispatch'] is None,
                    'dispatched_outcome_cannot_be_inferred_absent')
            require(verify(action,observation,intent,state) is True and
                    self.driver.guard(action,intent,state,inspection=True) is True,'complete_not_applied_boundary_not_verified')
            state['noEffect'].append(dict(intent=intent,observation=observation,disposition=disposition,
                                          afterEvents=len(state['events']),dispatch=state['dispatch']))
            state['pending']=None;state['dispatch']=None;self.journal.write(state)
            return state

    def begin_rollback(self):
        """No database restoration, no rollback after admission or unknown effect."""
        with self._locked():
            state=self._load()
            require(state['direction']=='forward' and not state['admissionOpened'] and state['pending'] is None,
                    'rollback_requires_closed_known_cutover')
            scope=('restart-only' if 'stop-predecessor' not in state['proofs'] else
                   'declared-v2' if 'start-bootstrap' in state['proofs'] else 'original-only')
            state.update(direction='rollback',rollbackScope=scope,index=0,phase='rollback-prepared',pending=None)
            self.journal.write(state)
            return state
