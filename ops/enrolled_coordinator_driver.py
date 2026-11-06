"""Concrete boundary driver for FirstEnrolledCutover, never another controller.

Only the existing journal dispatches effects. Importing/constructing this module
does not create containers, open queues, change services or touch NAS data.
"""
import base64
from datetime import datetime,timezone,timedelta
import hashlib
from pathlib import Path
import uuid

from . import enrolled_coordinator_cutover as journal
from . import coordinator_predecessor_census as census
from . import enrolled_coordinator_custody_ops as custody
from .enrolled_coordinator_boundary import BoundaryProbe
from .enrolled_coordinator_deployment import (validate_bundle,verify_private_artifacts,read_config_from_predecessor,
                                             derived_holder_settings)
from .enrolled_coordinator_transport import (DockerCLI,TrackedExec,PrivateProofs,HostOperation,require,canonical,digest,
    decode,private_bytes,environment_file,immutable_container_recipe,container_environment,create_arguments,
    operator_script,startup_health_script,gate_receipt_script,physical_binding_script,OWNER_LABEL,ROLE_LABEL,INTENT_LABEL)
from .enrolled_coordinator_ssh import TrackedWorkerProbe


class ConcreteCutoverDriver:
    def __init__(self,plan,bundle,operation_directory,*,docker=None):
        self.plan=journal.validate_plan(plan);self.bundle=validate_bundle(bundle,plan)
        self.root=Path(operation_directory);self.proofs=PrivateProofs(self.root)
        self.host_operation=HostOperation(self.root)
        self.docker=docker or DockerCLI(binary=bundle['host']['dockerBinary'])
        self.tracked=TrackedExec(self.docker,self.plan['operationId'],self.root)
        self.boundary=BoundaryProbe(plan,bundle,self.docker,self.tracked)
        self.settings=derived_holder_settings(bundle,plan)
        self.plan_sha=digest(self.plan)

    def global_lock(self,plan):
        require(plan==self.plan,'driver_plan_changed')
        require(all(pin is not None for pin in plan['gates'].values()),'first_cutover_prerequisites_unqualified')
        return self.host_operation.locked(plan)

    def _intent(self,event):
        action=event['action'];direction=event['direction'];index=event['step']
        return dict(schemaVersion=1,operationId=self.plan['operationId'],planSha256=self.plan_sha,
            direction=direction,step=index,action=action,intentId=str(uuid.uuid5(uuid.UUID(self.plan['operationId']),
                direction+':'+str(index)+':'+action)))

    def _facts(self,event):
        intent=self._intent(event);raw=private_bytes(self.root/self.proofs.name(intent,event['dispatch']))
        require(hashlib.sha256(raw).hexdigest()==event['observation']['factsSha256'],'journal_evidence_changed')
        value=decode(raw)
        require(value['intentSha256']==digest(intent) and value['dispatch']==event['dispatch'],'journal_intent_evidence_changed')
        return value['facts']

    def _prior(self,state,action,*,optional=False):
        events=[event for event in state['events'] if event['action']==action]
        if not events:
            require(optional,'required_prior_phase_evidence_missing');return None
        return self._facts(events[-1])

    def _preflight(self):
        intent=dict(intentId=str(uuid.uuid5(uuid.UUID(self.plan['operationId']),'readonly-preflight')))
        value=decode(private_bytes(self.root/self.proofs.name(intent)))
        require(value['intentSha256']==digest(intent) and value['dispatch'] is None,'preflight_evidence_changed')
        return value['facts']

    def _role(self,role):return 'lease-holder' if role=='holder' else role

    def _find(self,role):
        ids=self.docker.matching(['label='+OWNER_LABEL+'='+self.plan['operationId'],
                                 'label='+ROLE_LABEL+'='+self._role(role)])
        require(len(ids)<=1,'ambiguous_owned_candidate')
        if not ids:return None
        item=self.docker.inspect(ids[0]);require(item['Config']['Labels'][OWNER_LABEL]==self.plan['operationId'],
                                               'foreign_owned_candidate')
        return item

    def _creation(self,item):return item['Config']['Labels'][INTENT_LABEL]

    def _context(self,role,state):
        item=self._find(role);require(item is not None,'owned_boundary_container_missing')
        gate=None
        if role=='final':
            started=self._prior(state,'start-final')
            gate=dict(startupId=started['gate']['snapshot']['startupId'],ownerSha256=self._preflight()['ownerSha256'])
        return dict(containerId=item['Id'],creationIntentId=self._creation(item),gate=gate)

    def _private(self):
        return verify_private_artifacts(self.bundle,self.plan,
            config_reader=lambda role,entry:read_config_from_predecessor(self.docker,self.bundle,self.plan,role),
            require_all_qualifications=True)

    def _original(self):
        item=self.docker.inspect(self.plan['predecessor']['containerId'])
        require(item['Id']==self.plan['predecessor']['containerId'] and item['Image']==self.plan['predecessor']['imageId']
                and digest(immutable_container_recipe(item))==self.plan['bindings']['predecessorRecipeSha256']
                and digest(container_environment(item))==self.plan['bindings']['predecessorEnvironmentSha256'],
                'original_container_deployment_changed')
        return item

    def _metadata(self):
        counts=decode(private_bytes(self.bundle['artifacts']['reviewedCounts']['path']))
        key=private_bytes(self.bundle['artifacts']['censusKey']['path'],maximum=32)
        schema=private_bytes(self.bundle['artifacts']['predecessorSchema']['path'],maximum=65536)
        request=dict(schemaBase64=base64.b64encode(schema).decode(),keyHex=key.hex(),root='/prototype-state',
            authorityId=self.plan['authorityId'],nodeIds=[e['id'] for e in self.bundle['nodeEndpoints']],uid=10001,
            maxArtifactBytes=self.bundle['budgets']['maxArtifactBytes'],maxMetadataBytes=self.bundle['budgets']['maxMetadataBytes'],
            expectedCounts=counts,logicalRoot='/prototype-state',maxDurationSeconds=10,maxReadBytes=1024*1024,
            maxDatabaseBytes=8*1024*1024)
        result=self.boundary.fresh(self.plan['predecessor']['containerId'],census.serializable_probe_source(request).encode())
        require(result.get('ok') is True and result['result'].get('retainedFilesValidated') is False,
                'closed_original_metadata_required')
        return result['result']

    def preflight(self,plan):
        require(plan==self.plan,'driver_plan_changed');self.boundary.host();self._private()
        original=self._original()
        require(original['Name']=='/'+journal.NAME and original['State']['Running'] is True and
                original['HostConfig']['RestartPolicy']==plan['predecessor']['restartPolicy'],'original_preflight_state_changed')
        require(not any(self._find(role) for role in ('holder','bootstrap','final')),'candidate_already_present_before_preflight')
        metadata=self._metadata()
        physical=self.boundary.fresh(original['Id'],physical_binding_script(self.plan['authorityId'],self.settings['leaseDirectory']))
        require(digest(physical['state'])==plan['bindings']['stateSha256'] and
                digest(physical['authorityLease'])==plan['bindings']['authorityLeaseSha256'],'original_preflight_physical_changed')
        environment=container_environment(original)
        for role in ('bootstrap','final'):
            configured=environment_file(self.bundle['recipes'][role]['environmentFile'])
            for name in ('PROCESSING_PROTOTYPE_OPERATOR_TOKEN','PROCESSING_VIEWER_TOKEN','PROCESSING_VIEWER_STAGING_TOKEN'):
                require(configured.get(name)==environment.get(name) and configured.get(name),
                        'existing_operator_or_viewer_credentials_changed')
        owner=hashlib.sha256(environment['PROCESSING_PROTOTYPE_OPERATOR_TOKEN'].encode()).hexdigest()
        intent=dict(intentId=str(uuid.uuid5(uuid.UUID(plan['operationId']),'readonly-preflight')))
        self.proofs.retain(intent,dict(originalId=original['Id'],ownerSha256=owner,physical=physical,
                                       metadataSha256=digest(metadata)))
        return dict(deploymentSha256=plan['bindings']['deploymentSha256'],predecessorVerified=True,readOnly=True)

    def _custody(self):
        holder=self._find('holder');require(holder is not None,'original_holder_required')
        sources={name:Path(__file__).with_name(name+'.py').read_bytes() for name in custody.MODULE_NAMES}
        key=private_bytes(self.bundle['artifacts']['censusKey']['path'],maximum=32);budgets=self.bundle['budgets']
        return custody.CustodyOps(self.docker,holder_id=holder['Id'],holder_settings=self.settings,
            operation_directory=self.root,key=key,key_sha256=self.plan['bindings']['censusKeySha256'],
            module_sources=sources,module_sha256=self.bundle['custodySources'],
            reviewed_schema_bytes=private_bytes(self.bundle['artifacts']['predecessorSchema']['path'],maximum=65536),
            policy_bytes=private_bytes(self.bundle['artifacts']['policy']['path']),
            policy_sha256=self.plan['candidate']['declaredTransformSha256'],tracked_exec=self.tracked,boundary_check=self.boundary,
            max_artifact_bytes=budgets['maxArtifactBytes'],max_backup_bytes=budgets['maxBackupBytes'],
            max_metadata_bytes=budgets['maxMetadataBytes'],max_seconds=budgets['maxSeconds'],safety_floor_bytes=budgets['safetyFloorBytes'])

    def _summary(self,item):
        return dict(containerId=item['Id'],imageId=item['Image'],name=item['Name'],
            immutableSha256=digest(immutable_container_recipe(item)),environmentSha256=digest(container_environment(item)),
            running=item['State']['Running'],pid=item['State']['Pid'],startedAt=item['State']['StartedAt'],
            finishedAt=item['State']['FinishedAt'],exitCode=item['State']['ExitCode'],
            restartPolicy=item['HostConfig']['RestartPolicy'])

    def _stopped(self,item):
        require(item['State']['Running'] is False and item['State']['Pid']==0 and item['State']['ExitCode']==0
                and item['State'].get('OOMKilled') is False and not item['State'].get('Error'),
                'verified_graceful_container_stop_required')

    def _health(self,item,*,initial=False):
        if initial:
            require(item['State']['Running'] is True and item['State']['Pid']>0,'started_candidate_required')
        result=self.boundary.fresh(item['Id'],startup_health_script() if initial else operator_script('GET','/healthz'))
        require(result.get('status')==200,'actual_application_health_required')
        if initial:
            require(type(result.get('body')) is dict and result['body'].get('status')=='ok'
                    and result['body'].get('mode')=='routing_probe','invalid_startup_health_body')
            after=self.docker.inspect(item['Id'])
            require(after['Id']==item['Id'] and after['Image']==item['Image']
                    and after['State']['Running'] is True
                    and all(after['State'][key]==item['State'][key] for key in ('Pid','StartedAt')),
                    'startup_incarnation_changed')

    def _gate_request(self,action,state):
        startup=self._prior(state,'start-final' if action=='resume-final-gate' else 'start-bootstrap')['gate']['snapshot']['startupId']
        name={'intent-bootstrap-gate':'intent','drain-bootstrap-gate':'drain','seal-bootstrap-gate':'seal','resume-final-gate':'resume'}[action]
        return dict(requestId=str(uuid.uuid5(uuid.UUID(self.plan['operationId']),'gate:'+name)),
                    planId=self.plan['operationId'],expectedRevision={'intent':0,'drain':1,'seal':2,'resume':3}[name],
                    action=name,startupId=startup)

    def _window(self,action,intent=None,state=None):
        if intent is None:
            event=[e for e in state['events'] if e['action']==action][-1];intent=self._intent(event);dispatch=event['dispatch']
        else:dispatch=state['dispatch']
        token=dict(intentId=str(uuid.uuid5(uuid.UUID(intent['intentId']),'startup-window:'+dispatch['dispatchId'])))
        return token

    def _record_window(self,action,intent,state):
        now=datetime.now(timezone.utc).replace(microsecond=0)
        self.proofs.retain(self._window(action,intent,state),dict(earliest=now.strftime('%Y-%m-%d %H:%M:%S'),
                           latest=(now+timedelta(seconds=120)).strftime('%Y-%m-%d %H:%M:%S')))

    def _read_window(self,action,state):
        token=self._window(action,state=state)
        return decode(private_bytes(self.root/self.proofs.name(token)))['facts']

    def _binding(self,state,phase):
        baseline=self._prior(state,'capture-full-custody')['reference']
        original=self._custody().artifacts.load(baseline,'baseline')['body']['boundaryAfter']['physical']['operationBinding']
        bootstrap=self._prior(state,'start-bootstrap')['gate']['snapshot']['startupId']
        current=bootstrap;reader=custody_state=None
        if phase in ('final-frozen','rollback-final'):
            current=self._prior(state,'start-final')['gate']['snapshot']['startupId']
        if phase=='final-frozen':
            proof=self.boundary('final',self._context('final',state));reader=proof['physical']['sourceBinding'];custody_state=proof['custody']
        elif phase=='rollback-final':
            started=self._prior(state,'start-final');reader=started['boundary']['physical']['sourceBinding'];custody_state=started['boundary']['custody']
        return dict(bootstrapStartupId=bootstrap,currentStartupId=current,planId=self.plan['operationId'],
            ownerSha256=self._preflight()['ownerSha256'],requestIds={name:str(uuid.uuid5(uuid.UUID(self.plan['operationId']),'gate:'+name))
                for name in ('intent','drain','seal')},operationBinding=original,sourceReaderBinding=reader,sourceCustody=custody_state,
            bootstrapWindow=self._read_window('start-bootstrap',state),
            finalWindow=self._read_window('start-final',state) if self._prior(state,'start-final',optional=True) else None)

    def _old_schema_only(self,holder):
        schema=private_bytes(self.bundle['artifacts']['predecessorSchema']['path'],maximum=65536)
        source=Path(census.__file__).read_text()+'''\n
root=Path('/prototype-state');path=root/'jobs.sqlite3'
_require(not any((root/('jobs.sqlite3'+suffix)).exists() for suffix in ('-wal','-shm','-journal')),'sqlite_io_not_quiescent')
before=_owned(path,10001);_require(before.st_size<=8*1024*1024,'original_database_budget_exceeded')
db=sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=1)
try:
 db.execute('PRAGMA query_only=ON');db.execute('PRAGMA cache_size=-1024')
 db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH,1048576);db.setlimit(sqlite3.SQLITE_LIMIT_SQL_LENGTH,65536)
 deadline=time.monotonic()+5
 db.set_progress_handler(lambda:1 if time.monotonic()>deadline else 0,1000)
 objects=_schema(db,'''+repr(schema)+''')
 _require(_same(before)==_same(path.lstat()),'original_database_changed_during_schema_read')
 print(json.dumps(dict(schemaSha256=SCHEMA_SHA256,readOnly=True,hotJournalsAbsent=True)))
finally:db.close()
'''
        result=self.boundary.fresh(holder['Id'],source.encode())
        require(result==dict(schemaSha256=census.SCHEMA_SHA256,readOnly=True,hotJournalsAbsent=True),
                'original_only_schema_or_shutdown_uncertain')
        return result

    def guard(self,action,intent,state,*,inspection):
        require(intent['operationId']==self.plan['operationId'] and intent['planSha256']==self.plan_sha
                and action==intent['action'],'exact_driver_intent_required')
        from .first_worker_activation import guard_coordinator_progress
        guard_coordinator_progress(self,action)
        self.boundary.host();self._private();old=self._original()
        if action=='disable-predecessor-restart':
            require(old['State']['Running'] is True and old['Name']=='/'+journal.NAME,'running_original_required')
            require(old['HostConfig']['RestartPolicy'] in (self.plan['predecessor']['restartPolicy'],{'Name':'no','MaximumRetryCount':0}),
                    'original_restart_changed');return True
        require(old['HostConfig']['RestartPolicy']=={'Name':'no','MaximumRetryCount':0} or
                state['direction']=='rollback' and action=='restore-predecessor-restart',
                'original_restart_not_disabled')
        if action=='stop-predecessor':
            if old['State']['Running']:
                self._metadata()
            else:self._stopped(old)
            return True
        if state['direction']=='forward' or action not in ('start-exact-predecessor','verify-predecessor-readiness','restore-predecessor-restart'):
            self._stopped(old)
        if action in ('start-bootstrap','create-bootstrap','rename-predecessor','create-final','start-final'):
            holder=self._find('holder');require(holder is not None,'known_original_holder_required')
            self._stopped(holder)
        if action in ('acquire-original-leases','acquire-frozen-original-leases','acquire-rollback-original-leases'):
            # No other processing/coordinator writer may be live when a holder
            # is started. The same exact known helper may already own the lease.
            holder=self._find('holder');require(holder is not None,'known_holder_required')
            self.boundary.writers(holder['Id'])
        if action in ('intent-bootstrap-gate','drain-bootstrap-gate','seal-bootstrap-gate'):
            item=self._find('bootstrap');require(item is not None and item['State']['Running'] is True,'bootstrap_runtime_required')
            self.boundary.container(item['Id'],'bootstrap',self._creation(item))
        if action in ('verify-final-readiness','resume-final-gate'):
            final=self._find('final');require(final is not None,'known_final_required')
            current=self.boundary.gate(final['Id']) if action=='resume-final-gate' and inspection else None
            already_open=current is not None and current['snapshot']['phase']=='open'
            if already_open:
                # An accepted resume may already admit legitimate work. Do not
                # demand idle workers/empty projections or repost the request.
                # Exact stored receipt still must be verified by observe().
                self.boundary.container(final['Id'],'final',self._creation(final));self._health(final)
                expected=self._gate_request(action,state)
                require(current['snapshot']['revision']==4 and current['snapshot']['planId']==expected['planId']
                        and current['snapshot']['startupId']==expected['startupId']
                        and current['ownerSha256']==self._preflight()['ownerSha256'],'unknown_open_gate_cannot_be_adopted')
            else:
                self.boundary('final',self._context('final',state))
                if action=='resume-final-gate':
                    require(self._workers()==self._prior(state,'verify-final-readiness')['workers'],
                            'qualified_workers_changed_before_admission')
        if action=='enable-final-restart':
            final=self._find('final');require(final is not None,'final_runtime_required');self._health(final)
            gate=self.boundary.gate(final['Id'])
            require(gate['snapshot']['phase']=='open' and gate['snapshot']['revision']==4 and
                    gate['snapshot']['planId']==self.plan['operationId'] and
                    gate['ownerSha256']==self._preflight()['ownerSha256'],'acknowledged_open_final_gate_required')
        if action=='start-exact-predecessor':
            require(old['Name']=='/'+journal.NAME,'original_name_not_restored')
            holder=self._find('holder')
            if holder is not None:self._stopped(holder)
            for role in ('bootstrap','final'):
                item=self._find(role)
                if item is not None:require(item['State']['Running'] is False and item['State']['Pid']==0,'candidate_not_stopped')
        return True

    def guard_activation_rollback(self):
        from .first_worker_activation import guard_coordinator_progress
        guard_coordinator_progress(self,'begin-rollback')
        return True

    def _create(self,role,intent):
        existing=self._find(role)
        if existing is not None:
            require(role=='holder','owned_candidate_already_exists')
            self.boundary.container(existing['Id'],role,self._creation(existing));return
        args=create_arguments(self.bundle['recipes'][role],operation_id=self.plan['operationId'],role=self._role(role),
            intent_id=intent['intentId'],holder_settings=self.settings if role=='holder' else None)
        raw=self.docker.command(*args,mutation=True,timeout=60,output_limit=4096)
        identity=raw.decode('ascii').strip()
        require(len(identity)==64 and all(c in 'abcdef0123456789' for c in identity),'created_exact_container_id_required')

    def execute(self,action,intent,state):
        # Journal dispatch marker is checked again at the actual transport seam.
        dispatch=state['dispatch']
        require(type(dispatch) is dict and dispatch['intentSha256']==digest(intent),'durable_current_dispatch_required')
        if action=='disable-predecessor-restart':self.docker.restart_policy(self.plan['predecessor']['containerId'],{'Name':'no','MaximumRetryCount':0})
        elif action=='stop-predecessor':self.docker.stop(self.plan['predecessor']['containerId'])
        elif action in ('create-original-lease-holder','create-rollback-lease-holder'):self._create('holder',intent)
        elif action in ('create-bootstrap','create-final'):self._create(action.removeprefix('create-'),intent)
        elif action in ('acquire-original-leases','acquire-frozen-original-leases','acquire-rollback-original-leases'):
            item=self._find('holder')
            if item['State']['Running'] is not True:self.docker.start(item['Id'])
        elif action in ('release-original-leases','release-frozen-original-leases','release-rollback-original-leases'):
            self.docker.stop(self._find('holder')['Id'])
        elif action in ('start-bootstrap','start-final'):
            self._record_window(action,intent,state);self.docker.start(self._find(action.removeprefix('start-'))['Id'])
        elif action=='stop-bootstrap':self.docker.stop(self._find('bootstrap')['Id'])
        elif action in ('intent-bootstrap-gate','drain-bootstrap-gate','seal-bootstrap-gate','resume-final-gate'):
            role='final' if action=='resume-final-gate' else 'bootstrap';item=self._find(role)
            request=self._gate_request(action,state)
            source=operator_script('POST','/v1/prototype/coordinator-freeze',request)
            request_id=str(uuid.uuid5(uuid.UUID(intent['intentId']),'operator:'+dispatch['dispatchId']))
            packet=self.tracked(item['Id'],source,request_id=request_id,timeout=30,output_limit=65536)
            response=packet['result']
            if response.get('status')!=200:
                body=response.get('body',{})
                approved=(response.get('status')==409 and body.get('error')=='conflict' and
                          body.get('detail') in ('coordinator operations still draining','source IO still draining',
                                                'verified candidate health required'))
                if approved:
                    self.proofs.retain(intent,dict(request=request,containerId=item['Id'],response=response,
                        sourceSha256=hashlib.sha256(source).hexdigest(),requestId=request_id,
                        retiredProcess=packet['process']),dispatch=dispatch,kind='rejection')
                require(False,'operator_transition_not_accepted')
        elif action=='rename-predecessor':self.docker.rename(self.plan['predecessor']['containerId'],journal.NAME+'-retained-'+self.plan['operationId'].replace('-','')[:12])
        elif action=='enable-final-restart':self.docker.restart_policy(self._find('final')['Id'],self.plan['predecessor']['restartPolicy'])
        elif action=='copy-verified-backup':
            self._custody().backup(self._prior(state,'capture-full-custody')['reference'],intent,dispatch=dispatch,
                                  context=self._context('holder',state))
        elif action=='stop-exact-candidates':
            active=[self._find(role) for role in ('bootstrap','final')]
            active=[item for item in active if item is not None and item['State']['Running'] is True]
            require(len(active)<=1,'multiple_candidate_writers_unknown')
            if active:self.docker.stop(active[0]['Id'])
        elif action=='retain-final-name':
            final=self._find('final')
            if final is not None and final['Name']=='/'+journal.NAME:
                self.docker.rename(final['Id'],journal.NAME+'-candidate-retained-'+self.plan['operationId'].replace('-','')[:12])
        elif action=='restore-predecessor-name':
            old=self._original()
            if old['Name']!='/'+journal.NAME:self.docker.rename(old['Id'],journal.NAME)
        elif action=='start-exact-predecessor':
            old=self._original()
            if old['State']['Running'] is not True:self.docker.start(old['Id'])
        elif action=='restore-predecessor-restart':self.docker.restart_policy(self.plan['predecessor']['containerId'],self.plan['predecessor']['restartPolicy'])
        else:require(False,'unknown_driver_effect')

    def _workers(self):
        from .enrolled_worker_readiness_probe import serialize_probe
        from .first_worker_activation import activation_readiness_binding
        result=[]
        for worker in self.bundle['workers']:
            challenge=str(uuid.uuid4());source=serialize_probe(worker,challenge)
            tracker=TrackedWorkerProbe(worker,authority_id=self.plan['authorityId'],operation_id=self.plan['operationId'],
                                       operation_directory=self.root)
            packet=tracker(source,request_id=challenge,timeout=90,output_limit=65536)
            body=packet['result']
            require(packet['bodySucceeded'] is True and packet['process']['retired'] is True and
                    body.get('protocol')=='ltds-enrolled-worker-readiness-probe-v1' and body.get('challenge')==challenge
                    and body.get('nodeId')==worker['nodeId'] and body.get('settingsSha256')==digest(worker['settings']),
                    'fresh_authenticated_worker_probe_required')
            ready=body['readiness'];process=body['process']
            require(all(ready.get(k)==v for k,v in worker['expected'].items()) and ready.get('ready') is True
                    and ready.get('idle') is True and ready.get('pid')==process.get('pid') and process.get('active')=='active',
                    'actual_worker_identity_or_idle_changed')
            # Stable proof fields only; transient nonce is validated above but
            # does not turn a retry into a different immutable effect receipt.
            entry=dict(nodeId=worker['nodeId'],configSha256=ready['configSha256'],sourceSha256=ready['sourceSha256'],
                engineVersion=ready['engineVersion'],engineId=ready['engineId'],workerInstanceId=ready['workerInstanceId'],
                bootId=ready['bootId'],pid=process['pid'],startTime=process['startTime'],
                driverBindingSha256=ready['driverBindingSha256'],loadedSourceSha256=ready['loadedSourceSha256'],
                optionsSha256=ready['optionsSha256'],safeOptionsSha256=ready['safeOptionsSha256'])
            activated=activation_readiness_binding(self,worker,ready,process)
            if activated is not None:entry['firstActivation']=activated
            result.append(entry)
        return result

    def _observe(self,action,intent,state):
        if action=='disable-predecessor-restart':
            old=self._original();require(old['HostConfig']['RestartPolicy']=={'Name':'no','MaximumRetryCount':0},'restart_disable_not_observed')
            return self._summary(old)
        if action=='stop-predecessor':
            old=self._original();self._stopped(old);return self._summary(old)
        if action in ('create-original-lease-holder','create-rollback-lease-holder','create-bootstrap','create-final'):
            role='holder' if 'lease-holder' in action else action.removeprefix('create-')
            item=self._find(role);require(item is not None,'created_candidate_not_observed')
            self.boundary.container(item['Id'],role,self._creation(item))
            prior=any(e['action'] in ('create-original-lease-holder','create-rollback-lease-holder') for e in state['events'])
            if role=='holder' and prior:
                previous=self._prior(state,'create-original-lease-holder',optional=True) or self._prior(state,'create-rollback-lease-holder')
                require(previous['containerId']==item['Id'] and previous['immutableSha256']==digest(immutable_container_recipe(item))
                        and previous['environmentSha256']==digest(container_environment(item)),
                        'previously_created_holder_changed')
            if role!='holder' or not prior:
                require(self._creation(item)==intent['intentId'] and item['State']['Running'] is False and item['State']['Pid']==0
                        and item['State']['StartedAt'].startswith('0001-'),'newly_created_candidate_started_outside_journal')
            return dict(containerId=item['Id'],creationIntentId=self._creation(item),imageId=item['Image'],
                        immutableSha256=digest(immutable_container_recipe(item)),environmentSha256=digest(container_environment(item)))
        if action in ('acquire-original-leases','acquire-frozen-original-leases','acquire-rollback-original-leases'):
            return dict(boundary=self.boundary('holder',self._context('holder',state)))
        if action in ('release-original-leases','release-frozen-original-leases','release-rollback-original-leases'):
            item=self._find('holder');self._stopped(item);return self._summary(item)
        if action in ('start-bootstrap','start-final'):
            role=action.removeprefix('start-');item=self._find(role)
            self.boundary.container(item['Id'],role,self._creation(item));self._health(item,initial=True)
            gate=self.boundary.gate(item['Id']);snapshot=gate['snapshot']
            require(snapshot['authorityId']==self.plan['authorityId'] and gate['ownerSha256']==self._preflight()['ownerSha256']
                    and snapshot['phase']==('open' if role=='bootstrap' else 'frozen')
                    and snapshot['revision']==(0 if role=='bootstrap' else 3),'candidate_start_gate_changed')
            token=self._window(action,intent,state);window=decode(private_bytes(self.root/self.proofs.name(token)))['facts']
            try:
                started=datetime.fromisoformat(item['State']['StartedAt'].replace('Z','+00:00'))
                require(started.tzinfo is not None,'authenticated_container_start_time_required')
                stamp=started.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            except (ValueError,TypeError,AttributeError):
                require(False,'authenticated_container_start_time_required')
            # The immutable daemon start timestamp is bounded by the recorded
            # dispatch window. A delayed/restarted inspector is not an expiry.
            # Actual inventory startup-row time is additionally checked by the
            # declared projection before final admission.
            require(window['earliest']<=stamp<=window['latest'],'candidate_start_outside_dispatch_window')
            result=dict(container=self._summary(item),gate=gate,window=window)
            if role=='final':
                context=dict(containerId=item['Id'],creationIntentId=self._creation(item),
                             gate=dict(startupId=snapshot['startupId'],ownerSha256=self._preflight()['ownerSha256']))
                result['boundary']=self.boundary('final',context)
            return result
        if action in ('intent-bootstrap-gate','drain-bootstrap-gate','seal-bootstrap-gate','resume-final-gate'):
            role='final' if action=='resume-final-gate' else 'bootstrap';item=self._find(role)
            request=self._gate_request(action,state)
            found=self.boundary.fresh(item['Id'],gate_receipt_script(request))
            phase={'intent':'intent','drain':'draining','seal':'frozen','resume':'open'}[request['action']]
            expected=dict(authorityId=self.plan['authorityId'],startupId=request['startupId'],planId=request['planId'],
                          revision=request['expectedRevision']+1,phase=phase,requestId=request['requestId'])
            require(found.get('found') is True and found.get('receipt')==expected
                    and found.get('ownerSha256')==self._preflight()['ownerSha256'], 'exact_existing_gate_receipt_required')
            return dict(containerId=item['Id'],request=request,receipt=expected,ownerSha256=found['ownerSha256'])
        if action=='stop-bootstrap':
            item=self._find('bootstrap');self._stopped(item);return self._summary(item)
        if action=='capture-full-custody':
            return dict(reference=self._custody().capture(intent,context=self._context('holder',state)))
        if action=='copy-verified-backup':
            return dict(reference=self._custody().inspect_backup(self._prior(state,'capture-full-custody')['reference'],intent,
                        dispatch=state['dispatch'],context=self._context('holder',state)))
        if action in ('verify-frozen-transform','verify-final-readiness','verify-rollback-projection'):
            phase='bootstrap-frozen' if action=='verify-frozen-transform' else 'final-frozen' if action=='verify-final-readiness' else (
                'rollback-final' if self._prior(state,'start-final',optional=True) else 'rollback-bootstrap')
            owner='final' if phase=='final-frozen' else 'holder';binding=self._binding(state,phase)
            reference=self._custody().projection(self._prior(state,'capture-full-custody')['reference'],intent,phase=phase,
                binding=binding,context=self._context(owner,state),final_id=self._find('final')['Id'] if owner=='final' else None)
            return dict(reference=reference,binding=binding,workers=self._workers() if action=='verify-final-readiness' else None)
        if action=='rename-predecessor':
            old=self._original();require(old['Name']=='/'+journal.NAME+'-retained-'+self.plan['operationId'].replace('-','')[:12],
                                         'retained_original_name_not_observed');return self._summary(old)
        if action=='enable-final-restart':
            item=self._find('final');require(item['HostConfig']['RestartPolicy']==self.plan['predecessor']['restartPolicy'],
                                          'final_restart_policy_not_observed');return self._summary(item)
        if action=='stop-exact-candidates':
            result=[]
            for role in ('bootstrap','final'):
                item=self._find(role)
                if item is not None:self._stopped(item);result.append(self._summary(item))
            return dict(candidates=result)
        if action=='verify-original-before-candidate':
            require(state['rollbackScope']=='original-only' and not self._prior(state,'start-bootstrap',optional=True),
                    'original_only_rollback_not_proven')
            proof=self.boundary('holder',self._context('holder',state));holder=self._find('holder')
            return dict(boundary=proof,schema=self._old_schema_only(holder))
        if action=='retain-final-name':
            item=self._find('final')
            if item is None:
                require(not self._prior(state,'create-final',optional=True),'known_final_missing');return dict(final=None)
            require(item['Name']=='/'+journal.NAME+'-candidate-retained-'+self.plan['operationId'].replace('-','')[:12] or
                    item['Name']!='/'+journal.NAME and self.bundle['recipes']['final']['name']!=journal.NAME,
                    'final_name_not_retained')
            prior=self._prior(state,'create-final');require(digest(immutable_container_recipe(item))==prior['immutableSha256']
                and digest(container_environment(item))==prior['environmentSha256'],'retained_candidate_deployment_changed')
            return self._summary(item)
        if action=='restore-predecessor-name':
            old=self._original();require(old['Name']=='/'+journal.NAME,'original_name_not_restored');return self._summary(old)
        if action in ('start-exact-predecessor','verify-predecessor-readiness','restore-predecessor-restart'):
            old=self._original();require(old['State']['Running'] is True,'original_not_running');self._health(old)
            if action=='restore-predecessor-restart':
                require(old['HostConfig']['RestartPolicy']==self.plan['predecessor']['restartPolicy'],'original_restart_not_restored')
            return self._summary(old)
        require(False,'unknown_driver_observation')

    def observe(self,action,intent,state):
        facts=self._observe(action,intent,state)
        hashed=self.proofs.retain(intent,facts,dispatch=state['dispatch'])
        return dict(intentId=intent['intentId'],bindingSha256=digest(intent),factsSha256=hashed)

    def verify(self,action,observation,intent,state):
        raw=private_bytes(self.root/self.proofs.name(intent,state['dispatch']))
        require(hashlib.sha256(raw).hexdigest()==observation['factsSha256'],'actual_effect_evidence_changed')
        document=decode(raw)
        require(document['intentSha256']==digest(intent) and document['dispatch']==state['dispatch'],
                'actual_effect_dispatch_changed')
        # A fresh readback must still corroborate the retained immutable effect.
        require(self._observe(action,intent,state)==document['facts'],'effect_changed_before_journal_ack')
        return True

    def observe_not_applied(self,action,intent,state):
        dispatch=state['dispatch']
        if dispatch is None:
            facts=dict(proofKind='pre-dispatch',intentSha256=digest(intent))
            proof=self.proofs.retain(dict(intentId=str(uuid.uuid5(uuid.UUID(intent['intentId']),'no-dispatch'))),facts)
            return dict(intentId=intent['intentId'],bindingSha256=digest(intent),factsSha256=proof,outcome='not-applied',
                        proofKind='pre-dispatch',dispatchId=None)
        raw=private_bytes(self.root/self.proofs.name(intent,dispatch,kind='rejection'))
        document=decode(raw);facts=document['facts']
        require(document['intentSha256']==digest(intent) and document['dispatch']==dispatch
                and action in ('intent-bootstrap-gate','drain-bootstrap-gate','seal-bootstrap-gate','resume-final-gate')
                and facts['request']==self._gate_request(action,state)
                and facts['response']['status']==409 and facts['response']['body']['error']=='conflict'
                and facts['response']['body']['detail'] in ('coordinator operations still draining','source IO still draining',
                                                          'verified candidate health required'),
                'code_bound_current_authenticated_rejection_required')
        return dict(intentId=intent['intentId'],bindingSha256=digest(intent),factsSha256=hashlib.sha256(raw).hexdigest(),
                    outcome='not-applied',proofKind='authenticated-rejection',dispatchId=dispatch['dispatchId'])

    def verify_not_applied(self,action,observation,intent,state):
        require(self.observe_not_applied(action,intent,state)==observation,'not_applied_evidence_changed')
        return True


def main(argv=None):
    """One explicit operator action; no implicit apply, loop or live defaults."""
    import argparse
    import json
    import os
    import re
    parser=argparse.ArgumentParser(description='One reviewed enrolled-coordinator preparation/cutover action')
    parser.add_argument('--plan',required=True,help='existing private0600 reviewed plan JSON')
    parser.add_argument('--bundle',required=True,help='existing private0600 deployment bundle JSON')
    parser.add_argument('--receipt',required=True,help='existing private0700 operation directory')
    parser.add_argument('--action',required=True,choices=('status','prepare','advance','inspect','rollback',
        'reconcile-rearm','reconcile-rollback','config-create','config-start','config-inspect','config-cleanup'))
    parser.add_argument('--bootstrap-input',help='private operator0600 bootstrap payload, config actions only')
    parser.add_argument('--final-input',help='private operator0600 final payload, config actions only')
    args=parser.parse_args(argv)
    try:
        from .enrolled_coordinator_deployment import read_bundle
        from .enrolled_config_stage import ConfigPreparation
        require(os.name=='posix','linux_enrolled_driver_required')
        plan=journal.validate_plan(decode(private_bytes(args.plan,maximum=262144),262144))
        bundle=read_bundle(args.bundle,plan)
        if args.action.startswith('config-'):
            preparation=ConfigPreparation(plan,bundle,args.receipt)
            if args.action in ('config-inspect','config-cleanup'):
                require(args.bootstrap_input is None and args.final_input is None,'config_inputs_on_readback_or_cleanup_refused')
                result=preparation.inspect() if args.action=='config-inspect' else preparation.cleanup()
            else:
                require(args.bootstrap_input is not None and args.final_input is not None,'private_config_inputs_required')
                method=preparation.create if args.action=='config-create' else preparation.start
                result=method(args.bootstrap_input,args.final_input)
            print(json.dumps(dict(action=args.action,phase=result['phase'],scope='configuration-preparation-only')))
            return 0
        require(args.bootstrap_input is None and args.final_input is None,'config_inputs_on_cutover_action_refused')
        driver=ConcreteCutoverDriver(plan,bundle,args.receipt)
        controller=journal.FirstEnrolledCutover(args.receipt,plan,driver)
        if args.action=='status':state=controller._load()  # Pure private metadata read, no global lock/write.
        elif args.action=='prepare':state=controller.prepare()
        elif args.action=='advance':state=controller.advance()
        elif args.action=='inspect':state=controller.inspect()
        elif args.action=='rollback':state=controller.begin_rollback()
        else:state=controller.reconcile_not_applied(disposition='rearm' if args.action=='reconcile-rearm' else 'rollback')
        print(json.dumps(dict(action=args.action,phase=state['phase'],direction=state['direction'],
            index=state['index'],pendingAction=state['pending']['action'] if state['pending'] else None,
            admissionOpened=state['admissionOpened'],scope='journal-metadata-only' if args.action=='status' else 'reviewed-single-action')))
        return 0
    except Exception as error:
        from .enrolled_coordinator_transport import TransportHeld
        reason=str(error) if type(error) in (TransportHeld,journal.CutoverHeld) else 'enrolled_action_held'
        if not re.fullmatch('[a-z][a-z0-9_]{0,120}',reason):reason='enrolled_action_held'
        # Never print argv, private paths, payloads, tokens, stderr or tracebacks.
        print(json.dumps(dict(status='held',reason=reason)))
        return 2


if __name__=='__main__':raise SystemExit(main())
