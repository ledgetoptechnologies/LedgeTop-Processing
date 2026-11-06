"""Controller-owned first-worker activation journal, never a queue/controller.

The boundary supplies actual authenticated observations and narrowly tracked
service-user commands. Missing external pilot retirement is a held prerequisite,
never a boolean that this module can grant or an install-only receipt can imply.
"""
from pathlib import Path
import os
import re
import uuid

from .enrolled_coordinator_cutover import CutoverJournal
from .enrolled_coordinator_transport import canonical,decode,digest,require,private_bytes,PrivateProofs


PROTOCOL='ltds-first-worker-activation-v1'
STEPS=('reload','start')
BINDING=('operationId','cutoverPlanSha256','nodeId','workerSha256','manifestSha256',
         'installReceiptSha256','bootstrapStartupId','ownerSha256','ingressPolicySha256','nativeInventorySha256')


def binding(value):
    require(type(value) is dict and set(value)==set(BINDING),'closed_worker_activation_binding_required')
    for key in ('operationId','bootstrapStartupId'):
        require(type(value[key]) is str and str(uuid.UUID(value[key]))==value[key],
                'canonical_worker_activation_identity_required')
    require(type(value['nodeId']) is str and re.fullmatch('[A-Za-z0-9][A-Za-z0-9._-]{0,63}',value['nodeId']),
            'worker_activation_node_required')
    for key in ('cutoverPlanSha256','workerSha256','manifestSha256','installReceiptSha256','ownerSha256','ingressPolicySha256','nativeInventorySha256'):
        require(type(value[key]) is str and re.fullmatch('[a-f0-9]{64}',value[key]),'worker_activation_pin_required')
    return decode(canonical(value))


def journal_path(root,node):return Path(root)/('first-worker-activation-'+node+'.json')


def registration_intent(operation,node):
    return dict(intentId=str(uuid.uuid5(uuid.UUID(operation),PROTOCOL+':registration:'+node)))


def effect_intent(current):return dict(intentId=current['requestId'],activationIntentSha256=digest(current))


def effect_evidence(root,current):
    token=effect_intent(current);path=Path(root)/PrivateProofs(root).name(token)
    if not os.path.lexists(path):return None
    raw=private_bytes(path,maximum=262144);value=decode(raw,262144)
    require(type(value) is dict and set(value)=={'schemaVersion','intentSha256','dispatch','facts'}
            and value['schemaVersion']==1 and value['intentSha256']==digest(token) and value['dispatch'] is None
            and type(value['facts']) is dict,'activation_effect_evidence_changed')
    return value['facts'],__import__('hashlib').sha256(raw).hexdigest()


def registered_binding(root,operation,node):
    token=registration_intent(operation,node);proofs=PrivateProofs(root);path=Path(root)/proofs.name(token)
    if not os.path.lexists(path):return None
    value=decode(private_bytes(path,maximum=16384),16384)
    require(type(value) is dict and set(value)=={'schemaVersion','intentSha256','dispatch','facts'}
            and value['schemaVersion']==1 and value['intentSha256']==digest(token) and value['dispatch'] is None,
            'activation_registration_corrupt')
    result=binding(value['facts'])
    require(result['operationId']==operation and result['nodeId']==node,'activation_registration_owner_changed')
    return result


def read_activation(root,expected):
    expected=binding(expected);value=decode(private_bytes(journal_path(root,expected['nodeId']),maximum=262144),262144)
    require(registered_binding(root,expected['operationId'],expected['nodeId'])==expected,
            'original_activation_registration_required')
    require(type(value) is dict and set(value)=={'schemaVersion','protocol','binding','phase','pending','events','receipt'}
            and type(value['schemaVersion']) is int and value['schemaVersion']==1 and value['protocol']==PROTOCOL
            and value['binding']==expected and value['phase'] in ('prepared','reload-pending','reloaded','start-pending','ready')
            and type(value['events']) is list and len(value['events'])<=2,'closed_activation_journal_required')
    names=[]
    for event in value['events']:
        require(type(event) is dict and set(event)=={'intent','observation','proofSha256'} and event['intent']==intent(expected,len(names))
                and type(event['observation']) is dict,'activation_event_binding_changed')
        proof=effect_evidence(root,event['intent'])
        require(proof==(event['observation'],event['proofSha256']),'accepted_activation_evidence_missing_or_changed')
        names.append(event['intent']['action'])
    require(names==list(STEPS[:len(names)]),'activation_event_order_changed')
    pending=value['pending']
    if pending is not None:
        require(len(names)<2 and pending==intent(expected,len(names)) and value['phase']==STEPS[len(names)]+'-pending',
                'activation_pending_changed')
    else:
        require(value['phase']==('prepared','reloaded','ready')[len(names)],'activation_phase_changed')
    require((value['receipt'] is not None)==(value['phase']=='ready'),'activation_terminal_receipt_changed')
    if value['receipt'] is not None:
        require(value['receipt']==receipt(value),'activation_receipt_binding_changed')
    return value


def intent(expected,index):
    return dict(schemaVersion=1,operationId=expected['operationId'],bindingSha256=digest(expected),action=STEPS[index],
        requestId=str(uuid.uuid5(uuid.UUID(expected['operationId']),PROTOCOL+':'+digest(expected)+':'+STEPS[index])))


def receipt(state):
    return dict(schemaVersion=1,protocol=PROTOCOL,bindingSha256=digest(state['binding']),
                eventsSha256=digest(state['events']),readinessSha256=digest(state['events'][-1]['observation']),
                serviceEnableIssued=False,coordinatorResumeIssued=False)


def bootstrap_physical_source(authority,lease_directory,pid):
    from .enrolled_coordinator_transport import physical_binding_script
    source=physical_binding_script(authority,lease_directory,process_id=pid).decode()
    output="print(json.dumps(result,sort_keys=True,separators=(',',':')))"
    require(source.count(output)==1,'reviewed_bootstrap_physical_template_required')
    proof='''
pid=settings['processId']
if type(pid) is not int or pid<=0:raise RuntimeError('actual_bootstrap_process_required')
target=Path('/proc')/str(pid)
start=(target/'stat').read_text().rsplit(')',1)[1].split()[19]
found={}
for label,path,identity in [('main',str(lock),key(ls)),('external',str(lease),key(es))]:
 matches=[]
 for index,fd in enumerate((target/'fd').iterdir()):
  if index>=512:raise RuntimeError('bootstrap_descriptor_budget_exceeded')
  try:
   if os.readlink(fd)!=path:continue
   if key(fd.stat())!=identity:raise RuntimeError('bootstrap_descriptor_identity_changed')
   raw=(target/'fdinfo'/fd.name).read_text()
   rows=[line.split() for line in raw.splitlines() if line.startswith('lock:')]
   if len(raw)>4096 or len(rows)!=1 or rows[0][2:5]!=['FLOCK','ADVISORY','WRITE']:raise RuntimeError('bootstrap_kernel_flock_unproved')
   matches.append(int(fd.name))
  except FileNotFoundError:continue
 if len(matches)!=1:raise RuntimeError('bootstrap_original_lease_owner_unproved')
 found[label]=matches[0]
if start!=(target/'stat').read_text().rsplit(')',1)[1].split()[19]:raise RuntimeError('bootstrap_process_changed')
result.update(processId=pid,processStartTicks=start,heldDescriptors=found)
'''
    return (source.replace(output,proof+'\n'+output)).encode()


def installed_first_worker(worker):
    """Reserved install-only namespace, never infer start authority from absence."""
    path=Path(worker['settings']['sourceRoot']);base=Path('/opt/ledgetop-processing-reference')
    if base not in path.parents:return False
    require(path.name=='source' and path.parent.parent==base
            and str(uuid.UUID(path.parent.name))==path.parent.name,'unknown_first_worker_installation_profile')
    return True


def existing_activation(driver,worker,*,required=False):
    path=journal_path(driver.root,worker['nodeId'])
    registered=registered_binding(driver.root,driver.plan['operationId'],worker['nodeId'])
    if not os.path.lexists(path):
        require(registered is None,'registered_activation_journal_missing')
        require(not required,'installed_first_worker_activation_receipt_required');return None
    value=decode(private_bytes(path,maximum=262144),262144)
    require(type(value) is dict and 'binding' in value,'activation_journal_requires_inspection')
    expected=binding(value['binding'])
    require(registered==expected,'original_activation_registration_required')
    require(expected['operationId']==driver.plan['operationId'] and expected['cutoverPlanSha256']==digest(driver.plan)
            and expected['nodeId']==worker['nodeId'] and expected['workerSha256']==digest(worker),
            'activation_journal_owner_changed')
    return read_activation(driver.root,expected)


def guard_coordinator_progress(driver,action):
    for worker in driver.bundle['workers']:
        # Existing already-managed worker probes retain their ordinary behavior.
        # First installs in the installer's reserved namespace must activate at
        # bootstrap; an absent journal is never first-start authorization.
        required=action in ('stop-bootstrap','verify-final-readiness','resume-final-gate') and installed_first_worker(worker)
        state=existing_activation(driver,worker,required=required)
        if state is not None:
            require(state['phase']=='ready' and state['pending'] is None,'worker_activation_blocks_coordinator_progress')


def activation_readiness_binding(driver,worker,ready,process):
    state=existing_activation(driver,worker,required=installed_first_worker(worker))
    if state is None:return None
    require(state['phase']=='ready' and state['pending'] is None,'accepted_first_worker_activation_required')
    accepted=state['events'][-1]['observation']['readiness']
    require(type(accepted) is dict and set(accepted)=={'schemaVersion','protocol','challenge','nodeId','settingsSha256','readiness','process'}
            and accepted['protocol']=='ltds-enrolled-worker-readiness-probe-v1'
            and accepted['nodeId']==worker['nodeId'] and accepted['settingsSha256']==digest(worker['settings'])
            and accepted['process']==process
            and all(accepted['readiness'][key]==ready[key] for key in
                ('bootId','pid','nodeId','workerId','workerInstanceId','authorityId','engineId','gateGeneration',
                 'engineVersion','configSha256','sourceSha256','driverBindingSha256','readerEpoch','activation',
                 'optionsSha256','safeOptionsSha256','loadedSourceSha256')),
            'first_worker_changed_since_accepted_activation')
    return dict(receiptSha256=digest(state['receipt']),bindingSha256=digest(state['binding']),
                manifestSha256=state['binding']['manifestSha256'],installReceiptSha256=state['binding']['installReceiptSha256'])


class FirstWorkerActivation:
    def __init__(self,root,expected,*,boundary,driver):
        self.binding=binding(expected);self.boundary=boundary;self.driver=driver
        require(driver.plan['operationId']==self.binding['operationId'] and digest(driver.plan)==self.binding['cutoverPlanSha256'],
                'same_cutover_activation_owner_required')
        self.journal=CutoverJournal(root);self.journal.path=journal_path(root,self.binding['nodeId'])

    def _save(self,state):self.journal.write(state)

    def _load(self):return read_activation(self.journal.root,self.binding)

    def prepare(self):
        with self.driver.global_lock(self.driver.plan),self.journal.locked():
            if self.journal.path.exists():return self._load()
            require(registered_binding(self.journal.root,self.binding['operationId'],self.binding['nodeId']) is None,
                    'registered_activation_journal_missing')
            require(self.boundary.preflight(self.binding) is True,'actual_activation_preflight_required')
            PrivateProofs(self.journal.root).retain(registration_intent(self.binding['operationId'],self.binding['nodeId']),
                                                  self.binding,must_create=True)
            state=dict(schemaVersion=1,protocol=PROTOCOL,binding=self.binding,phase='prepared',pending=None,events=[],receipt=None)
            self._save(state);return state

    def advance(self):
        with self.driver.global_lock(self.driver.plan),self.journal.locked():
            state=self._load()
            require(state['pending'] is None,'unknown_activation_effect_is_inspection_only')
            if state['phase']=='ready':return state
            current=intent(self.binding,len(state['events']))
            require(self.boundary.guard(self.binding,current,inspection=False) is True,'actual_activation_guard_required')
            state.update(pending=current,phase=current['action']+'-pending');self._save(state)
            # The exact intent is durable before the only service mutation.
            # Any exception retains it; no caller repeats that request on inspect.
            self.boundary.execute(self.binding,current)
            return self._inspect(state)

    def _inspect(self,state):
        current=state['pending'];require(current is not None,'activation_pending_required')
        require(self.boundary.guard(self.binding,current,inspection=True) is True,'actual_activation_guard_required')
        observation,proof_sha=self._evidence(current)
        # Actual adapter verifies exact source/request and remote process
        # retirement plus fresh systemd/readiness. A bare ready flag is invalid.
        require(self.boundary.verify(self.binding,current,observation) is True,'actual_activation_verification_required')
        state['events'].append(dict(intent=current,observation=observation,proofSha256=proof_sha));state['pending']=None
        state['phase']='reloaded' if current['action']=='reload' else 'ready'
        if state['phase']=='ready':state['receipt']=receipt(state)
        self._save(state);return state

    def _evidence(self,current):
        saved=effect_evidence(self.journal.root,current)
        if saved is not None:
            # Known exact bytes after an uncertain journal ACK still require
            # both durability barriers and fresh actual observation verification.
            proof=PrivateProofs(self.journal.root).retain(effect_intent(current),saved[0])
            require(proof==saved[1],'accepted_activation_evidence_changed')
            return saved
        observation=self.boundary.observe(self.binding,current)
        proof=PrivateProofs(self.journal.root).retain(effect_intent(current),observation,must_create=True)
        return observation,proof

    def inspect(self):
        with self.driver.global_lock(self.driver.plan),self.journal.locked():
            state=self._load()
            if state['pending'] is None:return state
            return self._inspect(state)


class ControllerActivationBoundary:
    """Actual controller/SSH seams plus the exact historical pilot validator.

    Missing reviewed receipt, installation or live observations always hold.
    No caller-supplied ready flag or pilot-approval callback is accepted.
    """
    def __init__(self,driver,cutover,worker,manifest,install_receipt,ingress,native_inventory,*,tracker=None):
        from .enroll_reference_worker import validate_manifest
        from .enrolled_coordinator_ssh import TrackedWorkerProbe
        self.driver,self.cutover,self.worker=driver,cutover,decode(canonical(worker))
        self.manifest=validate_manifest(manifest);self.installed=decode(canonical(install_receipt))
        from .historical_pilot_closure import HistoricalPilotClosure
        self.ingress=decode(canonical(ingress));self.native_inventory=decode(canonical(native_inventory))
        self.pilot_closure=HistoricalPilotClosure()
        self.tracker=tracker or TrackedWorkerProbe(worker,authority_id=driver.plan['authorityId'],
            operation_id=driver.plan['operationId'],operation_directory=driver.root)

    def _installation(self,expected):
        m=self.manifest;w=self.worker
        wanted=dict(schemaVersion=1,protocol='ltds-first-worker-enrollment-v1',operationId=m['operationId'],
            manifestSha256=digest(m),sourceSha256=m['source']['footprintSha256'],configSha256=m['config']['sha256'],
            unitSha256=m['unitSha256'],unitInstalled=True,serviceStartIssued=False,serviceEnableIssued=False,engineMutationIssued=False,
            installedTreeSchemaVersion=1,installedTreeSha256=self.installed.get('installedTreeSha256'))
        require(type(self.installed.get('installedTreeSchemaVersion')) is int and self.installed['installedTreeSchemaVersion']==1
                and type(self.installed.get('installedTreeSha256')) is str and
                re.fullmatch('[a-f0-9]{64}',self.installed['installedTreeSha256']),'first_activation_requires_bound_installed_tree')
        require(self.installed==wanted and digest(self.installed)==expected['installReceiptSha256']
                and digest(self.ingress)==expected['ingressPolicySha256']
                and digest(self.native_inventory)==expected['nativeInventorySha256']
                and digest(m)==expected['manifestSha256'] and digest(w)==expected['workerSha256']
                and w['nodeId']==expected['nodeId']==m['gate']['nodeId']
                and w['sshUid']==m['service']['uid'] and w['expected']['authorityId']==self.driver.plan['authorityId']
                and m['gate']['authorityId']==self.driver.plan['authorityId'] and m['host']['dockerEngineId']==w['expected']['engineId']
                and m['gate']['generation']==w['expected']['gateGeneration'] and m['engine']['version']==w['expected']['engineVersion']
                and w['expected']['activation'] is None
                and w['expected']['sourceSha256']==m['source']['footprintSha256']
                and w['expected']['configSha256']==m['config']['sha256']
                and w['settings']['unitSha256']==m['unitSha256'],'exact_install_only_activation_binding_required')

    def _bootstrap(self,expected):
        from .enrolled_coordinator_transport import operator_script
        d=self.driver;state=self.cutover._load()
        require(state['phase']=='bootstrap-frozen' and state['direction']=='forward' and state['pending'] is None
                and state['admissionOpened'] is False,'same_bootstrap_frozen_operation_required')
        d.boundary.host();item=d._find('bootstrap');require(item is not None,'known_bootstrap_required')
        d.boundary.container(item['Id'],'bootstrap',d._creation(item));d._health(item)
        d.boundary.writers(item['Id']);gate=d.boundary.gate(item['Id']);snapshot=gate['snapshot']
        require(snapshot['phase']=='frozen' and snapshot['revision']==3 and snapshot['operations']==0
                and snapshot['authorityId']==d.plan['authorityId'] and snapshot['planId']==expected['operationId']
                and snapshot['startupId']==expected['bootstrapStartupId']
                and gate['ownerSha256']==expected['ownerSha256']==d._preflight()['ownerSha256'],
                'authenticated_bootstrap_activation_binding_changed')
        response=d.boundary.fresh(item['Id'],operator_script('GET','/v1/prototype/coordinator-custody'))
        require(response.get('status')==200,'authenticated_bootstrap_custody_required');custody=response['body']
        require(custody.get('status')=='absent' and custody.get('sourceReadersPresent') is False
                and custody.get('authorityId')==d.plan['authorityId'] and custody.get('startupId')==expected['bootstrapStartupId']
                and type(custody.get('processId')) is int and custody['processId']>0,
                'bootstrap_must_not_claim_final_reader_custody')
        physical=d.boundary.fresh(item['Id'],bootstrap_physical_source(d.plan['authorityId'],d.settings['leaseDirectory'],custody['processId']))
        require(digest(physical['state'])==d.plan['bindings']['stateSha256']
                and digest(physical['authorityLease'])==d.plan['bindings']['authorityLeaseSha256']
                and physical['processStartTicks']==custody['processStartTicks'],
                'bootstrap_physical_original_custody_changed')
        d.boundary.writers(item['Id']);current=d.docker.inspect(item['Id'])
        require(current['State']['Running'] is True and current['State']['Pid']==item['State']['Pid']
                and current['State']['StartedAt']==item['State']['StartedAt'],'bootstrap_changed_during_activation_guard')
        return dict(gate=gate,physicalSha256=digest(physical),custodySha256=digest(custody),containerId=item['Id'])

    def _pilot(self,expected):
        require(self.pilot_closure is not None,'pilot_supervisor_retirement_unproved')
        # This interface accepts only the output of an actual implemented
        # provenance verifier. Historical artifact hashes alone cannot supply it.
        proof=self.pilot_closure.observe_current(self.driver,self.manifest)
        fields={'schemaVersion','protocol','producerSourceSha256','originalInvocationSha256',
                'namespaceTerminationSha256','noPersistentResumerSha256'}
        require(type(proof) is dict and set(proof)==fields and type(proof['schemaVersion']) is int
                and proof['schemaVersion']==1 and proof['protocol']=='ltds-pilot-namespace-retirement-v1'
                and all(type(proof[k]) is str and re.fullmatch('[a-f0-9]{64}',proof[k])
                        for k in fields-{'schemaVersion','protocol'}),'actual_pilot_retirement_evidence_required')
        return proof

    def _source(self,action,request):
        from .first_worker_activation_service import serialize_service
        return serialize_service(self.worker,self.manifest,self.ingress,self.native_inventory,self.installed['installedTreeSha256'],action,request)

    def _packet(self,packet,source,request):
        fields={'schemaVersion','protocol','operationId','requestId','hostBindingSha256','sourceSha256','process','bodySucceeded','result'}
        require(type(packet) is dict and set(packet)==fields and packet['schemaVersion']==1
                and packet['protocol']=='ltds-cutover-ssh-exec-v1' and packet['operationId']==self.driver.plan['operationId']
                and packet['requestId']==request and packet['hostBindingSha256']==digest(self.worker)
                and packet['sourceSha256']==__import__('hashlib').sha256(source).hexdigest()
                and packet['bodySucceeded'] is True and type(packet['process']) is dict
                and set(packet['process'])=={'pid','startTicks','retired'} and packet['process']['retired'] is True
                and type(packet['process']['pid']) is int and packet['process']['pid']>0
                and type(packet['process']['startTicks']) is str and packet['process']['startTicks'].isdigit(),
                'exact_retired_activation_ssh_body_required')
        return packet['result']

    def _fresh(self,action):
        request=str(uuid.uuid4());source=self._source(action,request)
        packet=self.tracker(source,request_id=request,timeout=120,output_limit=65536)
        result=self._packet(packet,source,request)
        self._service_result(result,action,request);return result

    def _service_result(self,result,action,request):
        require(type(result) is dict and set(result)=={'schemaVersion','protocol','challenge','action','workerSha256',
                'manifestSha256','managerEnvironmentSha256','ingress','native','process'} and result['schemaVersion']==1
                and result['protocol']=='ltds-first-worker-service-v1' and result['challenge']==request
                and result['action']==action and result['workerSha256']==digest(self.worker)
                and result['manifestSha256']==digest(self.manifest),'closed_activation_service_observation_required')

    def preflight(self,expected):
        self._installation(expected);self._bootstrap(expected);self._fresh('preflight');self._pilot(expected);return True

    def guard(self,expected,current,*,inspection):
        self._installation(expected);self._bootstrap(expected);self._pilot(expected)
        if not inspection:self._fresh('preflight')
        return True

    def execute(self,expected,current):
        require(current['action'] in STEPS and current['bindingSha256']==digest(expected),'exact_activation_intent_required')
        pending=read_activation(self.driver.root,expected)
        require(pending['pending']==current and pending['phase']==current['action']+'-pending',
                'durable_activation_intent_before_service_effect_required')
        self._installation(expected);self._bootstrap(expected);self._pilot(expected)
        source=self._source(current['action'],current['requestId'])
        self.tracker(source,request_id=current['requestId'],timeout=120,output_limit=65536)

    def observe(self,expected,current):
        source=self._source(current['action'],current['requestId'])
        # Inspection NEVER dispatches the service command, including if the
        # original controller died before obtaining a remote start record.
        packet=self.tracker.inspect_request(source,request_id=current['requestId'],output_limit=65536)
        result=self._packet(packet,source,current['requestId']);self._service_result(result,current['action'],current['requestId'])
        observed=self._fresh('observe');ready=None
        if current['action']=='reload':require(observed['process']['pid']==0 and observed['process']['active']=='inactive',
                                             'reloaded_worker_must_remain_inactive')
        else:
            from .enrolled_worker_readiness_probe import serialize_probe
            request=str(uuid.uuid4());body=serialize_probe(self.worker,request)
            ready=self._packet(self.tracker(body,request_id=request,timeout=120,output_limit=65536),body,request)
            require(ready.get('protocol')=='ltds-enrolled-worker-readiness-probe-v1' and ready.get('challenge')==request
                    and ready.get('nodeId')==self.worker['nodeId']
                    and ready.get('settingsSha256')==digest(self.worker['settings'])
                    and str(uuid.UUID(ready['readiness']['bootId']))==ready['readiness']['bootId']
                    and ready['readiness']['ready'] is True and ready['readiness']['idle'] is True
                    and all(ready['readiness'][k]==v for k,v in self.worker['expected'].items())
                    and ready['process']==observed['process'],'actual_first_worker_tls_readiness_required')
        return dict(dispatch=packet,service=observed,readiness=ready,bootstrap=self._bootstrap(expected),pilot=self._pilot(expected))

    def verify(self,expected,current,observation):
        require(type(observation) is dict and set(observation)=={'dispatch','service','readiness','bootstrap','pilot'},
                'closed_activation_effect_proof_required')
        require(observation['dispatch']['requestId']==current['requestId'] and
                (observation['readiness'] is not None)==(current['action']=='start'),'activation_effect_proof_changed')
        # Reconciliation of an immutable accepted observation uses fresh TLS,
        # process/config/source/native/ingress and bootstrap proofs, while
        # comparing only nonce-independent facts. No service command is sent.
        import copy
        def stable(value):
            value=copy.deepcopy(value);value['service'].pop('challenge',None)
            if value['readiness'] is not None:
                value['readiness'].pop('challenge',None);value['readiness']['readiness'].pop('nonce',None)
            return value
        require(stable(self.observe(expected,current))==stable(observation),'activation_changed_before_receipt_ack')
        return True


def main(argv=None):
    """One explicit existing-controller action; never an implicit service start."""
    import argparse,json
    from .enrolled_coordinator_deployment import read_bundle
    from .enrolled_coordinator_cutover import FirstEnrolledCutover,validate_plan,CutoverHeld
    from .enrolled_coordinator_driver import ConcreteCutoverDriver
    from .enroll_reference_worker import validate_manifest
    from .enrolled_coordinator_transport import TransportHeld
    parser=argparse.ArgumentParser(description=__doc__)
    for key in ('plan','bundle','receipt','worker-manifest','install-receipt','ingress-policy','native-inventory','node'):
        parser.add_argument('--'+key,required=True)
    parser.add_argument('--action',choices=('prepare','advance','inspect'),required=True)
    args=parser.parse_args(argv)
    try:
        require(os.name=='posix','linux_activation_controller_required')
        plan=validate_plan(decode(private_bytes(args.plan,maximum=262144),262144));bundle=read_bundle(args.bundle,plan)
        workers=[worker for worker in bundle['workers'] if worker['nodeId']==args.node]
        require(len(workers)==1,'existing_reviewed_activation_node_required');worker=workers[0]
        manifest=validate_manifest(decode(private_bytes(args.worker_manifest,maximum=262144),262144))
        installed=decode(private_bytes(args.install_receipt,maximum=16384),16384)
        ingress=decode(private_bytes(args.ingress_policy,maximum=16384),16384)
        native=decode(private_bytes(args.native_inventory,maximum=65536),65536)
        driver=ConcreteCutoverDriver(plan,bundle,args.receipt)
        cutover=FirstEnrolledCutover(args.receipt,plan,driver);state=cutover._load()
        expected=dict(operationId=plan['operationId'],cutoverPlanSha256=digest(plan),nodeId=worker['nodeId'],
            workerSha256=digest(worker),manifestSha256=digest(manifest),installReceiptSha256=digest(installed),
            ingressPolicySha256=digest(ingress),nativeInventorySha256=digest(native),
            bootstrapStartupId=driver._prior(state,'start-bootstrap')['gate']['snapshot']['startupId'],
            ownerSha256=driver._preflight()['ownerSha256'])
        boundary=ControllerActivationBoundary(driver,cutover,worker,manifest,installed,ingress,native)
        activation=FirstWorkerActivation(args.receipt,expected,boundary=boundary,driver=driver)
        result=getattr(activation,args.action)()
        print(json.dumps(dict(action=args.action,nodeId=worker['nodeId'],phase=result['phase'],
            pendingAction=result['pending']['action'] if result['pending'] else None,
            scope='single-reviewed-activation-action',serviceEnableIssued=False,coordinatorResumeIssued=False)))
        return 0
    except Exception as error:
        reason=str(error) if isinstance(error,(TransportHeld,CutoverHeld)) else 'first_worker_activation_held'
        if not re.fullmatch('[a-z][a-z0-9_]{0,120}',reason):reason='first_worker_activation_held'
        print(json.dumps(dict(status='held',reason=reason)));return 2


if __name__=='__main__':raise SystemExit(main())
