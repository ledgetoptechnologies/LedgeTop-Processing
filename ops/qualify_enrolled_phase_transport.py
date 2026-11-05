"""Opt-in actual-Docker first-cutover qualification, never host enrollment.

The fixture profile below changes only process-local enrolled identities. The
production journal, driver, startup policy and custody validators are retained;
there is no alternate allowlist learned from candidate SQLite. Nothing executes
on import, and no image is pulled or built by this module.
"""
from contextlib import contextmanager
from pathlib import Path
import base64
import hashlib
import json
import secrets
import threading
import os
import socket
import sys
import re
import uuid

from . import enrolled_coordinator_cutover as journal
from . import enrolled_coordinator_deployment as deployment
from .enrolled_coordinator_transport import require
from .qualify_enrolled_docker_transport import TASK_LABEL


def private_write(path, raw):
    path=Path(path)
    with path.open('xb') as output:
        path.chmod(0o600);output.write(raw);output.flush();os.fsync(output.fileno())
    directory=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_CLOEXEC)
    try:os.fsync(directory)
    finally:os.close(directory)
    return dict(path=str(path),sha256=hashlib.sha256(raw).hexdigest())


def environment_bytes(value):
    require(all(type(k) is str and re.fullmatch('[A-Z_][A-Z0-9_]*',k) and
                type(v) is str and '\n' not in v and '\r' not in v for k,v in value.items()),
            'closed_fixture_environment_required')
    return ''.join(k+'='+v+'\n' for k,v in sorted(value.items())).encode()


def private_docker_logs(container_id, *, maximum=262144):
    """Read both multiplexed Docker log streams, privately and bounded.

    Docker CLI forwards container stderr to its own stderr, which the normal
    command API deliberately does not return. This exact-ID GET avoids losing
    constructor stderr; it never accepts another socket, URL or Docker action.
    """
    import http.client
    import time
    require(re.fullmatch('[a-f0-9]{64}',container_id) is not None and
            type(maximum) is int and 0<maximum<=262144,'bounded_owned_log_request_required')
    class UnixConnection(http.client.HTTPConnection):
        def connect(self):
            self.sock=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
            self.sock.settimeout(5);self.sock.connect('/run/docker.sock')
            self.log_socket=self.sock
    connection=UnixConnection('docker',timeout=5)
    response=None;deadline=time.monotonic()+10
    try:
        connection.request('GET','/containers/'+container_id+'/logs?stdout=1&stderr=1&timestamps=1&tail=200')
        response=connection.getresponse();require(response.status==200,'private_owned_logs_unavailable')
        chunks=[];size=0
        while size<=maximum:
            remaining=deadline-time.monotonic();require(remaining>0,'private_owned_log_deadline_reached')
            connection.log_socket.settimeout(min(1,remaining))
            chunk=response.read1(min(65536,maximum+1-size))
            if not chunk:break
            chunks.append(chunk);size+=len(chunk)
        raw=b''.join(chunks)
        return raw[:maximum],len(raw)>maximum
    finally:
        if response is not None:response.close()
        connection.close()


def capture_failure(docker, root, operation_id, *, driver=None, plan=None):
    """Diagnostic reads only; never remove, stop, restart or reconcile an app."""
    from . import enrolled_coordinator_transport as t
    root=Path(root);folder=root/'failure-diagnostic';folder.mkdir(mode=0o700)
    snapshots=[];failures=[]
    identities=docker.matching(['label='+TASK_LABEL+'='+operation_id])
    require(len(identities)<=16,'bounded_owned_failure_inventory_required')
    for identity in identities:
        try:
            item=docker.inspect(identity)
            require(item['Id']==identity and item['Config']['Labels'].get(TASK_LABEL)==operation_id,
                    'owned_failure_container_required')
            if plan is not None:
                require(item['Image'] in (plan['predecessor']['imageId'],plan['candidate']['imageId']),
                        'pinned_failure_image_required')
            role=item['Config']['Labels'].get(t.ROLE_LABEL)
            state={name:item['State'].get(name) for name in
                   ('Running','Pid','StartedAt','FinishedAt','ExitCode','OOMKilled','Error')}
            value=dict(containerId=identity,imageId=item['Image'],role=role,state=state,
                mounts=item.get('Mounts'),immutableRecipeSha256=t.digest(t.immutable_container_recipe(item)),
                environmentSha256=t.digest(t.container_environment(item)))
            snapshots.append(value)
            if role in ('bootstrap','final'):
                raw,truncated=private_docker_logs(identity)
                value['privateLog']=dict(private_write(folder/(identity+'.docker-log'),raw),
                    bytes=len(raw),truncated=truncated,multiplexedStdoutStderr=True)
                if driver is not None and state['Running'] is True:
                    try:
                        health=driver.boundary.fresh(identity,t.operator_script('GET','/healthz'))
                        value['freshDiagnosticHealth']=dict(status=health.get('status'),
                            bodySha256=t.digest(health.get('body')),separateReadOnlyObservation=True)
                    except BaseException:
                        value['freshDiagnosticHealth']=dict(unavailable=True,separateReadOnlyObservation=True)
        except BaseException:
            failures.append(dict(containerId=identity,classification='private_diagnostic_incomplete'))
    record=dict(schemaVersion=1,operationId=operation_id,retainedForInspection=True,
        mutationOutcomeUnknown=docker.mutation_unknown,planSha256=t.digest(plan) if plan is not None else None,
        containers=snapshots,failures=failures)
    raw=t.canonical(record);require(len(raw)<=1048576,'bounded_private_failure_summary_required')
    reference=private_write(folder/'summary.json',raw)
    print(json.dumps(dict(diagnosticRetained=True,containers=len(snapshots),incomplete=len(failures),
        summarySha256=reference['sha256'],mutationOutcomeUnknown=docker.mutation_unknown)),flush=True)
    return record


def fixture_bundle(root, *, operation_id, authority_id, old, candidate_image,
                   candidate_source, state_volume, config_source, lease_source,
                   spool_volume, physical, material, source_root, cli_sha, daemon_id, workers):
    """Bind files/config/source independently, before candidate constructors.

    All qualification receipts explicitly describe this synthetic fixture; they
    are not NAS, pilot-journal producer or installed-worker acceptance evidence.
    """
    from . import coordinator_startup_policy as policy
    from . import coordinator_predecessor_census as census
    from . import enrolled_coordinator_transport as t
    from .enrolled_coordinator_custody_ops import MODULE_NAMES
    root=Path(root);source_root=Path(source_root)
    raw_schema=(source_root/'contracts/coordinator-693cb7e-schema.json').read_bytes()
    key=secrets.token_bytes(32)
    files=dict(predecessorSchema=private_write(root/'schema.json',raw_schema),
        censusKey=private_write(root/'census-key',key),
        reviewedCounts=private_write(root/'counts.json',t.canonical(dict(jobs=22,nodes=2,viewer_tasks=7,viewer_reservations=0,
            viewer_files=155,viewer_option_evidence=7,owned_maintenance_schedules=16))))
    endpoints=[dict(id=node,url='http://127.0.0.1:'+str(19001+index))
               for index,node in enumerate(('cluster','main'))]
    hashes={name:hashlib.sha256((source_root/name).read_bytes()).hexdigest() for name in policy.SOURCES}
    configs={role:t.canonical(material[role]) for role in ('bootstrap','final')}
    policy_bytes=policy.build_startup_policy(source_root,expected_source_hashes=hashes,
        candidate_image_id=candidate_image,candidate_source_commit=candidate_source,
        bootstrap_config_bytes=configs['bootstrap'],final_config_bytes=configs['final'],
        node_endpoints=endpoints,authority_id=authority_id,reviewed_schema_bytes=raw_schema)
    files['policy']=private_write(root/'startup-policy.json',policy_bytes)
    recipes={};directory='first-cutover-'+operation_id
    native=dict(material['credentials'],PROCESSING_BIND_HOST='0.0.0.0',PROCESSING_PORT='8080',
        PROCESSING_PROTOTYPE_NODE_IDS='cluster,main',PROCESSING_TREEDAR_NODE_IDS='main',
        PROCESSING_PROTOTYPE_STATE_DIR='/prototype-state',PROCESSING_AUTHORITY_LEASE_DIR='/run/qualification-original-lease',
        PROCESSING_VIEWER_ENABLED='0',PYTHONNOUSERSITE='1',PYTHONDONTWRITEBYTECODE='1',
        LOCAL_NODE_URL=endpoints[0]['url'],MAIN_NODE_URL=endpoints[1]['url'])
    for role in ('bootstrap','final','holder'):
        env={} if role=='holder' else dict(native,
            PROCESSING_LEGACY_VIEWER_READ_ONLY='1' if role=='final' else '0',
            PROCESSING_REFERENCE_CONFIG='/run/processing-reference/'+role+'.json')
        files[role+'Environment']=private_write(root/(role+'.env'),environment_bytes(env))
        mounts=[dict(type='volume',source=state_volume,target='/prototype-state',readOnly=role=='holder'),
            dict(type='bind',source=lease_source,target='/run/qualification-original-lease',readOnly=False),
            dict(type='bind',source=config_source+'/'+directory,target='/run/processing-reference',readOnly=True),
            dict(type='bind',source=config_source,target='/run/phase-tls',readOnly=True)]
        if role=='final':mounts.append(dict(type='volume',source=spool_volume,target='/qualification-spool',readOnly=False))
        recipes[role]=dict(schemaVersion=1,name=journal.NAME if role=='final' else
            'ltds-phase-'+role+'-'+uuid.UUID(operation_id).hex[:16],imageId=candidate_image,
            user='10001:10001',workingDir='/app',environmentFile=files[role+'Environment']['path'],
            environmentSha256=files[role+'Environment']['sha256'],mounts=mounts,ports=[],
            network='bridge' if role=='final' else 'none',tmpfs={'/tmp':'rw,nosuid,nodev,noexec,size=64m,mode=1777'},
            labels={TASK_LABEL:operation_id},command=['python','-m','processing.app'],entrypoint=None,
            readOnly=True,init=role!='holder',pidsLimit=96,memoryBytes=384*1024**2,nanoCpus=250000000,
            stopSignal='SIGTERM',stopTimeout=-1,restartPolicy={'Name':'no','MaximumRetryCount':0})
        if role!='holder':files[role+'Config']=dict(path=config_source+'/'+directory+'/'+role+'.json',
            containerPath='/run/processing-reference/'+role+'.json',sha256=hashlib.sha256(configs[role]).hexdigest())
    gates={};qualifications={}
    for name in ('nasQualificationSha256','pilotClosureSha256','legacyDeliverySha256','workerQualificationSha256'):
        qualifications[name]=private_write(root/(name+'.json'),t.canonical(dict(schemaVersion=1,
            fixtureOnly=True,installedQualified=False,operationId=operation_id,kind=name)))
        gates[name]=qualifications[name]['sha256']
    target=dict(imageId=candidate_image,sourceCommit=candidate_source,
        bootstrapRecipeSha256=t.digest(recipes['bootstrap']),finalRecipeSha256=t.digest(recipes['final']),
        bootstrapConfigSha256=files['bootstrapConfig']['sha256'],finalConfigSha256=files['finalConfig']['sha256'],
        declaredTransformSha256=files['policy']['sha256'])
    plan=dict(schemaVersion=1,mode=journal.MODE,operationId=operation_id,authorityId=authority_id,
        predecessor=dict(containerId=old['Id'],imageId=old['Image'],sourceCommit=journal.PREDECESSOR_SOURCE,
                         restartPolicy=old['HostConfig']['RestartPolicy']),candidate=target,
        bindings=dict(stateSha256=t.digest(physical['state']),authorityLeaseSha256=t.digest(physical['authorityLease']),
            predecessorRecipeSha256=t.digest(t.immutable_container_recipe(old)),
            predecessorEnvironmentSha256=t.digest(t.container_environment(old)),censusKeySha256=files['censusKey']['sha256'],
            reviewedCountsSha256=files['reviewedCounts']['sha256'],deploymentSha256='0'*64),gates=gates)
    value=dict(schemaVersion=1,profile=deployment.PROFILE,operationId=operation_id,authorityId=authority_id,
        candidateSourceCommit=candidate_source,reviewedSourceRoot=str(source_root),
        host=dict(dockerEngineId=daemon_id,bootId=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
            dockerBinary='/usr/bin/docker',dockerBinarySha256=cli_sha,dockerSocket='unix:///run/docker.sock'),
        predecessor=dict(containerId=old['Id'],imageId=old['Image'],sourceCommit=journal.PREDECESSOR_SOURCE,
            name=journal.NAME,stateVolume=state_volume,leaseDirectory='/run/qualification-original-lease'),
        recipes=recipes,holderSettings=dict(schemaVersion=1,operationId=operation_id,authorityId=authority_id,
            stateRoot='/prototype-state',leaseDirectory='/run/qualification-original-lease'),
        artifacts=files,sourceHashes=hashes,custodySources={name:hashlib.sha256((source_root/'ops'/(name+'.py')).read_bytes()).hexdigest()
            for name in MODULE_NAMES},budgets=dict(maxArtifactBytes=16*1024**2,maxBackupBytes=64*1024**2,
            maxMetadataBytes=4*1024**2,maxSeconds=120,safetyFloorBytes=64*1024**2),nodeEndpoints=endpoints,
        workers=workers,qualifications=qualifications)
    plan['bindings']['deploymentSha256']=t.digest(value)
    deployment.validate_bundle(value,plan)
    return plan,value


def qualify(candidate_image, candidate_source, daemon_id, artifact_root, operation_id, cli_sha, *, rollback=False):
    """Run the production phase journal on NEW labeled resources only."""
    import importlib.util
    from . import enrolled_coordinator_transport as t
    from .enrolled_coordinator_driver import ConcreteCutoverDriver
    from .enrolled_coordinator_boundary import BoundaryProbe
    from .qualify_enrolled_docker_transport import QualificationDocker,cleanup_owned,initialize_fixture
    from .qualify_enrolled_holder_transport import owned_volume,verify_desktop_cli
    docker=QualificationDocker();volumes=[];events=[];result=None
    driver=plan=None;failed=False
    verify_desktop_cli(docker.binary,cli_sha)
    require(docker.command('info','--format','{{.ID}}',output_limit=256).decode().strip()==daemon_id,
            'explicit_fixture_daemon_required')
    for image,source in ((candidate_image,candidate_source),(journal.PREDECESSOR_IMAGE,journal.PREDECESSOR_SOURCE)):
        require(docker.image(image)['Config']['Labels']['org.opencontainers.image.revision']==source,
                'exact_packaged_fixture_runtime_required')
    require(str(uuid.UUID(operation_id))==operation_id,'canonical_fixture_operation_required')
    suffix=uuid.UUID(operation_id).hex[:16];name='ltds-phase-original-'+suffix
    authority=str(uuid.uuid5(uuid.UUID(operation_id),'synthetic-original-authority'))
    import stat
    outer=Path(artifact_root).absolute();info=outer.lstat()
    require(outer.resolve()==outer and not any(p.is_symlink() for p in outer.parents) and
            stat.S_ISDIR(info.st_mode) and info.st_uid==os.geteuid() and stat.S_IMODE(info.st_mode)==0o700,
            'original_private_task_artifact_root_required')
    root=outer/operation_id;root.mkdir(mode=0o700)
    operation=root/'operation';operation.mkdir(mode=0o700)
    source_root=Path(__file__).resolve().parents[1]
    names=dict(state=name+'-state',lease='ltds-phase-lease-'+suffix,
               config='ltds-phase-config-'+suffix,spool='ltds-phase-spool-'+suffix)
    controller=docker.inspect(socket.gethostname())
    require(controller['Config']['Labels'].get('org.ledgetop.phase-controller')==operation_id and
            controller['State']['Running'] is True and controller['HostConfig']['NetworkMode']=='bridge',
            'owned_private_bridge_controller_required')
    worker_host=controller['NetworkSettings']['Networks']['bridge']['IPAddress']
    material=configuration_material(operation_id,authority,worker_host)
    workers=None
    try:
        workers=LocalWorkerReadinessFixtures(root/'workers',material,authority).__enter__()
        refresh_configuration_seed(material,operation_id)
        owned={}
        for role,value in names.items():
            volumes.append(value);owned[role]=owned_volume(docker,value,operation_id)
        for role,source in (('lease','import pathlib,os,json\np=pathlib.Path("/prototype-state")\n'
             'assert p.stat().st_uid==p.stat().st_gid==10001 and next(p.iterdir(),None) is None\n'
             'p.chmod(0o711)\nprint(json.dumps({"uid":os.getuid(),"gid":os.getgid()}))\n'),
             ('config',material['configSeed']),('spool',material['spoolSeed'])):
            initialize_fixture(docker,candidate_image,operation_id,'ltds-phase-seed-'+role+'-'+suffix,names[role],source)
        external=owned['lease']['Mountpoint'];config_source=owned['config']['Mountpoint']
        fixture_path=source_root/'tests/fixtures/enrolled_coordinator_daemon_fixture.py'
        spec=importlib.util.spec_from_file_location('phase_original_fixture',fixture_path)
        fixture=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixture)
        seed=fixture.serializable_source(lease_directory='/run/qualification-original-lease',authority_id=authority,
            schema_bytes=(source_root/'contracts/coordinator-693cb7e-schema.json').read_bytes())
        seed_id=docker.command('container','create','--name','ltds-phase-state-seed-'+suffix,
            '--label',TASK_LABEL+'='+operation_id,'--network','none','--read-only','--user','10001:10001',
            '--cpus','0.125','--memory','128m','--memory-swap','128m','--pids-limit','32',
            '--mount','type=volume,src='+names['state']+',dst=/prototype-state',
            '--mount','type=bind,src='+external+',dst=/run/qualification-original-lease',
            '--entrypoint','python',candidate_image,'-I','-c',seed.decode(),mutation=True,output_limit=4096).decode().strip()
        docker.start(seed_id)
        require(docker.command('container','wait',seed_id,timeout=20,output_limit=256).decode().strip()=='0',
                'actual_693_fixture_seed_exit_required')
        seed_result=t.decode(docker.command('container','logs',seed_id,output_limit=16384))
        require(seed_result['expectedFileCount']==202,'exact_202_original_fixture_required')
        old_environment=dict(material['credentials'],PROCESSING_BIND_HOST='0.0.0.0',PROCESSING_PORT='8080',
            PROCESSING_PROTOTYPE_STATE_DIR='/prototype-state',PROCESSING_AUTHORITY_LEASE_DIR='/run/qualification-original-lease',
            PROCESSING_PROTOTYPE_NODE_IDS='cluster,main',PROCESSING_VIEWER_ENABLED='0',PYTHONDONTWRITEBYTECODE='1',
            LOCAL_NODE_URL='http://127.0.0.1:19001',MAIN_NODE_URL='http://127.0.0.1:19002')
        env=private_write(root/'predecessor.env',environment_bytes(old_environment))
        old_id=docker.command('container','create','--name',name,'--label',TASK_LABEL+'='+operation_id,
            '--network','none','--read-only','--user','10001:10001','--workdir','/app','--init',
            '--restart','no','--stop-signal','SIGTERM','--stop-timeout','-1',
            '--cpus','0.25','--memory','384m','--memory-swap','384m','--pids-limit','96',
            '--cap-drop','ALL','--security-opt','no-new-privileges','--tmpfs','/tmp:rw,nosuid,nodev,noexec,size=64m,mode=1777',
            '--mount','type=volume,src='+names['state']+',dst=/prototype-state',
            '--mount','type=bind,src='+external+',dst=/run/qualification-original-lease',
            '--mount','type=bind,src='+config_source+',dst=/run/processing-reference,readonly',
            '--env-file',env['path'],journal.PREDECESSOR_IMAGE,mutation=True,output_limit=4096).decode().strip()
        docker.start(old_id)
        physical_source=t.physical_binding_script(authority,'/run/qualification-original-lease')
        physical=docker.exec_python(old_id,physical_source,timeout=10,output_limit=32768)
        with fixture_enrollment(docker,operation_id=operation_id,predecessor_id=old_id,authority_id=authority,
                original_name=name,state_volume=names['state'],original_image_id=journal.PREDECESSOR_IMAGE,
                original_source=journal.PREDECESSOR_SOURCE,operation_directory=operation) as profile:
            old=docker.inspect(old_id)
            class DesktopBoundary(BoundaryProbe):
                def host(self):
                    verify_desktop_cli(self.bundle['host']['dockerBinary'],cli_sha)
                    require(self.docker.command('info','--format','{{.ID}}',output_limit=256).decode().strip()==daemon_id,
                            'fixture_daemon_changed')
                    require(Path('/proc/sys/kernel/random/boot_id').read_text().strip()==self.bundle['host']['bootId'],
                            'fixture_boot_changed')
            if workers is not None:
                plan,bundle=fixture_bundle(operation,operation_id=operation_id,authority_id=authority,old=old,
                    candidate_image=candidate_image,candidate_source=candidate_source,state_volume=names['state'],
                    config_source=config_source,lease_source=external,spool_volume=names['spool'],physical=physical,
                    material=material,source_root=source_root,cli_sha=cli_sha,daemon_id=daemon_id,
                    workers=workers.manifests())
                # Preserve authoritative reviewed inputs before the journal can
                # dispatch. A failed diagnostic never reconstructs them from
                # candidate rows or replaces the original expected window.
                private_write(operation/'reviewed-plan.json',t.canonical(plan))
                private_write(operation/'reviewed-bundle.json',t.canonical(bundle))
                for role in ('bootstrap','final'):
                    private_write(operation/('reviewed-'+role+'-config.json'),t.canonical(material[role]))
                class PhaseDriver(ConcreteCutoverDriver):
                    def _workers(self):return workers.ready(self.bundle['workers'])
                driver=PhaseDriver(plan,bundle,operation,docker=docker)
                driver.boundary=DesktopBoundary(plan,bundle,docker,driver.tracked)
                controller=journal.FirstEnrolledCutover(operation,plan,driver);controller.prepare()
                for _ in range(40):
                    state=controller.journal.read()
                    if rollback and state['phase']=='final-ready':controller.begin_rollback()
                    if state['phase'] in ('complete','rolled-back-rebaseline-required'):break
                    state=controller.advance();events.append(state['phase'])
                    print(json.dumps({'checkpoint':state['phase']}),flush=True)
                expected='rolled-back-rebaseline-required' if rollback else 'complete'
                require(state['phase']==expected,'actual_phase_sequence_incomplete')
                require(state['admissionOpened'] is (not rollback),'actual_admission_projection_changed')
                catalogs=None
                if not rollback:
                    final=driver._find('final')['Id']
                    first=driver.boundary.fresh(final,catalog_observer_source(authority),maximum=65536)
                    for instance in workers.instances:
                        instance['backend'].fixture_options.append(dict(name='fixture-catalog-refresh',type='bool',value=False))
                    second=driver.boundary.fresh(final,catalog_observer_source(authority),maximum=65536)
                    require('fixture-catalog-refresh' not in first['names'] and
                            'fixture-catalog-refresh' in second['names'] and
                            first['fingerprint']!=second['fingerprint'],'actual_live_catalog_refresh_required')
                    catalogs=dict(before=first,after=second)
                result=dict(schemaVersion=1,profile=profile,operationId=operation_id,
                    originalImageId=journal.PREDECESSOR_IMAGE,candidateImageId=candidate_image,candidateSourceCommit=candidate_source,
                    phases=events,terminalPhase=state['phase'],admissionOpened=state['admissionOpened'],
                    actualDockerPhases=True,actualApplicationGate=True,actualFullCustodyTar=True,
                    actualFactoryWorkerTLS=True,connectedWorkerSSHQualified=False,
                    postResumeConnectedCatalog=catalogs,
                    fixtureSystemdEngineCoordinatorTransport=True,productionHostEnrollmentQualified=False,
                    desktopReadonly0777Exemption=True,installedQualified=False)
    except BaseException:
        failed=True
        try:capture_failure(docker,root,operation_id,driver=driver,plan=plan)
        except BaseException:
            print(json.dumps(dict(diagnosticRetained=True,classification='private_capture_incomplete',
                                  operationId=operation_id)),flush=True)
        raise
    finally:
        try:
            if workers is not None:workers.__exit__(None,None,None)
        except BaseException:
            failed=True
            raise
        finally:
            if not failed and result is not None:
                cleanup_owned(docker,operation_id,volumes)
            else:
                # A failed app/state is diagnostic evidence even when Docker
                # mutation outcomes are known. Stopping/removing it requires a
                # separate reviewed action, never this finally clause.
                print(json.dumps(dict(retainedFailedOperation=operation_id,taskVolumes=volumes,
                    inspectionOnly=True,mutationOutcomeUnknown=docker.mutation_unknown)),flush=True)
    print(json.dumps(result,sort_keys=True),flush=True)
    return result


def catalog_observer_source(authority):
    """Actual app GET plus production relay client from FINAL's namespace."""
    return ('import os,json,ssl,http.client,sys\nsys.path.insert(0,"/app")\n'
        'from processing.reference_worker_catalog import WorkerCatalogClient\n'
        'from processing.reference_recovery import RecoveryNodeBinding\n'
        'cfg=json.load(open(os.environ["PROCESSING_REFERENCE_CONFIG"]))\n'
        'context=ssl.create_default_context(cafile=cfg["listener"]["clientCA"])\n'
        'conn=http.client.HTTPSConnection("127.0.0.1",cfg["listener"]["port"],context=context,timeout=10)\n'
        'try:\n'
        ' conn.request("GET","/ltds/v1/options",headers={"Authorization":"Bearer "+cfg["viewerBearers"]["production"]})\n'
        ' response=conn.getresponse();raw=response.read(1048577)\n'
        ' assert response.status==200 and len(raw)<=1048576\n'
        ' body=json.loads(raw)\n'
        'finally:conn.close()\n'
        'bindings={node:RecoveryNodeBinding(node_id=node,engine_id=value["engineId"],authority_id=value["authorityId"],'
        'coordinator_origin=value["coordinatorOrigin"],gate_generation=value["gateGeneration"],output_budget=value["outputBudget"])'
        ' for node,value in cfg["recoveryBindings"].items()}\n'
        'client=WorkerCatalogClient(cfg["workers"],node_ids=("cluster","main"),authority_id='+repr(authority)+','
        'ssl_context=context,recovery_bindings=bindings)\n'
        'snapshot,capacities=client.discover()\n'
        'assert len(snapshot["nodes"])==2 and all(node["available"] for node in snapshot["nodes"])\n'
        'assert capacities=={"cluster":{"ready":True,"maxImages":1000,"taskQueueCount":0},'
        '"main":{"ready":True,"maxImages":None,"taskQueueCount":0}}\n'
        'print(json.dumps({"names":sorted(option["name"] for option in body["options"]),'
        '"fingerprint":body["optionsFingerprint"],"capacities":capacities,"availableNodes":2}))\n').encode()


def main(argv=None):
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate-image',required=True);parser.add_argument('--candidate-source',required=True)
    parser.add_argument('--daemon',required=True);parser.add_argument('--artifact-root',required=True)
    parser.add_argument('--operation',required=True);parser.add_argument('--cli-sha256',required=True)
    parser.add_argument('--rollback',action='store_true')
    parser.add_argument('--outer-seconds',type=int,default=900,
        help='Qualification-only alarm,1..900 seconds; never changes application/command timeouts.')
    args=parser.parse_args(argv)
    require(os.name=='posix' and 1<=args.outer_seconds<=900,'bounded_linux_qualification_budget_required')
    import signal
    require(signal.getitimer(signal.ITIMER_REAL)==(0.0,0.0),'existing_alarm_must_not_be_replaced')
    previous=signal.getsignal(signal.SIGALRM)
    def deadline(*_):raise RuntimeError('qualification_outer_deadline_hold')
    signal.signal(signal.SIGALRM,deadline);signal.alarm(args.outer_seconds)
    try:
        qualify(args.candidate_image,args.candidate_source,args.daemon,args.artifact_root,args.operation,args.cli_sha256,
                rollback=args.rollback)
    finally:
        signal.alarm(0);signal.signal(signal.SIGALRM,previous)


def configuration_material(operation_id, authority_id, worker_host='127.0.0.1'):
    """Make ephemeral synthetic TLS/signing inputs for NEW owned volumes only.

    No original or user key is read. The returned seeder runs as UID10001 and
    writes only into the caller's newly created, empty config/spool volumes.
    Source release remains disabled and bootstrap has no source reconciliation.
    """
    from datetime import datetime, timedelta, timezone
    import ipaddress
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa, ed25519
    from cryptography.x509.oid import NameOID
    for value in (operation_id, authority_id):
        require(str(uuid.UUID(value)) == value, 'canonical_fixture_identity_required')
    worker_ip=ipaddress.ip_address(worker_host)
    require(worker_ip.is_private and not worker_ip.is_unspecified, 'private_fixture_worker_address_required')
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'localhost')])
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
        .serial_number(x509.random_serial_number()).not_valid_before(datetime.now(timezone.utc)-timedelta(minutes=1))
        .not_valid_after(datetime.now(timezone.utc)+timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName('localhost'),
            x509.IPAddress(ipaddress.ip_address('127.0.0.1')),x509.IPAddress(worker_ip)]), critical=False)
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256()))
    pem_key = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                               serialization.NoEncryption())
    pem_cert = cert.public_bytes(serialization.Encoding.PEM)
    public = ed25519.Ed25519PrivateKey.generate().public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo).decode()
    credentials = {name:'synthetic-phase-'+name.lower()+'-'+secrets.token_hex(24) for name in
                   ('PROCESSING_API_TOKEN','PROCESSING_PROTOTYPE_OPERATOR_TOKEN',
                    'PROCESSING_VIEWER_TOKEN','PROCESSING_VIEWER_STAGING_TOKEN')}
    workers = {node:dict(bearer='synthetic-worker-'+secrets.token_hex(24), workerId='worker-'+node,
        instanceId='instance-'+node, resultOrigin='https://127.0.0.1:'+str(19991+index),
        resultBearer='synthetic-result-'+secrets.token_hex(24))
        for index,node in enumerate(('cluster','main'))}
    common = dict(schemaVersion=2,listener=dict(host='0.0.0.0',port=8443,
        certificate='/run/phase-tls/fixture-cert.pem',
        privateKey='/run/phase-tls/fixture-key.pem',clientCA='/run/phase-tls/fixture-cert.pem'),
        # Native compatibility credentials must be preserved, but the V2
        # runtime deliberately rejects reusing any legacy credential.
        viewerBearers={environment:'synthetic-v2-viewer-'+environment+'-'+secrets.token_hex(24)
                       for environment in ('production','staging')},
        workers=workers,streamBudget=dict(maxConcurrent=1,maxObjectBytes=1048576),
        recoveryBindings={node:dict(engineId='fixture-engine-'+node,authorityId=authority_id,
            coordinatorOrigin='http://127.0.0.1:8080',gateGeneration=1,outputBudget=1048576)
            for node in workers})
    marker = secrets.token_hex(32)
    final = dict(common, schemaVersion=6, viewerAcceptance={},
        viewerReceiptKeys={'fixture-result':dict(issuerId='synthetic-phase-viewer',publicKeyPem=public)},
        sourceSpool=dict(root='/qualification-spool/source',mountPath='/qualification-spool',
            mountId=marker,markerName='.ltds-phase-marker',safetyFloorBytes=0,maxObjectBytes=1048576,chunkBytes=65536),
        sourceAttestationKeys=[dict(environment=environment,issuerId='synthetic-phase-viewer',
            keyId='fixture-source-'+environment,principalId='viewer-'+environment,publicKeyPem=public)
            for environment in ('production','staging')])
    encode = lambda value: json.dumps(value,sort_keys=True,separators=(',',':')).encode()
    directory = 'first-cutover-'+operation_id
    files = {directory+'/bootstrap.json':encode(common),directory+'/final.json':encode(final),
             'fixture-key.pem':pem_key,'fixture-cert.pem':pem_cert}
    packet = {name:base64.b64encode(raw).decode() for name,raw in files.items()}
    config_seed = ('import base64,json,os,pathlib\nroot=pathlib.Path("/prototype-state")\n'
        'assert os.getuid()==os.getgid()==10001 and next(root.iterdir(),None) is None\n'
        'root.chmod(0o700)\nfiles='+repr(packet)+'\n'
        'for name,raw in files.items():\n'
        ' p=root/name;p.parent.mkdir(mode=0o700,parents=True,exist_ok=True)\n'
        ' with p.open("xb") as out:out.write(base64.b64decode(raw))\n'
        ' p.chmod(0o600)\n'
        'print(json.dumps({"uid":os.getuid(),"gid":os.getgid()}))\n')
    spool_seed = ('import json,os,pathlib\nroot=pathlib.Path("/prototype-state")\n'
        'assert os.getuid()==os.getgid()==10001 and next(root.iterdir(),None) is None\n'
        'root.chmod(0o700)\n(root/"source").mkdir(mode=0o700)\n'
        'p=root/".ltds-phase-marker";p.write_bytes('+repr(marker.encode())+');p.chmod(0o600)\n'
        'print(json.dumps({"uid":os.getuid(),"gid":os.getgid()}))\n')
    return dict(bootstrap=common,final=final,configurationFiles=files,credentials=credentials,workerHost=worker_host,
                configSeed=config_seed,spoolSeed=spool_seed,
                configurationSha256={name:hashlib.sha256(raw).hexdigest() for name,raw in files.items()})


def refresh_configuration_seed(material,operation_id):
    """Seal actual worker origins into config bytes BEFORE seeding or hashing."""
    from .enrolled_coordinator_transport import canonical
    directory='first-cutover-'+operation_id
    for role in ('bootstrap','final'):
        material['configurationFiles'][directory+'/'+role+'.json']=canonical(material[role])
    packet={name:base64.b64encode(raw).decode() for name,raw in material['configurationFiles'].items()}
    material['configurationSha256']={name:hashlib.sha256(raw).hexdigest()
        for name,raw in material['configurationFiles'].items()}
    material['configSeed']=('import base64,json,os,pathlib\nroot=pathlib.Path("/prototype-state")\n'
        'assert os.getuid()==os.getgid()==10001 and next(root.iterdir(),None) is None\n'
        'root.chmod(0o700)\nfiles='+repr(packet)+'\n'
        'for name,raw in files.items():\n'
        ' p=root/name;p.parent.mkdir(mode=0o700,parents=True,exist_ok=True)\n'
        ' with p.open("xb") as out:out.write(base64.b64decode(raw))\n'
        ' p.chmod(0o600)\n'
        'print(json.dumps({"uid":os.getuid(),"gid":os.getgid()}))\n')


class LocalWorkerReadinessFixtures:
    """Actual factory/TLS/pins/readers; synthetic engine and coordinator I/O.

    This replaces only the driver-to-systemd/SSH transport in the Desktop
    qualification. It never claims connected SSH, physical host ownership,
    NodeODM processing, or the installed worker configuration.
    """
    def __init__(self, root, material, authority_id):
        self.root = Path(root); self.material = material; self.authority = authority_id
        self.instances = []; self.previous_state_home = os.environ.get('XDG_STATE_HOME')

    def __enter__(self):
        from . import reference_worker_daemon as daemon
        from .native_node_fixture import NativeNodeFixture
        from .node_ingress import save
        from processing.node_gate import NodeGate
        from processing.reference_worker_readiness import source_digest
        self.root.mkdir(mode=0o700)
        os.environ['XDG_STATE_HOME'] = str(self.root/'node-gates')
        try:
            for node in ('cluster','main'):
                root = self.root/node; root.mkdir(mode=0o700)
                for name in ('fixture-key.pem','fixture-cert.pem'):
                    target=root/name;target.write_bytes(self.material['configurationFiles'][name]);target.chmod(0o600)
                class CatalogNativeFixture(NativeNodeFixture):
                    def _dispatch(self,method,path,headers,body):
                        if method=='GET' and path=='/options':return 200,self.fixture_options,False
                        status,value,extra=super()._dispatch(method,path,headers,body)
                        if method=='GET' and path=='/info':value=dict(value,maxImages=self.fixture_max)
                        return status,value,extra
                backend=CatalogNativeFixture(root/'native.sqlite',engine_version='3.8.5',synthetic_enabled=True)
                from .native_node_fixture import OPTIONS
                backend.fixture_options=[dict(value) for value in OPTIONS]+[dict(name=name,type='bool',value=False)
                    for name in ('pc-ept','gltf','3d-tiles')]
                backend.fixture_max=1000 if node=='cluster' else None
                backend.start()
                instance=dict(node=node,root=root,backend=backend,thread=None,stop=None,server=None)
                self.instances.append(instance)
                with socket.socket() as probe:
                    probe.bind(('127.0.0.1',0));port=probe.getsockname()[1]
                source=Path(__file__).resolve().parents[1]
                engine='fixture-engine-'+node
                with NodeGate(engine) as gate:
                    gate.provision(node,'http://127.0.0.1:8080',self.authority)
                worker=self.material['bootstrap']['workers'][node]
                config=dict(schemaVersion=1,stateRoot=str(root/'state'),scratchRoot=str(root/'scratch'),
                    nodeId=node,workerId=worker['workerId'],workerInstanceId=worker['instanceId'],
                    authorityId=self.authority,engineId=engine,gateGeneration=1,
                    gateCoordinatorOrigin='http://127.0.0.1:8080',
                    coordinatorOrigin='https://coordinator.invalid',brokerOrigin='https://broker.invalid',
                    engineOrigin=backend.origin,engineToken='',workerBearer=worker['bearer'],
                    resultReadBearer=worker['resultBearer'],caFile=str(root/'fixture-cert.pem'),
                    serverCertFile=str(root/'fixture-cert.pem'),serverKeyFile=str(root/'fixture-key.pem'),
                    listenHost='0.0.0.0',listenPort=port,outputBudget=1048576,safetyFloor=0,
                    transferTtl=600,cleanupGrace=3600,pollSeconds=0.1,engineVersion='3.8.5',shareResults=True,
                    maintenanceReadiness=dict(schemaVersion=1,bearer='synthetic-readiness-'+secrets.token_hex(24),
                        sourceRoot=str(source),sourceSha256=source_digest(source),activation=None))
                path=root/'worker.json';save(path,config)
                worker['resultOrigin']='https://'+self.material['workerHost']+':'+str(port)
                driver,server=daemon.build_worker(daemon.load_config(str(path)))
                instance['server']=server
                driver.worker.engine_probe=lambda deadline, engine=engine:engine
                authority=self.authority
                def request(method,route,payload=None,**kwargs):
                    if route=='/authority':return 200,dict(authorityId=authority)
                    if route=='/next':return 200,dict(job=None)
                    raise RuntimeError('unexpected_fixture_worker_route')
                driver._request=request
                stop=threading.Event();service=daemon.WorkerService(driver,server,poll_seconds=0.1)
                thread=threading.Thread(target=service.run,args=(stop,),name='phase-worker-'+node)
                unit=root/'fixture-worker.service'
                unit.write_bytes(b'# Explicit qualification thread process-control seam; never loaded by systemd.\n')
                unit.chmod(0o600)
                manifest=dict(nodeId=node,sshHost='127.0.0.1',sshUid=os.geteuid(),
                    settings=dict(schemaVersion=1,unitName='fixture-'+node+'.service',unitFile=str(unit),
                        unitSha256=hashlib.sha256(unit.read_bytes()).hexdigest(),pythonExecutable=sys.executable,
                        configPath=str(path),sourceRoot=str(source),sourceSha256=config['maintenanceReadiness']['sourceSha256'],
                        readinessOrigin=worker['resultOrigin'],readinessCAFile=config['caFile']),
                    expected=dict(nodeId=node,workerId=config['workerId'],workerInstanceId=config['workerInstanceId'],
                        authorityId=self.authority,engineId=engine,gateGeneration=1,engineVersion=config['engineVersion'],
                        configSha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                        sourceSha256=config['maintenanceReadiness']['sourceSha256'],activation=None))
                instance.update(stop=stop,thread=thread,service=service,path=path,config=config,port=port,manifest=manifest)
                thread.start()
            return self
        except BaseException:
            self.__exit__(None,None,None)
            raise

    def manifests(self):
        from .enrolled_coordinator_transport import canonical,decode
        return decode(canonical([instance['manifest'] for instance in self.instances]))

    def ready(self, planned_workers=None):
        import time
        from urllib.error import HTTPError
        from .enrolled_worker_readiness_probe import probe
        planned=self.manifests() if planned_workers is None else planned_workers
        require(type(planned) is list and len(planned)==2 and
                [item['nodeId'] for item in planned]==['cluster','main'], 'complete_planned_worker_inventory_required')
        result=[]
        for instance,manifest in zip(self.instances,planned):
            # Pin the entire manifest before any TLS request. A valid but
            # unplanned second worker may never substitute for this slot.
            require(manifest==instance['manifest'],'planned_worker_fixture_changed')
            class LocalControl:
                def probe(self):
                    active=instance['thread'].is_alive()
                    return dict(pid=os.getpid() if active else 0,active='active' if active else 'inactive',
                        substate='running' if active else 'dead',cgroup='/fixture-thread/'+instance['node'],
                        invocationId=instance['node'],startTime=Path('/proc/self/stat').read_text().rsplit(')',1)[1].split()[19])
            deadline=time.monotonic()+10
            while True:
                try:
                    packet=probe(manifest,str(uuid.uuid4()),control=LocalControl())
                    break
                except HTTPError as error:
                    code=error.code;error.close()
                    if code==503 and time.monotonic()<deadline:
                        time.sleep(0.02);continue
                    require(False,'actual_fixture_worker_unready')
            value=packet['readiness'];stamp=packet['process']['startTime']
            result.append(dict(nodeId=instance['node'],configSha256=value['configSha256'],
                sourceSha256=value['sourceSha256'],engineVersion=value['engineVersion'],
                instanceId=value['workerInstanceId'],bootId=value['bootId'],pid=value['pid'],startTime=stamp,
                driverBindingSha256=value['driverBindingSha256'],loadedSourceSha256=value['loadedSourceSha256'],
                optionsSha256=value['optionsSha256'],safeOptionsSha256=value['safeOptionsSha256']))
        require(len(result)==2,'complete_fixture_worker_inventory_required')
        return result

    def __exit__(self, *_):
        for instance in reversed(self.instances):
            if instance.get('stop') is not None:instance['stop'].set()
            if instance.get('thread') is not None:
                instance['thread'].join(10)
                require(not instance['thread'].is_alive(),'task_worker_thread_drain_uncertain')
            elif instance.get('server') is not None:
                instance['server'].server_close()
            instance['backend'].close()
        if self.previous_state_home is None:os.environ.pop('XDG_STATE_HOME',None)
        else:os.environ['XDG_STATE_HOME']=self.previous_state_home


@contextmanager
def fixture_enrollment(docker, *, operation_id, predecessor_id, authority_id,
                       original_name, state_volume, original_image_id,
                       original_source, operation_directory):
    """Explicit test-only fixed-ID substitution, after task ownership checks.

    The image/source remain the real immutable693 predecessor. This context
    never alters a file, container, authority row or production configuration.
    It is deliberately separate from validate_plan()/validate_bundle(), and
    callers must report productionHostEnrollmentQualified=false.
    """
    from . import install_reference_coordinator as installer
    require(str(uuid.UUID(operation_id)) == operation_id and
            str(uuid.UUID(authority_id)) == authority_id,
            'exact_fixture_operation_authority_required')
    require(re.fullmatch('[a-f0-9]{64}', predecessor_id) is not None and
            re.fullmatch('ltds-phase-original-[a-f0-9]{16}', original_name) is not None and
            state_volume == original_name + '-state', 'closed_fixture_identity_required')
    require(original_image_id == journal.PREDECESSOR_IMAGE and
            original_source == journal.PREDECESSOR_SOURCE,
            'real_frozen693_predecessor_required')
    old = docker.inspect(predecessor_id)
    require(old['Id'] == predecessor_id and old['Image'] == original_image_id and
            old['Name'] == '/' + original_name and
            old['Config']['Labels'].get(TASK_LABEL) == operation_id,
            'task_owned_original_fixture_required')
    mounts = [m for m in old['Mounts'] if m['Destination'] == '/prototype-state']
    require(len(mounts) == 1 and mounts[0]['Type'] == 'volume' and
            mounts[0]['Name'] == state_volume and mounts[0]['RW'] is True,
            'owned_original_state_volume_required')
    volume = docker.command('volume', 'inspect', state_volume, output_limit=65536)
    from .enrolled_coordinator_transport import decode
    values = decode(volume)
    require(type(values) is list and len(values) == 1 and
            values[0]['Name'] == state_volume and
            values[0].get('Labels', {}).get(TASK_LABEL) == operation_id,
            'owned_original_volume_label_required')
    root = Path(operation_directory).absolute()
    require(root.resolve() == root and root.is_dir(), 'existing_fixture_operation_directory_required')
    changes = [(journal, 'PREDECESSOR_ID', predecessor_id),
               (journal, 'AUTHORITY', authority_id), (journal, 'NAME', original_name),
               (deployment, 'PREDECESSOR_ID', predecessor_id),
               (deployment, 'NAME', original_name),
               (installer, 'LOCK_PATH', root / 'fixture-install.lock')]
    before = [(module, name, getattr(module, name)) for module, name, _ in changes]
    try:
        for module, name, value in changes:
            setattr(module, name, value)
        yield dict(profile='task-owned-actual-693-first-cutover-fixture-v1',
                   productionHostEnrollmentQualified=False,
                   fixtureEnrollmentIdentitySubstitution=True,
                   originalImageId=original_image_id, originalSourceCommit=original_source)
    finally:
        for module, name, value in reversed(before):
            setattr(module, name, value)


if __name__=='__main__':main()

