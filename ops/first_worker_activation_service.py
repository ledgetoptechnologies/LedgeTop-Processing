"""Narrow service-user observer/actuator, serialized into tracked SSH stdin.

No root privileges, service enablement, stop, configuration write or engine task
effect. The enclosing controller journal owns reload/start dispatch authority.
"""
import hashlib
import inspect
import json
import os
from pathlib import Path
import stat
import sys
import time
import uuid

from .enrolled_coordinator_transport import canonical,decode,require


PROTOCOL='ltds-first-worker-service-v1'


def retained_native_census(client,manifest,inventory):
    """Quarantine exact declared terminal tasks; never remove/accept results."""
    fields={'schemaVersion','protocol','nodeId','authorityId','engineId','containerId','imageId','tasks'}
    require(type(inventory) is dict and set(inventory)==fields and type(inventory['schemaVersion']) is int
            and inventory['schemaVersion']==1 and inventory['protocol']=='ltds-retained-native-quarantine-v1'
            and inventory['nodeId']==manifest['gate']['nodeId'] and inventory['authorityId']==manifest['gate']['authorityId']
            and inventory['engineId']==manifest['host']['dockerEngineId']
            and inventory['containerId']==manifest['engine']['containerId'] and inventory['imageId']==manifest['engine']['imageId']
            and type(inventory['tasks']) is list and len(inventory['tasks'])<=64,'reviewed_native_quarantine_inventory_required')
    expected={}
    for row in inventory['tasks']:
        require(type(row) is dict and set(row)=={'uuid','statusCode','infoSha256'}
                and type(row['uuid']) is str and str(uuid.UUID(row['uuid']))==row['uuid'] and row['uuid'] not in expected
                and type(row['statusCode']) is int and row['statusCode'] in (30,40,50)
                and type(row['infoSha256']) is str and len(row['infoSha256'])==64,'closed_terminal_native_inventory_required')
        expected[row['uuid']]=row
    deadline=time.monotonic()+30
    def check():require(time.monotonic()<deadline,'native_census_duration_exceeded')
    def listing():
        check();info=client.read('/info',limit=65536);rows=client.read('/task/list',limit=262144)
        require(type(info) is dict and info.get('engineVersion')==manifest['engine']['version']
                and type(info.get('taskQueueCount')) is int and info['taskQueueCount']==0
                and type(rows) is list and len(rows)<=64,'native_engine_not_idle')
        identifiers=[]
        for row in rows:
            require(type(row) is dict and type(row.get('uuid')) is str
                    and str(uuid.UUID(row['uuid']))==row['uuid'],'ambiguous_native_task_listing')
            identifiers.append(row['uuid'])
        require(len(identifiers)==len(set(identifiers)) and set(identifiers)==set(expected),
                'native_task_set_differs_from_quarantine')
        check();return sorted(identifiers)
    def metadata(identifiers):
        result=[]
        for identifier in identifiers:
            check();value=client.read('/task/'+identifier+'/info',limit=262144)
            require(type(value) is dict and value.get('uuid')==identifier and type(value.get('status')) is dict
                    and type(value['status'].get('code')) is int and value['status']['code'] in (30,40,50),
                    'native_task_active_unknown_or_ambiguous')
            row=dict(uuid=identifier,statusCode=value['status']['code'],infoSha256=hashlib.sha256(canonical(value)).hexdigest())
            require(row==expected[identifier],'retained_native_task_metadata_changed');result.append(row);check()
        return result
    before=listing();first=metadata(before);after=listing();second=metadata(after)
    require(before==after and first==second,'native_census_changed_during_observation')
    return dict(inventorySha256=hashlib.sha256(canonical(inventory)).hexdigest(),taskCount=len(first),
                observedSha256=hashlib.sha256(canonical(first)).hexdigest())


def clean_ingress(manifest,policy,boundary):
    from ops.enroll_reference_worker import read_file,absolute
    from ops.node_ingress import Ingress,configuration
    require(type(policy) is dict and set(policy)=={'root','configSha256','stateSha256','closedConfigSha256','containerId'},
            'closed_activation_ingress_binding_required')
    root=absolute(policy['root']);owner=manifest['service']['uid']
    values={}
    for label,path,mode,pin in (
        ('config',root/'gateway.json',0o600,policy['configSha256']),
        ('state',root/'state.json',0o600,policy['stateSha256']),
        ('closed',root/'public-config/nginx.conf',0o644,policy['closedConfigSha256'])):
        raw=read_file(path,owner=owner,maximum=65536,mode=mode)
        require(hashlib.sha256(raw).hexdigest()==pin,'activation_ingress_files_changed')
        values[label]=raw
    class ReadOnlyIngress(Ingress):
        def docker(self,args):
            if args[:2]==['context','inspect']:return json.dumps('unix:///run/docker.sock')
            require(args and args[0] in ('info','inspect','top'),'activation_ingress_is_read_only')
            return boundary.run(['/usr/bin/docker','--host','unix:///run/docker.sock',*args],
                                output_limit=65536,timeout=20).decode()
    ingress=ReadOnlyIngress(root);state=decode(values['state'],65536)
    require(set(state)=={'state','containerId','oldWorkers','expectedBackendImage'} and state['state']=='closed'
            and state['containerId']==policy['containerId'] and type(state['oldWorkers']) is list
            and all(type(pid) is int and pid>0 for pid in state['oldWorkers'])
            and state['expectedBackendImage'] in (None,manifest['engine']['imageId']),
            'exact_closed_ingress_receipt_required')
    require(values['closed']==configuration(ingress.config['listenPort'],ingress.config['backendPort'],True,
                                           ingress.config.get('allowedClients',())).encode()
            and ingress.config['backendPort']==manifest['engine']['loopbackPort'],'closed_ingress_configuration_required')
    proxy=ingress.inspect()
    require(proxy['Id']==policy['containerId'] and not set(state['oldWorkers'])&ingress.workers(proxy)
            and ingress.http(ingress.config['listenPort'])[0]==503,'closed_ingress_or_old_worker_retirement_changed')
    native=decode(boundary.run(['/usr/bin/docker','--host','unix:///run/docker.sock','inspect',
                               '--format','{{json .Id}}',ingress.config['backendName']],output_limit=4096))
    require(native==manifest['engine']['containerId'],'closed_ingress_native_identity_changed')
    return dict(containerId=proxy['Id'],pid=proxy['State']['Pid'],stateSha256=policy['stateSha256'],
                configSha256=policy['configSha256'],closedConfigSha256=policy['closedConfigSha256'])


def observe_service(worker,manifest,ingress_policy,native_inventory,installed_tree_sha256,action,challenge):
    from ops.enroll_reference_worker import validate_manifest,ActualBoundary,Installer,read_file,absolute
    from ops.reference_worker_handover import SystemdWorkerControl,ReferenceWorkerHandover
    from ops.enrolled_coordinator_transport import BoundedSubprocess,digest
    from processing.viewer_service import NativeEngine
    require(action in ('preflight','observe','reload','start') and str(uuid.UUID(challenge))==challenge,'fixed_activation_service_action_required')
    manifest=validate_manifest(manifest);service=manifest['service'];settings=worker['settings']
    import pwd
    account=pwd.getpwnam(service['name'])
    require((account.pw_uid,account.pw_gid,account.pw_dir)==(service['uid'],service['gid'],service['home'])
            and os.getgid()==service['gid'],'activation_service_account_changed')
    require(os.geteuid()==worker['sshUid']==service['uid'] and str(Path.home())==service['home']
            and Path('/proc/sys/kernel/random/boot_id').read_text().strip()==manifest['host']['bootId']
            and Path(sys.executable).resolve()==Path(settings['pythonExecutable']).resolve(),
            'installed_activation_user_or_boot_changed')
    require(settings['sourceRoot']==str(absolute(manifest['installationRoot'])/'source')
            and settings['sourceSha256']==manifest['source']['footprintSha256']
            and settings['unitSha256']==manifest['unitSha256'],'activation_installed_source_or_unit_changed')
    python=Path(settings['pythonExecutable']);info=python.lstat()
    require(python.resolve()==python and info.st_uid==0 and stat.S_IMODE(info.st_mode)==0o550
            and hashlib.sha256(python.read_bytes()).hexdigest()==manifest['dependencies']['pythonSha256'],
            'activation_installed_interpreter_changed')
    os.environ['XDG_RUNTIME_DIR']='/run/user/'+str(service['uid'])
    os.environ['DBUS_SESSION_BUS_ADDRESS']='unix:path='+os.environ['XDG_RUNTIME_DIR']+'/bus'
    os.environ['XDG_STATE_HOME']=str(Path(service['home'])/'.local/state')
    from processing.node_gate import canonical_gate_path
    require(canonical_gate_path(manifest['host']['dockerEngineId'])==Path(manifest['gate']['path']).parent,
            'activation_default_gate_namespace_changed')
    class ServiceBoundary(ActualBoundary):
        def as_user(self,m,args,**kwargs):
            require(m['service']['uid']==os.geteuid(),'activation_service_user_changed')
            return self.run(args,**kwargs)
    boundary=ServiceBoundary()
    require(boundary.run(['/usr/bin/hostname'],output_limit=4096).decode().strip()==manifest['host']['hostname']
            and boundary.run(['/usr/bin/docker','--host','unix:///run/docker.sock','info','--format','{{.ID}}'],
                             output_limit=4096).decode().strip()==manifest['host']['dockerEngineId'],
            'activation_installed_host_changed')
    manager=boundary.assert_clean_manager_environment(manifest)
    def runner(argv,**kwargs):
        import subprocess
        require(argv[:2]==['/usr/bin/systemctl','--user'],'fixed_activation_systemctl_required')
        require(argv[2]=='show' or argv[2:]==['daemon-reload'] or
                argv[2:]==['--no-block','start',settings['unitName']],'narrow_activation_systemctl_required')
        raw=BoundedSubprocess().run(argv,timeout=15,output_limit=32768,mutation=argv[2]!='show')
        return subprocess.CompletedProcess(argv,0,raw.decode(),'')
    control=SystemdWorkerControl(settings,runner=runner)
    handover=ReferenceWorkerHandover(settings,'/unused-activation-read-only',
        guard=lambda:require(False,'activation_is_not_managed_handover'),control=control)
    raw,config=handover._config()
    require(hashlib.sha256(raw).hexdigest()==manifest['config']['sha256']==worker['expected']['configSha256'],
            'activation_installed_config_changed')
    require(settings['readinessCAFile']==config['caFile'] and worker['expected']['activation'] is None
            and config['maintenanceReadiness']['activation'] is None
            and all(config.get(key)==worker['expected'][key] for key in
                    ('nodeId','workerId','workerInstanceId','authorityId','engineId','gateGeneration','engineVersion')),
            'activation_private_role_or_tls_binding_changed')
    home=Path(service['home'])
    for name,category in (('stateRoot','worker-state'),('scratchRoot','worker-scratch')):
        path=home/'.local/share/ledgetop-processing-reference'/category/manifest['operationId']
        require(config.get(name)==str(path),'exact_first_worker_new_state_required')
        if action!='observe':require(not os.path.lexists(path),'first_worker_state_adoption_refused')
    installer=Installer(manifest,boundary=boundary)
    require(installer.observe_installed_tree()==installed_tree_sha256,'activation_installed_dependency_tree_changed')
    gate_fd=None
    try:
        if action!='observe':
            import fcntl
            path=Path(manifest['gate']['path']).parent/'gate.lock'
            gate_fd=os.open(path,os.O_RDWR|os.O_NOFOLLOW|os.O_CLOEXEC)
            state=os.fstat(gate_fd)
            require(path.resolve()==path and stat.S_ISREG(state.st_mode) and state.st_nlink==1
                    and state.st_uid==os.geteuid() and stat.S_IMODE(state.st_mode)==0o600,
                    'original_activation_gate_lock_required')
            fcntl.flock(gate_fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        installer._gate()
        boundary.engine(manifest,config)
        client=NativeEngine(config['engineOrigin'],config['engineToken'])
        native=retained_native_census(client,manifest,native_inventory)
        # Reuse the existing image-specific non-mutating upload/process census;
        # retained completed results are never cleanup targets here.
        from processing.container_update import ComposeNode
        node=ComposeNode.__new__(ComposeNode)
        def readonly_docker(argv,**kwargs):
            require(argv[:1]==['docker'] and argv[1] in ('top','exec'),'fixed_native_idle_observer_required')
            return boundary.run(['/usr/bin/docker','--host','unix:///run/docker.sock',*argv[1:]],
                                output_limit=65536,timeout=20).decode()
        node.docker=readonly_docker
        require(node.uploads_idle(manifest['engine']['containerId']) and node.processing_idle(manifest['engine']['containerId']),
                'native_upload_or_processing_child_active')
        ingress=clean_ingress(manifest,ingress_policy,boundary)
    finally:
        # Existing lock only; never NodeGate.__enter__, which can create/chmod.
        # A failed close holds the command before systemctl start, without retry.
        if gate_fd is not None:
            import errno
            closing=gate_fd;gate_fd=None;os.close(closing)
            try:os.fstat(closing)
            except OSError as error:require(error.errno==errno.EBADF,'activation_gate_close_unknown')
            else:require(False,'activation_gate_close_unknown')
    if action=='reload':control._run(['daemon-reload'])
    if action=='preflight':
        raw=boundary.run(['/usr/bin/systemctl','--user','show','--no-pager',
            '--property=LoadState,ActiveState,MainPID,UnitFileState',settings['unitName']],output_limit=4096).decode()
        process=dict(line.split('=',1) for line in raw.splitlines() if '=' in line)
        require(set(process)=={'LoadState','ActiveState','MainPID','UnitFileState'}
                and process['LoadState'] in ('loaded','not-found') and process['ActiveState']=='inactive'
                and process['MainPID']=='0' and process['UnitFileState'] in ('','disabled'),
                'first_activation_requires_inactive_unit')
    else:process=control.probe()
    if action!='preflight':
        import shlex
        from ops.enroll_reference_worker import UNSET_STARTUP_NAMES
        fields=dict(line.split('=',1) for line in control._run(['show','--no-pager',
            '--property=Environment,UnsetEnvironment',settings['unitName']]).splitlines() if '=' in line)
        require(set(fields)=={'Environment','UnsetEnvironment'}
                and set(shlex.split(fields['Environment']))=={'HOME='+service['home'],
                    'XDG_STATE_HOME='+str(Path(service['home'])/'.local/state'),'PYTHONNOUSERSITE=1'}
                and set(shlex.split(fields['UnsetEnvironment']))==set(UNSET_STARTUP_NAMES),
                'loaded_activation_startup_environment_changed')
    if action in ('reload','start'):
        require(process['pid']==0 and process['active']=='inactive','first_activation_requires_inactive_unit')
    if action!='preflight':
        raw=boundary.run(['/usr/bin/systemctl','--user','show','--no-pager','--property=UnitFileState',settings['unitName']],output_limit=4096)
        require(raw.decode().strip()=='UnitFileState=disabled','first_activation_must_not_enable_unit')
    if action=='start':control.command('start')
    return dict(schemaVersion=1,protocol=PROTOCOL,challenge=challenge,action=action,workerSha256=digest(worker),
                manifestSha256=digest(manifest),managerEnvironmentSha256=manager['managerEnvironmentSha256'],
                ingress=ingress,native=native,process=process)


def serialize_service(worker,manifest,ingress,native_inventory,installed_tree_sha256,action,challenge):
    from ops.enrolled_worker_readiness_probe import validate_request,_source_digest
    validate_request(worker,challenge)
    require(action in ('preflight','observe','reload','start'),'fixed_activation_service_action_required')
    # Canonical order is part of durable exact-source replay, including after a
    # controller restart reading its private sorted JSON journal.
    worker,manifest,ingress,native_inventory=(decode(canonical(value)) for value in (worker,manifest,ingress,native_inventory))
    module=Path(__file__).read_text().split('\ndef serialize_service(',1)[0]
    module=module.replace('from .enrolled_coordinator_transport import canonical,decode,require','')
    boot=('import sys,os,stat,json,hashlib\nfrom pathlib import Path\n'
          'def require(value,reason):\n if not value:raise RuntimeError(reason)\n'
          'worker='+repr(worker)+'\n'+inspect.getsource(_source_digest)+'\n'
          'sys.dont_write_bytecode=True\n'
          'require(os.geteuid()==worker["sshUid"],"activation_service_uid_changed")\n'
          'require(Path(sys.executable).resolve()==Path(worker["settings"]["pythonExecutable"]).resolve(),"activation_interpreter_changed")\n'
          'require(_source_digest(worker["settings"]["sourceRoot"])==worker["expected"]["sourceSha256"],"activation_readonly_source_changed")\n'
          'sys.path.insert(0,worker["settings"]["sourceRoot"])\n'
          'from ops.enrolled_coordinator_transport import canonical,decode,require\n')
    tail='\nprint(json.dumps(observe_service(worker,'+repr(manifest)+','+repr(ingress)+','+repr(native_inventory)+','+repr(installed_tree_sha256)+','+repr(action)+','+repr(challenge)+'),sort_keys=True,separators=(",",":")))\n'
    return (boot+module+tail).encode()
