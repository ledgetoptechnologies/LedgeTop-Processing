"""Deterministic host-local update tick; no AI scheduler or default release policy."""
import argparse
from dataclasses import asdict,dataclass,fields
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import stat
import sys


class ServiceConfigurationError(ValueError):pass


def closed(value,fields):
    if type(value) is not dict or set(value)!=set(fields):raise ServiceConfigurationError('closed_configuration_required')
    return value


def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)
def sha(value):return hashlib.sha256(canonical(value).encode()).hexdigest()


def policy_map(value,policy_type):
    return dict(closed(value,(field.name for field in fields(policy_type))))


def code_binding_sha256():
    """Pin executable project sources, excluding data, docs and generated caches."""
    root=Path(__file__).resolve().parents[1];files={}
    for folder in ('processing','ops'):
        for path in sorted((root/folder).rglob('*.py')):
            if path.is_symlink() or any(p.is_symlink() for p in path.parents):raise ServiceConfigurationError('real_source_files_required')
            files[path.relative_to(root).as_posix()]=hashlib.sha256(path.read_bytes()).hexdigest()
    if not files:raise ServiceConfigurationError('source_binding_unavailable')
    return sha(files)


def private_path(value,*,directory=False):
    if type(value) is not str or not value or any(c in value for c in '\r\n\0'):raise ServiceConfigurationError('absolute_private_path_required')
    path=Path(value)
    if not path.is_absolute() or path.resolve()!=path or any(p.is_symlink() for p in (path,*path.parents)):
        raise ServiceConfigurationError('absolute_private_path_required')
    check=path if directory or path.exists() else path.parent
    info=check.stat()
    if (info.st_uid!=os.geteuid() or info.st_mode&0o077
            or not (stat.S_ISDIR(info.st_mode) if check!=path or directory else stat.S_ISREG(info.st_mode))
            or check==path and not directory and info.st_nlink!=1):raise ServiceConfigurationError('private_owned_path_required')
    return path


def read_private_json(value):
    path=private_path(str(value));info=path.stat()
    if info.st_size>65536:raise ServiceConfigurationError('bounded_configuration_required')
    def unique(pairs):
        result={}
        for key,item in pairs:
            if key in result:raise ServiceConfigurationError('duplicate_configuration_field')
            result[key]=item
        return result
    descriptor=os.open(path,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
    try:
        before=os.fstat(descriptor)
        if (before.st_dev,before.st_ino)!=(info.st_dev,info.st_ino):raise ServiceConfigurationError('configuration_changed')
        with os.fdopen(descriptor,'rb',closefd=False) as stream:raw=stream.read(65537)
        after=path.lstat()
        if (len(raw)>65536 or len(raw)!=info.st_size
                or (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns)!=(info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns)):
            raise ServiceConfigurationError('configuration_changed')
    finally:os.close(descriptor)
    return json.loads(raw.decode('utf-8'),object_pairs_hook=unique,
        parse_constant=lambda _:(_ for _ in ()).throw(ServiceConfigurationError('finite_configuration_required')))


@dataclass(frozen=True)
class Configuration:
    raw:dict
    registry_policy:object
    startup_policy:object
    supervisor_policy:object
    capacity_policy:object
    window:object

    @property
    def config_id(self):return sha(self.raw)


def parse_configuration(raw):
    from processing.update_registry import RegistryPolicy,RegistryChannel
    from processing.update_qualifier import StartupPolicy
    from processing.update_supervisor import SupervisorPolicy
    from processing.update_capacity import CapacityPolicy
    from ops.update_maintenance_adapter import ScheduleWindow
    raw=json.loads(canonical(raw))
    closed(raw,('schemaVersion','serviceId','codeBindingSha256','registry','qualification','maintenance','supervisor','capacity','service'))
    if type(raw['schemaVersion']) is not int or raw['schemaVersion']!=1:raise ServiceConfigurationError('configuration_version_required')
    if type(raw['serviceId']) is not str or not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,63}',raw['serviceId']):raise ServiceConfigurationError('fixed_service_identity_required')
    if type(raw['codeBindingSha256']) is not str or not re.fullmatch(r'[a-f0-9]{64}',raw['codeBindingSha256']):raise ServiceConfigurationError('reviewed_source_binding_required')
    for key in ('registry','qualification','supervisor'):closed(raw[key],('statePath','policy'))
    closed(raw['maintenance'],('managedRoot','statePath','receiptRoot','window'))
    closed(raw['capacity'],('policy',))
    closed(raw['service'],('statePath','discoveryIntervalSeconds','errorCooldownSeconds','maxTransitions'))
    rp=policy_map(raw['registry']['policy'],RegistryPolicy)
    if type(rp.get('channels')) is not list or not 1<=len(rp['channels'])<=8:raise ServiceConfigurationError('explicit_channels_required')
    rp['channels']=tuple(RegistryChannel(**closed(c,('name','repository','tag'))) for c in rp['channels'])
    registry=RegistryPolicy(**rp)
    qp=policy_map(raw['qualification']['policy'],StartupPolicy)
    if 'write_paths' in qp:
        if type(qp['write_paths']) is not list:raise ServiceConfigurationError('fixed_write_paths_required')
        qp['write_paths']=tuple(qp['write_paths'])
    startup=StartupPolicy(**qp);supervisor=SupervisorPolicy(**policy_map(raw['supervisor']['policy'],SupervisorPolicy))
    cp=policy_map(raw['capacity']['policy'],CapacityPolicy)
    if type(cp.get('legacy_capacity_journals')) is not list:raise ServiceConfigurationError('explicit_legacy_inventory_required')
    cp['legacy_capacity_journals']=tuple(cp['legacy_capacity_journals'])
    capacity=CapacityPolicy(**cp);window=ScheduleWindow(**policy_map(raw['maintenance']['window'],ScheduleWindow))
    if (startup.engine_id!=registry.engine_id or supervisor.node_id!=startup.node_id
            or supervisor.authority_id!=startup.authority_id or supervisor.qualifier_id!=startup.qualifier_id
            or supervisor.capacity_bytes!=registry.staging_reservation_bytes
            or supervisor.channel not in {c.name for c in registry.channels}
            or capacity.engine_id!=registry.engine_id or capacity.docker_executable!=registry.docker_executable
            or capacity.docker_endpoint!=registry.docker_endpoint or capacity.reservation_bytes!=registry.staging_reservation_bytes
            or Path(startup.supervisor_state_root)!=Path(raw['supervisor']['statePath']).parent
            or Path(raw['service']['statePath']).parent!=Path(raw['supervisor']['statePath']).parent
            or Path(raw['maintenance']['receiptRoot'])!=Path(raw['supervisor']['statePath']).parent/'receipts'):
        raise ServiceConfigurationError('composition_binding_mismatch')
    for key,minimum,maximum in (('discoveryIntervalSeconds',60,604800),('errorCooldownSeconds',1,86400),('maxTransitions',1,8)):
        value=raw['service'][key]
        if type(value) is not int or not minimum<=value<=maximum:raise ServiceConfigurationError('bounded_service_settings_required')
    return Configuration(raw,registry,startup,supervisor,capacity,window)


def runtime_identity(configuration):
    if os.name!='posix' or not sys.platform.startswith('linux'):raise ServiceConfigurationError('linux_host_required')
    import pwd
    node=configuration.supervisor_policy.node_id
    if (os.geteuid()!=1000 or pwd.getpwuid(os.geteuid()).pw_name!='bkoltz'
            or socket.gethostname().split('.')[0].lower()!={'main':'node','cluster':'cluster'}[node]):
        raise ServiceConfigurationError('configured_processing_host_required')


def control_clients(managed_root):
    from processing.odx_smoke_worker import Client
    from processing.container_update import loopback
    root=private_path(str(managed_root),directory=True)
    settings=read_private_json(root/'maintenance.json')
    origin=loopback(settings['coordinator'])
    path=private_path(str(root/'maintenance.env'))
    if path.stat().st_size>16384:raise ServiceConfigurationError('bounded_control_credentials_required')
    values={}
    for line in path.read_text().splitlines():
        if not line or line.startswith('#'):continue
        key,value=line.split('=',1)
        if key in values:raise ServiceConfigurationError('duplicate_control_credential')
        values[key]=value
    if set(values)!={'PROCESSING_API_TOKEN','PROCESSING_PROTOTYPE_OPERATOR_TOKEN'}:
        raise ServiceConfigurationError('exact_control_roles_required')
    return Client(origin,values['PROCESSING_API_TOKEN']),Client(origin,values['PROCESSING_PROTOTYPE_OPERATOR_TOKEN'])


def compose(configuration):
    """Construct actual trusted components; no discovery or deployment here."""
    from processing.update_registry import DockerRegistry
    from processing.update_qualifier import StartupQualifier
    from processing.update_supervisor import UpdateSupervisor
    from processing.update_capacity import ImageStagingCapacity
    from ops.update_maintenance_adapter import UpdateMaintenanceAdapter
    c=configuration;raw=c.raw
    if code_binding_sha256()!=raw['codeBindingSha256']:raise ServiceConfigurationError('reviewed_source_changed')
    for key in ('registry','qualification','supervisor','service'):private_path(raw[key]['statePath'])
    private_path(raw['maintenance']['statePath']);private_path(raw['maintenance']['managedRoot'],directory=True)
    capacity=ImageStagingCapacity(c.capacity_policy)
    registry=DockerRegistry(raw['registry']['statePath'],c.registry_policy,capacity_admit=capacity)
    qualifier=StartupQualifier(raw['qualification']['statePath'],c.startup_policy,registry=registry)
    if qualifier.policy_sha256!=c.supervisor_policy.qualification_policy_sha256:
        raise ServiceConfigurationError('qualification_policy_changed')
    api,operator=control_clients(raw['maintenance']['managedRoot'])
    adapter=UpdateMaintenanceAdapter(raw['maintenance']['managedRoot'],state_path=raw['maintenance']['statePath'],
        receipt_root=raw['maintenance']['receiptRoot'],api=api,operator=operator,node_id=c.supervisor_policy.node_id,
        authority_id=c.supervisor_policy.authority_id,engine_id=c.registry_policy.engine_id,
        adapter_id=c.supervisor_policy.maintenance_adapter_id,window=c.window)
    supervisor=UpdateSupervisor(raw['supervisor']['statePath'],c.supervisor_policy,registry=registry,qualifier=qualifier,maintenance=adapter)
    return supervisor,capacity,adapter,api


def readiness_probes(configuration,adapter,api):
    from processing.node_gate import NodeGate,GateBusy
    c=configuration
    def idle(status):
        authority=api.json('GET','/v1/prototype/authority')
        if authority!={'authorityId':c.supervisor_policy.authority_id}:raise ServiceConfigurationError('coordinator_authority_changed')
        current=api.json('GET','/v1/prototype/nodes/'+c.supervisor_policy.node_id)['node']
        if type(current) is not dict or current.get('id')!=c.supervisor_policy.node_id:raise ServiceConfigurationError('coordinator_node_changed')
        if current.get('state')!='active':return False
        jobs=api.json('GET','/v1/prototype/jobs')['jobs']
        if any(j.get('nodeId')==c.supervisor_policy.node_id and j['status'] in ('running','held') for j in jobs):return False
        candidate=NodeGate(c.registry_policy.engine_id)
        root=candidate.root
        private_path(str(root),directory=True)
        private_path(str(root/'gate.lock'));private_path(str(root/'gate.json'))
        try:gate=candidate.__enter__()
        except GateBusy:return False
        try:
            state=gate.snapshot()
            if (state.get('engineId')!=c.registry_policy.engine_id or state.get('nodeId')!=c.supervisor_policy.node_id
                    or state.get('authorityId')!=c.supervisor_policy.authority_id
                    or state.get('coordinatorOrigin')!=api.origin):raise ServiceConfigurationError('physical_gate_binding_changed')
            if state.get('reservation') is not None:return False
            settings,ingress,backend=adapter._context(Path(c.raw['supervisor']['statePath']).parent)
            return backend.idle() is True
        finally:gate.__exit__(None,None,None)
    def can_schedule(status):
        node=api.json('GET','/v1/prototype/nodes/'+c.supervisor_policy.node_id)['node']
        if type(node) is not dict or node.get('id')!=c.supervisor_policy.node_id:raise ServiceConfigurationError('coordinator_node_changed')
        return node['state']=='active' and idle(status)
    return idle,can_schedule


def admit_stage(capacity,candidate,registry):
    """Cached artifact reuse needs no new reservation or implicit repull."""
    from processing.update_registry import RegistryNotStaged
    try:
        registry.inspect_staged_artifact(candidate)
        return True
    except RegistryNotStaged:
        return capacity(candidate,registry.policy.staging_reservation_bytes)


def execute(configuration,action):
    from processing.update_service import UpdateService
    if action not in ('bootstrap','tick','status'):raise ServiceConfigurationError('explicit_service_action_required')
    runtime_identity(configuration)
    supervisor,capacity,adapter,api=compose(configuration)
    with supervisor:
        if action=='bootstrap':
            for path in configuration.capacity_policy.legacy_capacity_journals:capacity.reconcile_legacy(path,supervisor.registry)
            capacity.verify_legacy()
            return dict(status='bootstrapped',serviceId=configuration.raw['serviceId'],configId=configuration.config_id,
                legacyJournals=len(configuration.capacity_policy.legacy_capacity_journals))
        capacity.verify_legacy()
        if action=='status':
            return dict(status='observed',serviceId=configuration.raw['serviceId'],configId=configuration.config_id,
                pending=supervisor.pending(),host=adapter.inspect_host())
        idle,can_schedule=readiness_probes(configuration,adapter,api)
        settings=configuration.raw['service']
        service=UpdateService(settings['statePath'],supervisor=supervisor,config_id=configuration.config_id,
            code_binding_sha256=configuration.raw['codeBindingSha256'],capacity_admitter=lambda candidate,registry:admit_stage(capacity,candidate,registry),
            capacity_releaser=capacity.release_staged,idle_probe=idle,can_schedule=can_schedule,
            discovery_interval_seconds=settings['discoveryIntervalSeconds'],error_cooldown_seconds=settings['errorCooldownSeconds'],max_transitions=settings['maxTransitions'])
        result=service.tick()
        if code_binding_sha256()!=configuration.raw['codeBindingSha256']:raise ServiceConfigurationError('reviewed_source_changed')
        return dict(serviceId=configuration.raw['serviceId'],**result)


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=('bootstrap','tick','status'))
    parser.add_argument('--config',required=True)
    args=parser.parse_args(argv)
    try:
        if os.name!='posix' or not sys.platform.startswith('linux'):raise ServiceConfigurationError('linux_host_required')
        config=parse_configuration(read_private_json(args.config))
        result=execute(config,args.action)
        print(canonical(result))
        return 2 if result.get('status') in ('held','capacity_release_held') else 0
    except Exception as error:
        print(canonical(dict(status='held',category=type(error).__name__)),file=sys.stderr);return 2


if __name__=='__main__':raise SystemExit(main())
