"""Fresh physical/runtime observations for the one enrolled cutover driver.

No controller, queue, reader session or engine task is constructed here.
Recovered observer results are never substituted for a fresh boundary read.
"""
import hashlib
import json
import os
from pathlib import Path
import stat
import uuid

from .enrolled_coordinator_cutover import validate_plan
from .enrolled_coordinator_deployment import validate_bundle, derived_holder_settings
from .enrolled_coordinator_transport import (require,digest,canonical,environment_file,private_bytes,
    create_arguments,container_environment,physical_binding_script,operator_script,
    OWNER_LABEL,ROLE_LABEL,INTENT_LABEL,RECIPE_LABEL)
from .enrolled_coordinator_lease_helper import serializable_source


class BoundaryProbe:
    def __init__(self,plan,bundle,docker,tracked_exec):
        self.plan=validate_plan(plan);self.bundle=validate_bundle(bundle,plan)
        self.docker,self.tracked=docker,tracked_exec
        self.settings=derived_holder_settings(bundle,plan)

    def fresh(self,container,source,*,maximum=1048576):
        request=str(uuid.uuid4())
        value=self.tracked(container,source,request_id=request,timeout=30,output_limit=maximum)
        require(type(value) is dict and value.get('protocol')=='ltds-cutover-exec-v1'
                and value.get('operationId')==self.plan['operationId'] and value.get('requestId')==request
                and value.get('containerId')==container and value.get('sourceSha256')==hashlib.sha256(source).hexdigest()
                and value.get('bodySucceeded') is True and value.get('process',{}).get('retired') is True,
                'fresh_retired_boundary_observer_required')
        return value['result']

    def host(self):
        path=Path(self.bundle['host']['dockerBinary']);before=path.lstat()
        readonly_mount=bool(os.statvfs(path).f_flag&os.ST_RDONLY)
        require(path.resolve()==path and stat.S_ISREG(before.st_mode) and before.st_nlink==1
                and before.st_uid in (0,os.geteuid()) and (not before.st_mode&0o022 or readonly_mount)
                and not os.access(path,os.W_OK) and before.st_size<=256*1024*1024,
                'reviewed_readonly_docker_binary_required')
        fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC)
        key=lambda s:(s.st_dev,s.st_ino,s.st_mode,s.st_uid,s.st_gid,s.st_nlink,s.st_size,s.st_mtime_ns,s.st_ctime_ns)
        try:
            require(key(before)==key(os.fstat(fd)),'docker_binary_identity_changed')
            hashed=hashlib.sha256();count=0
            while True:
                chunk=os.read(fd,65536)
                if not chunk:break
                count+=len(chunk);require(count<=256*1024*1024,'docker_binary_budget_exceeded');hashed.update(chunk)
            require(key(before)==key(os.fstat(fd))==key(path.lstat()) and
                    hashed.hexdigest()==self.bundle['host']['dockerBinarySha256'],'reviewed_docker_binary_changed')
        finally:os.close(fd)
        raw=self.docker.command('info','--format','{{.ID}}',output_limit=256,timeout=10)
        require(raw.decode('ascii').strip()==self.bundle['host']['dockerEngineId'],'original_docker_engine_changed')
        require(Path('/proc/sys/kernel/random/boot_id').read_text().strip()==self.bundle['host']['bootId'],
                'original_host_boot_changed')

    def container(self,identity,role,creation_intent):
        recipe=self.bundle['recipes'][role]
        r=role if role!='holder' else 'lease-holder'
        args=create_arguments(recipe,operation_id=self.plan['operationId'],role=r,intent_id=creation_intent,
                              holder_settings=self.settings if role=='holder' else None)
        labels={args[n+1].split('=',1)[0]:args[n+1].split('=',1)[1] for n,x in enumerate(args[:-1]) if x=='--label'}
        item=self.docker.inspect(identity);image=self.docker.image(self.plan['candidate']['imageId'])
        config,host=item['Config'],item['HostConfig'];image_config=image['Config']
        require(item['Id']==identity and item['Image']==self.plan['candidate']['imageId']
                and item['Name']=='/'+recipe['name'],'exact_owned_candidate_changed')
        expected_labels=dict(image_config.get('Labels') or {},**labels)
        require(config.get('Labels')==expected_labels,'owned_candidate_labels_changed')
        environment=environment_file(recipe['environmentFile'])
        require(hashlib.sha256(private_bytes(recipe['environmentFile'],maximum=65536)).hexdigest()==recipe['environmentSha256'],
                'private_candidate_environment_changed')
        require(container_environment(item)==dict(container_environment(image),**environment),'loaded_candidate_environment_changed')
        expected_command=recipe['command'];expected_entry=recipe['entrypoint'] or image_config.get('Entrypoint')
        if role=='holder':expected_command=['-I','-c',serializable_source(self.settings).decode()];expected_entry=['python']
        require(config.get('User')==recipe['user'] and config.get('WorkingDir')==recipe['workingDir']
                and config.get('Cmd')==expected_command and config.get('Entrypoint')==expected_entry
                and config.get('StopSignal')=='SIGTERM' and config.get('StopTimeout')==-1,
                'owned_candidate_command_changed')
        require(host.get('ReadonlyRootfs') is True and host.get('Privileged') is False
                and not host.get('CapAdd') and host.get('CapDrop')==['ALL']
                and host.get('SecurityOpt') in (['no-new-privileges'],['no-new-privileges=true'])
                and host.get('PidMode','')=='' and host.get('IpcMode','') in ('','private')
                and host.get('UsernsMode','')=='' and not host.get('Devices') and not host.get('DeviceRequests')
                and not host.get('VolumesFrom') and not host.get('ExtraHosts') and not host.get('Dns')
                and host.get('AutoRemove') is False,'private_candidate_host_policy_changed')
        require(host.get('NetworkMode')==recipe['network'] and host.get('Memory',0)==recipe['memoryBytes']
                and host.get('MemorySwap',0)==recipe['memoryBytes'] and host.get('NanoCpus',0)==recipe['nanoCpus']
                and (host.get('PidsLimit') or 0)==recipe['pidsLimit']
                and bool(host.get('Init'))==recipe['init'],'owned_candidate_resource_policy_changed')
        require(host.get('RestartPolicy')=={'Name':'no','MaximumRetryCount':0},'candidate_restart_must_remain_disabled')
        require(host.get('Tmpfs')==recipe['tmpfs'],'owned_candidate_tmpfs_changed')
        ports={}
        for p in recipe['ports']:ports.setdefault(p['containerPort'],[]).append(dict(HostIp=p['hostIp'],HostPort=str(p['hostPort'])))
        actual=host.get('PortBindings') or {}
        require({k:sorted(v,key=lambda x:(x['HostIp'],x['HostPort'])) for k,v in actual.items()}==
                {k:sorted(v,key=lambda x:(x['HostIp'],x['HostPort'])) for k,v in ports.items()},'owned_candidate_ports_changed')
        mounts=item['Mounts'];require(len(mounts)==len(recipe['mounts']),'owned_candidate_mount_count_changed')
        for expected in recipe['mounts']:
            found=[m for m in mounts if m['Destination']==expected['target']]
            require(len(found)==1 and found[0]['Type']==expected['type'] and
                    found[0]['RW']==(not expected['readOnly']) and
                    (found[0].get('Name') if expected['type']=='volume' else found[0]['Source'])==expected['source'],
                    'owned_candidate_mount_changed')
        if role=='holder':require(config.get('Healthcheck',{}).get('Test')==['NONE'],'holder_healthcheck_must_be_disabled')
        else:require(config.get('Healthcheck')==image_config.get('Healthcheck'),'candidate_healthcheck_changed')
        return item

    def writers(self,target):
        """Census every local container, including bind aliases of original data."""
        old=self.docker.inspect(self.plan['predecessor']['containerId'])
        original=[m for m in old['Mounts'] if m['Destination']=='/prototype-state']
        require(len(original)==1 and original[0].get('Type')=='volume' and
                original[0].get('Name')==self.bundle['predecessor']['stateVolume'] and
                type(original[0].get('Source')) is str and original[0]['Source'].startswith('/'),
                'original_volume_identity_changed')
        source=original[0]['Source'];rows=[]
        for item in self.docker.writer_inventory():
            identity=item['Id']
            if identity==target:continue
            shares=any(m.get('Name')==self.bundle['predecessor']['stateVolume'] or
                       m.get('Source')==source or str(m.get('Source','')).startswith(source+'/') or
                       type(m.get('Source')) is str and m['Source'].startswith('/') and
                       source.startswith(m['Source'].rstrip('/')+'/')
                       for m in item.get('Mounts',()))
            if not shares:continue
            state=item['State']
            require(state.get('Running') is False and state.get('Pid')==0 and
                    type(state.get('FinishedAt')) is str,'another_original_state_owner_is_running')
            rows.append(dict(containerId=identity,running=False,pid=0,finishedAt=state['FinishedAt']))
        require(any(r['containerId']==self.plan['predecessor']['containerId'] for r in rows),
                'stopped_original_writer_required')
        return sorted(rows,key=lambda r:r['containerId'])

    def gate(self,container):
        # HTTP is authenticated by the existing in-container operator token;
        # expected owner is not learned from a database row.
        source=operator_script('GET','/v1/prototype/coordinator-freeze',owner_digest=True)
        value=self.fresh(container,source)
        require(type(value) is dict and set(value)=={'status','body','ownerSha256'} and value['status']==200,
                'authenticated_gate_status_required')
        snapshot=value['body']
        require(type(snapshot) is dict and set(snapshot)=={'authorityId','startupId','revision','planId','phase',
                'operations','reservedControlBytes'} and snapshot['authorityId']==self.plan['authorityId'],
                'closed_actual_gate_snapshot_required')
        return dict(snapshot=snapshot,ownerSha256=value['ownerSha256'])

    def __call__(self,role,context):
        require(role in ('holder','final') and type(context) is dict and
                set(context)=={'containerId','creationIntentId','gate'},'closed_current_boundary_context_required')
        self.host();identity=context['containerId']
        item=self.container(identity,role,context['creationIntentId']);state=item['State']
        require(state.get('Running') is True and type(state.get('Pid')) is int and state['Pid']>0,
                'actual_boundary_owner_not_running')
        writers=self.writers(identity);held=custody=gate=None
        if role=='holder':
            require(context['gate'] is None,'holder_has_no_gate_authority')
            held=self.fresh(identity,serializable_source(self.settings,observer=True))
            physical=self.fresh(identity,physical_binding_script(self.plan['authorityId'],self.settings['leaseDirectory']))
            require(held['settings']==self.settings and held['pid']==1 and
                    held['mainIdentity']==physical['state']['main'] and
                    held['leaseIdentity']==physical['operationBinding']['authorityLease']['fileIdentity'],
                    'actual_holder_kernel_binding_changed')
        else:
            expected=context['gate'];require(type(expected) is dict and set(expected)=={'startupId','ownerSha256'},
                                            'independent_frozen_gate_binding_required')
            gate=self.gate(identity);snapshot=gate['snapshot']
            require(snapshot['phase']=='frozen' and snapshot['revision']==3 and snapshot['operations']==0
                    and snapshot['planId']==self.plan['operationId'] and snapshot['startupId']==expected['startupId']
                    and gate['ownerSha256']==expected['ownerSha256'],'actual_frozen_gate_changed')
            response=self.fresh(identity,operator_script('GET','/v1/prototype/coordinator-custody'))
            require(response.get('status')==200,'authenticated_runtime_custody_required');custody=response['body']
            require(custody.get('status')=='verified' and custody.get('authorityId')==self.plan['authorityId']
                    and custody.get('startupId')==expected['startupId'] and custody.get('sourceReadersPresent') is True
                    and custody.get('sessionVerified') is True and custody.get('leaseVerified') is True
                    and custody.get('journalMatches') is True,'live_runtime_source_custody_required')
            physical=self.fresh(identity,physical_binding_script(self.plan['authorityId'],self.settings['leaseDirectory'],
                                   process_id=custody['processId'],include_source=True))
            require(physical['sourceBindingSha256']==custody['bindingSha256'] and
                    physical['processStartTicks']==custody['processStartTicks'],'independent_runtime_source_binding_changed')
        require(digest(physical['state'])==self.plan['bindings']['stateSha256'] and
                digest(physical['authorityLease'])==self.plan['bindings']['authorityLeaseSha256'],
                'original_physical_custody_changed')
        self.writers(identity)
        current=self.docker.inspect(identity)
        require(current['State']['Running'] is True and current['State']['Pid']==state['Pid'] and
                current['State']['StartedAt']==state['StartedAt'],'boundary_owner_restarted_during_observation')
        return dict(schemaVersion=1,role=role,container=dict(id=identity,imageId=item['Image'],
                    startedAt=state['StartedAt'],pid=state['Pid']),holder=held,physical=physical,custody=custody,
                    writers=writers,gate=gate)
