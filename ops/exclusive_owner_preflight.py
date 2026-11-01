"""Read-only, redacted preflight for the historical coordinator snapshot.

This never provisions, changes a Docker object, authorizes cutover or claims
that network or lifecycle ownership is exclusive. Run only by an operator who
has independently verified the target host. No environment variables or raw
inspect JSON are printed.
"""
import json
import subprocess
import sys

NAME='ledgetop-processing-prototype-c3fcb3a'
CONTAINER='525ca6e24ad3b9e57e2a3be36567471c10745a4aaa8ebc03c7f2d2f215dfb977'
IMAGE='sha256:7fae9385fb5ca829783fe05eef2fdb1983abf4d491b2b9f795e55b617a5a0004'
REVISION='693cb7eca32295da959a4f42399f317577ef8a8a'
PORTS={
    '8080/tcp':[('127.0.0.1','18080'),('192.168.50.89','3005')],
    '8443/tcp':[('192.168.50.89','18443')],
}
MOUNTS={
    ('volume','ledgetop-processing-prototype-c3fcb3a-state','/prototype-state',True),
    ('bind','/run/processing-reference','/run/processing-reference',False),
    ('bind','/run/ledgetop-authority-lease','/run/ledgetop-authority-lease',True),
}


def inspect(command):
    try:
        result=subprocess.run(command,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,check=True,timeout=8)
        if len(result.stdout)>4*1024*1024: raise ValueError()
        value=json.loads(result.stdout)
        if not isinstance(value,list) or len(value)!=1 or not isinstance(value[0],dict): raise ValueError()
        return value[0]
    except (OSError,subprocess.SubprocessError,ValueError,TypeError):
        raise ValueError('read-only Docker inspection unavailable') from None


def evaluate(container,image):
    """Emit only fixed check names; never copy values from Docker output."""
    try:
        host=container['HostConfig']; config=container['Config']
        raw=host['PortBindings']
        ports={key:sorted((p['HostIp'],p['HostPort']) for p in value) for key,value in raw.items()}
        mounts={(m['Type'],m.get('Name') if m['Type']=='volume' else m['Source'],m['Destination'],m['RW'])
                for m in container['Mounts']}
        labels=config.get('Labels') or {}
        image_labels=image['Config'].get('Labels') or {}
        checks={
            'container_identity':container['Id']==CONTAINER and container['Name']=='/'+NAME,
            'image_identity':container['Image']==IMAGE and image['Id']==IMAGE,
            'image_revision':image_labels.get('org.opencontainers.image.revision')==REVISION,
            'healthy_running':container['State']['Running'] is True and container['State'].get('Health',{}).get('Status')=='healthy',
            'expected_bindings':ports=={key:sorted(value) for key,value in PORTS.items()},
            'expected_mounts':mounts==MOUNTS and len(container['Mounts'])==len(MOUNTS),
            'restart_and_network':host['RestartPolicy']['Name']=='unless-stopped' and host['NetworkMode']=='bridge',
            'security_baseline':host['ReadonlyRootfs'] is True and config['User']=='10001:10001'
                and 'ALL' in (host.get('CapDrop') or []) and 'no-new-privileges' in (host.get('SecurityOpt') or []),
            'no_compose_labels':not labels.get('com.docker.compose.project') and not labels.get('com.docker.compose.service'),
        }
    except (KeyError,TypeError,ValueError,AttributeError):
        raise ValueError('unexpected Docker inspection shape') from None
    return {'historical_snapshot_matches':all(checks.values()),'checks':checks,
            'cutover_authorized':False,'exclusive_routing_proven':False,
            'backup_or_rollback_proven':False}


def main():
    if len(sys.argv)!=1: raise SystemExit('no arguments accepted')
    try:
        container=inspect(['docker','inspect',NAME])
        image=inspect(['docker','image','inspect',IMAGE])
        print(json.dumps(evaluate(container,image),sort_keys=True))
    except ValueError:
        print(json.dumps({'error':'preflight_unavailable','cutover_authorized':False}))
        return 1
    return 0

if __name__=='__main__': raise SystemExit(main())
