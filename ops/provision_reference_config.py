"""Stage private reference credentials/configuration without activating services.

The fixed addresses describe the verified two-PC installation. No NAS broker is
assumed and no worker intake configuration is created before that route exists.
All generated secrets remain in new owner-private host files, never stdout.
"""
import argparse
import json
import os
from pathlib import Path
import secrets
import stat


def private_file(path):
    path = Path(path)
    if not path.is_absolute() or path.resolve() != path or any(p.is_symlink() for p in (path,*path.parents)):
        raise ValueError('explicit private regular path required')
    info = path.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.geteuid()
            or info.st_mode & 0o077):
        raise ValueError('owned private regular file required')
    return path


def provision(output,native_env):
    if os.name != 'posix':
        raise ValueError('Linux host provisioner required')
    native = private_file(native_env)
    root = Path(output)
    if not root.is_absolute() or root.resolve() != root or any(p.is_symlink() for p in (root,*root.parents)):
        raise ValueError('new absolute private output directory required')
    parent = root.parent.stat()
    if parent.st_uid != os.geteuid() or parent.st_mode & 0o077:
        raise ValueError('owned private output parent required')
    legacy = {line.split('=',1)[1] for line in native.read_text().splitlines() if '=' in line}
    root.mkdir(mode=0o700)
    old_umask = os.umask(0o077)
    def write(name,value):
        with (root/name).open('x',encoding='utf-8') as stream:
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
    try:
        values = {}
        for role in ('viewerStaging','clusterWorker','mainWorker','clusterResultReader','mainResultReader'):
            candidate = secrets.token_urlsafe(48)
            while candidate in legacy or candidate in values.values():
                candidate = secrets.token_urlsafe(48)
            values[role] = candidate
        workers = {node:dict(bearer=values[node+'Worker'],workerId=node+'-reference',
            instanceId=node+'-reference-v1',resultOrigin='https://'+address+':18445',
            resultBearer=values[node+'ResultReader']) for node,address in
            (('cluster','192.168.50.89'),('main','192.168.50.88'))}
        settings = dict(schemaVersion=1,listener=dict(host='0.0.0.0',port=8443,
            certificate='/run/processing-reference/coordinator.crt',
            privateKey='/run/processing-reference/coordinator.key',clientCA='/run/processing-reference/ca.crt'),
            viewerBearers={'staging':values['viewerStaging']},workers=workers,
            streamBudget=dict(maxConcurrent=2,maxObjectBytes=100*1024**3))
        write('credentials.json',json.dumps(values,indent=2)+'\n')
        write('runtime.json',json.dumps(settings,indent=2)+'\n')
        write('viewer-staging.token',values['viewerStaging']+'\n')
        receipt = dict(schemaVersion=1,distinctNewCredentials=len(values),
            referenceEnvironments=['staging'],coordinatorOrigin='https://192.168.50.89:18443',
            workers={node:dict(workerId=worker['workerId'],instanceId=worker['instanceId'],
                resultOrigin=worker['resultOrigin']) for node,worker in workers.items()},
            brokerConfigured=False,workerIntakeConfigured=False,servicesActivated=False)
        write('provisioning-receipt.json',json.dumps(receipt,indent=2)+'\n')
        for directory in (root,root.parent):
            descriptor=os.open(directory,os.O_RDONLY|os.O_DIRECTORY)
            try:os.fsync(descriptor)
            finally:os.close(descriptor)
        return receipt
    finally:
        os.umask(old_umask)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',required=True)
    parser.add_argument('--native-env',required=True)
    args=parser.parse_args()
    try:print(json.dumps(provision(args.output,args.native_env)))
    except Exception:raise SystemExit('Private configuration staging unavailable; inspect retained files locally') from None


if __name__=='__main__':
    main()
