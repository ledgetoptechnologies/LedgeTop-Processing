"""Read-only root-owned direct-worker enrollment binding; no installation."""
import os
from pathlib import PurePosixPath
from processing import reference_protocol as p
from processing.direct_protected_transport import _read

PROTOCOL='ltds-worker-direct-enrollment-v2'


def load(path, *, config_path):
    """Trust only a root-owned immutable-to-service file and its ancestors."""
    if os.name!='posix' or os.geteuid()==0:
        raise ValueError('unprivileged worker service required')
    raw=_read(path,private=False,maximum=16384,trusted_uids={0})
    value=p.decode_submission(raw,max_bytes=16384)
    p._closed(value,('schemaVersion','protocol','configPath','configSha256',
        'authorityId','nodeId','workerId','workerInstanceId','credentialSha256'),'worker direct enrollment')
    if type(value['schemaVersion']) is not int or value['schemaVersion']!=2 or value['protocol']!=PROTOCOL:
        raise ValueError('worker direct enrollment version invalid')
    if (type(config_path) is not str or not config_path.startswith('/')
            or str(PurePosixPath(config_path))!=config_path or value['configPath']!=config_path):
        raise ValueError('worker direct config identity changed')
    p._hash(value['configSha256'],'worker config digest')
    p._closed(value['credentialSha256'],('certificate','privateKey','clientCA','signingKey'),'direct credential digests')
    for digest in value['credentialSha256'].values():p._hash(digest,'direct credential digest')
    p._uuid(value['authorityId'],'worker authority')
    for field in ('nodeId','workerId','workerInstanceId'):p._slug(value[field],field)
    return value


def require_identity(record,config):
    for field in ('authorityId','nodeId','workerId','workerInstanceId'):
        if record[field]!=config[field]:raise ValueError('worker direct identity changed')
    return True
