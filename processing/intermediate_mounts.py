"""Backing-filesystem overlap checks for a constrained closed host topology.

Bind aliases use mountinfo root/device coordinates, not only visible pathnames.
Overlay/FUSE/network/device mappings are unsupported for writable sources and
hold cleanup rather than pretending absence of a lexical overlap proves safety.
"""
import os
import hashlib
import json
from pathlib import Path, PurePosixPath
import re


class MountTopologyHeld(ValueError):
    pass


def need(ok, code):
    if not ok:
        raise MountTopologyHeld(code)


def unescape(value):
    return re.sub(r'\\([0-7]{3})',lambda m:chr(int(m.group(1),8)),value)


def mounts_fingerprint(mounts):
    """Normalize array order only, keeping complete records and duplicates."""
    need(type(mounts) is list and all(type(row) is dict for row in mounts),
         'invalid_mount_fingerprint_input')
    canonical=lambda value:json.dumps(value,sort_keys=True,separators=(',',':')).encode()
    return hashlib.sha256(canonical(sorted(mounts,key=canonical))).hexdigest()


def constrained_peer_host(host):
    """Fail closed on malformed authority and equivalent mount-capability names."""
    if type(host) is not dict or host.get('Privileged') is not False:
        return False
    caps=host.get('CapAdd')
    if caps is None:
        return True
    if type(caps) is not list:
        return False
    for cap in caps:
        if type(cap) is not str or re.fullmatch(r'[A-Za-z][A-Za-z0-9_]*',cap) is None:
            return False
        normalized=cap.upper().removeprefix('CAP_')
        known={'AUDIT_CONTROL','AUDIT_READ','AUDIT_WRITE','BLOCK_SUSPEND','BPF','CHECKPOINT_RESTORE',
               'CHOWN','DAC_OVERRIDE','DAC_READ_SEARCH','FOWNER','FSETID','IPC_LOCK','IPC_OWNER','KILL',
               'LEASE','LINUX_IMMUTABLE','MAC_ADMIN','MAC_OVERRIDE','MKNOD','NET_ADMIN','NET_BIND_SERVICE',
               'NET_BROADCAST','NET_RAW','PERFMON','SETFCAP','SETGID','SETPCAP','SETUID','SYS_BOOT',
               'SYS_CHROOT','SYS_MODULE','SYS_NICE','SYS_PACCT','SYS_PTRACE','SYS_RAWIO','SYS_RESOURCE',
               'SYS_TIME','SYS_TTY_CONFIG','SYSLOG','WAKE_ALARM'}
        if normalized not in known:
            return False
    return True


def sealed_metrics_observer(container, approved):
    """Exact reviewed role only; caller still exempts RO mounts individually."""
    canonical=lambda value:json.dumps(value,sort_keys=True,separators=(',',':')).encode()
    digest=lambda value:hashlib.sha256(canonical(value)).hexdigest()
    host=container['HostConfig']
    return (isinstance(approved,dict) and approved.get('metricsOnlyReviewed') is True
            and constrained_peer_host(host)
            and approved.get('imageId')==container['Image']
            and approved.get('applicationConfigSha256')==digest(container['Config'])
            and approved.get('hostConfigSha256')==digest(host)
            and approved.get('mountsSha256')==mounts_fingerprint(container['Mounts']))


def parse_mountinfo(text):
    found=[]
    for line in text.splitlines():
        fields=line.split()
        split=fields.index('-')
        need(split>=6 and len(fields)>split+2,'invalid_mountinfo')
        root,point=unescape(fields[3]),unescape(fields[4])
        kind=fields[split+1]
        opaque_ns=(kind=='nsfs' and re.fullmatch(
            r'(?:cgroup|ipc|mnt|net|pid|pid_for_children|time|time_for_children|user|uts):\[[0-9]+\]',root))
        need((root.startswith('/') or opaque_ns) and point.startswith('/'),'relative_mount_identity')
        found.append({'id':int(fields[0]),'device':fields[2],'root':root,
                      'point':point,'type':kind})
    need(bool(found),'mount_inventory_missing')
    return found


def backing_location(path,mounts,*,resolve=lambda p:Path(p).resolve(strict=True),stat_path=os.stat):
    visible=PurePosixPath(str(resolve(path)))
    matches=[m for m in mounts if visible==PurePosixPath(m['point'])
             or PurePosixPath(m['point']) in visible.parents]
    need(bool(matches),'mount_source_unresolved')
    longest=max(len(PurePosixPath(m['point']).parts) for m in matches)
    matches=[m for m in matches if len(PurePosixPath(m['point']).parts)==longest]
    need(len(matches)==1,'stacked_mount_source_ambiguous')
    item=matches[0]
    # Do not guess how a driver forwards writes into a lower/upper/server tree.
    need(item['type'] in {'ext4','xfs','btrfs','tmpfs'},'unsupported_mount_source_topology')
    info=stat_path(str(visible))
    need(item['device']==str(os.major(info.st_dev))+':'+str(os.minor(info.st_dev)),
         'mount_stat_device_mismatch')
    suffix=visible.relative_to(PurePosixPath(item['point']))
    backing=PurePosixPath(item['root'])/suffix
    return item['device'],backing


def mount_overlaps(data,source,mounts,**kwargs):
    a,root=backing_location(data,mounts,**kwargs)
    b,other=backing_location(source,mounts,**kwargs)
    return a==b and (root==other or root in other.parents or other in root.parents)
