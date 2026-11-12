"""Backing-filesystem overlap checks for a constrained closed host topology.

Bind aliases use mountinfo root/device coordinates, not only visible pathnames.
Overlay/FUSE/network/device mappings are unsupported for writable sources and
hold cleanup rather than pretending absence of a lexical overlap proves safety.
"""
import os
from pathlib import Path, PurePosixPath
import re


class MountTopologyHeld(ValueError):
    pass


def need(ok, code):
    if not ok:
        raise MountTopologyHeld(code)


def unescape(value):
    return re.sub(r'\\([0-7]{3})',lambda m:chr(int(m.group(1),8)),value)


def parse_mountinfo(text):
    found=[]
    for line in text.splitlines():
        fields=line.split()
        split=fields.index('-')
        need(split>=6 and len(fields)>split+2,'invalid_mountinfo')
        root,point=unescape(fields[3]),unescape(fields[4])
        need(root.startswith('/') and point.startswith('/'),'relative_mount_identity')
        found.append({'id':int(fields[0]),'device':fields[2],'root':root,
                      'point':point,'type':fields[split+1]})
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
