"""Disabled on-node NodeODX 2.4.0 UUID guard experiment.

Requires an independently trusted supervisor to fence every engine client and
filesystem writer for the whole check and initialize; no local observation is
an exclusive ingress fence. The caller must hold the correct NodeGate and
provide authenticated configuration, private durable intent store, and pinned
NodeODX tmp/data directory identities. Nothing here is runtime-connected.
"""
import os
from pathlib import Path
import stat
import uuid
from . import reference_protocol as p
from .node_gate import NodeGate
from .reference_v2_controlled_sqlite import ControlledSQLite

class NodeUuidHeld(ValueError):pass


def guarded_initialize(*,enabled=False,intent,fence,gate,engine,job_id,task_name,
 options,tmp_dir,data_dir,tmp_identity,data_identity):
 if enabled is not True:raise NodeUuidHeld('UUID guard disabled')
 if type(intent) is not ControlledSQLite or intent.fence is not fence or type(gate) is not NodeGate:raise NodeUuidHeld('supervised owner required')
 p._uuid(job_id,'job id');p._string(task_name,'task name',255)
 if type(options) is not list or len(p.canonical_json(options))>16384:raise NodeUuidHeld('bounded authenticated options required')
 fence.check();state=gate.snapshot();owner=state['reservation']
 if owner is None or owner['job'] is None or owner['job']['id']!=job_id or owner['completionDigest'] is not None:
  raise NodeUuidHeld('bound cooperative node gate required')
 if intent.lookup(job_id) is not None:raise NodeUuidHeld('prior initialization intent requires reconciliation')
 candidate=str(uuid.uuid4());fds=[]
 try:
  for directory,expected in ((tmp_dir,tmp_identity),(data_dir,data_identity)):
   path=Path(directory).absolute()
   if path.resolve(strict=True)!=path or any(x.is_symlink() for x in (path,*path.parents)):raise NodeUuidHeld('node namespace path invalid')
   fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC);fds.append((fd,path,expected))
   info=os.fstat(fd)
   if not stat.S_ISDIR(info.st_mode) or (info.st_dev,info.st_ino)!=expected:raise NodeUuidHeld('node namespace identity changed')
  def absence():
   fence.check()
   for fd,path,expected in fds:
    a=os.fstat(fd);b=os.stat(path,follow_symlinks=False)
    if (a.st_dev,a.st_ino)!=expected or (b.st_dev,b.st_ino)!=expected:raise NodeUuidHeld('node namespace identity changed')
    try:os.stat(candidate,dir_fd=fd,follow_symlinks=False)
    except FileNotFoundError:pass
    else:raise NodeUuidHeld('engine UUID already exists in node namespace')
   tasks=engine.read('/task/list')
   if type(tasks) is not list or len(tasks)>32768:raise NodeUuidHeld('engine task census unavailable')
   ids=[]
   for task in tasks:
    if type(task) is not dict:raise NodeUuidHeld('engine task census invalid')
    try:p._uuid(task.get('uuid'),'engine task uuid')
    except ValueError:raise NodeUuidHeld('engine task census invalid') from None
    ids.append(task['uuid'])
   if len(ids)!=len(set(ids)) or candidate in ids:raise NodeUuidHeld('engine UUID already exists or census invalid')
   fence.check()
  absence()
  identity=p.canonical_json(dict(jobId=job_id,engineUuid=candidate,taskName=task_name,options=options,
             nodeId=state['nodeId'],authorityId=state['authorityId'],gateGeneration=state['generation'],receiptId=owner['receiptId'])).decode()
  intent.record_once(job_id,identity)
  absence()
  # Any response loss or crash after intent is terminally ambiguous here.
  engine.initialize(dict(name=task_name,engine_uuid=candidate,outputs=None,options=options))
  return {'state':'initialization_requires_reconciliation','engineUuid':candidate}
 finally:
  for fd,_,_ in fds:os.close(fd)
