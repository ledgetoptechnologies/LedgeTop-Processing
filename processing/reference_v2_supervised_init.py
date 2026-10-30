"""Disabled synthetic v2 initialization using supervised durable intent.

This does not implement upload, commit, completion, authorization or engine
identity. The trusted supervisor must provide an authenticated claim and fixed
engine, persist a unique operation key, and exclude path replacement, database
restore, old workers, direct engine clients and file writers for the lifetime
of this operation. SQLite and cooperative gate checks are not a security fence.
"""
import ipaddress
from urllib.parse import urlsplit
from . import reference_protocol as p
from .node_gate import NodeGate
from .reference_v2_controlled_sqlite import ControlledSQLite,ControlledOpenHeld
from .reference_v2_node_preflight import operation_receipt
from .reference_v2_local_input import verify_complete_input_set

class InitHeld(ValueError): pass


def validate_claim(authenticated_claim,binding,engine_id,engine):
 try:
  p._closed(authenticated_claim,('jobId','manifestSha256','generation','engineId','engineOrigin','options'),'claim')
  if (authenticated_claim['jobId'],authenticated_claim['manifestSha256'],authenticated_claim['generation'],authenticated_claim['engineId']) != (binding['jobId'],binding['manifestSha256'],binding['generation'],engine_id):raise ValueError()
  endpoint=authenticated_claim['engineOrigin'];u=urlsplit(endpoint)
  if (u.scheme not in ('http','https') or not ipaddress.ip_address(u.hostname).is_loopback or u.username is not None or u.password is not None or u.path not in ('','/') or u.query or u.fragment or endpoint!=engine.origin):raise ValueError()
  options=authenticated_claim['options']
  if type(options) is not list or len(options)>256 or len(p.canonical_json(options))>32768:raise ValueError()
  seen=set()
  for item in options:
   p._closed(item,('name','value'),'option');p._slug(item['name'],'option name')
   if item['name'] in seen or type(item['value']) not in (bool,int,str) or len(p.canonical_json(item['value']))>1024:raise ValueError()
   seen.add(item['name'])
 except (ValueError,TypeError,KeyError,AttributeError):raise InitHeld('trusted claim invalid') from None
 return options


def initialize_once(*, enabled=False, intent, engine, fence, journal, ledger,
 reservation_id,binding,manifest_bytes,engine_id,node_id,origin,authority_id,
 generation,worker_id,operation_key,authenticated_claim):
 if enabled is not True:raise InitHeld('supervised initialization disabled')
 if type(intent) is not ControlledSQLite or intent.fence is not fence:raise InitHeld('owned supervised intent required')
 options=validate_claim(authenticated_claim,binding,engine_id,engine)
 receipt=operation_receipt(operation_key=operation_key,binding=binding,reservation_id=reservation_id,engine_id=engine_id,node_id=node_id,origin=origin,authority_id=authority_id,generation=generation,worker_id=worker_id)
 identity=p.canonical_json(dict(binding=binding,reservationId=reservation_id,engineId=engine_id,nodeId=node_id,origin=origin,authorityId=authority_id,generation=generation,workerId=worker_id,receiptId=receipt,claim=authenticated_claim)).decode()
 fence.check()
 with NodeGate(engine_id) as gate:
  state=gate.snapshot();owner=state['reservation']
  if ((state['nodeId'],state['coordinatorOrigin'],state['authorityId'],state['generation'])!=(node_id,origin,authority_id,generation)
   or owner is None or owner['receiptId']!=receipt or owner['completionDigest'] is not None
   or owner['job']!=dict(id=binding['jobId'],workerId=worker_id,nodeId=node_id,authorityId=authority_id)):
   raise InitHeld('exact retained node gate required')
  verify_complete_input_set(fence=fence,journal=journal,ledger=ledger,reservation_id=reservation_id,binding=binding,manifest_bytes=manifest_bytes)
  fence.check()
  try:intent.record_once(binding['jobId'],identity)
  except ControlledOpenHeld:raise InitHeld('intent unavailable or already recorded') from None
  fence.check();intent.check()
  # Once recorded, any error or process exit is ambiguous and cannot repeat.
  engine.initialize(dict(name=binding['jobId'],engine_uuid=binding['jobId'],outputs=None,options=options))
  return {'state':'initialization_requires_reconciliation','receiptId':receipt}
