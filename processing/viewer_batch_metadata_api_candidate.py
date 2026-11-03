"""Unmounted bounded batch metadata API; mount only on reference_http."""
import re
from .reference_api import Response
from .reference_protocol import decode_submission
from .maintenance import Conflict

class BatchMetadataAPI:
 def __init__(self,identity):self.identity=identity
 def handle(self,method,path,headers,body=b'',*,protected_transport=False):
  if protected_transport is not True:return Response(503,{'error':'transport_required'})
  auth=[v for k,v in headers.items() if k.lower()=='authorization'];caps=[v for k,v in headers.items() if k.lower()=='x-ledgetop-capability']
  if len(auth)!=1 or not auth[0].startswith('Bearer '):return Response(401,{'error':'unauthorized'})
  try:self.identity.authenticate(auth[0][7:],True)
  except (Conflict,ValueError):return Response(401,{'error':'unauthorized'})
  if len(caps)!=1:return Response(404,{'error':'unavailable'})
  match=re.fullmatch(r'/ltds/held-batches/v1/tasks/([0-9a-f-]{36})/(reserve|prepare)',path)
  if method!='POST' or not match:return Response(404,{'error':'not_found'})
  try:
   payload=decode_submission(body)
   task,action=match.groups()
   if action=='reserve':
    if set(payload)!={'expectedAuthorityId'} or payload['expectedAuthorityId']!=self.identity.authority:raise Conflict('authority mismatch')
    self.identity.reserve_fresh(task,caps[0],auth[0][7:],True)
    return Response(200,{'state':'held_reserved'})
   if set(payload)!={'expectedAuthorityId','generation','batchId','files'} or payload['expectedAuthorityId']!=self.identity.authority:raise Conflict('authority mismatch')
   return Response(200,self.identity.prepare(task,caps[0],auth[0][7:],True,payload['generation'],payload['batchId'],payload['files']))
  except (Conflict,KeyError):return Response(409,{'error':'unavailable'})
  except (TypeError,ValueError):return Response(400,{'error':'invalid_request'})
