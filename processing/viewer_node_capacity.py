"""Opt-in loopback-only node capacity endpoint; not wired into Viewer dispatch.

A successful reservation is accounting for a configured budget, not a disk
allocation or a guarantee that ODM output stays within that budget.
"""
import hmac
import json
import math
import os
import sqlite3
from pathlib import Path
from http.server import BaseHTTPRequestHandler, HTTPServer
from . import reference_protocol as p
from .scratch_policy import ScratchLedger, PolicyConflict, _integer, _json

MAX_BODY = 4096
FIELDS = ('jobId','generation','reservationId','manifestSha256','inputBytes','environment','ttlSeconds')

class CapacityService:
 def __init__(self, ledger, *, token, output_budget, engine_storage_path, enabled=False, synthetic_mode=False):
  if enabled is not True or synthetic_mode is not True or not isinstance(ledger,ScratchLedger): raise ValueError('synthetic node capacity service disabled')
  if type(token) is not str or not token.isascii() or not 32<=len(token)<=4096 or any(ord(c)<33 or ord(c)>126 for c in token): raise ValueError('private credential required')
  p._integer(output_budget,'output budget')
  path=Path(engine_storage_path).absolute()
  if path.is_symlink() or any(parent.is_symlink() for parent in path.parents) or path.resolve()!=path or path != ledger.root or not path.is_dir(): raise ValueError('exact trusted engine storage path required')
  info=path.stat();self._identity=(info.st_dev,info.st_ino)
  self.ledger,self._token,self.output_budget,self.path=ledger,token,output_budget,path

 def _check_path(self):
  if self.path.is_symlink() or any(parent.is_symlink() for parent in self.path.parents) or self.path.resolve()!=self.path or not self.path.is_dir(): raise PolicyConflict('engine storage unavailable')
  info=self.path.stat()
  if (info.st_dev,info.st_ino)!=self._identity: raise PolicyConflict('engine storage changed')

 def _request(self, body):
  p._closed(body,FIELDS,'capacity request')
  for key in ('jobId','reservationId'):p._slug(body[key],key)
  p._integer(body['generation'],'generation');p._hash(body['manifestSha256'],'manifest')
  p._integer(body['inputBytes'],'input bytes');p._integer(body['ttlSeconds'],'TTL')
  p._require(body['ttlSeconds']<=86400,'TTL exceeds limit');p._environment(body['environment'])
  # ScratchLedger requires worker_ready; this synthetic-only adapter cannot
  # observe worker readiness or authenticate an execution claim.
  return dict(job_id=body['jobId'],generation=body['generation'],reservation_id=body['reservationId'],manifest_sha256=body['manifestSha256'],input_bytes=body['inputBytes'],output_budget=self.output_budget,environment=body['environment'],ttl_seconds=body['ttlSeconds'],worker_ready=True)

 def apply(self, route, body):
  if route not in ('/v1/reserve','/v1/start'): raise ValueError('invalid route')
  self._check_path();args=self._request(body)
  expected=_json({k:v for k,v in args.items() if k!='worker_ready'})
  outcome=None;expired=False
  with self.ledger._transaction() as db:
   now=self.ledger._now(db);self.ledger._expire(db,now)
   if route=='/v1/reserve':
    binding=db.execute('SELECT environment FROM job_environments WHERE job=?',(args['job_id'],)).fetchone()
    if binding and binding[0]!=args['environment']:raise PolicyConflict('environment changed')
    row=db.execute('SELECT * FROM reservations WHERE id=?',(args['reservation_id'],)).fetchone()
    if row:
     if row['settings']!=expected or row['state']=='expired':raise PolicyConflict('reservation changed or expired')
    else:
     if db.execute("SELECT 1 FROM reservations WHERE job=? AND generation=? AND state!='expired' LIMIT 1",(args['job_id'],args['generation'])).fetchone():raise PolicyConflict('job generation already reserved')
     outstanding=db.execute('SELECT COALESCE(SUM(remaining),0) FROM reservations').fetchone()[0]
     outstanding+=db.execute('SELECT COALESCE(SUM(bytes),0) FROM control_charges').fetchone()[0]
     self._check_path();free=_integer(self.ledger.probe(self.ledger.root),'free bytes')
     total=_integer(args['input_bytes']+args['output_budget'],'total')
     if free-outstanding-self.ledger.floor<total:raise PolicyConflict('insufficient unreserved scratch')
     observed=self.ledger.clock()
     if type(observed) not in (int,float) or not math.isfinite(observed) or observed<0:raise PolicyConflict('clock unavailable')
     after=max(now,observed)
     db.execute("UPDATE metadata SET value=? WHERE key='clock'",(str(after),))
     expired=after>=now+args['ttl_seconds']
     db.execute('INSERT OR IGNORE INTO job_environments VALUES(?,?)',(args['job_id'],args['environment']))
     db.execute("INSERT INTO reservations VALUES(?,?,?,?,?,?,?, ?,0,NULL)",
       (args['reservation_id'],expected,args['job_id'],args['generation'],args['environment'],now+args['ttl_seconds'],
        'expired' if expired else 'reserved',0 if expired else total))
     row=self.ledger._reservation(db,args['reservation_id'])
    if not expired:
     observed=self.ledger.clock()
     if type(observed) not in (int,float) or not math.isfinite(observed) or observed<0:raise PolicyConflict('clock unavailable')
     after=max(self.ledger._now(db),observed)
     db.execute("UPDATE metadata SET value=? WHERE key='clock'",(str(after),))
     if after>=row['expires']:
      self.ledger._expire(db,after);expired=True
     else:outcome={'reservationId':args['reservation_id'],'state':row['state'],'reservedBytes':row['remaining']+row['allocated']}
   else:
    row=self.ledger._reservation(db,args['reservation_id'])
    if row['settings']!=expected or row['state']!='reserved' or row['expires']<=now:raise PolicyConflict('reservation held or expired')
    outstanding=db.execute('SELECT COALESCE(SUM(remaining),0) FROM reservations').fetchone()[0]
    outstanding+=db.execute('SELECT COALESCE(SUM(bytes),0) FROM control_charges').fetchone()[0]
    self._check_path();free=_integer(self.ledger.probe(self.ledger.root),'free bytes')
    if free-outstanding-self.ledger.floor<0:raise PolicyConflict('insufficient unreserved scratch')
    observed=self.ledger.clock()
    if type(observed) not in (int,float) or not math.isfinite(observed) or observed<0:raise PolicyConflict('clock unavailable')
    after=max(now,observed)
    db.execute("UPDATE metadata SET value=? WHERE key='clock'",(str(after),))
    if after>=row['expires']:
     self.ledger._expire(db,after);expired=True
    else:
     db.execute("UPDATE reservations SET state='engine_started' WHERE id=?",(args['reservation_id'],))
     outcome={'reservationId':args['reservation_id'],'state':'engine_started','reservedBytes':row['remaining']+row['allocated']}
  if expired:raise PolicyConflict('reservation expired during probe')
  return outcome



def handler_for(service):
 if not isinstance(service,CapacityService): raise ValueError('capacity service required')
 class Handler(BaseHTTPRequestHandler):
  def log_message(self,*_): pass
  def reply(self,status,obj):
   raw=json.dumps(obj,separators=(',',':')).encode();self.send_response(status);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(raw)));self.send_header('Cache-Control','no-store');self.end_headers();self.wfile.write(raw)
  def do_POST(self):
   self.close_connection=True
   auth=self.headers.get_all('Authorization',[])
   if len(auth)!=1 or not hmac.compare_digest(auth[0].encode('utf-8'),('Bearer '+service._token).encode('ascii')):self.reply(401,{'error':'unauthorized'});return
   if self.path not in ('/v1/reserve','/v1/start'):self.reply(404,{'error':'not_found'});return
   lengths=self.headers.get_all('Content-Length',[])
   if self.headers.get('Transfer-Encoding') or len(lengths)!=1 or len(lengths[0])>10 or not lengths[0].isascii() or not lengths[0].isdecimal() or not 1<=int(lengths[0])<=MAX_BODY:self.reply(400,{'error':'invalid_request'});return
   try:
    raw=self.rfile.read(int(lengths[0]))
    if len(raw)!=int(lengths[0]):raise ValueError()
    def pairs(items):
     out={}
     for k,v in items:
      if k in out:raise ValueError()
      out[k]=v
     return out
    body=json.loads(raw.decode('utf-8'),object_pairs_hook=pairs,parse_constant=lambda _:(_ for _ in ()).throw(ValueError()))
    result=service.apply(self.path,body);self.reply(200,result)
   except (ValueError,TypeError,KeyError,UnicodeError,RecursionError): self.reply(409,{'error':'capacity_unavailable'})
   except (OSError,sqlite3.Error): self.reply(503,{'error':'capacity_unavailable'})
  def setup(self):
   super().setup();self.connection.settimeout(5)
 return Handler


def create_loopback_server(service, *, host='127.0.0.1', port=0):
 if host!='127.0.0.1' or type(port) is not int or not 0<=port<=65535:raise ValueError('loopback-only binding required')
 return HTTPServer((host,port),handler_for(service))

def main():
 """Manual node-only entrypoint; explicitly enabled, never registered by app."""
 import argparse, stat
 parser=argparse.ArgumentParser(description='Experimental loopback node capacity service')
 parser.add_argument('--enable-experimental',action='store_true');parser.add_argument('--synthetic-only',action='store_true')
 parser.add_argument('--database',required=True);parser.add_argument('--engine-storage-path',required=True)
 parser.add_argument('--token-file',required=True);parser.add_argument('--output-budget',type=int,required=True)
 parser.add_argument('--safety-floor',type=int,required=True);parser.add_argument('--port',type=int,required=True)
 args=parser.parse_args()
 if not args.enable_experimental or not args.synthetic_only:parser.error('service disabled; experimental synthetic-only flags required')
 token_path=Path(args.token_file).absolute()
 if token_path.is_symlink() or any(x.is_symlink() for x in token_path.parents):raise ValueError('private token file required')
 descriptor=os.open(token_path,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
 try:
  info=os.fstat(descriptor)
  if not stat.S_ISREG(info.st_mode) or info.st_size>4096 or (os.name!='nt' and (info.st_uid!=os.geteuid() or info.st_mode&0o077)):raise ValueError('private token file required')
  with os.fdopen(descriptor,'r',encoding='ascii') as stream:
   descriptor=None;token=stream.read().rstrip('\n')
 finally:
  if descriptor is not None:os.close(descriptor)
 ledger=ScratchLedger(args.database,args.engine_storage_path,safety_floor_bytes=args.safety_floor)
 service=CapacityService(ledger,token=token,output_budget=args.output_budget,engine_storage_path=args.engine_storage_path,enabled=True,synthetic_mode=True)
 server=create_loopback_server(service,port=args.port)
 try:server.serve_forever()
 finally:server.server_close()

if __name__=='__main__':main()
