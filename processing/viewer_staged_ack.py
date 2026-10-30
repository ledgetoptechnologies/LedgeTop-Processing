"""Authenticated Viewer staged-byte assertions; never deletion or custody proof."""
import hashlib
import hmac
import json
import base64
from pathlib import Path
import re

from . import reference_protocol as p
from .maintenance import Conflict
from .reference_api import Response

ROUTE = p.PREFIX + '/viewer-staged-acks/'
FIELDS = ('schemaVersion','taskId','generation','archiveSha256','archiveBytes',
          'coordinatorReceiptSha256','attemptId','modelId','modelVersionId','stagingCommitId')


class ViewerStagedAck:
    """One immutable assertion per task generation; no acceptance side effects."""
    def __init__(self, service, bearers):
        if service is None or type(bearers) is not dict or not bearers:
            raise ValueError('scoped staged acknowledgment configuration required')
        for token,environment in bearers.items():
            if type(token) is not str or not re.fullmatch(r'[A-Za-z0-9_-]{43}',token):
                raise ValueError('invalid staged acknowledgment credential')
            raw=base64.urlsafe_b64decode(token+'=')
            if len(raw)!=32 or base64.urlsafe_b64encode(raw).decode().rstrip('=')!=token:
                raise ValueError('invalid staged acknowledgment credential')
            p._environment(environment)
        self.service,self._bearers=service,dict(bearers)
        with service.queue._db() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS viewer_staged_acks (
                task_id TEXT NOT NULL, generation INTEGER NOT NULL, environment TEXT NOT NULL,
                attempt_id TEXT NOT NULL UNIQUE, staging_commit_id TEXT NOT NULL UNIQUE,
                assertion_sha256 TEXT NOT NULL, assertion_json TEXT NOT NULL,
                PRIMARY KEY(task_id,generation))''')

    def handle(self,method,path,headers,body=b'',*,protected_transport=False):
        if protected_transport is not True:return Response(503,{'error':'protected_transport_required'})
        auth=[v for k,v in headers.items() if isinstance(k,str) and k.lower()=='authorization']
        caps=[v for k,v in headers.items() if isinstance(k,str) and k.lower()=='x-ledgetop-capability']
        if len(auth)!=1 or not isinstance(auth[0],str) or not auth[0].startswith('Bearer '):return Response(401,{'error':'unauthorized'})
        supplied=auth[0][7:];environment=None
        if not supplied.isascii():return Response(401,{'error':'unauthorized'})
        for token,env in self._bearers.items():
            if hmac.compare_digest(token,supplied):environment=env
        if environment is None:return Response(401,{'error':'unauthorized'})
        if len(caps)!=1 or not isinstance(caps[0],str):return Response(404,{'error':'not_found'})
        tail=path[len(ROUTE):] if path.startswith(ROUTE) else ''
        is_commitment=tail.endswith('/commitment')
        identifier=tail[:-len('/commitment')] if is_commitment else tail
        try:
            p._uuid(identifier,'task ID')
            if method not in ('GET','POST'):return Response(405,{'error':'method_not_allowed'})
            if is_commitment and method!='GET':return Response(405,{'error':'method_not_allowed'})
            value=None
            if method=='POST':
                if type(body) is not bytes or len(body)>4096:raise ValueError()
                value=p.decode_submission(body);p._closed(value,FIELDS,'staged acknowledgment')
                if type(value['schemaVersion']) is not int or value['schemaVersion']!=1 or value['taskId']!=identifier:raise ValueError()
                p._integer(value['generation'],'generation');p._integer(value['archiveBytes'],'archive bytes')
                for key in ('archiveSha256','coordinatorReceiptSha256'):p._hash(value[key],key)
                for key in ('attemptId','modelId','modelVersionId','stagingCommitId'):p._uuid(value[key],key)
            store=self.service.store
            with self.service._lock,store._lock,store.queue._db() as db:
                db.execute('BEGIN IMMEDIATE')
                reservation=store._reservation(db,identifier,environment,caps[0])
                if reservation is None:raise KeyError()
                row=store._row(db,identifier)
                if row['environment']!=environment:raise KeyError()
                if method=='GET' and not is_commitment:
                    prior=db.execute('SELECT assertion_json FROM viewer_staged_acks WHERE task_id=? AND generation=? AND environment=?',(identifier,row['generation'],environment)).fetchone()
                    return Response(200,{'assertion':json.loads(prior[0])}) if prior else Response(404,{'error':'not_found'})
                task=store._task(db,row)
                if not task['job'] or task['job']['status']!='succeeded' or not task['archive_path']:
                    raise Conflict('result not available')
                path=Path(task['archive_path'])
                if path!=Path(task['directory'])/'all.zip':raise Conflict('archive path changed')
                receipt=self.service._archive_receipt(task,path.with_suffix('.zip.receipt'))
                receipt_sha=hashlib.sha256(p.canonical_json(receipt)).hexdigest()
                if is_commitment:
                    return Response(200,{'schemaVersion':1,'taskId':identifier,'generation':row['generation'],
                        'archiveSha256':receipt['sha256'],'archiveBytes':receipt['bytes'],
                        'coordinatorReceiptSha256':receipt_sha})
                if (value['generation']!=row['generation'] or value['archiveSha256']!=receipt['sha256']
                        or value['archiveBytes']!=receipt['bytes'] or value['coordinatorReceiptSha256']!=receipt_sha):
                    raise Conflict('result identity mismatch')
                encoded=p.canonical_json(value);digest=hashlib.sha256(encoded).hexdigest()
                old=db.execute('SELECT environment,assertion_sha256,assertion_json FROM viewer_staged_acks WHERE task_id=? AND generation=?',(identifier,row['generation'])).fetchone()
                if old and (old['environment']!=environment or old['assertion_sha256']!=digest or old['assertion_json']!=encoded.decode()):raise Conflict('assertion differs')
                if not old:
                    if db.execute('SELECT COUNT(*) FROM viewer_staged_acks').fetchone()[0]>=100000:raise Conflict('assertion capacity reached')
                    if db.execute('SELECT 1 FROM viewer_staged_acks WHERE attempt_id=? OR staging_commit_id=?',(value['attemptId'],value['stagingCommitId'])).fetchone():
                        raise Conflict('assertion identity already bound')
                    db.execute('INSERT INTO viewer_staged_acks VALUES (?,?,?,?,?,?,?)',(identifier,row['generation'],environment,value['attemptId'],value['stagingCommitId'],digest,encoded.decode()))
                return Response(200,{'assertion':value,'created':old is None})
        except KeyError:return Response(404,{'error':'not_found'})
        except (p.ProtocolError,ValueError,TypeError):return Response(400,{'error':'invalid_request'})
        except Conflict:return Response(409,{'error':'conflict'})
        except Exception:return Response(500,{'error':'internal_error'})
