"""Authenticated Viewer staged-byte assertions; never deletion or custody proof.

This is an unqualified draft. Runtime activation is blocked until migration,
rollback, restore, credential provisioning, rotation and revocation are
qualified. Historical lookup retains the accepted capability verifier; task
removal or reservation cancellation alone does not revoke that capability.
The staging commit is an issuer assertion, not proof of retained Viewer bytes.
"""
import hashlib
import hmac
import json
import base64
from pathlib import Path
import re
from types import MappingProxyType

from . import reference_protocol as p
from .maintenance import Conflict
from .reference_api import Response
from .viewer_staged_ack_schema import validate_staged_ack_schema

ROUTE = p.PREFIX + '/viewer-staged-acks/'
FIELDS = ('schemaVersion','taskId','generation','archiveSha256','archiveBytes',
          'coordinatorReceiptSha256','attemptId','modelId','modelVersionId','stagingCommitId',
          'issuer','audience','environment','namespace')
NAMESPACE='ledgetop-viewer-staged-byte-claim-v1'


class ViewerStagedAck:
    """One immutable assertion per task generation; no acceptance side effects."""
    def __init__(self, service, bearers):
        if service is None or type(bearers) is not dict or not bearers:
            raise ValueError('scoped staged acknowledgment configuration required')
        principal_environments={}
        for token,binding in bearers.items():
            if type(token) is not str or not re.fullmatch(r'[A-Za-z0-9_-]{43}',token):
                raise ValueError('invalid staged acknowledgment credential')
            raw=base64.urlsafe_b64decode(token+'=')
            if len(raw)!=32 or base64.urlsafe_b64encode(raw).decode().rstrip('=')!=token:
                raise ValueError('invalid staged acknowledgment credential')
            p._closed(binding,('environment','principalId'),'staged credential binding')
            p._environment(binding['environment']);p._slug(binding['principalId'],'Viewer principal')
            prior=principal_environments.setdefault(binding['principalId'],binding['environment'])
            if prior!=binding['environment']:raise ValueError('principal environment changed')
        self.service=service
        self._bearers=MappingProxyType({token:MappingProxyType(dict(binding)) for token,binding in bearers.items()})
        with service.queue._db() as db:
            validate_staged_ack_schema(db)

    def handle(self,method,path,headers,body=b'',*,protected_transport=False):
        if protected_transport is not True:return Response(503,{'error':'protected_transport_required'})
        auth=[v for k,v in headers.items() if isinstance(k,str) and k.lower()=='authorization']
        caps=[v for k,v in headers.items() if isinstance(k,str) and k.lower()=='x-ledgetop-capability']
        if len(auth)!=1 or not isinstance(auth[0],str) or not auth[0].startswith('Bearer '):return Response(401,{'error':'unauthorized'})
        supplied=auth[0][7:];binding=None
        if len(supplied)!=43 or not supplied.isascii():return Response(401,{'error':'unauthorized'})
        for token,configured in self._bearers.items():
            if hmac.compare_digest(token,supplied):binding=configured
        if binding is None:return Response(401,{'error':'unauthorized'})
        environment,principal=binding['environment'],binding['principalId']
        if len(caps)!=1 or not isinstance(caps[0],str):return Response(404,{'error':'not_found'})
        tail=path[len(ROUTE):] if path.startswith(ROUTE) else ''
        is_commitment=tail.endswith('/commitment')
        is_preflight=tail.endswith('/preflight')
        is_lookup='/generations/' in tail
        if is_preflight:identifier=tail[:-len('/preflight')]
        elif is_commitment:identifier=tail[:-len('/commitment')]
        elif is_lookup:identifier,generation_text=tail.split('/generations/',1)
        else:identifier=tail
        try:
            p._uuid(identifier,'task ID')
            if method not in ('GET','POST'):return Response(405,{'error':'method_not_allowed'})
            if (is_commitment or is_preflight) and method!='GET':return Response(405,{'error':'method_not_allowed'})
            if is_lookup:
                if method!='GET' or not re.fullmatch(r'[1-9][0-9]{0,15}',generation_text):raise ValueError()
                lookup_generation=int(generation_text);p._integer(lookup_generation,'generation')
            elif method=='GET' and not (is_commitment or is_preflight):return Response(404,{'error':'not_found'})
            value=None
            if method=='POST':
                if type(body) is not bytes or len(body)>4096:raise ValueError()
                value=p.decode_submission(body);p._closed(value,FIELDS,'staged acknowledgment')
                if type(value['schemaVersion']) is not int or value['schemaVersion']!=1 or value['taskId']!=identifier:raise ValueError()
                if value['issuer']!=principal or value['audience']!=self.service.queue.authority_id or value['environment']!=environment or value['namespace']!=NAMESPACE:raise Conflict('authorization binding mismatch')
                p._integer(value['generation'],'generation');p._integer(value['archiveBytes'],'archive bytes')
                for key in ('archiveSha256','coordinatorReceiptSha256'):p._hash(value[key],key)
                for key in ('attemptId','modelId','modelVersionId','stagingCommitId'):p._uuid(value[key],key)
            store=self.service.store
            if is_preflight:
                with self.service._lock,store._lock,store.queue._db() as db:
                    db.execute('BEGIN')
                    reservation=store._reservation(db,identifier,environment,caps[0],allow_cancelled=True)
                    if reservation is None:raise KeyError()
                    row=db.execute('SELECT generation,environment FROM viewer_tasks WHERE uuid=?',(identifier,)).fetchone()
                    if row is not None and row['environment']!=environment:raise KeyError()
                    return Response(200,{'schemaVersion':1,'taskId':identifier,
                        'generation':row['generation'] if row is not None else None,
                        'issuer':principal,'audience':store.queue.authority_id,
                        'environment':environment,'namespace':NAMESPACE})
            with self.service._lock,store._lock,store.queue._db() as db:
                db.execute('BEGIN IMMEDIATE')
                if is_lookup:
                    prior=db.execute('SELECT capability_sha256,assertion_json FROM viewer_staged_acks WHERE task_id=? AND generation=? AND environment=? AND principal_id=? AND audience=? AND namespace=?',(identifier,lookup_generation,environment,principal,store.queue.authority_id,NAMESPACE)).fetchone()
                    if not prior:raise KeyError()
                    try: verifier=store._capability_hash(identifier,environment,caps[0])
                    except ValueError:raise KeyError() from None
                    if not hmac.compare_digest(prior['capability_sha256'],verifier):raise KeyError()
                    return Response(200,{'assertion':json.loads(prior['assertion_json'])})
                reservation=store._reservation(db,identifier,environment,caps[0])
                if reservation is None:raise KeyError()
                row=store._row(db,identifier)
                if row['environment']!=environment:raise KeyError()
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
                        'coordinatorReceiptSha256':receipt_sha,'issuer':principal,
                        'audience':store.queue.authority_id,'environment':environment,'namespace':NAMESPACE})
                if (value['generation']!=row['generation'] or value['archiveSha256']!=receipt['sha256']
                        or value['archiveBytes']!=receipt['bytes'] or value['coordinatorReceiptSha256']!=receipt_sha):
                    raise Conflict('result identity mismatch')
                encoded=p.canonical_json(value);digest=hashlib.sha256(encoded).hexdigest()
                old=db.execute('SELECT environment,principal_id,audience,namespace,assertion_sha256,assertion_json FROM viewer_staged_acks WHERE task_id=? AND generation=?',(identifier,row['generation'])).fetchone()
                if old and (old['environment']!=environment or old['principal_id']!=principal or old['audience']!=store.queue.authority_id or old['namespace']!=NAMESPACE or old['assertion_sha256']!=digest or old['assertion_json']!=encoded.decode()):raise Conflict('assertion differs')
                if not old:
                    if db.execute('SELECT COUNT(*) FROM viewer_staged_acks').fetchone()[0]>=100000:raise Conflict('assertion capacity reached')
                    if db.execute('SELECT 1 FROM viewer_staged_acks WHERE attempt_id=? OR staging_commit_id=?',(value['attemptId'],value['stagingCommitId'])).fetchone():
                        raise Conflict('assertion identity already bound')
                    db.execute('INSERT INTO viewer_staged_acks VALUES (?,?,?,?,?,?,?,?,?,?,?)',(identifier,row['generation'],environment,principal,store.queue.authority_id,NAMESPACE,value['attemptId'],value['stagingCommitId'],reservation['capability_hash'],digest,encoded.decode()))
                return Response(200,{'assertion':value,'created':old is None})
        except KeyError:return Response(404,{'error':'not_found'})
        except (p.ProtocolError,ValueError,TypeError):return Response(400,{'error':'invalid_request'})
        except Conflict:return Response(409,{'error':'conflict'})
        except Exception:return Response(500,{'error':'internal_error'})
