"""Unmounted v2 prepared-copy experiment. All readiness gates are hard false.

No production migration, custody authorization, route mounting, key reload or
cleanup is implemented. Tests may patch gates only against owned fixture DBs.
"""
import hashlib
import hmac
import json
import re
from . import reference_protocol as p
from .maintenance import Conflict
from .reference_api import Response
from .reference_store import JCSRuntimeUnavailable
from .viewer_result_manifest import derive_viewer_result_manifest
from .viewer_copy_v2_candidate import _verify_v2_for_fixtures
from .viewer_service_identity_candidate import _registry_for_fixtures, _resolve_for_fixtures
from .fixture_v2_copy_census_candidate import SCHEMA

_PATH=re.compile(r'/ltds/prepared/v3/submissions/([0-9a-f-]{36})/accepted-copies-v2')
def _fixture_quota(store, db, *, body, job_id, digest, ids):
    """Account all fixture rows and the new row in the already-held transaction."""
    if not db.in_transaction:raise ValueError('transaction required')
    count,used,bad=db.execute('''SELECT COUNT(*),
        COALESCE(SUM(length(raw)+length(CAST(job_id AS BLOB))+length(CAST(digest AS BLOB))+
          length(CAST(receipt_id AS BLOB))+length(CAST(storage_id AS BLOB))+
          length(CAST(outer_id AS BLOB))+length(CAST(outer_storage_id AS BLOB))+128),0),
        COALESCE(SUM(CASE WHEN typeof(raw)!='blob' OR typeof(job_id)!='text' OR typeof(digest)!='text'
          OR typeof(receipt_id)!='text' OR typeof(storage_id)!='text' OR typeof(outer_id)!='text'
          OR typeof(outer_storage_id)!='text' THEN 1 ELSE 0 END),0) FROM fixture_v3_copy_v2''').fetchone()
    if bad or count>=10000:raise Conflict('fixture copy accounting unavailable')
    extra=used+len(body)+sum(len(x.encode('utf-8')) for x in (job_id,digest,*ids))+128
    # Shared quota now includes previous v2 rows; only add this proposed row.
    store._quota(db,extra-used)

def migration_ready():
    return False

def migrate(*_args, **_kwargs):
    # No schema change can be qualified by a caller-controlled boolean.
    raise JCSRuntimeUnavailable('v2 copy migration unqualified')

class PreparedCopyV2Candidate:
    def __init__(self, prepared_api, complete_fixture_registry):
        self.api=prepared_api
        self.registry=_registry_for_fixtures(complete_fixture_registry)

    @staticmethod
    def integration_ready():
        return False

    def handle(self, method, path, headers, body, *, protected_transport=False):
        if protected_transport is not True:return Response(503,{'error':'protected_transport_required'})
        identity=self.api._authenticate(headers)
        if identity is None:return Response(401,{'error':'unauthorized'})
        match=_PATH.fullmatch(path) if type(path) is str and method=='POST' else None
        if match is None:return Response(404,{'error':'not_found'})
        pins=[v for k,v in headers.items() if type(k) is str and k.lower()=='x-ltds-expected-coordinator-authority']
        if len(pins)!=1 or type(pins[0]) is not str:return Response(400,{'error':'authority_pin_required'})
        authority=self.api._expected_authority_id
        if not authority or not hmac.compare_digest(pins[0],authority):return Response(503,{'error':'prepared_runtime_unavailable'})
        if not self.integration_ready() or not migration_ready():return Response(503,{'error':'v2_copy_unavailable'})
        try:
            if type(body) is not bytes:raise ValueError('raw bytes required')
            envelope=p.decode_submission(body);p._closed(envelope,('acceptance','viewerReceipt'),'copy')
            receipt=envelope['viewerReceipt'];owner,environment=identity
            scope=self.registry.get((owner,environment,authority))
            if scope is None:raise Conflict('identity unavailable')
            service=scope['service']
            key=_resolve_for_fixtures(self.registry,owner=owner,environment=environment,authority=authority,
                service=service,key_id=receipt['keyId'],issuer_id=receipt['issuerId'])
            store=self.api.store
            with store.queue._db() as db:
                db.execute('BEGIN IMMEDIATE');store._check_jcs_capability(db);store._require_viewer_authority(db,authority)
                found=store._scoped_prepared_job_id(db,match[1],owner,environment,'ltds-viewer-prepared-envelope-jcs-v3')
                if found is None:return Response(404,{'error':'not_found'})
                if not store._v3_result_contract_ready() or not store._receipt_namespace_writer_boundary_ready() or store._receipt_namespace_gate is None:
                    raise JCSRuntimeUnavailable('custody unavailable')
                store._receipt_namespace_gate.check_in_transaction(db)
                row=store._row(db,found['job_id']);activation=store._activation(db,row)
                prepared=db.execute('SELECT * FROM reference_viewer_envelopes_v2 WHERE job_id=?',(row['job_id'],)).fetchone()
                if activation is None or prepared is None or row['status']!='succeeded' or row['phase'] not in ('awaiting_acceptance','completed') or not row['result_json']:
                    raise Conflict('result unavailable')
                result=derive_viewer_result_manifest(json.loads(row['result_json']),job_id=row['job_id'],generation=row['generation'],environment=row['environment'],pipeline_digest=row['pipeline_digest'])
                expected={'coordinatorAuthorityId':authority,'viewerPrincipalId':service,'submissionId':match[1],
                    'activationId':activation['activationId'],'viewerTaskId':activation['viewerTaskId'],
                    'viewerAttemptId':activation['viewerAttemptId'],'preparedEnvelopeSha256':prepared['envelope_sha256'],
                    'jobId':row['job_id'],'generation':row['generation'],'environment':environment,
                    'inputManifestSha256':row['manifest_sha'],'pipelineDigest':row['pipeline_digest'],
                    'resultManifestSha256':result['resultManifestSha256']}
                pins={key['keyId']:{'issuerId':key['issuerId'],'publicKeyPem':key['publicKeyPem'],
                    'coordinatorAuthorityId':authority,'viewerPrincipalId':service,'environment':environment}}
                signed=_verify_v2_for_fixtures(body,expected=expected,result_manifest=result,trusted_keys=pins)
                ids=[signed[x] for x in ('receiptId','storageReceiptId','acceptanceReceiptId','acceptanceStorageReceiptId')]
                prior=db.execute('SELECT raw,digest FROM fixture_v3_copy_v2 WHERE job_id=?',(row['job_id'],)).fetchone()
                digest=hashlib.sha256(body).hexdigest()
                if prior:
                    if (prior['raw'],prior['digest'])!=(body,digest):raise Conflict('copy differs')
                    return Response(202,{'state':'verified_held','created':False})
                store._check_copy_identity_namespaces(db,set(ids))
                for item in db.execute('SELECT receipt_id,storage_id,outer_id,outer_storage_id FROM fixture_v3_copy_v2'):
                    if set(ids)&set(item):raise Conflict('copy identity conflict')
                _fixture_quota(store,db,body=body,job_id=row['job_id'],digest=digest,ids=ids)
                db.execute('INSERT INTO fixture_v3_copy_v2 VALUES(?,?,?,?,?,?,?)',(row['job_id'],body,digest,*ids))
                return Response(202,{'state':'verified_held','created':True})
        except JCSRuntimeUnavailable:return Response(503,{'error':'v2_copy_unavailable'})
        except (ValueError,TypeError,KeyError,Conflict):return Response(409,{'error':'v2_copy_conflict'})
