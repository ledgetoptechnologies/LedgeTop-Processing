"""One coordinator-owned source release journal; disabled without explicit role.

No wall-clock age, failure, cancellation or missing file grants deletion authority.
The separately qualified filesystem executor consumes only this journal.
"""
from datetime import datetime, timezone
import hashlib
import json
import math
import threading
import time
import uuid
from collections import OrderedDict

from . import reference_protocol as p
from .maintenance import Conflict
from . import source_release_contract as wire
from .reference_acceptance_observed import lookup as observed_lookup
from .viewer_source_attestation import validate_trusted_keys

MINIMUM_NS=86400*1_000_000_000
MAX_CHECKPOINT_NS=300*1_000_000_000
OBJECT_JOURNAL_RESERVE=1280
TERMINAL_JOURNAL_RESERVE=4096
TABLE_COLUMNS={
    'source_releases':('job_id','generation','release_id','principal_id','request_json','request_sha256',
        'request_canonical_sha256','state','hold_reason','accepted_at','remaining_ns','clock_epoch','clock_ns',
        'preserved_json','receipt_json','reserved_metadata_bytes','binding_sha256','namespace_json'),
    'source_release_objects':('release_id','object_kind','object_id','phase','physical_json'),
    'source_release_schedule':('singleton','last_release_id')}


class ReleaseStopped(RuntimeError):
    """A joined runtime shutdown stops before the next unissued effect."""


def reservation_bytes(request,preserved):
    return (len(p.canonical_json(request))+2*len(p.canonical_json(request['authorization']['eligibleObjects']))
        +len(p.canonical_json(preserved))+OBJECT_JOURNAL_RESERVE*len(request['authorization']['eligibleObjects'])
        +TERMINAL_JOURNAL_RESERVE)


def validate_schema(db):
    primary_keys={'source_releases':('job_id','generation'),
        'source_release_objects':('release_id','object_kind','object_id'),'source_release_schedule':('singleton',)}
    for name,columns in TABLE_COLUMNS.items():
        catalog=db.execute('SELECT type FROM sqlite_master WHERE name=?',(name,)).fetchone()
        info=db.execute('PRAGMA table_info('+name+')').fetchall()
        primary=tuple(row['name'] for row in sorted((row for row in info if row['pk']),key=lambda row:row['pk']))
        if not catalog or catalog[0]!='table' or tuple(row['name'] for row in info)!=columns or primary!=primary_keys[name]:
            raise Conflict('source release schema unsupported')
    indexes=[]
    for index in db.execute('PRAGMA index_list(source_releases)'):
        if index['unique']:
            quoted='"'+index['name'].replace('"','""')+'"'
            indexes.append(tuple(item['name'] for item in db.execute('PRAGMA index_info('+quoted+')')))
    if ('release_id',) not in indexes:raise Conflict('source release uniqueness unavailable')
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND tbl_name IN (?,?,?) LIMIT 1",tuple(TABLE_COLUMNS)).fetchone():
        raise Conflict('source release trigger schema unsupported')


def ensure_schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS source_releases(
        job_id TEXT NOT NULL,generation INTEGER NOT NULL,release_id TEXT NOT NULL UNIQUE,
        principal_id TEXT NOT NULL,request_json TEXT NOT NULL,request_sha256 TEXT NOT NULL,request_canonical_sha256 TEXT NOT NULL,
        state TEXT NOT NULL,hold_reason TEXT,accepted_at TEXT NOT NULL,
        remaining_ns INTEGER NOT NULL,clock_epoch TEXT NOT NULL,clock_ns INTEGER NOT NULL,
        preserved_json TEXT NOT NULL,receipt_json TEXT,reserved_metadata_bytes INTEGER NOT NULL,
        binding_sha256 TEXT NOT NULL,namespace_json TEXT,
        PRIMARY KEY(job_id,generation))''')
    db.execute('''CREATE TABLE IF NOT EXISTS source_release_objects(
        release_id TEXT NOT NULL,object_kind TEXT NOT NULL,object_id TEXT NOT NULL,
        phase TEXT NOT NULL,physical_json TEXT NOT NULL,
        PRIMARY KEY(release_id,object_kind,object_id))''')
    db.execute('''CREATE TABLE IF NOT EXISTS source_release_schedule(
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),last_release_id TEXT)''')
    if db.execute('SELECT 1 FROM source_release_schedule WHERE singleton=1').fetchone() is None:
        if db.execute('SELECT 1 FROM source_releases LIMIT 1').fetchone():
            raise Conflict('source release cursor missing from retained journal')
        db.execute('INSERT INTO source_release_schedule VALUES(1,NULL)')
    validate_schema(db)
    db.execute('CREATE INDEX IF NOT EXISTS source_release_due ON source_releases(state,remaining_ns,release_id)')
    db.execute("CREATE INDEX IF NOT EXISTS source_release_pending ON source_release_objects(release_id,object_kind,object_id) WHERE phase!='removed'")


def metadata_bytes(db):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name='source_releases'").fetchone():return 0
    validate_schema(db)
    # Reserved once before effects; includes the bounded terminal receipt,
    # physical object journals and stable status/timer fields, not just live size.
    return db.execute('SELECT COALESCE(SUM(reserved_metadata_bytes),0) FROM source_releases').fetchone()[0]+128


def scalar_binding(store,db,job_id):
    """Current immutable scalar CAS; full payloads validate at epoch/checkpoints.

    No NAS access, full manifest parse, signature verification or quota SUM is
    performed by this per-effect check. App-owned accepted payloads are immutable.
    Full journal/current payload validation repeats on restart and before ACK.
    """
    row=db.execute('''SELECT r.job_id,r.generation,r.environment,r.submission_id,r.manifest_sha,r.pipeline_digest,
        r.phase,(r.result_json IS NOT NULL) AS result_present,j.status,j.environment AS queue_environment,j.profile,
        s.request_digest AS source_request_digest,s.viewer_task_id,s.provider_task_id,s.source_attestation_sha,
        s.source_manifest_sha,s.derived_manifest_sha,s.pipeline_manifest_sha,s.source_receipt_sha,s.derived_receipt_sha,
        s.source_state,s.derived_state,c.node_id,c.worker_id,c.instance_id,c.token,
        o.observation_id,o.request_sha256 AS observation_request_sha256,o.ack_json,o.claim_sha256
        FROM reference_jobs r JOIN jobs j ON j.id=r.job_id JOIN reference_source_intakes s ON s.job_id=r.job_id
        JOIN reference_claims c ON c.job_id=r.job_id JOIN reference_worker_acceptance_observations o
        ON o.job_id=r.job_id AND o.generation=r.generation WHERE r.job_id=?''',(job_id,)).fetchone()
    if not row:raise Conflict('source release immutable binding changed')
    values=dict(row)
    state={name:values.pop(name) for name in ('phase','result_present','status','source_state','derived_state')}
    token=values.pop('token');ack=values.pop('ack_json')
    if (type(token) is not str or not token.isascii() or not 1<=len(token)<=128
            or type(ack) is not str or not ack.isascii() or not 1<=len(ack)<=4096):
        raise Conflict('source release immutable binding changed')
    values['claimTokenSha256']=hashlib.sha256(token.encode('ascii')).hexdigest()
    values['observationAckCanonicalSha256']=hashlib.sha256(ack.encode('ascii')).hexdigest()
    values['coordinatorAuthorityId']=store.queue.authority_id
    if values['queue_environment']!=values['environment'] or values['profile']!='reference-pipeline-v1':
        raise Conflict('source release immutable binding changed')
    return p.digest(values),state


def validate_retained_journal(store,db,*,release_keys=None,release_id=None):
    """Read-only closed census hook; does not enable release or repair evidence.

    An updater must explicitly recognize this schema and supply its verified
    Store/authority. Missing keys do not grant a new release role: historical
    rows are integrity/binding checked only and remain non-executable.
    """
    from .source_release_filesystem import validate_physical,encode_physical,validate_namespace
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name='source_releases'").fetchone():return []
    validate_schema(db)
    results=[]
    schedule=db.execute('SELECT * FROM source_release_schedule').fetchall()
    if len(schedule)!=1 or schedule[0]['singleton']!=1:raise Conflict('source release cursor unavailable')
    if schedule[0]['last_release_id'] is not None:
        p._uuid(schedule[0]['last_release_id'],'source release cursor')
        if not db.execute('SELECT 1 FROM source_releases WHERE release_id=?',(schedule[0]['last_release_id'],)).fetchone():
            raise Conflict('source release cursor identity changed')
    rows=db.execute('SELECT * FROM source_releases'+(' WHERE release_id=?' if release_id is not None else '')+' ORDER BY job_id,generation',() if release_id is None else (release_id,)).fetchall()
    for row in rows:
        request=json.loads(row['request_json'])
        p._closed(request,('authorization','signature','authorizationSha256'),'retained release request')
        auth=wire.validate_authorization(request['authorization'])
        if (row['request_json']!=p.canonical_json(request).decode('ascii')
                or p.digest(request)!=row['request_canonical_sha256']
                or row['request_sha256']!=request['authorizationSha256']
                or hashlib.sha256(wire.DOMAIN+p.canonical_json(auth)).hexdigest()!=row['request_sha256']
                or (row['job_id'],row['generation'],row['release_id'],row['principal_id'])!=
                   (auth['jobId'],auth['generation'],auth['releaseId'],auth['principalId'])):
            raise Conflict('source release retained request changed')
        if release_keys is not None:wire.verify(request,release_keys)
        if (row['state'] not in ('waiting_policy','draining','deleting','held','released')
                or row['hold_reason'] is not None and row['hold_reason'] not in wire.HOLD_REASONS
                or type(row['remaining_ns']) is not int or not 0<=row['remaining_ns']<=MINIMUM_NS
                or type(row['clock_ns']) is not int or not 0<=row['clock_ns']<2**63
                or row['state'] in ('draining','deleting','released') and row['remaining_ns']!=0):
            raise Conflict('source release retained state invalid')
        p._hash(row['clock_epoch'],'retained release clock epoch');wire.utc(row['accepted_at'])
        preserved=validate_job_binding(store,db,auth,row['principal_id'],historical=True)
        p._hash(row['binding_sha256'],'source release scalar binding')
        if scalar_binding(store,db,auth['jobId'])[0]!=row['binding_sha256']:
            raise Conflict('source release scalar binding changed')
        if row['preserved_json']!=p.canonical_json(preserved).decode('ascii'):
            raise Conflict('source release permanent closure changed')
        reserve=reservation_bytes(request,preserved)
        if type(row['reserved_metadata_bytes']) is not int or row['reserved_metadata_bytes']!=reserve:
            raise Conflict('source release quota reservation changed')
        objects=db.execute('SELECT * FROM source_release_objects WHERE release_id=? ORDER BY object_kind,object_id',(row['release_id'],)).fetchall()
        if [(item['object_kind'],item['object_id']) for item in objects]!=[(obj['objectKind'],obj['id']) for obj in auth['eligibleObjects']]:
            raise Conflict('source release object journal closure changed')
        for item,obj in zip(objects,auth['eligibleObjects']):
            physical=json.loads(item['physical_json'])
            if item['physical_json']!=encode_physical(item['phase'],physical,obj):
                raise Conflict('source release physical journal changed')
            validate_physical(item['phase'],physical,obj)
        namespace=json.loads(row['namespace_json']) if row['namespace_json'] else None
        if namespace:
            if row['namespace_json']!=validate_namespace(namespace,authorization_sha256=row['request_sha256'],
                    eligible_sha256=auth['eligibleClosureSha256'],preserved_sha256=p.digest(preserved),release_id=row['release_id']):
                raise Conflict('source namespace checkpoint changed')
            for item in objects:
                physical=json.loads(item['physical_json'])
                if item['phase']=='planned' or any(physical.get(name)!=value for name,value in namespace['parents'].items()):
                    raise Conflict('source namespace object inventory changed')
        elif row['state'] in ('deleting','released') or any(item['phase']!='planned' for item in objects):
            raise Conflict('source namespace checkpoint unavailable')
        fence=db.execute('SELECT release_id FROM source_reader_fences WHERE job_id=? AND generation=?',(row['job_id'],row['generation'])).fetchone()
        needs_fence=row['state'] in ('draining','deleting','released') or any(item['phase']!='planned' for item in objects)
        if (needs_fence and (not fence or fence['release_id']!=row['release_id'])
                or fence is not None and fence['release_id']!=row['release_id']):
            raise Conflict('source release retained fence changed')
        receipt=json.loads(row['receipt_json']) if row['receipt_json'] else None
        if (receipt is not None)!=(row['state']=='released'):raise Conflict('source release terminal state changed')
        if receipt:
            expected=dict(schemaVersion=1,protocol='ltds-source-release-receipt-v1',coordinatorAuthorityId=store.queue.authority_id,
                jobId=auth['jobId'],generation=auth['generation'],environment=auth['environment'],principalId=auth['principalId'],
                releaseId=auth['releaseId'],authorizationSha256=row['request_sha256'],sourceSnapshotId=auth['sourceSnapshotId'],
                sourceSnapshotSha256=auth['sourceSnapshotSha256'],workerObservationAckSha256=auth['workerObservationAckSha256'],
                eligibleClosureSha256=auth['eligibleClosureSha256'],preservedClosureSha256=p.digest(preserved),
                removedObjectCount=len(auth['eligibleObjects']),removedBytes=sum(obj['byteSize'] for obj in auth['eligibleObjects']))
            p._closed(receipt,tuple(expected)+('completedAt','receiptSha256'),'retained source release receipt')
            wire.utc(receipt['completedAt'])
            checked=dict(receipt);digest=checked.pop('receiptSha256')
            if (any(receipt[key]!=value or type(receipt[key]) is not type(value) for key,value in expected.items())
                    or digest!=p.digest(checked) or row['receipt_json']!=p.canonical_json(receipt).decode('ascii')
                    or any(item['phase']!='removed' for item in objects)):
                raise Conflict('source release retained terminal receipt changed')
        results.append(dict(jobId=row['job_id'],generation=row['generation'],releaseId=row['release_id'],
            state=row['state'],authorization=auth,preserved=preserved,namespace=namespace,objects=[dict(item) for item in objects]))
    if release_id is None and db.execute('''SELECT 1 FROM source_release_objects o LEFT JOIN source_releases r
        ON r.release_id=o.release_id WHERE r.release_id IS NULL LIMIT 1''').fetchone():
        raise Conflict('orphan source release object journal')
    return results


def reconcile_source_release_custody(store,spool,job_id,generation):
    """Read-only tombstone-aware startup audit; never enables an executor."""
    with store.queue._db() as db:
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name='source_releases'").fetchone():return False
        row=db.execute('SELECT release_id FROM source_releases WHERE job_id=? AND generation=?',(job_id,generation)).fetchone()
        if not row:return False
        fence=db.execute('SELECT release_id FROM source_reader_fences WHERE job_id=? AND generation=?',(job_id,generation)).fetchone()
        if not fence:return False
        if fence['release_id']!=row['release_id']:raise Conflict('source release audit fence changed')
        release_id=row['release_id']
        records=validate_retained_journal(store,db,release_id=release_id)
    if len(records)!=1:raise Conflict('source release custody journal unavailable')
    record=records[0];readers=store.source_readers()
    def authorize(db):
        saved=db.execute('SELECT release_id FROM source_releases WHERE job_id=? AND generation=?',(job_id,generation)).fetchone()
        if not saved or saved['release_id']!=release_id:raise Conflict('source release audit identity changed')
    handle=readers.reserve_release(job_id,generation,release_id,authorize=authorize)
    try:
        from .source_release_filesystem import SourceReleaseFilesystem
        # This ephemeral identity is only for read-only auditing. It is not an
        # operator qualification, setting, persisted approval, or delete role.
        qualification=dict(mountId=spool.mount_id,renameNoReplace=True,directoryFsync=True)
        for prefix,identity in (('root',spool._root_identity),('mount',spool._mount_identity),('marker',spool._marker_identity)):
            qualification[prefix+'Device']=str(identity[0]);qualification[prefix+'Inode']=str(identity[1])
        SourceReleaseFilesystem(spool,qualification).audit(record['authorization'],record['objects'],record['preserved'],
            verify_remaining=True,namespace=record['namespace'])
        return True
    except BaseException as error:
        from .source_spool import SourceSpoolCloseUncertain
        if isinstance(error,SourceSpoolCloseUncertain):handle.uncertain=True
        raise
    finally:
        if not handle.uncertain:handle.close()


class ReleaseSettings:
    def __init__(self,value,*,viewer_bearers,spool):
        p._closed(value,('enabled','trustedKeys','principalBindings','filesystemQualification'),'source release settings')
        p._require(type(value['enabled']) is bool,'explicit release enable flag required')
        self.enabled=value['enabled']
        self.keys=validate_trusted_keys(value['trustedKeys'])
        bindings=value['principalBindings'];p._require(type(bindings) is list and 1<=len(bindings)<=32,'bounded release principal bindings required')
        available={hashlib.sha256(token.encode('ascii')).hexdigest():environment for token,environment in viewer_bearers.items()}
        self.principals={}
        for binding in bindings:
            p._closed(binding,('environment','principalId','bearerSha256'),'release principal binding')
            p._environment(binding['environment']);p._slug(binding['principalId'],'release principal')
            p._hash(binding['bearerSha256'],'release Bearer digest')
            p._require(available.get(binding['bearerSha256'])==binding['environment'],'release binding requires actual Viewer Bearer')
            p._require(binding['bearerSha256'] not in self.principals,'duplicate release Bearer binding')
            p._require(any(environment==binding['environment'] and principal==binding['principalId']
                for (environment,_,_),(principal,_) in self.keys.records.items()),'release principal lacks explicit role key')
            self.principals[binding['bearerSha256']]=(binding['environment'],binding['principalId'])
        self.qualification=value['filesystemQualification']
        if self.enabled:
            p._require(spool is not None,'source release requires configured spool')
            from .source_release_filesystem import SourceReleaseFilesystem
            SourceReleaseFilesystem(spool,self.qualification).check_qualification()
        elif self.qualification is not None:
            # Disabled metadata/receipt recovery must not acquire a NAS handle.
            from .source_release_filesystem import validate_qualification_value
            validate_qualification_value(self.qualification)

    def principal(self,token,environment):
        if type(token) is not str or not token.isascii():return None
        bound=self.principals.get(hashlib.sha256(token.encode('ascii')).hexdigest())
        return bound[1] if bound and bound[0]==environment else None


def validate_job_binding(store,db,auth,principal,*,historical=False):
    row=store._row(db,auth['jobId'])
    original=json.loads(row['source_attestation_json'] or '{}').get('proof',{})
    from .viewer_source_attestation import PROOF_FIELDS, PROTOCOL as INTAKE_PROTOCOL, proof_digest
    from .reference_source_intake import _validate_source_manifest, _validate_derived_manifest
    p._closed(original,PROOF_FIELDS,'persisted intake proof')
    if (type(original['schemaVersion']) is not int or original['schemaVersion']!=2
            or original['protocol']!=INTAKE_PROTOCOL or proof_digest(original)!=row['source_attestation_sha']
            or json.loads(row['source_attestation_json'])['attestationSha256']!=row['source_attestation_sha']
            or json.loads(row['input_json']).get('sourceIntakeVersion')!=2):
        raise Conflict('source release intake history changed')
    expected=dict(coordinatorAuthorityId=store.queue.authority_id,jobId=row['job_id'],
        generation=row['generation'],environment=row['environment'],principalId=original.get('principalId'),
        viewerTaskId=row['viewer_task_id'],providerTaskId=row['provider_task_id'],
        viewerAttemptId=original.get('viewerAttemptId'),activationId=original.get('activationId'),
        preparedEnvelopeSha256=original.get('preparedEnvelopeSha256'),sourceSnapshotSha256=original.get('sourceSnapshotSha256'),
        inputManifestSha256=row['manifest_sha'],pipelineDigest=row['pipeline_digest'],
        sourceManifestSha256=row['source_manifest_sha'],derivedManifestSha256=row['derived_manifest_sha'],
        sourceReceiptSha256=row['source_receipt_sha'],derivedReceiptSha256=row['derived_receipt_sha'])
    if (principal!=original.get('principalId') or any(auth[key]!=value for key,value in expected.items())
            or original.get('authorityId')!=store.queue.authority_id
            or row['phase']!='completed' or not row['result_json']):
        raise Conflict('source release immutable binding changed')
    if not historical and (row['source_state']!='accepted' or row['derived_state']!='accepted' or row['status']!='succeeded'):
        raise Conflict('source release custody held')
    result=p.validate_result_manifest(json.loads(row['result_json']),job_id=row['job_id'],
        generation=row['generation'],environment=row['environment'],pipeline_digest=row['pipeline_digest'])
    if auth['resultManifestSha256']!=result['resultManifestSha256']:raise Conflict('source release result changed')
    source,derived=json.loads(row['source_manifest_json']),json.loads(row['derived_manifest_json'])
    _validate_source_manifest(source);_validate_derived_manifest(derived,row['submission_id'])
    receipt_rows=db.execute('SELECT * FROM reference_source_object_receipts WHERE job_id=? AND generation=?',
        (row['job_id'],row['generation'])).fetchall()
    store._source_completion_ack(row,receipt_rows)
    expected_original=dict(sourceManifestSha256=auth['sourceManifestSha256'],
        derivedManifestSha256=auth['derivedManifestSha256'],pipelineManifestSha256=auth['inputManifestSha256'],
        pipelineDigest=auth['pipelineDigest'],submissionId=auth['viewerAttemptId'])
    if any(original[key]!=value for key,value in expected_original.items()):
        raise Conflict('source release original intake changed')
    if (auth['eligibleObjects']!=wire.closure(source,derived)
            or p.digest(json.loads(row['source_receipt_json']))!=auth['sourceReceiptSha256']
            or p.digest(json.loads(row['derived_receipt_json']))!=auth['derivedReceiptSha256']):
        raise Conflict('source release closure changed')
    observed=observed_lookup(store,row['job_id'])
    saved=db.execute('SELECT request_json FROM reference_worker_acceptance_observations WHERE job_id=? AND generation=?',(row['job_id'],row['generation'])).fetchone()
    if (not observed or not saved or observed['observationId']!=auth['workerObservationId']
            or observed['ackSha256']!=auth['workerObservationAckSha256']
            or json.loads(saved['request_json'])['receipts']!=auth['receipts']):
        raise Conflict('source release worker observation changed')
    return wire.closure(source,derived,retained=True)



class SourceReleaseService:
    def __init__(self,store,spool,settings,*,monotonic_ns=None,stop_requested=None):
        self.store,self.spool,self.settings=store,spool,settings
        self.clock=monotonic_ns or time.monotonic_ns
        self.stop_requested=stop_requested or (lambda:False)
        self._mutex=threading.Lock()
        self._timer_mutex=threading.Lock()
        self._validated=OrderedDict()
        with store.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            ensure_schema(db)

    def _clock(self):
        value=self.clock()
        if type(value) is not int or not 0<=value<2**63:
            raise Conflict('source release monotonic clock invalid')
        return value

    def _now(self):
        value=self.store.queue._now()
        if value.tzinfo is None or value.utcoffset() is None:raise Conflict('aware coordinator clock required')
        return value.astimezone(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z')

    def _binding(self,db,auth,principal,*,historical=False):
        return validate_job_binding(self.store,db,auth,principal,historical=historical)

    def submit(self,job_id,request,*,principal):
        with self.store.queue.coordinator_operation('source-release-submit'):
            return self._submit(job_id,request,principal=principal)

    def _submit(self,job_id,request,*,principal):
        if not self.settings.enabled:raise Conflict('source release disabled')
        auth=wire.verify(request,self.settings.keys)
        if job_id!=auth['jobId'] or principal!=auth['principalId']:raise Conflict('source release owner mismatch')
        readers=self.store.source_readers()
        encoded=p.canonical_json(request).decode('ascii')
        with readers._db() as db:
            old=db.execute('SELECT * FROM source_releases WHERE job_id=? AND generation=?',(job_id,auth['generation'])).fetchone()
            preserved=self._binding(db,auth,principal,historical=bool(old))
            if old:
                if old['request_json']!=encoded or old['request_sha256']!=request['authorizationSha256']:
                    raise Conflict('source release replay conflict')
                validate_retained_journal(self.store,db,release_keys=self.settings.keys,release_id=auth['releaseId'])
                return self._status(old),False
            if db.execute('SELECT 1 FROM source_releases WHERE release_id=?',(auth['releaseId'],)).fetchone():
                raise Conflict('source release ID already bound')
            readers._unfenced(db,job_id,auth['generation'])
            # Conservative age begins now, never at the asserted earlier event.
            now=self._clock()
            accepted_at=self._now()
            if wire.utc(auth['readyProof']['verifiedAt'])>wire.utc(accepted_at):
                raise Conflict('ready proof is in the future')
            preserved_json=p.canonical_json(preserved).decode('ascii')
            reserve=reservation_bytes(request,preserved)
            self.store._quota(db,reserve)
            db.execute('''INSERT INTO source_releases(job_id,generation,release_id,principal_id,request_json,
                request_sha256,request_canonical_sha256,state,hold_reason,accepted_at,remaining_ns,clock_epoch,clock_ns,preserved_json,
                receipt_json,reserved_metadata_bytes,binding_sha256,namespace_json) VALUES(?,?,?,?,?,?,?,'waiting_policy',NULL,?,?,?,?,?,NULL,?,?,NULL)''',
                (job_id,auth['generation'],auth['releaseId'],principal,encoded,request['authorizationSha256'],p.digest(request),
                 accepted_at,MINIMUM_NS,readers.epoch,now,preserved_json,reserve,scalar_binding(self.store,db,job_id)[0]))
            for obj in auth['eligibleObjects']:
                db.execute("INSERT INTO source_release_objects VALUES(?,?,?,'planned','{}')",(auth['releaseId'],obj['objectKind'],obj['id']))
            return self._status(db.execute('SELECT * FROM source_releases WHERE release_id=?',(auth['releaseId'],)).fetchone()),True

    def _status(self,row):
        receipt=json.loads(row['receipt_json']) if row['receipt_json'] else None
        if receipt:
            checked=dict(receipt);digest=checked.pop('receiptSha256',None)
            if digest!=p.digest(checked):raise Conflict('source release receipt changed')
        return dict(schemaVersion=1,protocol='ltds-source-release-status-v1',
            coordinatorAuthorityId=self.store.queue.authority_id,jobId=row['job_id'],generation=row['generation'],
            releaseId=row['release_id'],authorizationSha256=row['request_sha256'],state=row['state'],
            remainingMinimumAgeSeconds=(row['remaining_ns']+999999999)//1_000_000_000,
            holdReason=row['hold_reason'],receipt=receipt)

    def lookup(self,job_id,release_id,*,principal):
        with self.store.queue._db() as db:
            row=db.execute('SELECT * FROM source_releases WHERE job_id=? AND release_id=?',(job_id,release_id)).fetchone()
            if not row or row['principal_id']!=principal:return None
            request=json.loads(row['request_json']);auth=wire.validate_authorization(request['authorization'])
            if (row['request_json']!=p.canonical_json(request).decode('ascii')
                    or request['authorizationSha256']!=row['request_sha256']
                    or p.digest(request)!=row['request_canonical_sha256']
                    or hashlib.sha256(wire.DOMAIN+p.canonical_json(auth)).hexdigest()!=row['request_sha256']
                    or auth['releaseId']!=release_id or auth['generation']!=row['generation']):
                raise Conflict('source release journal changed')
            validate_retained_journal(self.store,db,release_id=release_id)
            return self._status(row)

    def _hold(self,release_id,reason):
        if reason not in wire.HOLD_REASONS:reason='journal_invalid'
        with self.store.queue._db() as db:
            db.execute("UPDATE source_releases SET state='held',hold_reason=? WHERE release_id=? AND state!='released'",(reason,release_id))

    def checkpoint_timers(self):
        """One bounded local-only batch, independent of slow NAS cleanup I/O."""
        from .coordinator_freeze import CoordinatorFrozen
        if not self.settings.enabled or self.stop_requested():return False
        try:
            with self.store.queue.coordinator_operation('source-release'):
                if not self._timer_mutex.acquire(blocking=False):return False
                try:
                    readers=self.store.source_readers();now=self._clock()
                    with readers._db() as db:
                        rows=db.execute("SELECT release_id,remaining_ns,clock_epoch,clock_ns FROM source_releases WHERE state='waiting_policy' LIMIT 10001").fetchall()
                        if len(rows)>10000:raise Conflict('source release timer batch bound exceeded')
                        values=[]
                        for row in rows:
                            p._uuid(row['release_id'],'release timer identity');p._hash(row['clock_epoch'],'release timer epoch')
                            remaining=row['remaining_ns'];previous=row['clock_ns']
                            if (type(remaining) is not int or not 0<=remaining<=MINIMUM_NS
                                    or type(previous) is not int or not 0<=previous<2**63):
                                raise Conflict('source release timer journal invalid')
                            if row['clock_epoch']==readers.epoch and now<previous:
                                db.execute("UPDATE source_releases SET state='held',hold_reason='timer_clock_invalid' WHERE release_id=?",(row['release_id'],))
                                continue
                            if row['clock_epoch']==readers.epoch and now-previous<=MAX_CHECKPOINT_NS:
                                remaining=max(0,remaining-(now-previous))
                            values.append((remaining,readers.epoch,now,row['release_id']))
                        db.executemany('UPDATE source_releases SET remaining_ns=?,clock_epoch=?,clock_ns=? WHERE release_id=? AND state=\'waiting_policy\'',values)
                        return bool(rows)
                finally:self._timer_mutex.release()
        except CoordinatorFrozen:return False

    def _cached(self,db,row,readers):
        saved=self._validated.get(row['release_id'])
        if (saved is not None and saved['epoch']==readers.epoch and saved['keys'] is self.settings.keys
                and saved['request_sha']==row['request_sha256'] and saved['canonical_sha']==row['request_canonical_sha256']
                and saved['binding_sha']==row['binding_sha256'] and saved['namespace_json']==row['namespace_json']):
            self._fast_authorize(db,row['release_id'],saved,require_ready=False)
            self._validated.move_to_end(row['release_id'])
            return saved
        records=validate_retained_journal(self.store,db,release_keys=self.settings.keys,release_id=row['release_id'])
        if len(records)!=1:raise Conflict('source release journal unavailable')
        record=records[0];auth=record['authorization']
        saved=dict(epoch=readers.epoch,keys=self.settings.keys,request_sha=row['request_sha256'],
            canonical_sha=row['request_canonical_sha256'],binding_sha=row['binding_sha256'],
            auth=auth,preserved=record['preserved'],namespace=record['namespace'],namespace_json=row['namespace_json'],
            by_id={(obj['objectKind'],obj['id']):obj for obj in auth['eligibleObjects']})
        self._validated[row['release_id']]=saved
        while len(self._validated)>16:self._validated.popitem(last=False)
        self._fast_authorize(db,row['release_id'],saved,require_ready=False)
        return saved

    def _fast_authorize(self,db,release_id,cached,*,require_ready=True):
        row=db.execute('''SELECT job_id,generation,principal_id,request_sha256,request_canonical_sha256,
            binding_sha256,state,remaining_ns,namespace_json FROM source_releases WHERE release_id=?''',(release_id,)).fetchone()
        auth=cached['auth']
        if (not row or cached['keys'] is not self.settings.keys or not self.settings.enabled
                or row['request_sha256']!=cached['request_sha'] or row['request_canonical_sha256']!=cached['canonical_sha']
                or row['binding_sha256']!=cached['binding_sha'] or row['namespace_json']!=cached['namespace_json']
                or (row['job_id'],row['generation'],row['principal_id'])!=(auth['jobId'],auth['generation'],auth['principalId'])
                or row['state'] not in ('waiting_policy','draining','deleting')
                or require_ready and (row['remaining_ns']!=0 or row['state']=='waiting_policy')):
            raise Conflict('source release immutable binding changed')
        fingerprint,state=scalar_binding(self.store,db,auth['jobId'])
        if fingerprint!=cached['binding_sha']:raise Conflict('source release immutable binding changed')
        if (state['phase']!='completed' or state['status']!='succeeded' or not state['result_present']
                or state['source_state']!='accepted' or state['derived_state']!='accepted'):
            self._validated.pop(release_id,None)
            raise Conflict('source release custody held')
        if require_ready:
            fence=db.execute('SELECT release_id FROM source_reader_fences WHERE job_id=? AND generation=?',(auth['jobId'],auth['generation'])).fetchone()
            if not fence or fence['release_id']!=release_id:raise Conflict('source release fence changed')
        return row

    def run_once(self):
        """Convenience synchronous turn; Runtime separates timer and I/O loops."""
        self.checkpoint_timers()
        return self.run_io_once()

    def run_io_batch(self):
        """Amortize bounded CONTROL admission using one real same-thread token.

        Never start another object after maintenance intent. Each already-issued
        effect may finish; its next effect checks the gate independently too.
        """
        from .coordinator_freeze import CoordinatorFrozen
        if not self.settings.enabled or self.stop_requested():return False
        progress=False
        try:
            with self.store.queue.coordinator_operation('source-release'):
                for _ in range(16):
                    if self.stop_requested() or self.store.queue._coordinator_freeze.status()['phase']!='open':break
                    worked=self.run_io_once();progress=progress or worked
                    if not worked:break
        except (CoordinatorFrozen,ReleaseStopped):pass
        return progress

    def run_io_once(self):
        from .coordinator_freeze import CoordinatorFrozen
        if not self.settings.enabled or self.stop_requested():return False
        with self.store.queue._db() as db:
            if not db.execute("SELECT 1 FROM source_releases WHERE (state='waiting_policy' AND remaining_ns=0) OR state IN ('draining','deleting') LIMIT 1").fetchone():return False
        try:
            with self.store.queue.coordinator_operation('source-release'):
                return self._run_io_once()
        except (CoordinatorFrozen,ReleaseStopped):return False

    def _run_io_once(self):
        """One inventory checkpoint or exact object; no NAS under Queue locks."""
        if not self._mutex.acquire(blocking=False):return False
        try:
            readers=self.store.source_readers()
            with readers._db() as db:
                cursor=db.execute('SELECT last_release_id FROM source_release_schedule WHERE singleton=1').fetchone()
                if not cursor:raise Conflict('source release cursor unavailable')
                columns=','.join(name for name in TABLE_COLUMNS['source_releases'] if name not in ('request_json','preserved_json'))
                eligible="((state='waiting_policy' AND remaining_ns=0) OR state IN ('draining','deleting'))"
                row=db.execute('SELECT '+columns+' FROM source_releases WHERE '+eligible+' AND release_id>? ORDER BY release_id LIMIT 1',(cursor[0] or '',)).fetchone()
                if row is None:row=db.execute('SELECT '+columns+' FROM source_releases WHERE '+eligible+' ORDER BY release_id LIMIT 1').fetchone()
                if not row:return False
                release_id=row['release_id']
                cached=self._cached(db,row,readers);auth=cached['auth']
                db.execute('UPDATE source_release_schedule SET last_release_id=? WHERE singleton=1',(release_id,))
                if row['state']=='waiting_policy':
                    readers.fence_in_transaction(db,auth['jobId'],auth['generation'],release_id)
                    db.execute("UPDATE source_releases SET state='draining',hold_reason='active_source_io' WHERE release_id=?",(release_id,))
            return self._execute_one(auth,cached)
        except Exception as error:
            from .coordinator_freeze import CoordinatorFrozen
            if isinstance(error,(CoordinatorFrozen,ReleaseStopped)):raise
            if 'release_id' not in locals():raise
            self._validated.pop(release_id,None)
            reason='binding_changed'
            from .source_spool import SourceSpoolCloseUncertain
            if isinstance(error,SourceSpoolCloseUncertain):reason='durability_uncertain'
            elif hasattr(error,'release_reason'):reason=error.release_reason
            elif str(error)=='source release custody held':reason='custody_held'
            elif str(error)=='source release monotonic clock invalid':reason='timer_clock_invalid'
            elif str(error)=='reference metadata quota reached':reason='metadata_quota'
            elif str(error)=='untrusted release role':reason='release_role_unavailable'
            elif isinstance(error,OSError):reason='durability_uncertain'
            self._hold(release_id,reason)
            return True
        finally:self._mutex.release()

    def _execute_one(self,auth,cached):
        from .source_release_filesystem import SourceReleaseFilesystem,encode_physical
        readers=self.store.source_readers()
        def authorize(db):
            self._fast_authorize(db,auth['releaseId'],cached)
        try:handle=readers.reserve_release(auth['jobId'],auth['generation'],auth['releaseId'],authorize=authorize)
        except Conflict as error:
            if str(error)=='source_operation_busy':return False
            raise
        try:
            filesystem=SourceReleaseFilesystem(self.spool,self.settings.qualification)
            if cached['namespace'] is None:
                namespace,entries=filesystem.capture_namespace(auth,cached['preserved'],cached['request_sha'])
                encoded=p.canonical_json(namespace).decode('ascii')
                with readers._db() as db:
                    authorize(db)
                    current=db.execute('SELECT namespace_json FROM source_releases WHERE release_id=?',(auth['releaseId'],)).fetchone()
                    if current['namespace_json'] is not None or db.execute("SELECT 1 FROM source_release_objects WHERE release_id=? AND phase!='planned' LIMIT 1",(auth['releaseId'],)).fetchone():
                        raise Conflict('source namespace checkpoint changed')
                    for entry in entries:
                        changed=db.execute("UPDATE source_release_objects SET phase='listed',physical_json=? WHERE release_id=? AND object_kind=? AND object_id=? AND phase='planned'",
                            (entry['physical'],auth['releaseId'],entry['objectKind'],entry['id']))
                        if changed.rowcount!=1:raise Conflict('source namespace inventory changed')
                    db.execute("UPDATE source_releases SET namespace_json=?,state='deleting',hold_reason=NULL WHERE release_id=?",(encoded,auth['releaseId']))
                cached['namespace']=namespace;cached['namespace_json']=encoded
                return True
            with self.store.queue._db() as db:
                authorize(db)
                item=db.execute("SELECT * FROM source_release_objects WHERE release_id=? AND phase!='removed' ORDER BY object_kind,object_id LIMIT 1",(auth['releaseId'],)).fetchone()
            preserved=cached['preserved']
            if item is not None:
                item=dict(item)
                expected=cached['by_id'].get((item['object_kind'],item['object_id']))
                if expected is None:raise Conflict('source release object inventory changed')
                def persist(phase,physical):
                    # A new phase snapshot subsumes the previous completed
                    # effect intent. Uncertain intents never reach this point.
                    physical=dict(physical);physical.pop('effectIntent',None)
                    encoded=encode_physical(phase,physical,expected)
                    with readers._db() as db:
                        authorize(db)
                        db.execute('UPDATE source_release_objects SET phase=?,physical_json=? WHERE release_id=? AND object_kind=? AND object_id=?',
                            (phase,encoded,auth['releaseId'],item['object_kind'],item['object_id']))
                def effect(kind,operation):
                    # This commit is the authority linearization point. NAS I/O
                    # MUST remain outside Queue._db: even fsync may block on NFS.
                    effect_id=str(uuid.uuid4())
                    if self.stop_requested() or self.store.queue._coordinator_freeze.status()['phase']!='open':raise ReleaseStopped()
                    with readers._db() as db:
                        authorize(db)
                        current=db.execute('SELECT phase,physical_json FROM source_release_objects WHERE release_id=? AND object_kind=? AND object_id=?',
                            (auth['releaseId'],item['object_kind'],item['object_id'])).fetchone()
                        evidence=json.loads(current['physical_json'])
                        evidence['effectIntent']=dict(id=effect_id,kind=kind,outcome='prepared')
                        encoded=encode_physical(current['phase'],evidence,expected)
                        db.execute('UPDATE source_release_objects SET physical_json=? WHERE release_id=? AND object_kind=? AND object_id=?',
                            (encoded,auth['releaseId'],item['object_kind'],item['object_id']))
                    result=operation()
                    # Recording an already-issued outcome grants no next effect
                    # and is valid after a concurrent hold. Do not clear it.
                    with readers._db() as db:
                        current=db.execute('SELECT phase,physical_json FROM source_release_objects WHERE release_id=? AND object_kind=? AND object_id=?',
                            (auth['releaseId'],item['object_kind'],item['object_id'])).fetchone()
                        evidence=json.loads(current['physical_json'])
                        if evidence.get('effectIntent',{}).get('id')!=effect_id:
                            raise Conflict('source release effect identity changed')
                        evidence['effectIntent']['outcome']='completed'
                        encoded=encode_physical(current['phase'],evidence,expected)
                        db.execute('UPDATE source_release_objects SET physical_json=? WHERE release_id=? AND object_kind=? AND object_id=?',
                            (encoded,auth['releaseId'],item['object_kind'],item['object_id']))
                    return result
                filesystem.remove_one(auth,expected,item,persist,effect,namespace=cached['namespace'])
                return True
            # Terminal and restart checkpoints revalidate the WHOLE immutable
            # closure and namespace once, never once per target object.
            with self.store.queue._db() as db:
                records=validate_retained_journal(self.store,db,release_keys=self.settings.keys,release_id=auth['releaseId'])
                if len(records)!=1:raise Conflict('source release terminal journal unavailable')
            filesystem.audit(auth,records[0]['objects'],preserved,namespace=cached['namespace'])
            receipt=dict(schemaVersion=1,protocol='ltds-source-release-receipt-v1',
                coordinatorAuthorityId=self.store.queue.authority_id,jobId=auth['jobId'],generation=auth['generation'],
                environment=auth['environment'],principalId=auth['principalId'],releaseId=auth['releaseId'],
                authorizationSha256=cached['request_sha'],sourceSnapshotId=auth['sourceSnapshotId'],
                sourceSnapshotSha256=auth['sourceSnapshotSha256'],workerObservationAckSha256=auth['workerObservationAckSha256'],
                eligibleClosureSha256=auth['eligibleClosureSha256'],preservedClosureSha256=p.digest(preserved),
                removedObjectCount=len(auth['eligibleObjects']),removedBytes=sum(obj['byteSize'] for obj in auth['eligibleObjects']),
                completedAt=self._now())
            receipt['receiptSha256']=p.digest(receipt)
            with readers._db() as db:
                authorize(db)
                if db.execute("SELECT 1 FROM source_release_objects WHERE release_id=? AND phase!='removed' LIMIT 1",(auth['releaseId'],)).fetchone():
                    raise Conflict('source release closure not removed')
                db.execute("UPDATE source_releases SET state='released',hold_reason=NULL,receipt_json=? WHERE release_id=?",(p.canonical_json(receipt).decode('ascii'),auth['releaseId']))
            return True
        except BaseException as error:
            from .source_spool import SourceSpoolCloseUncertain
            if isinstance(error,SourceSpoolCloseUncertain):handle.uncertain=True
            raise
        finally:
            if not handle.uncertain:handle.close()
