"""Unqualified, metadata-only native input identity binding; no upload authority."""
import hashlib,os
from . import native_input_manifest as manifest
from .maintenance import Conflict
from .viewer_tasks import checked_uuid

DDL='''CREATE TABLE native_manifest_bindings (uuid TEXT PRIMARY KEY, environment TEXT NOT NULL,
 authority TEXT NOT NULL, generation INTEGER NOT NULL, capability_hash TEXT NOT NULL,
 manifest_id TEXT NOT NULL, revision INTEGER NOT NULL, manifest_sha TEXT NOT NULL,
 manifest_bytes BLOB NOT NULL, UNIQUE(environment,manifest_id))'''
MAX_RECORDS=1000
MAX_BYTES=64*1024*1024

def blocks_legacy(db,identifier):
    """Any extant marker blocks legacy task operations, including malformed rows."""
    table=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='native_manifest_bindings'").fetchone()
    if table and db.execute('SELECT 1 FROM native_manifest_bindings WHERE uuid=?',(identifier,)).fetchone():
        raise Conflict('native manifest task requires qualified transfer')

class NativeManifestBindings:
    def __init__(self,store):
        from . import config
        if config.NATIVE_MANIFEST_BINDING_QUALIFIED is not True:
            raise Conflict('native manifest binding not qualified')
        self.store=store;self.queue=store.queue
        self._authority()
        with store._lock,self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='native_manifest_bindings'").fetchone()
            if row is None:db.execute(DDL)
            elif row[0]!=DDL:raise Conflict('native manifest schema changed')

    def _authority(self):
        lease=getattr(self.queue,'authority_lease',None)
        if (lease is None or getattr(lease,'_descriptor',None) is None
                or getattr(lease,'authority',None)!=self.queue.authority_id or self.queue._lock.closed):
            raise Conflict('native manifest authority unavailable')

    def _check(self,db,identifier,environment,capability):
        self._authority()
        checked_uuid(identifier)
        reservation=self.store._reservation(db,identifier,environment,capability)
        if reservation is None:raise Conflict('task capability reservation required')
        if db.execute('SELECT 1 FROM viewer_tasks WHERE uuid=?',(identifier,)).fetchone() or db.execute('SELECT 1 FROM viewer_incarnations WHERE uuid=?',(identifier,)).fetchone():
            raise Conflict('historical task requires inspection')
        # A failed legacy init can leave a directory before its DB transaction.
        # Never infer that missing DB rows imply an unused generation.
        with os.scandir(self.store.root) as entries:
            for index,entry in enumerate(entries):
                if index>=10000:raise Conflict('task directory census exceeds limit')
                if entry.name.startswith(identifier+'-'):raise Conflict('historical task files require inspection')
        return reservation

    def bind(self,identifier,environment,capability,body):
        from . import reference_protocol as ref
        decoded=ref.decode_submission(body)
        if type(decoded) is not dict or type(decoded.get('id')) is not str:raise ref.ProtocolError('manifest identity missing')
        checked=manifest.validate(body,expected_manifest_id=decoded['id'],max_input_bytes=min(self.store.max_input_bytes,manifest.MAX_INPUT_BYTES))
        with self.store._lock,self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');reservation=self._check(db,identifier,environment,capability)
            existing=db.execute('SELECT * FROM native_manifest_bindings WHERE uuid=?',(identifier,)).fetchone()
            if existing:
                if (existing['environment']!=environment or existing['authority']!=self.queue.authority_id
                    or existing['generation']!=1 or existing['capability_hash']!=reservation['capability_hash']
                    or existing['manifest_id']!=checked['id'] or existing['revision']!=1
                    or existing['manifest_sha']!=checked['sha256'] or bytes(existing['manifest_bytes'])!=body):
                    raise Conflict('native manifest binding changed')
            else:
                count,used=db.execute('SELECT COUNT(*),COALESCE(SUM(length(manifest_bytes)),0) FROM native_manifest_bindings').fetchone()
                if count>=MAX_RECORDS or used+len(body)>MAX_BYTES:raise Conflict('native manifest history limit reached')
                if db.execute('SELECT 1 FROM native_manifest_bindings WHERE environment=? AND manifest_id=?',(environment,checked['id'])).fetchone():
                    raise Conflict('native manifest identity already bound')
                db.execute('INSERT INTO native_manifest_bindings VALUES(?,?,?,?,?,?,?,?,?)',
                    (identifier,environment,self.queue.authority_id,1,reservation['capability_hash'],checked['id'],1,checked['sha256'],body))
            return self._receipt(identifier,environment,reservation,checked)

    def _receipt(self,identifier,environment,reservation,checked):
        from . import reference_protocol as ref
        bound={'authority':self.queue.authority_id,'environment':environment,'taskUuid':identifier,'generation':1,
               'capabilityHash':reservation['capability_hash'],'manifestId':checked['id'],'manifestRevision':1,'manifestSha256':checked['sha256']}
        binding_id=hashlib.sha256(b'native-manifest-binding-v1\0'+ref.canonical_json(bound)).hexdigest()
        return {'protocol':'native-odm-input-v1-candidate','version':1,'taskUuid':identifier,'generation':1,
                'environment':environment,'manifestId':checked['id'],'manifestRevision':1,
                'manifestSha256':checked['sha256'],'bindingId':binding_id,'state':'identity_bound','capacityReady':False}

    def status(self,identifier,environment,capability):
        from . import reference_protocol as ref
        with self.store._lock,self.queue._db() as db:
            db.execute('BEGIN');reservation=self._check(db,identifier,environment,capability)
            row=db.execute('SELECT * FROM native_manifest_bindings WHERE uuid=?',(identifier,)).fetchone()
            if row is None:raise KeyError('unknown manifest binding')
            if (row['environment']!=environment or row['authority']!=self.queue.authority_id or row['generation']!=1
                    or row['capability_hash']!=reservation['capability_hash'] or row['revision']!=1):
                raise Conflict('native manifest binding changed')
            try:checked=manifest.validate(bytes(row['manifest_bytes']),expected_manifest_id=row['manifest_id'],max_input_bytes=min(self.store.max_input_bytes,manifest.MAX_INPUT_BYTES))
            except (ValueError,TypeError,KeyError,OverflowError):raise Conflict('native manifest binding changed') from None
            if row['manifest_sha']!=checked['sha256']:raise Conflict('native manifest binding changed')
            return self._receipt(identifier,environment,reservation,checked)
