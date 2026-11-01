"""Disabled internal v2 epoch prototype. No wire route, migration, or dispatch."""
import os
from . import native_input_manifest as inputs, reference_protocol as ref
from .maintenance import Conflict
from .native_manifest_binding import NativeManifestBindings, MAX_RECORDS, MAX_BYTES
from .native_plan_binding import NativePlanBindings, catalog_sha, plan_sha, validate
from .viewer_tasks import RESERVATION_LIMIT

EPOCH_DDL='''CREATE TABLE native_plan_epochs (operation_id TEXT PRIMARY KEY, uuid TEXT NOT NULL UNIQUE,
 authority TEXT NOT NULL, environment TEXT NOT NULL, capability_hash TEXT NOT NULL, generation INTEGER NOT NULL,
 epoch INTEGER NOT NULL, manifest_id TEXT NOT NULL, manifest_sha TEXT NOT NULL, state TEXT NOT NULL,
 manifest_receipt BLOB, plan_bytes BLOB, plan_sha TEXT, plan_receipt BLOB, close_receipt BLOB)'''
REVOKE_DDL='''CREATE TABLE native_plan_epoch_revokes (request_id TEXT PRIMARY KEY, operation_id TEXT NOT NULL UNIQUE,
 request_bytes BLOB NOT NULL, receipt BLOB NOT NULL)'''


class NativePlanEpochs:
    @staticmethod
    def _common(row):
        return {'operationId':row['operation_id'],'operationKind':'plan_bind','taskUuid':row['uuid'],
                'environment':row['environment'],'generation':1,'epoch':1,
                'manifestId':row['manifest_id'],'manifestSha256':row['manifest_sha']}

    def __init__(self,service,manifest,plan):
        from . import config
        if config.NATIVE_PLAN_EPOCH_QUALIFIED is not True or getattr(service.config,'viewer_plan_epoch',False) is not True:
            raise Conflict('native plan epoch not qualified')
        if (not isinstance(manifest,NativeManifestBindings) or not isinstance(plan,NativePlanBindings)
                or manifest.store is not service.store or plan.service is not service or plan.manifest is not manifest):
            raise Conflict('native plan epoch authority unavailable')
        self.service=service;self.store=service.store;self.queue=service.queue;self.manifest=manifest;self.plan=plan
        self.manifest._authority()
        with self.store._lock,self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            tables={name:db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",(name,)).fetchone()
                    for name in ('native_plan_epochs','native_plan_epoch_revokes')}
            if all(row is None for row in tables.values()):
                db.execute(EPOCH_DDL);db.execute(REVOKE_DDL)
            elif (tables['native_plan_epochs'] is None or tables['native_plan_epochs'][0]!=EPOCH_DDL
                  or tables['native_plan_epoch_revokes'] is None or tables['native_plan_epoch_revokes'][0]!=REVOKE_DDL):
                raise Conflict('native plan epoch schema changed')

    def _identity(self,db,operation,identifier,environment,capability,*,closed=False):
        self.manifest._authority();ref._uuid(operation,'operationId');ref._uuid(identifier,'taskUuid');ref._environment(environment)
        row=db.execute('SELECT * FROM native_plan_epochs WHERE operation_id=?',(operation,)).fetchone()
        reservation=self.store._reservation(db,identifier,environment,capability,allow_cancelled=closed)
        if row is None or reservation is None:raise KeyError('unknown epoch')
        if (row['uuid']!=identifier or row['environment']!=environment
                or row['authority']!=self.queue.authority_id or row['capability_hash']!=reservation['capability_hash']
                or row['generation']!=1 or row['epoch']!=1 or row['state'] not in ('open','closed_without_bind','bound_then_closed')
                or (row['state']=='open') != (reservation['state']=='reserved')):
            raise Conflict('native plan epoch identity changed')
        if (db.execute('SELECT 1 FROM viewer_tasks WHERE uuid=?',(identifier,)).fetchone()
                or db.execute('SELECT 1 FROM viewer_incarnations WHERE uuid=?',(identifier,)).fetchone()):
            raise Conflict('native plan epoch task history collision')
        try:ref._slug(row['manifest_id'],'manifestId');ref._hash(row['manifest_sha'],'manifestSha256')
        except (ValueError,TypeError):raise Conflict('native plan epoch identity changed') from None
        revoke=db.execute('SELECT 1 FROM native_plan_epoch_revokes WHERE operation_id=?',(operation,)).fetchone()
        if db.execute('SELECT 1 FROM native_plan_epoch_revokes WHERE request_id=?',(operation,)).fetchone():
            raise Conflict('native plan epoch identifier collision')
        if ((row['state']=='open' and (row['close_receipt'] is not None or revoke is not None))
                or (row['state']!='open' and (row['close_receipt'] is None or revoke is None))):
            raise Conflict('native plan epoch history changed')
        if row['state']!='open' and not closed:raise Conflict('native plan epoch closed')
        return row,reservation

    def authorize(self,operation,identifier,environment,capability,*,allow_closed=False):
        """Pre-body identity check; each mutation rechecks inside its transaction."""
        if type(allow_closed) is not bool:raise ValueError('invalid epoch authorization policy')
        with self.store._lock,self.queue._db() as db:
            db.execute('BEGIN');row,reservation=self._identity(db,operation,identifier,environment,capability,closed=allow_closed)
            self._history(db,row,reservation)

    def _history(self,db,row,reservation):
        """Recheck linked actual manifest/plan rows before reporting history."""
        identifier=row['uuid'];manifest_record=db.execute('SELECT * FROM native_manifest_bindings WHERE uuid=?',(identifier,)).fetchone()
        if row['manifest_receipt'] is None:
            if (manifest_record is not None or db.execute('SELECT 1 FROM native_plan_bindings WHERE uuid=?',(identifier,)).fetchone()
                    or row['plan_bytes'] is not None or row['plan_sha'] is not None or row['plan_receipt'] is not None):
                raise Conflict('native plan epoch history changed')
            return None,None,None
        if manifest_record is None:raise Conflict('native plan epoch history changed')
        try:
            checked=inputs.validate(bytes(manifest_record['manifest_bytes']),expected_manifest_id=row['manifest_id'],max_input_bytes=manifest_record['accepted_limit'])
            manifest_receipt=self.manifest._receipt(identifier,row['environment'],reservation,checked,manifest_record['accepted_limit'])
            if (checked['sha256']!=row['manifest_sha'] or manifest_record['manifest_sha']!=row['manifest_sha']
                    or manifest_record['manifest_id']!=row['manifest_id']
                    or manifest_record['authority']!=row['authority'] or manifest_record['environment']!=row['environment']
                    or manifest_record['capability_hash']!=row['capability_hash'] or manifest_record['generation']!=1
                    or manifest_record['revision']!=1 or manifest_record['binding_id']!=manifest_receipt['bindingId']
                    or bytes(row['manifest_receipt'])!=ref.canonical_json(manifest_receipt)):
                raise Conflict('native plan epoch history changed')
            plan_record=db.execute('SELECT * FROM native_plan_bindings WHERE uuid=?',(identifier,)).fetchone()
            if row['plan_bytes'] is None:
                if row['plan_sha'] is not None or row['plan_receipt'] is not None or plan_record is not None:raise Conflict('native plan epoch history changed')
                return manifest_receipt,None,None
            body=bytes(row['plan_bytes']);plan=validate(body);digest=plan_sha(body)
            if (row['plan_sha']!=digest or plan['taskUuid']!=identifier or plan['environment']!=row['environment']
                    or plan['bindingId']!=manifest_receipt['bindingId'] or plan['manifestSha256']!=row['manifest_sha']
                    or plan['expectedCatalogDigest']!=catalog_sha(b'[]') or plan['options']):raise Conflict('native plan epoch history changed')
            if row['plan_receipt'] is None:
                if plan_record is not None:raise Conflict('native plan epoch history changed')
                return manifest_receipt,digest,None
            receipt=self.plan._receipt(plan,digest,catalog_sha(b'[]'))
            if (plan_record is None or plan_record['authority']!=row['authority'] or plan_record['environment']!=row['environment']
                    or plan_record['capability_hash']!=row['capability_hash'] or plan_record['manifest_binding_id']!=manifest_receipt['bindingId']
                    or bytes(plan_record['plan_bytes'])!=body or plan_record['plan_sha']!=digest
                    or bytes(plan_record['catalog_bytes'])!=b'[]' or plan_record['catalog_sha']!=catalog_sha(b'[]')
                    or bytes(row['plan_receipt'])!=ref.canonical_json(receipt)):
                raise Conflict('native plan epoch history changed')
            return manifest_receipt,digest,receipt
        except (ValueError,TypeError,KeyError,OverflowError):raise Conflict('native plan epoch history changed') from None

    def open(self,operation,identifier,environment,capability,manifest_id,manifest_sha):
        self.manifest._authority();ref._uuid(operation,'operationId');ref._uuid(identifier,'taskUuid');ref._environment(environment)
        ref._slug(manifest_id,'manifestId');ref._hash(manifest_sha,'manifestSha256')
        cap_hash=self.store._capability_hash(identifier,environment,capability)
        with self.store._lock,self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            prior=db.execute('SELECT * FROM native_plan_epochs WHERE operation_id=?',(operation,)).fetchone()
            if prior is not None:
                row,reservation=self._identity(db,operation,identifier,environment,capability)
                if row['manifest_id']!=manifest_id or row['manifest_sha']!=manifest_sha:raise Conflict('native plan epoch changed')
                self._history(db,row,reservation)
                return {**self._common(row),'state':'open'}
            if (db.execute('SELECT 1 FROM native_plan_epoch_revokes WHERE request_id=?',(operation,)).fetchone()
                    or db.execute('SELECT 1 FROM native_plan_epochs WHERE uuid=?',(identifier,)).fetchone()
                    or db.execute('SELECT 1 FROM viewer_reservations WHERE uuid=?',(identifier,)).fetchone()
                    or db.execute('SELECT 1 FROM viewer_tasks WHERE uuid=?',(identifier,)).fetchone()
                    or db.execute('SELECT 1 FROM viewer_incarnations WHERE uuid=?',(identifier,)).fetchone()
                    or db.execute('SELECT 1 FROM native_manifest_bindings WHERE uuid=?',(identifier,)).fetchone()
                    or db.execute('SELECT 1 FROM native_plan_bindings WHERE uuid=?',(identifier,)).fetchone()):
                raise Conflict('historical identity requires inspection')
            with os.scandir(self.store.root) as entries:
                for index,entry in enumerate(entries):
                    if index>=10000 or entry.name.startswith(identifier+'-'):raise Conflict('historical files require inspection')
            if (db.execute('SELECT COUNT(*) FROM native_plan_epochs').fetchone()[0]>=MAX_RECORDS
                    or db.execute('SELECT COUNT(*) FROM viewer_reservations').fetchone()[0]>=RESERVATION_LIMIT):raise Conflict('epoch history limit reached')
            db.execute("INSERT INTO viewer_reservations VALUES(?,?,?,'reserved')",(identifier,environment,cap_hash))
            db.execute("INSERT INTO native_plan_epochs VALUES(?,?,?,?,?,1,1,?,?,'open',NULL,NULL,NULL,NULL,NULL)",
                       (operation,identifier,self.queue.authority_id,environment,cap_hash,manifest_id,manifest_sha))
            row=db.execute('SELECT * FROM native_plan_epochs WHERE operation_id=?',(operation,)).fetchone()
            return {**self._common(row),'state':'open'}

    def bind_manifest(self,operation,identifier,environment,capability,body):
        with self.store._lock,self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');row,reservation=self._identity(db,operation,identifier,environment,capability)
            existing,_,_=self._history(db,row,reservation)
            saved=db.execute('SELECT accepted_limit,manifest_bytes FROM native_manifest_bindings WHERE uuid=?',(identifier,)).fetchone()
            limit=saved['accepted_limit'] if saved is not None else min(self.store.max_input_bytes,inputs.MAX_INPUT_BYTES)
            try:checked=inputs.validate(body,expected_manifest_id=row['manifest_id'],max_input_bytes=limit)
            except (ValueError,TypeError,KeyError,OverflowError):raise Conflict('manifest changed') from None
            if checked['sha256']!=row['manifest_sha']:raise Conflict('manifest changed')
            if existing is not None:
                if bytes(saved['manifest_bytes'])!=body:raise Conflict('manifest changed')
                return existing
            count,used=db.execute('SELECT COUNT(*),COALESCE(SUM(length(manifest_bytes)),0) FROM native_manifest_bindings').fetchone()
            if count>=MAX_RECORDS or used+len(body)>MAX_BYTES:raise Conflict('manifest history limit reached')
            if db.execute('SELECT 1 FROM native_manifest_bindings WHERE environment=? AND manifest_id=?',(environment,row['manifest_id'])).fetchone():raise Conflict('manifest identity already bound')
            receipt=self.manifest._receipt(identifier,environment,reservation,checked,limit)
            db.execute('INSERT INTO native_manifest_bindings VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                       (identifier,environment,self.queue.authority_id,1,reservation['capability_hash'],row['manifest_id'],1,row['manifest_sha'],limit,receipt['bindingId'],body))
            db.execute('UPDATE native_plan_epochs SET manifest_receipt=? WHERE operation_id=?',(ref.canonical_json(receipt),operation))
            return receipt

    def plan_intent(self,operation,identifier,environment,capability,body):
        plan=validate(body)
        with self.store._lock,self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');row,reservation=self._identity(db,operation,identifier,environment,capability)
            manifest,prior,_=self._history(db,row,reservation)
            if (manifest is None or plan['taskUuid']!=identifier or plan['environment']!=environment or plan['options']
                    or plan['bindingId']!=manifest['bindingId'] or plan['manifestSha256']!=row['manifest_sha']
                    or plan['expectedCatalogDigest']!=catalog_sha(b'[]')):raise Conflict('invalid plan intent')
            digest=plan_sha(body)
            if prior is not None:
                if prior!=digest or bytes(row['plan_bytes'])!=body:raise Conflict('plan intent changed')
            else:db.execute('UPDATE native_plan_epochs SET plan_bytes=?,plan_sha=? WHERE operation_id=?',(body,digest,operation))
            return {**self._common(row),'planSha256':digest,'state':'intent'}

    def accept(self,operation,identifier,environment,capability,body):
        plan=validate(body)
        with self.store._lock,self.queue._db() as db:
            db.execute('BEGIN');row,reservation=self._identity(db,operation,identifier,environment,capability)
            manifest,intent,receipt=self._history(db,row,reservation)
            if intent is None or bytes(row['plan_bytes'])!=body:raise Conflict('plan intent changed')
            if receipt is not None:return receipt
        if self.plan.catalog_digest()!=plan['expectedCatalogDigest']:raise Conflict('catalog changed')
        with self.store._lock,self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');row,reservation=self._identity(db,operation,identifier,environment,capability)
            manifest,intent,receipt=self._history(db,row,reservation)
            if intent is None or bytes(row['plan_bytes'])!=body:raise Conflict('plan intent changed')
            if receipt is not None:return receipt
            count,used=db.execute('SELECT COUNT(*),COALESCE(SUM(length(plan_bytes)+length(catalog_bytes)),0) FROM native_plan_bindings').fetchone()
            if count>=MAX_RECORDS or used+len(body)+2>MAX_BYTES:raise Conflict('plan history limit reached')
            receipt=self.plan._receipt(plan,intent,catalog_sha(b'[]'))
            db.execute('INSERT INTO native_plan_bindings VALUES(?,?,?,?,?,?,?,?,?)',
                       (identifier,self.queue.authority_id,environment,reservation['capability_hash'],manifest['bindingId'],body,intent,b'[]',catalog_sha(b'[]')))
            db.execute('UPDATE native_plan_epochs SET plan_receipt=? WHERE operation_id=?',(ref.canonical_json(receipt),operation))
            return receipt

    def revoke(self,operation,identifier,environment,capability,request_id,expected_manifest_sha,expected_manifest_binding,expected_plan_sha):
        ref._uuid(request_id,'requestId')
        ref._hash(expected_manifest_sha,'expected manifest hash')
        if expected_manifest_binding is not None:ref._hash(expected_manifest_binding,'expected manifest binding')
        if expected_plan_sha is not None:ref._hash(expected_plan_sha,'expected plan hash')
        request=ref.canonical_json({'operationId':operation,'requestId':request_id,'taskUuid':identifier,'environment':environment,
                                    'operationKind':'plan_bind','generation':1,'epoch':1,'expectedManifestSha256':expected_manifest_sha,
                                    'expectedManifestBindingId':expected_manifest_binding,'expectedPlanSha256':expected_plan_sha})
        with self.store._lock,self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');row,reservation=self._identity(db,operation,identifier,environment,capability,closed=True)
            manifest,intent,plan=self._history(db,row,reservation)
            if row['manifest_sha']!=expected_manifest_sha:raise Conflict('manifest identity changed')
            prior=db.execute('SELECT * FROM native_plan_epoch_revokes WHERE request_id=?',(request_id,)).fetchone()
            if prior is not None:
                expected_state='bound_then_closed' if plan is not None else 'closed_without_bind'
                receipt={**self._common(row),'requestId':request_id,'state':expected_state,'manifestReceipt':manifest,
                         'planIntentPresent':intent is not None,'planIntentSha256':intent,'planReceipt':plan}
                encoded=ref.canonical_json(receipt)
                if (prior['operation_id']!=operation or bytes(prior['request_bytes'])!=request or row['state']!=expected_state
                        or row['close_receipt'] is None or bytes(prior['receipt'])!=encoded or bytes(row['close_receipt'])!=encoded):
                    raise Conflict('revoke request changed')
                return receipt
            if (row['state']!='open' or db.execute('SELECT 1 FROM native_plan_epochs WHERE operation_id=?',(request_id,)).fetchone()
                    or (manifest['bindingId'] if manifest else None)!=expected_manifest_binding or intent!=expected_plan_sha):
                raise Conflict('stale revoke expectation')
            state='bound_then_closed' if plan is not None else 'closed_without_bind'
            receipt={**self._common(row),'requestId':request_id,'state':state,'manifestReceipt':manifest,
                     'planIntentPresent':intent is not None,'planIntentSha256':intent,'planReceipt':plan}
            encoded=ref.canonical_json(receipt)
            db.execute('INSERT INTO native_plan_epoch_revokes VALUES(?,?,?,?)',(request_id,operation,request,encoded))
            db.execute('UPDATE native_plan_epochs SET state=?,close_receipt=? WHERE operation_id=?',(state,encoded,operation))
            db.execute("UPDATE viewer_reservations SET state='cancelled' WHERE uuid=?",(identifier,))
            return receipt

    def status(self,operation,identifier,environment,capability):
        with self.store._lock,self.queue._db() as db:
            db.execute('BEGIN');row,reservation=self._identity(db,operation,identifier,environment,capability,closed=True)
            manifest,intent,plan=self._history(db,row,reservation)
            if row['state']=='open':
                if row['close_receipt'] is not None:raise Conflict('native plan epoch history changed')
                return {**self._common(row),'state':'open_bound' if plan else 'open_unbound',
                        'manifestReceipt':manifest,'planIntentPresent':intent is not None,'planIntentSha256':intent,'planReceipt':plan}
            if row['close_receipt'] is None:raise Conflict('native plan epoch history changed')
            try:
                encoded=bytes(row['close_receipt']);receipt=ref.decode_submission(encoded)
                if ref.canonical_json(receipt)!=encoded:raise Conflict('native plan epoch history changed')
                ref._closed(receipt,('operationId','operationKind','requestId','taskUuid','environment','generation','epoch','manifestId','manifestSha256','state','manifestReceipt','planIntentPresent','planIntentSha256','planReceipt'),'epoch receipt')
                ref._uuid(receipt['requestId'],'requestId')
            except (ValueError,TypeError,KeyError,OverflowError):raise Conflict('native plan epoch history changed') from None
            if (receipt.get('state')!=row['state'] or receipt.get('manifestReceipt')!=manifest
                    or receipt.get('planIntentSha256')!=intent or receipt.get('planReceipt')!=plan
                    or receipt.get('planIntentPresent') is not (intent is not None)
                    or receipt.get('operationId')!=operation or receipt.get('taskUuid')!=identifier
                    or receipt.get('operationKind')!='plan_bind' or receipt.get('manifestId')!=row['manifest_id'] or receipt.get('manifestSha256')!=row['manifest_sha']
                    or receipt.get('environment')!=environment or type(receipt.get('generation')) is not int or receipt.get('generation')!=1
                    or type(receipt.get('epoch')) is not int or receipt.get('epoch')!=1
                    or (row['state']=='bound_then_closed')!=(plan is not None)):
                raise Conflict('native plan epoch history changed')
            prior=db.execute('SELECT * FROM native_plan_epoch_revokes WHERE operation_id=?',(operation,)).fetchone()
            expected=ref.canonical_json({'operationId':operation,'requestId':receipt['requestId'],'taskUuid':identifier,'environment':environment,
                                         'operationKind':'plan_bind','generation':1,'epoch':1,'expectedManifestSha256':row['manifest_sha'],
                                         'expectedManifestBindingId':manifest['bindingId'] if manifest else None,'expectedPlanSha256':intent})
            if (prior is None or prior['request_id']!=receipt['requestId'] or bytes(prior['request_bytes'])!=expected
                    or bytes(prior['receipt'])!=bytes(row['close_receipt'])):raise Conflict('native plan epoch history changed')
            return receipt
