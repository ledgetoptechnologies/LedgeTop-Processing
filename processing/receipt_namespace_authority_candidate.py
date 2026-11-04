"""Candidate shared receipt namespace gate; not a process or host writer fence."""
import json
import os
import sqlite3
from pathlib import Path
from . import reference_protocol as p
from .reference_copy_hold_integrity import held_copy_ids
from .viewer_import_confirmation_store import ImportConfirmationStore
from .viewer_odm_import_confirmation_store_v2 import OdmImportConfirmationStoreV2

MAX_ROWS=10000
MAX_BYTES=64*1024*1024


class ReceiptNamespaceHeld(ValueError):
    pass


def _id(value):
    try:p._slug(value,'receipt identity')
    except (TypeError,ValueError):raise ReceiptNamespaceHeld('receipt_identity_invalid') from None
    return value


def _legacy_receipt(raw, table_id):
    def unique_pairs(pairs):
        out={}
        for key,value in pairs:
            if key in out: raise ValueError('duplicate JSON key')
            out[key]=value
        return out
    def reject_constant(_): raise ValueError('nonfinite JSON')
    obj=json.loads(raw,object_pairs_hook=unique_pairs,parse_constant=reject_constant)
    p._closed(obj, ('schemaVersion','receiptId','storageReceiptId','jobId','generation','environment',
        'inputManifestSha256','pipelineDigest','resultManifestSha256','acceptedObjectIds','acceptedAt','validationVersion'), 'legacy acceptance')
    p._slug(table_id,'legacy row receipt ID')
    p._environment(obj['environment'])
    if table_id!=obj['receiptId'] or obj['receiptId']==obj['storageReceiptId']:
        raise ValueError('ambiguous legacy receipt identity')
    ids=obj['acceptedObjectIds']
    if type(ids) is not list: raise ValueError('invalid accepted objects')
    expected={key:obj[key] for key in ('jobId','generation','environment','pipelineDigest','resultManifestSha256')}
    expected['objects']=[{'id':value} for value in ids]
    p.validate_acceptance(obj,result_manifest=expected,input_manifest_sha256=obj['inputManifestSha256'])
    return obj


class ReceiptNamespaceGate:
    """Bind actual journal instances to one queue DB; no external fencing claimed."""
    def __init__(self, queue, v1, v2):
        if type(v1) is not ImportConfirmationStore or type(v2) is not OdmImportConfirmationStoreV2:
            raise ReceiptNamespaceHeld('exact_journal_instances_required')
        self.queue,self.v1,self.v2=queue,v1,v2

    def _binding(self,db):
        if os.getpid()!=self.queue._creator_pid or self.queue._lock.closed:
            raise ReceiptNamespaceHeld('queue_authority_unavailable')
        try:
            q=Path(self.queue.path).absolute()
            if q.is_symlink() or q.resolve()!=q:raise ValueError()
            stat=q.stat();identity=(stat.st_dev,stat.st_ino)
            for store in (self.v1,self.v2):
                path=Path(store.path).absolute()
                if path!=q or path.is_symlink() or path.resolve()!=path or store._check_file()!=identity or store.file_identity!=identity:
                    raise ValueError()
            attached=db.execute('PRAGMA database_list').fetchall()
            main=[row[2] for row in attached if row[1]=='main']
            if len(attached)!=1 or len(main)!=1 or Path(main[0]).absolute()!=q:raise ValueError()
            rows=db.execute("SELECT CASE WHEN typeof(singleton)='integer' THEN singleton END, CASE WHEN typeof(authority_id)='text' AND instr(authority_id,char(0))=0 AND length(authority_id)<=36 THEN authority_id END FROM coordinator_identity LIMIT 2").fetchall()
            if len(rows)!=1 or rows[0][0]!=1 or rows[0][1]!=self.queue.authority_id:raise ValueError()
        except (OSError,ValueError,TypeError,sqlite3.Error):
            raise ReceiptNamespaceHeld('shared_receipt_database_required') from None

    def check_in_transaction(self,db,*,proposed=()):
        if not db.in_transaction:raise ReceiptNamespaceHeld('receipt_snapshot_required')
        self._binding(db)
        seen={};count=0;total=0
        def add(value,scope):
            nonlocal count
            _id(value);count+=1
            if value in seen:raise ReceiptNamespaceHeld('receipt_namespace_collision')
            seen[value]=scope
        def rows(table,columns,required=False):
            if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(table,)).fetchone():
                if required:raise ReceiptNamespaceHeld('receipt_namespace_missing')
                return []
            def generate():
                for index,row in enumerate(db.execute('SELECT '+columns+' FROM '+table+' LIMIT ?',(MAX_ROWS+1,))):
                    if index>=MAX_ROWS:raise ReceiptNamespaceHeld('receipt_census_limit')
                    yield row
            return generate()
        def safe_id(column):
            return "CASE WHEN typeof("+column+")='text' AND instr("+column+",char(0))=0 AND length("+column+") BETWEEN 1 AND 128 THEN "+column+" END"
        def payload_budget(table,column):
            nonlocal total
            row=db.execute("SELECT COUNT(*),COALESCE(SUM(CASE WHEN typeof("+column+")='text' AND instr("+column+",char(0))=0 THEN length("+column+") ELSE 0 END),0),COALESCE(SUM(CASE WHEN typeof("+column+")!='text' OR instr("+column+",char(0))!=0 THEN 1 ELSE 0 END),0) FROM "+table).fetchone()
            # Four bytes per character is a conservative UTF-8 upper bound;
            # avoid allocating a BLOB for every potentially hostile value.
            if row[0]>MAX_ROWS or row[2] or row[1]*4>MAX_BYTES-total:raise ReceiptNamespaceHeld('receipt_census_limit')
            total+=row[1]*4
        try:
            payload_budget('reference_acceptances','receipt_json')
            payload_budget('reference_viewer_v3_copy_hold','payload_json')
            from .viewer_import_confirmation_store import _preflight_stored_json
            for row in rows('reference_acceptances',safe_id('receipt_id')+',receipt_json',True):
                raw=row[1]
                if type(raw) is not str or len(raw.encode('utf-8'))>p.MAX_BODY_BYTES:raise ReceiptNamespaceHeld('receipt_census_limit')
                _preflight_stored_json(raw.encode('utf-8'))
                value=_legacy_receipt(raw,row[0])
                add(row[0],'acceptance');add(value['storageReceiptId'],'acceptance-storage')
            for row in rows('reference_viewer_receipts',safe_id('receipt_id')+','+safe_id('storage_receipt_id'),True):
                add(row[0],'viewer-copy');add(row[1],'viewer-storage')
            for row in rows('reference_terminal_acceptances',safe_id('receipt_id')+','+safe_id('storage_receipt_id'),True):
                add(row[0],'terminal');add(row[1],'terminal-storage')
            for table in ('viewer_import_confirmations_candidate_v1','viewer_odm_import_confirmations_candidate_v2'):
                for row in rows(table,safe_id('confirmation_id'),True):add(row[0],table)
            # Validate all retained proof checksums and provenance before using IDs.
            held_copy_ids(db)
            from .fixture_v2_copy_census_candidate import census as fixture_census
            fixture_ids,fixture_bytes=fixture_census(db)
            if fixture_bytes>MAX_BYTES-total:raise ReceiptNamespaceHeld('receipt_census_limit')
            total+=fixture_bytes
            for value in fixture_ids:add(value,'v2-fixture-held')
            for row in rows('reference_viewer_v3_copy_hold','payload_json',True):
                raw=row[0]
                proof=json.loads(raw)
                for part in ('acceptance','viewerReceipt'):
                    for key in ('receiptId','storageReceiptId'):add(proof[part][key],'v3-held')
        except ReceiptNamespaceHeld:raise
        except (sqlite3.Error,TypeError,ValueError,KeyError,RecursionError):
            raise ReceiptNamespaceHeld('receipt_census_unavailable') from None
        proposed=list(proposed)
        if len(proposed)>4:raise ReceiptNamespaceHeld('receipt_census_limit')
        for value in proposed:_id(value)
        if len(proposed)!=len(set(proposed)) or any(value in seen for value in proposed):
            raise ReceiptNamespaceHeld('receipt_namespace_collision')
        return {'state':'local_scan_clear_history_unverified','identities':count,'censusBytes':total}

    def census(self):
        # No DDL or receipt data writes; queue connection retains its own lease.
        try:
            with self.queue._db() as db:
                db.execute('BEGIN')
                return self.check_in_transaction(db)
        except ReceiptNamespaceHeld:raise
        except Exception:raise ReceiptNamespaceHeld('receipt_census_unavailable') from None
