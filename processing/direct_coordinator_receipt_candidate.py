"""Unwired candidate coordinator receipt contract; no custody or release authority.

The caller must separately authenticate the client and verify actual retained
source, worker closure, authority, destination and policy. This module alone
must not be mounted as an API or used to acknowledge a Viewer.
"""
import base64
import copy
import hmac
import rfc8785
import re
from datetime import datetime, timezone
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from . import reference_protocol as p

PROTOCOL='ltds.viewer.source-acceptance/v1'
DOMAIN=b'ltds.viewer.source-acceptance/v1\0'
GUARANTEE='until_explicit_source_release_v1'
MAX_OBJECTS=20000
MAX_BODY_BYTES=8*1024*1024
FIELDS=frozenset(('receiptId','operationId','issuedAt','coordinatorAuthorityId','viewerPrincipalId','viewerDestinationId',
    'submissionId','viewerTaskId','viewerAttemptId','jobId','generation','environment',
    'intakeProtocol','mode','modeRevision','submissionIntentSha256','sourceSnapshotSha256',
    'originalPathInventorySha256','sourceManifestSha256','derivedManifestSha256',
    'engineMappingSha256','workerClosureReceiptSha256','sourceObjects','derivedObjects','retentionGuarantee'))
OBJECT_FIELDS=frozenset(('id','byteSize','sha256'))


class CoordinatorReceiptRejected(ValueError):pass


def validate_body(body):
    try:
        if type(body) is not dict or set(body)!=FIELDS:raise ValueError()
        for key in ('receiptId','operationId','coordinatorAuthorityId','submissionId','viewerTaskId','viewerAttemptId','jobId'):
            p._uuid(body[key],key)
        timestamp=body['issuedAt']
        if (type(timestamp) is not str or not re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z',timestamp)
                or datetime.fromisoformat(timestamp.replace('Z','+00:00')).astimezone(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z')!=timestamp):
            raise ValueError()
        for key in ('viewerPrincipalId','viewerDestinationId'):
            p._slug(body[key],key)
        for key in ('submissionIntentSha256','sourceSnapshotSha256','originalPathInventorySha256',
                    'sourceManifestSha256','derivedManifestSha256','engineMappingSha256','workerClosureReceiptSha256'):
            p._hash(body[key],key)
        p._environment(body['environment']);p._integer(body['generation'],'generation');p._integer(body['modeRevision'],'modeRevision')
        if (body['intakeProtocol']!='ltds-direct-intake-v1' or body['mode']!='direct'
                or body['retentionGuarantee']!=GUARANTEE):raise ValueError()
        all_ids=set()
        for field in ('sourceObjects','derivedObjects'):
            objects=body[field]
            if type(objects) is not list or len(objects)>10000 or (field=='sourceObjects' and not objects):raise ValueError()
            previous=None
            for obj in objects:
                if type(obj) is not dict or set(obj)!=OBJECT_FIELDS:raise ValueError()
                p._slug(obj['id'],'object id');p._integer(obj['byteSize'],'object size');p._hash(obj['sha256'],'object hash')
                if obj['id'] in all_ids or (previous is not None and obj['id']<=previous):raise ValueError()
                all_ids.add(obj['id']);previous=obj['id']
        if len(all_ids)>MAX_OBJECTS or len(rfc8785.dumps(body))>MAX_BODY_BYTES:raise ValueError()
        return copy.deepcopy(body)
    except (ValueError,TypeError,KeyError):raise CoordinatorReceiptRejected('invalid coordinator receipt body') from None


def issue(body,*,issuer_id,key_id,private_key):
    """Sign only; does not establish or attest actual custody."""
    clean=validate_body(body);p._slug(issuer_id,'issuer');p._slug(key_id,'key')
    if not isinstance(private_key,Ed25519PrivateKey):raise CoordinatorReceiptRejected('Ed25519 key required')
    envelope=dict(protocol=PROTOCOL,schemaVersion=1,issuerId=issuer_id,keyId=key_id,body=clean)
    signature=private_key.sign(DOMAIN+rfc8785.dumps(envelope))
    return dict(envelope,signature=base64.urlsafe_b64encode(signature).decode('ascii').rstrip('='))


def verify(receipt,*,expected_body,issuer_id,key_id,public_key):
    expected=validate_body(expected_body)
    if (type(receipt) is not dict or set(receipt)!={'protocol','schemaVersion','issuerId','keyId','body','signature'}
            or receipt['protocol']!=PROTOCOL or type(receipt['schemaVersion']) is not int or receipt['schemaVersion']!=1
            or receipt['issuerId']!=issuer_id or receipt['keyId']!=key_id or not isinstance(public_key,Ed25519PublicKey)):
        raise CoordinatorReceiptRejected('coordinator receipt identity mismatch')
    body=validate_body(receipt['body'])
    if not hmac.compare_digest(rfc8785.dumps(body),rfc8785.dumps(expected)):
        raise CoordinatorReceiptRejected('coordinator receipt binding mismatch')
    try:
        value=receipt['signature']
        if type(value) is not str or len(value)!=86:raise ValueError()
        signature=base64.b64decode(value+'==',altchars=b'-_',validate=True)
        if len(signature)!=64 or base64.urlsafe_b64encode(signature).decode().rstrip('=')!=value:raise ValueError()
        public_key.verify(signature,DOMAIN+rfc8785.dumps({k:v for k,v in receipt.items() if k!='signature'}))
    except (TypeError,ValueError,InvalidSignature):raise CoordinatorReceiptRejected('coordinator receipt signature invalid') from None
    return copy.deepcopy(receipt)


def persist_candidate(db,receipt,*,expected_body,issuer_id,key_id,public_key):
    """Synthetic durable replay primitive; no verified-custody assertion.

    The isolated database must already have FULL synchronous mode. This is
    intentionally not integrated with coordinator freeze, quotas or runtime.
    A production adapter must prove actual custody before ever signing.
    """
    checked=verify(receipt,expected_body=expected_body,issuer_id=issuer_id,key_id=key_id,public_key=public_key)
    if db.in_transaction or db.execute('PRAGMA synchronous').fetchone()[0] not in (2,3):
        raise CoordinatorReceiptRejected('exclusive FULL transaction required')
    wire=rfc8785.dumps(checked)
    if len(wire)>MAX_BODY_BYTES+4096:raise CoordinatorReceiptRejected('receipt size exceeds bound')
    body=checked['body'];principal=body['viewerPrincipalId'];operation=body['operationId']
    db.execute('BEGIN IMMEDIATE')
    try:
        db.execute('''CREATE TABLE IF NOT EXISTS direct_source_receipt_candidate (
            principal TEXT NOT NULL,operation TEXT NOT NULL,intent_sha TEXT NOT NULL,receipt BLOB NOT NULL,
            PRIMARY KEY(principal,operation))''')
        row=db.execute('SELECT intent_sha,receipt FROM direct_source_receipt_candidate WHERE principal=? AND operation=?',
                       (principal,operation)).fetchone()
        if row is None:
            db.execute('INSERT INTO direct_source_receipt_candidate VALUES(?,?,?,?)',
                       (principal,operation,body['submissionIntentSha256'],wire))
        elif row[0]!=body['submissionIntentSha256'] or not hmac.compare_digest(bytes(row[1]),wire):
            raise CoordinatorReceiptRejected('persisted receipt conflict')
        db.commit()
    except BaseException:
        db.rollback();raise
    return wire
