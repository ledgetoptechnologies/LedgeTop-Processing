"""Synthetic vector generator only. The fixed seed is public and unsafe for use."""
import base64
import hashlib
import json
from pathlib import Path
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

DOMAIN = b'LTDS-VIEWER-ACCEPTED-COPY-ACK-V2\0'
key = Ed25519PrivateKey.from_private_bytes(bytes(range(32)))
acceptance = {
    'schemaVersion':1, 'receiptId':'33333333-3333-4333-8333-333333333333',
    'storageReceiptId':'44444444-4444-4444-8444-444444444444',
    'jobId':'job-test', 'generation':1, 'environment':'staging',
    'inputManifestSha256':'b'*64, 'pipelineDigest':'c'*64,
    'resultManifestSha256':'d'*64, 'acceptedObjectIds':['object-b','object-a'],
    'acceptedAt':'2026-11-04T00:00:00.000Z','validationVersion':'ltds-reference-v1',
}
acceptance_wire=json.dumps(acceptance,sort_keys=True,separators=(',', ':'),ensure_ascii=False).encode()
body = {
    'protocol': 'ltds-viewer-accepted-copy-v2', 'schemaVersion': 2,
    'issuerId': 'viewer-test-issuer', 'keyId': 'viewer-test-key',
    'receiptId': '11111111-1111-4111-8111-111111111111',
    'storageReceiptId': '22222222-2222-4222-8222-222222222222',
    'acceptanceReceiptId': '33333333-3333-4333-8333-333333333333',
    'acceptanceStorageReceiptId': '44444444-4444-4444-8444-444444444444',
    'acceptancePayloadSha256':hashlib.sha256(b'LTDS-OUTER-ACCEPTANCE-V1\0'+acceptance_wire).hexdigest(),
    'coordinatorAuthorityId': 'coord-test', 'viewerPrincipalId': 'viewer-test',
    'submissionId': '55555555-5555-4555-8555-555555555555',
    'activationId': '66666666-6666-4666-8666-666666666666',
    'viewerTaskId': '77777777-7777-4777-8777-777777777777',
    'viewerAttemptId': '88888888-8888-4888-8888-888888888888',
    'preparedEnvelopeSha256': 'a'*64, 'providerTaskId': 'job-test', 'jobId': 'job-test', 'generation': 1,
    'environment': 'staging', 'inputManifestSha256': 'b'*64,
    'pipelineDigest': 'c'*64, 'resultManifestSha256': 'd'*64,
    'sourceManifestSha256': 'd'*64, 'derivedManifestSha256': 'e'*64,
    'sourceReceiptSha256': 'f'*64, 'derivedReceiptSha256': '0'*64,
    'acceptedObjectIds': ['object-b','object-a'],
    'objects': [
        {'id':'object-b','byteSize':0,'sha256':hashlib.sha256(b'').hexdigest()},
        {'id':'object-a','byteSize':3,'sha256':hashlib.sha256(b'abc').hexdigest()},
    ], 'acceptedAt': '2026-11-04T00:00:00.000Z',
}
canonical = json.dumps(body, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()
signature = base64.urlsafe_b64encode(key.sign(DOMAIN + canonical)).decode().rstrip('=')
public = key.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo).decode()
vector = {'status':'synthetic_proposal_not_for_production', 'publicKeyPem':public,
          'bodyCanonicalSha256':hashlib.sha256(canonical).hexdigest(),
          'acceptance':acceptance,
          'receipt':{**body,'signature':signature},
          'negativeCases':[
              {'case':'authority-swap','field':'coordinatorAuthorityId','value':'coord-other','expected':'reject_scope'},
              {'case':'principal-swap','field':'viewerPrincipalId','value':'viewer-other','expected':'reject_scope'},
              {'case':'attempt-swap','field':'viewerAttemptId','value':'99999999-9999-4999-8999-999999999999','expected':'reject_scope'},
              {'case':'envelope-swap','field':'preparedEnvelopeSha256','value':'e'*64,'expected':'reject_scope'},
              {'case':'provider-task-swap','field':'providerTaskId','value':'other-job','expected':'reject_scope'},
              {'case':'source-manifest-swap','field':'sourceManifestSha256','value':'1'*64,'expected':'reject_scope'},
              {'case':'derived-manifest-swap','field':'derivedManifestSha256','value':'1'*64,'expected':'reject_scope'},
              {'case':'source-receipt-swap','field':'sourceReceiptSha256','value':'1'*64,'expected':'reject_scope'},
              {'case':'derived-receipt-swap','field':'derivedReceiptSha256','value':'1'*64,'expected':'reject_scope'},
              {'case':'outer-acceptance-swap','field':'acceptanceReceiptId','value':'99999999-9999-4999-8999-999999999999','expected':'reject_scope'},
              {'case':'outer-acceptance-digest-swap','field':'acceptancePayloadSha256','value':'f'*64,'expected':'reject_scope'},
              {'case':'missing-object','field':'acceptedObjectIds','value':['object-a'],'expected':'reject_closure'},
              {'case':'reordered-objects','field':'acceptedObjectIds','value':['object-a','object-b'],'expected':'reject_closure'},
              {'case':'boolean-generation','field':'generation','value':True,'expected':'reject_type'},
              {'case':'wrong-protocol','field':'protocol','value':'ltds-viewer-accepted-copy-v1','expected':'reject_version'},
          ]}
Path(__file__).with_name('accepted_copy_v2_proposal.json').write_text(json.dumps(vector,indent=2)+'\n',encoding='utf8')
