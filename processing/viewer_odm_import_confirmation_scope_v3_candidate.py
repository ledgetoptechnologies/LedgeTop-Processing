"""Held-only scoped wrapper over the independently signed ODM v2 assertion.

PROPOSAL ONLY: this v3 envelope has no matching production signer. No route,
acceptance persistence, result cleanup or authorization is implemented. The
trusted constructor inputs must eventually come from protected installation
configuration and authenticated durable Viewer/Processing job provenance;
this class alone cannot establish that provisioning boundary.
"""
import copy
import hashlib
from cryptography.exceptions import InvalidSignature
from . import reference_protocol as p
from .viewer_acceptance_receipt import ViewerReceiptRejected, _canonical_signature, _load_key
from .viewer_import_confirmation import ImportConfirmationRejected, _hash, _id
from .viewer_odm_import_confirmation_v2 import verify_odm_import_confirmation

PROTOCOL='ltds-viewer-odm-scoped-import-confirmation-v3'
DOMAIN=b'LTDS-VIEWER-ODM-SCOPED-IMPORT-CONFIRMATION-V3\0'
SCOPE_FIELDS=frozenset(('coordinatorAuthorityId','viewerPrincipalId','activationId','preparedEnvelopeSha256'))
FIELDS=frozenset(('protocol','schemaVersion','issuerId','keyId',*SCOPE_FIELDS,'confirmationV2','signature'))


class ScopedImportVerifier:
    def __init__(self, *, provisioned_scope, provisioned_import_keys, provisioned_copy_keys,
                 durable_expected, durable_manifest, durable_accepted_envelopes):
        # Inputs are copied to prevent later mutation; constructors are not a
        # substitute for authenticated provisioning or durable replay storage.
        self.scope=copy.deepcopy(provisioned_scope)
        self.import_keys=copy.deepcopy(provisioned_import_keys)
        self.copy_keys=copy.deepcopy(provisioned_copy_keys)
        self.expected=copy.deepcopy(durable_expected)
        self.manifest=copy.deepcopy(durable_manifest)
        self.envelopes=copy.deepcopy(durable_accepted_envelopes)
        try:
            if type(self.scope) is not dict or set(self.scope)!=SCOPE_FIELDS: raise ValueError()
            p._uuid(self.scope['coordinatorAuthorityId'],'coordinator authority')
            p._uuid(self.scope['activationId'],'activation')
            if not _id(self.scope['viewerPrincipalId']) or not _hash(self.scope['preparedEnvelopeSha256']):raise ValueError()
        except (TypeError, ValueError, KeyError):
            raise ImportConfirmationRejected('invalid_provisioned_scope') from None

    def inspect(self, envelope, *, prior_sha256=None):
        """Verify scope + both signatures; return held evidence, never permission."""
        envelope=copy.deepcopy(envelope)
        if (type(envelope) is not dict or set(envelope)!=FIELDS or envelope['protocol']!=PROTOCOL
                or type(envelope['schemaVersion']) is not int or envelope['schemaVersion']!=3):
            raise ImportConfirmationRejected('invalid_scoped_confirmation')
        if any(type(envelope[field]) is not type(self.scope[field]) or envelope[field]!=self.scope[field] for field in SCOPE_FIELDS):
            raise ImportConfirmationRejected('scoped_confirmation_binding_mismatch')
        inner=envelope['confirmationV2']
        if (type(inner) is not dict or envelope['issuerId']!=inner.get('issuerId')
                or envelope['keyId']!=inner.get('keyId')):
            raise ImportConfirmationRejected('scoped_signer_mismatch')
        # Reuse the actual v2 verifier: full signed archive, result and Viewer
        # accepted-copy closure must succeed before even considering the wrapper.
        verify_odm_import_confirmation(inner,trusted_import_keys=self.import_keys,trusted_copy_keys=self.copy_keys,
            expected=self.expected,result_manifest=self.manifest,accepted_envelopes=self.envelopes)
        try:
            record=self.import_keys[envelope['keyId']]
            if type(record) is not dict or set(record)!={'issuerId','publicKeyPem'} or record['issuerId']!=envelope['issuerId']:
                raise ValueError()
            signature=_canonical_signature(envelope['signature'])
            body={key:value for key,value in envelope.items() if key!='signature'}
            _load_key(record['publicKeyPem']).verify(signature,DOMAIN+p.canonical_json(body))
            digest=hashlib.sha256(p.canonical_json(envelope)).hexdigest()
        except (KeyError,TypeError,ValueError,ViewerReceiptRejected,InvalidSignature):
            raise ImportConfirmationRejected('invalid_scoped_signature') from None
        if prior_sha256 is not None and (not _hash(prior_sha256) or prior_sha256!=digest):
            raise ImportConfirmationRejected('scoped_replay_changed')
        return {'state':'held','reason':'scoped_import_not_authorized','confirmationSha256':digest}
