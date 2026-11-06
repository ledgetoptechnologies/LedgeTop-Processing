"""Emit public synthetic interoperability vector; never emits private key."""
import hashlib
import json
import rfc8785
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from processing import direct_coordinator_receipt_candidate as c
from tests.test_direct_coordinator_receipt_candidate import TEST_SEED,body

key=Ed25519PrivateKey.from_private_bytes(TEST_SEED)
receipt=c.issue(body(),issuer_id='coordinator-test',key_id='test-key',private_key=key)
unsigned={key:value for key,value in receipt.items() if key!='signature'}
wire=rfc8785.dumps(unsigned)
print(json.dumps(dict(description='Public synthetic RFC 8032 test key; never production',
    publicKeyPem=key.public_key().public_bytes(serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo).decode(),
    domainHex=c.DOMAIN.hex(),unsignedCanonicalUtf8=wire.decode(),unsignedSha256=hashlib.sha256(wire).hexdigest(),
    receipt=receipt),sort_keys=True,indent=2))
