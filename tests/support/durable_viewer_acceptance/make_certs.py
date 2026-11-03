import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID
root = Path(os.environ['LT_TEST_DIR']).resolve()
now = datetime.now(timezone.utc)
ca_key = Ed25519PrivateKey.generate()
ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Synthetic E2E CA")])
ca_cert = (x509.CertificateBuilder().subject_name(ca_name).issuer_name(ca_name)
    .public_key(ca_key.public_key()).serial_number(x509.random_serial_number())
    .not_valid_before(now - timedelta(days=1)).not_valid_after(now + timedelta(days=1))
    .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
    .add_extension(x509.KeyUsage(digital_signature=True, content_commitment=False, key_encipherment=False,
        data_encipherment=False, key_agreement=False, key_cert_sign=True, crl_sign=True,
        encipher_only=False, decipher_only=False), critical=True)
    .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), critical=False)
    .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), critical=False)
    .sign(ca_key, algorithm=None))
server_key = Ed25519PrivateKey.generate()
server_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
server_cert = (x509.CertificateBuilder().subject_name(server_name).issuer_name(ca_name)
    .public_key(server_key.public_key()).serial_number(x509.random_serial_number())
    .not_valid_before(now - timedelta(days=1)).not_valid_after(now + timedelta(days=1))
    .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False)
    .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
    .add_extension(x509.KeyUsage(digital_signature=True, content_commitment=False, key_encipherment=False,
        data_encipherment=False, key_agreement=False, key_cert_sign=False, crl_sign=False,
        encipher_only=False, decipher_only=False), critical=True)
    .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
    .add_extension(x509.SubjectKeyIdentifier.from_public_key(server_key.public_key()), critical=False)
    .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), critical=False)
    .sign(ca_key, algorithm=None))
(root / "ca.pem").write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))
(root / "server.crt").write_bytes(server_cert.public_bytes(serialization.Encoding.PEM))
(root / "server.key").write_bytes(server_key.private_bytes(serialization.Encoding.PEM,
    serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))

