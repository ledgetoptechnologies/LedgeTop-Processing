"""Create private Processing-only TLS material; never install system trust.

Run as the intended Linux host owner with a new absolute output directory. A
partial/previous directory is retained and refused, never overwritten. All
private keys stay on that host; stdout contains only public certificate facts.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import stat
import subprocess

LEAVES = {
    'coordinator': '192.168.50.89',
    'cluster-worker': '192.168.50.89',
    'main-worker': '192.168.50.88',
}


def provision(directory):
    root = Path(directory)
    if os.name != 'posix' or not root.is_absolute() or root.resolve() != root:
        raise ValueError('new absolute Linux private output directory required')
    if any(part.is_symlink() for part in (root, *root.parents)):
        raise ValueError('private output path must not contain symlinks')
    parent = root.parent.stat()
    if parent.st_uid != os.geteuid() or stat.S_IMODE(parent.st_mode) & 0o077:
        raise ValueError('output parent must be owned and private')
    root.mkdir(mode=0o700)  # Existing or partial material must be reconciled explicitly.
    parent_descriptor = os.open(root.parent,os.O_RDONLY|os.O_DIRECTORY)
    try:
        os.fsync(parent_descriptor)
    finally:
        os.close(parent_descriptor)
    previous = os.umask(0o077)

    def openssl(*args):
        result = subprocess.run(['/usr/bin/openssl', *map(str, args)],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            check=False, timeout=90, env={'PATH':'/usr/bin:/bin', 'HOME':str(root)})
        if result.returncode:
            raise ValueError('private TLS provisioning failed; retained files need review')
        return result.stdout.decode('ascii').strip()

    try:
        ca_key, ca_cert = root/'ca.key', root/'ca.crt'
        openssl('req','-x509','-newkey','rsa:3072','-nodes','-sha256','-days','365',
            '-config','/dev/null','-subj','/CN=LedgeTop Processing private CA',
            '-addext','basicConstraints=critical,CA:TRUE',
            '-addext','keyUsage=critical,keyCertSign,cRLSign',
            '-keyout',ca_key,'-out',ca_cert)
        certificates = {}
        for name, address in LEAVES.items():
            key, csr, cert, extensions = (root/(name+suffix) for suffix in ('.key','.csr','.crt','.ext'))
            openssl('req','-new','-newkey','rsa:3072','-nodes','-sha256',
                '-config','/dev/null','-subj','/CN='+name+'.processing.internal',
                '-keyout',key,'-out',csr)
            extensions.write_text('basicConstraints=critical,CA:FALSE\n'
                'keyUsage=critical,digitalSignature,keyEncipherment\n'
                'extendedKeyUsage=serverAuth\n'
                'subjectAltName=IP:'+address+',IP:127.0.0.1\n', encoding='ascii')
            openssl('x509','-req','-in',csr,'-CA',ca_cert,'-CAkey',ca_key,
                '-set_serial','0x'+secrets.token_hex(16),'-days','90','-sha256',
                '-extfile',extensions,'-out',cert)
            openssl('verify','-CAfile',ca_cert,'-verify_ip',address,cert)
            openssl('verify','-CAfile',ca_cert,'-verify_ip','127.0.0.1',cert)
            certificates[name] = dict(address=address,sha256=hashlib.sha256(cert.read_bytes()).hexdigest(),
                expires=openssl('x509','-in',cert,'-noout','-enddate').removeprefix('notAfter='))
        # Sync every file before the public-facts receipt becomes the checkpoint.
        for path in root.iterdir():
            os.chmod(path,0o600)
            with path.open('rb') as stream:
                os.fsync(stream.fileno())
        receipt = dict(schemaVersion=1,purpose='private-processing-tls',
            caSha256=hashlib.sha256(ca_cert.read_bytes()).hexdigest(),certificates=certificates,
            systemTrustInstalled=False,servicesActivated=False)
        with (root/'provisioning-receipt.json').open('x',encoding='utf-8') as stream:
            json.dump(receipt,stream,indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        descriptor = os.open(root,os.O_RDONLY|os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return receipt
    finally:
        os.umask(previous)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',required=True)
    args = parser.parse_args()
    try:
        print(json.dumps(provision(args.output)))
    except Exception:
        raise SystemExit('TLS provisioning unavailable; inspect retained private directory locally') from None


if __name__ == '__main__':
    main()
