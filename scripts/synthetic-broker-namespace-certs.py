"""REVIEW ONLY: generate run-owned synthetic TLS credentials; do not run yet.

This creates a new explicitly named directory and never deletes it. Certificates
and keys are for a synthetic isolated test only, not production credentials.
"""
import argparse
import os
from pathlib import Path
import shutil
import subprocess


def openssl(*args):
    result=subprocess.run(['openssl',*args],capture_output=True,timeout=20,check=False)
    if result.returncode:raise RuntimeError('synthetic certificate generation failed')


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--new-root',required=True);args=parser.parse_args()
    root=Path(args.new_root)
    if not root.is_absolute() or root.parent.resolve()!=root.parent or root.exists() or root.is_symlink():
        raise ValueError('new absolute directory with real existing parent required')
    root.mkdir(mode=0o755)
    private=root/'issuer-private';server=root/'server';client=root/'client'
    private.mkdir(mode=0o700)
    for path in (server,client):path.mkdir(mode=0o755)
    if os.name=='posix':
        for path in (root,server,client):os.chmod(path,0o755)
    ca_key=private/'ca.key';ca_cert=private/'ca.crt'
    openssl('req','-x509','-newkey','rsa:2048','-nodes','-days','1','-subj','/CN=SYNTHETIC-ONLY-CA',
            '-keyout',str(ca_key),'-out',str(ca_cert))
    for name,folder in (('server',server),('client',client)):
        key=folder/(name+'.key');request=private/(name+'.csr');cert=folder/(name+'.crt')
        openssl('req','-new','-newkey','rsa:2048','-nodes','-subj','/CN=SYNTHETIC-ONLY-'+name,
                '-keyout',str(key),'-out',str(request))
        openssl('x509','-req','-days','1','-in',str(request),'-CA',str(ca_cert),'-CAkey',str(ca_key),
                '-CAcreateserial','-out',str(cert))
        shutil.copyfile(ca_cert,folder/'ca.crt')
        (folder/'SYNTHETIC-ONLY').write_bytes(b'SYNTHETIC TEST ONLY\n')
        # Synthetic-only private keys are intentionally world-readable within
        # their separately mounted directories for non-root Docker test UIDs.
        # Never put production keys or data in these directories.
        if os.name=='posix':os.chmod(key,0o644)
    shutil.copyfile(server/'server.crt',client/'server.crt')
    shutil.copyfile(client/'client.crt',server/'client.crt')
    if os.name=='posix':
        os.chmod(ca_key,0o600)
        for folder in (server,client):
            for path in folder.iterdir():os.chmod(path,0o644)
    print('retained synthetic credential root:',root,flush=True)
    print('server directory:',server,'client directory:',client,flush=True)


if __name__=='__main__':main()
