"""Signed manifest, signed run authorization and signed receipt.

Three roles with separate keys: the sealer fixes what may run, the operator authorizes
one run, the writer signs the receipt bytes. Signatures are detached OpenSSH signatures
checked with `ssh-keygen -Y verify` against one allowed-signers file. The reviewer
supplies that file's SHA-256; it never comes from the run. The launcher's own record is
compared with the signed documents and is never the source of an expected value.
"""
import hashlib
import json
import re
import subprocess
import tempfile
from pathlib import Path

from .errors import FrameRefused

MANIFEST = 'declared-frame-manifest/v1'
AUTHORIZATION = 'declared-frame-authorization/v1'
ROLES = {'sealer': 'declared-frame-manifest', 'operator': 'declared-frame-authorization',
         'writer': 'declared-frame-receipt'}
_SHA = re.compile('[0-9a-f]{64}')
_IMAGE = re.compile('sha256:[0-9a-f]{64}')
_MANIFEST_KEYS = {'schema', 'image', 'base_image', 'engine_version', 'engine_files',
                  'candidate_files', 'profile_sha256', 'grid_sha256', 'proj_db_sha256',
                  'runtime', 'command', 'container'}
_AUTHORIZATION_KEYS = {'schema', 'manifest_sha256', 'run_nonce', 'operator', 'writer',
                       'ledger_id', 'purpose'}


def sha256(raw):
    return hashlib.sha256(raw).hexdigest()


def verify_signature(raw, signature, *, role, principal, signers, trust_sha256, run=subprocess.run):
    if sha256(signers) != trust_sha256:
        raise FrameRefused('trust_root_mismatch')
    if not re.fullmatch('[A-Za-z0-9._-]{1,64}', principal or ''):
        raise FrameRefused('principal_invalid')
    with tempfile.TemporaryDirectory() as folder:
        trust, sig = Path(folder) / 'signers', Path(folder) / 'sig'
        trust.write_bytes(signers)
        sig.write_bytes(signature)
        result = run(['ssh-keygen', '-Y', 'verify', '-f', str(trust), '-I', principal,
                      '-n', ROLES[role], '-s', str(sig)],
                     input=raw, capture_output=True, timeout=20, check=False)
    if result.returncode != 0:
        raise FrameRefused(f'{role}_signature_invalid')


def _document(raw, schema, keys, code):
    try:
        value = json.loads(raw)
    except ValueError as error:
        raise FrameRefused(code) from error
    if not isinstance(value, dict) or set(value) != keys or value.get('schema') != schema:
        raise FrameRefused(code)
    return value


def verify_run(bundle, *, trust_sha256, sealer, run=subprocess.run):
    """Return (manifest, authorization, receipt) or refuse. Every byte is signed."""
    signers = bundle['signers']
    check = lambda raw, sig, role, who: verify_signature(
        raw, sig, role=role, principal=who, signers=signers, trust_sha256=trust_sha256, run=run)
    check(bundle['manifest'], bundle['manifest_sig'], 'sealer', sealer)
    manifest = _document(bundle['manifest'], MANIFEST, _MANIFEST_KEYS, 'manifest_malformed')
    if (not _IMAGE.fullmatch(manifest['image']) or not _IMAGE.fullmatch(manifest['base_image'])
            or not manifest['engine_files'] or not manifest['candidate_files']):
        raise FrameRefused('manifest_malformed')
    authorization = _document(bundle['authorization'], AUTHORIZATION, _AUTHORIZATION_KEYS,
                              'authorization_malformed')
    operator, writer = authorization['operator'], authorization['writer']
    if len({sealer, operator, writer}) != 3:
        raise FrameRefused('roles_must_be_separate')
    _require_separate_keys(signers, (sealer, operator, writer))
    check(bundle['authorization'], bundle['authorization_sig'], 'operator', operator)
    if (authorization['manifest_sha256'] != sha256(bundle['manifest'])
            or not _SHA.fullmatch(authorization['run_nonce'])
            or authorization['purpose'] != 'synthetic'):
        raise FrameRefused('authorization_mismatch')
    check(bundle['receipt'], bundle['receipt_sig'], 'writer', writer)
    try:
        receipt = json.loads(bundle['receipt'])
        bound = (receipt['run_nonce'] == authorization['run_nonce']
                 and receipt['manifest_sha256'] == authorization['manifest_sha256']
                 and receipt['authorization_sha256'] == sha256(bundle['authorization'])
                 and receipt['ledger_id'] == authorization['ledger_id'])
    except (ValueError, KeyError, TypeError) as error:
        raise FrameRefused('receipt_malformed') from error
    if not bound:
        raise FrameRefused('receipt_not_bound')
    _compare_observed(receipt, manifest)
    _compare_launcher(bundle['launcher'], manifest, receipt, sha256(bundle['receipt']))
    return manifest, authorization, receipt


def _require_separate_keys(signers, principals):
    keys = {}
    for line in signers.decode('utf-8', errors='replace').splitlines():
        parts = line.split()
        if not parts or parts[0].startswith('#'):
            continue
        for who in parts[0].split(','):
            keys.setdefault(who, set()).update(p for p in parts[1:] if p.startswith('AAAA'))
    chosen = [keys.get(who, set()) for who in principals]
    if any(not group for group in chosen) or len(set().union(*chosen)) != sum(map(len, chosen)):
        raise FrameRefused('signing_keys_must_be_separate')


def _compare_observed(receipt, manifest):
    try:
        observed = {**receipt['observed']}
    except (KeyError, TypeError) as error:
        raise FrameRefused('receipt_malformed') from error
    for key in ('engine_version', 'engine_files', 'candidate_files', 'profile_sha256',
                'grid_sha256', 'proj_db_sha256', 'runtime'):
        if observed.get(key) != manifest[key]:
            raise FrameRefused('observed_manifest_mismatch', {'field': key})


def _compare_launcher(launcher, manifest, receipt, receipt_sha256):
    """Containment facts. A disagreement refuses; agreement proves nothing by itself."""
    try:
        same = (launcher['image'] == manifest['image']
                and launcher['command'] == manifest['command']
                and launcher['container'] == manifest['container']
                and len(receipt['container']) >= 12
                and launcher['container_id'].startswith(receipt['container'])
                and launcher['exit_code'] == 0
                and launcher['receipt_sha256'] == receipt_sha256)
    except (KeyError, TypeError, AttributeError) as error:
        raise FrameRefused('launcher_malformed') from error
    if not same:
        raise FrameRefused('launcher_mismatch')
