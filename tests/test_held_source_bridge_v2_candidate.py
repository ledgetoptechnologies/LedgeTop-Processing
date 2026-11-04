"""Synthetic cross-language held bridge; no real data or authorization."""
import base64
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sqlite3
import tempfile
import unittest
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import load_pem_public_key
from processing import reference_protocol as p
from processing.held_source_bridge_v2_candidate import verify, DOMAIN, PROTOCOL
from processing.held_source_bridge_journal_candidate import initialize, stage
from processing.held_viewer_snapshot_verifier_candidate import DOMAIN as LOCAL_DOMAIN
from processing.large_paged_manifest_candidate import build, _digest
from processing.viewer_paged_frame_candidate import verify as verify_source, VERSION as SOURCE_VERSION, DOMAIN as SOURCE_DOMAIN
from processing.viewer_selected_frames_candidate import VERSION as SELECTED_VERSION, DOMAIN as SELECTED_DOMAIN

ROOT = Path(__file__).resolve().parents[1]


class HeldSourceBridgeTests(unittest.TestCase):
    def test_actual_viewer_local_snapshot_with_scoped_synthetic_outer_signature(self):
        if not os.environ.get('PINNED_VIEWER_ROOT'):
            if os.environ.get('GCP_VIEWER_PAIR_REQUIRED') == '1': self.fail('pinned Viewer checkout required')
            self.skipTest('pinned Viewer checkout required')
        result = json.loads(subprocess.run(['node', str(ROOT / 'tests/support/viewer_gcp_export_pair.cjs'), 'snapshot'],
            env=os.environ, capture_output=True, check=True, timeout=45).stdout)
        local = result['intent']
        intent, signature, source, selected, evidence, generated = (base64.b64decode(result[key], validate=True)
            for key in ('intentBytes', 'signature', 'sourceFrames', 'selectedFrames', 'evidenceFrames', 'fileFrames'))
        _, selected_source = verify_source(io.BytesIO(source), expected_page_hashes=local['sourcePageHashes'],
                                           expected_items=local['sourceCount'], _capture_selection=True)
        selected_records = [record for line in selected.splitlines() for record in json.loads(line)['records']]
        objects = []
        for src, rec in zip(selected_source, selected_records):
            objects.append(dict(id=rec['pageObjectId'], name=src['relativePath'].rsplit('/',1)[-1],
                byteSize=src['byteSize'], sha256=src['sha256'],
                kind=src['kind'] if src['type']=='dataset_file' else 'auxiliary',
                mediaType=src['mediaType'] if src['type']=='dataset_file' else rec['mediaType']))
        closure, pages = build(objects)
        random_uuid = lambda: str(uuid.uuid4())
        context = dict(issuerId='outer-issuer', keyId='outer-key', principalId='viewer-a', environment='staging',
            coordinatorAuthorityId=random_uuid(), jobId=random_uuid(), generation=1, submissionId=random_uuid(),
            activationId=random_uuid(), viewerTaskId=local['taskId'], viewerAttemptId=local['attemptId'],
            preparedEnvelopeSha256='a'*64, datasetId=local['datasetId'],
            datasetManifestSha256=local['datasetManifestSha256'], pipelineDigest='b'*64,
            processingClosureSha256=_digest(closure), revocationEpoch=1,
            localIssuerId='synthetic-issuer', localKeyId='synthetic-key')
        outer_private = Ed25519PrivateKey.generate()
        def pair(payload):
            return (hashlib.sha256(payload).hexdigest(), len(payload)) if payload is not None else (None, None)
        bridge = dict(protocol=local['protocol'], snapshotId=local['snapshotId'], issuerId='synthetic-issuer', keyId='synthetic-key')
        for name, payload in (('intent',intent),('signature',signature),('sourceFrameStream',source),
                              ('selectedFrameStream',selected),('gcpEvidenceStream',evidence),('gcpFileStream',generated)):
            bridge[name+'Sha256'], bridge[name+'ByteCount'] = pair(payload)
        g = local['gcp']
        outer = {k:v for k,v in context.items() if k not in ('revocationEpoch','localIssuerId','localKeyId')}
        outer.update(protocol=PROTOCOL, schemaVersion=2, snapshotId=local['snapshotId'],
            sourcePageHashes=local['sourcePageHashes'], sourcePageBytes=[len(x)-1 for x in source.splitlines(keepends=True)],
            sourceCount=local['sourceCount'], selectedPageHashes=local['selectedPageHashes'],
            selectedPageBytes=[len(x)-1 for x in selected.splitlines(keepends=True)], selectedCount=local['selectedCount'],
            inputManifestSha256=local['inputManifestSha256'], gcpEvidenceVersion=g['evidenceVersion'],
            gcpEvidencePageHashes=g['evidencePageHashes'], gcpEvidencePageBytes=g['evidencePageBytes'],
            gcpEvidencePageCount=g['evidencePageCount'], gcpEvidenceByteCount=g['evidenceByteCount'],
            gcpEvidenceSha256=g['evidenceSha256'], issuedAt='2030-01-01T00:00:00.000Z',
            expiresAt='2030-01-01T00:10:00.000Z', viewerHeldSource=bridge)
        def signed(value, key=outer_private):
            value=copy.deepcopy(value)
            value['signature']=base64.urlsafe_b64encode(key.sign(DOMAIN+p.canonical_json(value))).decode().rstrip('=')
            return p.canonical_json(value)
        args = dict(statement_bytes=signed(outer), outer_key=outer_private.public_key(),
            local_key=load_pem_public_key(result['publicKey'].encode()), context=context, now_ms=1893456300000,
            intent_bytes=intent, local_signature=signature, source_frames=source, selected_frames=selected,
            evidence_frames=evidence, file_frames=generated, processing_closure=closure, processing_pages=pages)
        self.assertEqual(verify(**args), {'state':'held_bridge_not_authorized'})
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'bridge.sqlite'
            db = sqlite3.connect(path)
            initialize(db)
            with self.assertRaises(ValueError):stage(db,arguments=args,max_journal_bytes=1)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM held_source_bridge_v2').fetchone()[0],0)
            for point in ('statement','proof','intent','signature','source','selected','evidence','file'):
                def fault(at):
                    if at == point: raise OSError('synthetic crash')
                with self.subTest(crash=point), self.assertRaises(ValueError):
                    stage(db,arguments=args,_fault=fault)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM held_source_bridge_v2').fetchone()[0],0)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM held_source_bridge_blobs_v2').fetchone()[0],0)
            self.assertEqual(stage(db,arguments=args),{'state':'held_bridge_journaled','created':True})
            db.close()
            db = sqlite3.connect(path)
            try:
                self.assertEqual(stage(db,arguments={**args,'now_ms':1893456600000},max_journal_bytes=1,max_snapshots=1),
                                 {'state':'held_bridge_journaled','created':False})
                with self.assertRaises(ValueError):stage(db,arguments={**args,'file_frames':generated[:-1]})
                changed = copy.deepcopy(outer); changed['expiresAt'] = '2030-01-01T00:11:00.000Z'
                with self.assertRaises(ValueError):stage(db,arguments={**args,'statement_bytes':signed(changed)})
                self.assertEqual(db.execute('SELECT COUNT(*) FROM held_source_bridge_v2').fetchone()[0],1)
            finally: db.close()
        for mutation in ({'outer_key':Ed25519PrivateKey.generate().public_key()},
                         {'local_key':Ed25519PrivateKey.generate().public_key()},
                         {'context':{**context,'principalId':'other'}}, {'now_ms':1893456600000},
                         {'selected_frames':selected[:-1]}, {'file_frames':generated[:-1]},
                         {'processing_pages':pages[:-1]}):
            with self.subTest(mutation=list(mutation)), self.assertRaises(ValueError):
                verify(**{**args, **mutation})
        for field in ('snapshotId','sourceCount','datasetManifestSha256','inputManifestSha256'):
            forged=copy.deepcopy(outer); forged[field]=0 if field=='sourceCount' else '0'*64
            with self.subTest(forged=field), self.assertRaises(ValueError):
                verify(**{**args,'statement_bytes':signed(forged)})
        for change in ('source-page-size', 'local-intent-hash', 'gcp-file-size', 'wrong-attempt', 'version'):
            forged=copy.deepcopy(outer)
            if change=='source-page-size': forged['sourcePageBytes'][0] += 1
            elif change=='local-intent-hash': forged['viewerHeldSource']['intentSha256'] = '0'*64
            elif change=='gcp-file-size': forged['viewerHeldSource']['gcpFileStreamByteCount'] += 1
            elif change=='wrong-attempt': forged['viewerAttemptId'] = random_uuid()
            else: forged['schemaVersion'] = 1
            with self.subTest(forged=change), self.assertRaises(ValueError):
                verify(**{**args,'statement_bytes':signed(forged)})
        # A separate photo-only case covers the near-term non-GCP workflow.
        src = selected_source[0]; rec = selected_records[0]
        source_raw = p.canonical_json(dict(version=SOURCE_VERSION,index=0,records=[src]))
        selected_raw = p.canonical_json(dict(version=SELECTED_VERSION,index=0,records=[rec]))
        source2, selected2 = source_raw+b'\n', selected_raw+b'\n'
        input_value = [dict(relativePath=src['relativePath'],byteSize=src['byteSize'],sha256=src['sha256'])]
        input_sha = hashlib.sha256(json.dumps(input_value,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()
        local2 = copy.deepcopy(local)
        local2.update(snapshotId=random_uuid(),sourceCount=1,datasetFileCount=1,sourcePageHashes=[hashlib.sha256(SOURCE_DOMAIN+source_raw).hexdigest()],
            sourceFrameBytes=len(source2),selectedCount=1,selectedPageHashes=[hashlib.sha256(SELECTED_DOMAIN+selected_raw).hexdigest()],
            selectedFrameBytes=len(selected2),inputManifestSha256=input_sha,gcp=None)
        local_private = Ed25519PrivateKey.generate(); intent2=p.canonical_json(local2)
        signature2=local_private.sign(LOCAL_DOMAIN+intent2); closure2,pages2=build([objects[0]])
        context2={**context,'processingClosureSha256':_digest(closure2)}
        bridge2=dict(protocol=local2['protocol'],snapshotId=local2['snapshotId'],issuerId='synthetic-issuer',keyId='synthetic-key')
        for name,payload in (('intent',intent2),('signature',signature2),('sourceFrameStream',source2),
                             ('selectedFrameStream',selected2),('gcpEvidenceStream',None),('gcpFileStream',None)):
            bridge2[name+'Sha256'],bridge2[name+'ByteCount']=pair(payload)
        outer2=copy.deepcopy(outer)
        outer2.update(snapshotId=local2['snapshotId'],processingClosureSha256=context2['processingClosureSha256'],sourcePageHashes=local2['sourcePageHashes'],
            sourcePageBytes=[len(source_raw)],sourceCount=1,selectedPageHashes=local2['selectedPageHashes'],
            selectedPageBytes=[len(selected_raw)],selectedCount=1,inputManifestSha256=input_sha,viewerHeldSource=bridge2)
        for key in ('gcpEvidenceVersion','gcpEvidencePageHashes','gcpEvidencePageBytes','gcpEvidencePageCount',
                    'gcpEvidenceByteCount','gcpEvidenceSha256'):outer2[key]=None
        photo_args={**args,'statement_bytes':signed(outer2),'local_key':local_private.public_key(),'context':context2,
                    'intent_bytes':intent2,'local_signature':signature2,'source_frames':source2,'selected_frames':selected2,
                    'evidence_frames':None,'file_frames':None,'processing_closure':closure2,'processing_pages':pages2}
        self.assertEqual(verify(**photo_args),{'state':'held_bridge_not_authorized'})
        with self.assertRaises(ValueError):verify(**{**photo_args,'evidence_frames':b'unexpected'})
        # Even two valid signatures cannot reuse one key across trust domains.
        with self.assertRaises(ValueError):
            verify(**{**photo_args,'outer_key':local_private.public_key(),
                      'statement_bytes':signed(outer2,local_private)})
        # Two valid distinct snapshots cannot race past the journal count cap.
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'concurrent.sqlite'
            setup=sqlite3.connect(path);initialize(setup);setup.close()
            barrier=threading.Barrier(2)
            def write(value):
                connection=sqlite3.connect(path,timeout=10)
                try:
                    barrier.wait(timeout=5)
                    try:return stage(connection,arguments=value,max_snapshots=1)['created']
                    except ValueError:return False
                finally:connection.close()
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures=[pool.submit(write,value) for value in (args,photo_args)]
                self.assertEqual(sum(f.result(timeout=15) for f in futures),1)
            db=sqlite3.connect(path)
            try:self.assertEqual(db.execute('SELECT COUNT(*) FROM held_source_bridge_v2').fetchone()[0],1)
            finally:db.close()


if __name__ == '__main__': unittest.main()
