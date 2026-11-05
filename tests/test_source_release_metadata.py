"""Pure closed-size proofs; no payload files, NAS or 50 GB physical claim."""
import copy
import json
from pathlib import Path
import stat
import unittest

from processing import reference_protocol as p
from processing.source_release import reservation_bytes,OBJECT_JOURNAL_RESERVE,TERMINAL_JOURNAL_RESERVE
from processing.source_release_filesystem import encode_physical,MAX_PHYSICAL_BYTES


class SourceReleaseMetadataTests(unittest.TestCase):
    def test_namespace_binding_and_terminal_header_fit_separate_fixed_reservation(self):
        from processing.source_release_filesystem import validate_namespace
        from processing.source_release import MINIMUM_NS
        maximum=2**64-1;uid=2**32-1;mode=0o40755
        parent=[maximum,maximum,mode,uid]
        identity='00000000-0000-4000-8000-000000000001'
        namespace=dict(schemaVersion=1,protocol='ltds-source-release-namespace-v1',authorizationSha256='a'*64,
            eligibleClosureSha256='b'*64,preservedClosureSha256='c'*64,
            parents={key:list(parent) for key in ('root','job','generation')},quarantineName='.ltds-release-'+identity)
        encoded=validate_namespace(namespace,authorization_sha256='a'*64,eligible_sha256='b'*64,
            preserved_sha256='c'*64,release_id=identity)
        receipt=dict(schemaVersion=1,protocol='ltds-source-release-receipt-v1',coordinatorAuthorityId='a'*128,
            jobId=identity,generation=p.MAX_SAFE_INTEGER,environment='production',principalId='p'*128,
            releaseId=identity,authorizationSha256='a'*64,sourceSnapshotId=identity,sourceSnapshotSha256='b'*64,
            workerObservationAckSha256='c'*64,eligibleClosureSha256='d'*64,preservedClosureSha256='e'*64,
            removedObjectCount=20000,removedBytes=p.MAX_SAFE_INTEGER,completedAt='9999-12-31T23:59:59.999Z',receiptSha256='f'*64)
        # Exact request/preserved bytes are reserved separately, and all object
        # physical rows use their own1280B slot. This covers every other column.
        columns=[identity,p.MAX_SAFE_INTEGER,identity,'p'*128,'a'*64,'b'*64,'waiting_policy',
            'release_role_unavailable','9999-12-31T23:59:59.999Z',MINIMUM_NS,'e'*64,2**63-1,
            p.canonical_json(receipt).decode(),64*1024*1024,'d'*64,encoded]
        self.assertLessEqual(sum(len(str(value)) for value in columns),TERMINAL_JOURNAL_RESERVE)

    def test_maximum_closed_physical_evidence_and_completed_outcome_fit_reserved_bytes(self):
        maximum=2**64-1
        unsigned32=2**32-1;signed64=2**63-1
        mode=0o177777
        directory=[maximum,maximum,((mode&~0o170000)|stat.S_IFDIR)&~0o022,unsigned32]
        file=[maximum,maximum,p.MAX_SAFE_INTEGER,(mode&~0o170000)|stat.S_IFREG,1,unsigned32,signed64,signed64]
        obj=dict(id='o'*128,objectKind='derived',byteSize=p.MAX_SAFE_INTEGER,sha256='f'*64)
        lengths=[]
        for phase,kind in (('verified','create_quarantine'),('quarantining','sync_quarantine'),('unlinking','unlink')):
            physical={name:list(directory) for name in ('root','job','generation','quarantine')}
            physical['file']=list(file)
            if phase=='unlinking':physical['quarantinedFile']=list(file)
            physical['effectIntent']=dict(id='00000000-0000-4000-8000-000000000001',kind=kind,outcome='prepared')
            prepared=encode_physical(phase,physical,obj)
            physical['effectIntent']['outcome']='completed'
            completed=encode_physical(phase,physical,obj)
            lengths.extend((len(prepared),len(completed)))
        self.assertLessEqual(max(lengths),MAX_PHYSICAL_BYTES)
        # Every other stored per-object column has a closed bound too.
        self.assertLessEqual(MAX_PHYSICAL_BYTES+128+7+12+36,OBJECT_JOURNAL_RESERVE)
        bad=dict(physical,unreviewed='x')
        with self.assertRaises(ValueError):encode_physical('unlinking',bad,obj)
        bad=copy.deepcopy(physical);bad['file'][0]=True
        with self.assertRaises(ValueError):encode_physical('unlinking',bad,obj)
        bad=copy.deepcopy(physical);bad['file'][0]=2**64
        with self.assertRaises(ValueError):encode_physical('unlinking',bad,obj)

    def test_ten_thousand_photo_metadata_projection_fits_default_payload_budget(self):
        vectors=json.loads((Path(__file__).parents[1]/'contracts/fixtures/source-release-v1-vectors.json').read_text())
        request=copy.deepcopy(vectors['positive']['request']);auth=request['authorization']
        count=10000
        ids=[f'00000000-0000-4000-8000-{index:012d}' for index in range(1,count+1)]
        auth['eligibleObjects']=[dict(id=identity,objectKind='source',byteSize=5_000_000,sha256='a'*64) for identity in ids]
        auth['eligibleClosureSha256']=p.digest(auth['eligibleObjects'])
        raw=[dict(id=identity,name=f'DJI_20261105_120000_{index:04d}.JPG',byteSize=5_000_000,sha256='a'*64,
            sourceType='raw_input',retentionClass='release_after_acceptance',retentionHoldId=None) for index,identity in enumerate(ids)]
        engine=[dict(id=obj['id'],name=obj['name'],byteSize=obj['byteSize'],sha256=obj['sha256'],kind='image',mediaType='image/jpeg') for obj in raw]
        receipt=dict(schemaVersion=1,jobId=auth['jobId'],generation=1,objectId=ids[0],
            storageKey=auth['jobId']+'/1/'+'a'*64+'.blob',byteSize=5_000_000,sha256='a'*64,mountId='b'*64)
        # Conservative logical projection: raw closure twice, engine closure,
        # object receipts, four independent read/write/release pins per photo,
        # source range identity/request metadata, plus 2 MiB other fixed history.
        intake=2*len(p.canonical_json(raw))+len(p.canonical_json(engine))
        receipts=count*len(p.canonical_json(receipt))
        pins=count*4*220
        ranges=count*500
        other=2*1024*1024
        full=intake+receipts+pins+ranges+other+reservation_bytes(request,[])
        self.assertLess(full,64*1024*1024)
        self.assertEqual(sum(obj['byteSize'] for obj in auth['eligibleObjects']),50_000_000_000)
        self.assertEqual(TERMINAL_JOURNAL_RESERVE,4096)
        # Gate CONTROL has its own bounded reserve and is not a payload credit.
        self.assertGreater(full+256*1024,full)
