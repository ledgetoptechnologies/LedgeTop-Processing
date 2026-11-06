"""Unwired frame checks; synthetic signatures/bytes are not custody proof."""
import base64
import copy
import hashlib
import json
import os
import unittest
from processing import reference_protocol as p
from processing.direct_selected_gcp_frames import verify_selected,verify_gcp_chunks,SELECTED_DOMAIN,EVIDENCE_DOMAIN,FILE_DOMAIN

def frame(value,domain):
    raw=p.canonical_json(value)
    return raw,hashlib.sha256(domain+raw).hexdigest()

class FrameTests(unittest.TestCase):
    @unittest.skipUnless(os.environ.get('LTDS_VIEWER_FRAME_VECTOR'),'Viewer Node vector not supplied')
    def test_actual_viewer_node_frame_encoders(self):
        with open(os.environ['LTDS_VIEWER_FRAME_VECTOR'],'r',encoding='utf-8') as stream: vector=json.load(stream)
        selected=vector['selected'];wire=base64.b64decode(selected['bytes'])
        intent={'selectedPageHashes':selected['hashes'],'selectedFrameBytes':len(wire),'selectedCount':1}
        source=[{'type':'dataset_file','datasetFileId':'img','relativePath':'a.jpg','processingRole':'image','byteSize':3,'sha256':'a'*64}]
        manifest={'objects':[{'id':'img','name':'a.jpg','byteSize':3,'sha256':'a'*64}]}
        self.assertFalse(verify_selected(intent,wire,source_records=source,pipeline_manifest=manifest)['sourceCustodyAccepted'])
        for kind in ('evidence','file'):
            v=vector[kind];raw=base64.b64decode(v['bytes'])
            result=verify_gcp_chunks(raw,hashes=v['hashes'],page_bytes=v['pageBytes'],stream_bytes=v['streamBytes'],
                byte_count=v['byteCount'],sha256=v['sha256'],attempt_id='attempt',kind=kind,
                evidence_sha256=v.get('evidenceSha256'))
            self.assertFalse(result['sourceCustodyAccepted'])

    def test_selected_exact_order_and_binding(self):
        page={'version':'ltds-viewer-selected-frames-v1','index':0,'records':[
            {'type':'dataset_file','ordinal':0,'pageObjectId':'img','datasetFileId':'img'}]}
        raw,sha=frame(page,SELECTED_DOMAIN);wire=raw+b'\n'
        intent={'selectedPageHashes':[sha],'selectedFrameBytes':len(wire),'selectedCount':1}
        source=[{'type':'dataset_file','datasetFileId':'img','relativePath':'flight/a.jpg',
                 'processingRole':'image','byteSize':3,'sha256':'a'*64}]
        manifest={'objects':[{'id':'img','name':'a.jpg','byteSize':3,'sha256':'a'*64}]}
        result=verify_selected(intent,wire,source_records=source,pipeline_manifest=manifest)
        self.assertFalse(result['sourceCustodyAccepted'])
        changed=copy.deepcopy(manifest);changed['objects'][0]['id']='other'
        with self.assertRaises(ValueError):verify_selected(intent,wire,source_records=source,pipeline_manifest=changed)
        with self.assertRaises(ValueError):verify_selected(intent,wire+b'x',source_records=source,pipeline_manifest=manifest)

    def test_chunk_domains_and_truncation(self):
        content='hello ü'.encode();sha=hashlib.sha256(content).hexdigest();attempt='attempt'
        for kind,version,domain in (('evidence','ltds-viewer-gcp-evidence-chunks-v2',EVIDENCE_DOMAIN),
                                    ('file','ltds-viewer-generated-gcp-file-chunks-v1',FILE_DOMAIN)):
            extra={'evidenceSha256':sha} if kind=='evidence' else {'evidenceSha256':'b'*64,'fileSha256':sha}
            first=dict(version=version,attemptId=attempt,index=0,offset=0,decodedByteCount=len(content),
                       data=base64.urlsafe_b64encode(content).decode().rstrip('='),**extra)
            last=dict(version=version,attemptId=attempt,index=1,offset=len(content),totalBytes=len(content),complete=True,**extra)
            a,ah=frame(first,domain);b,bh=frame(last,domain);wire=a+b'\n'+b+b'\n'
            kw=dict(hashes=[ah,bh],page_bytes=[len(a),len(b)],stream_bytes=len(wire),byte_count=len(content),
                    sha256=sha,attempt_id=attempt,kind=kind,evidence_sha256='b'*64)
            self.assertFalse(verify_gcp_chunks(wire,**kw)['sourceCustodyAccepted'])
            with self.assertRaises(ValueError):verify_gcp_chunks(wire[:-1],**kw)
            with self.assertRaises(ValueError):verify_gcp_chunks(wire,**dict(kw,byte_count=len(content)+1))
