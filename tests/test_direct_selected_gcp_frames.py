"""Unwired frame checks; synthetic signatures/bytes are not custody proof."""
import base64
import copy
import hashlib
import json
import os
import unittest
from processing import reference_protocol as p
from processing.direct_selected_gcp_frames import verify_selected,verify_gcp_chunks,SELECTED_DOMAIN,EVIDENCE_DOMAIN,FILE_DOMAIN
from processing.direct_selected_gcp_frames import verify_gcp_semantics,_js_stringify

def frame(value,domain):
    raw=p.canonical_json(value)
    return raw,hashlib.sha256(domain+raw).hexdigest()

class FrameTests(unittest.TestCase):
    @unittest.skipUnless(os.environ.get('LTDS_VIEWER_FRAME_VECTOR'),'Viewer Node vector not supplied')
    def test_actual_viewer_node_frame_encoders(self):
        with open(os.environ['LTDS_VIEWER_FRAME_VECTOR'],'r',encoding='utf-8') as stream: vector=json.load(stream)
        selected=vector['selected'];wire=base64.b64decode(selected['bytes'])
        intent={'selectedPageHashes':selected['hashes'],'selectedFrameBytes':len(wire),'selectedCount':1}
        source=[{'type':'dataset_file','datasetFileId':'img','relativePath':'a.jpg','processingRole':'image','byteSize':3,'sha256':'a'*64,'kind':'image','mediaType':'image/jpeg'}]
        manifest={'objects':[{'id':'img','name':'a.jpg','byteSize':3,'sha256':'a'*64,'kind':'image','mediaType':'image/jpeg'}]}
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
                 'processingRole':'image','byteSize':3,'sha256':'a'*64,'kind':'image','mediaType':'image/jpeg'}]
        manifest={'objects':[{'id':'img','name':'a.jpg','byteSize':3,'sha256':'a'*64,'kind':'image','mediaType':'image/jpeg'}]}
        result=verify_selected(intent,wire,source_records=source,pipeline_manifest=manifest)
        self.assertFalse(result['sourceCustodyAccepted'])
        changed=copy.deepcopy(manifest);changed['objects'][0]['id']='other'
        with self.assertRaises(ValueError):verify_selected(intent,wire,source_records=source,pipeline_manifest=changed)
        for field,value in (('kind','auxiliary'),('mediaType','image/png')):
            changed=copy.deepcopy(manifest);changed['objects'][0][field]=value
            with self.assertRaises(ValueError):verify_selected(intent,wire,source_records=source,pipeline_manifest=changed)
        with self.assertRaises(ValueError):verify_selected(intent,wire+b'x',source_records=source,pipeline_manifest=manifest)

    def test_retained_gcp_correspondence_and_generated_nodeodm_file_are_recomputed(self):
        file_bytes=b'EPSG:6319\n-88.1 44.5 243.9 12 20 photo.jpg\n'
        marks=[dict(correspondenceId='mark-1',gcpPointId='point-1',gcpSetId='set-1',externalId='A',label='Hub',
            latitude=44.5,longitude=-88.1,elevationM=244,ellipsoidalHeightM=243.9,sourceGcpDatasetFileId='gcp-file',
            sourceGcpSha256='a'*64,sourceGcpRelativePath='control.csv',imageDatasetFileId='photo-1',
            imageRelativePath='flight/photo.jpg',imageWidth=100,imageHeight=100,pixelX=12,pixelY=20)]
        evidence=dict(schemaVersion=3,coordinateContract=dict(schemaVersion=2,horizontalCrs='EPSG:6318',verticalSystem='NAVD88(GEOID18) height',
            heightSource='ellipsoidal',outputVerticalSystem='ellipsoidal height',zSource='Elevation',sourceUnits='ftUS',outputZUnit='m',
            sourceCoordinateSystem='NAD83(2011) / WISCRS',parser='emlid-all-columns-v1'),
            snapshot=dict(snapshotId='attempt-1',byteSize=len(file_bytes),sha256=hashlib.sha256(file_bytes).hexdigest(),
                sourceGcpDatasetFileIds=['gcp-file'],imageDatasetFileIds=['photo-1'],correspondenceCount=1),correspondences=marks)
        evidence_bytes=json.dumps(evidence,ensure_ascii=False,separators=(',',':')).encode()
        generated=dict(byteSize=len(file_bytes),sha256=hashlib.sha256(file_bytes).hexdigest(),attemptId='attempt-1',
            correspondenceSha256=hashlib.sha256(_js_stringify(marks).encode()).hexdigest(),
            sourceFiles=[dict(datasetFileId='gcp-file',relativePath='control.csv',sha256='a'*64)])
        sources=[dict(type='dataset_file',datasetFileId='gcp-file',relativePath='control.csv',processingRole='gcp_source',
                byteSize=10,sha256='a'*64,kind='auxiliary',mediaType='text/csv'),
            dict(type='dataset_file',datasetFileId='unused-gcp-file',relativePath='unused.csv',processingRole='gcp_source',
                byteSize=8,sha256='d'*64,kind='auxiliary',mediaType='text/csv'),
            dict(type='dataset_file',datasetFileId='photo-1',relativePath='flight/photo.jpg',processingRole='image',
                byteSize=12,sha256='b'*64,kind='image',mediaType='image/jpeg')]
        result=verify_gcp_semantics(evidence_bytes=evidence_bytes,file_bytes=file_bytes,generated_gcp=generated,
            source_records=sources,attempt_id='attempt-1',selected_image_ids=['photo-1'])
        self.assertEqual(result['state'],'gcp_evidence_and_generated_file_verified_not_source_csv_reparsed')
        self.assertFalse(result['rawSourceCsvBytesReparsed'])
        self.assertFalse(result['sourceCustodyAccepted']);self.assertFalse(result['processingAuthorized'])
        changed=copy.deepcopy(evidence);changed['correspondences'][0]['longitude']=-88.2
        changed_bytes=json.dumps(changed,ensure_ascii=False,separators=(',',':')).encode()
        with self.assertRaises(ValueError):verify_gcp_semantics(evidence_bytes=changed_bytes,file_bytes=file_bytes,generated_gcp=generated,
            source_records=sources,attempt_id='attempt-1',selected_image_ids=['photo-1'])
        with self.assertRaises(ValueError):verify_gcp_semantics(evidence_bytes=evidence_bytes,file_bytes=file_bytes+b' ',generated_gcp=generated,
            source_records=sources,attempt_id='attempt-1',selected_image_ids=['photo-1'])
        with self.assertRaisesRegex(ValueError,'selected image binding'):
            verify_gcp_semantics(evidence_bytes=evidence_bytes,file_bytes=file_bytes,generated_gcp=generated,
                source_records=sources,attempt_id='attempt-1',selected_image_ids=[])
        for changed in (dict(evidence,schemaVersion=True),
                        {**evidence,'coordinateContract':dict(evidence['coordinateContract'],schemaVersion=True)}):
            with self.subTest(schema=changed.get('schemaVersion'),contract=changed.get('coordinateContract',{}).get('schemaVersion')):
                with self.assertRaises(ValueError):verify_gcp_semantics(evidence_bytes=json.dumps(changed,separators=(',',':')).encode(),
                    file_bytes=file_bytes,generated_gcp=generated,source_records=sources,attempt_id='attempt-1',selected_image_ids=['photo-1'])

        unknown_vertical=copy.deepcopy(evidence)
        unknown_vertical['coordinateContract']['heightSource']='source'
        unknown_vertical['coordinateContract']['outputVerticalSystem']=None
        unknown_vertical['coordinateContract']['verticalSystem']='Local vertical datum 2025'
        with self.assertRaisesRegex(ValueError,'unsupported vertical coordinate system'):
            verify_gcp_semantics(evidence_bytes=json.dumps(unknown_vertical,separators=(',',':')).encode(),
                file_bytes=file_bytes,generated_gcp=generated,source_records=sources,attempt_id='attempt-1',selected_image_ids=['photo-1'])

        navd_file=b'EPSG:6349\n-88.1 44.5 244 12 20 photo.jpg\n'
        navd=dict(schemaVersion=2,coordinateContract=dict(schemaVersion=1,horizontalCrs='EPSG:6318',verticalSystem='NAVD88(GEOID18) height',
            zSource='Elevation',sourceUnits='ftUS',outputZUnit='m',sourceCoordinateSystem='NAD83(2011) / WISCRS',parser='emlid-all-columns-v1'),
            snapshot=dict(snapshotId='attempt-1',byteSize=len(navd_file),sha256=hashlib.sha256(navd_file).hexdigest(),
                sourceGcpDatasetFileIds=['gcp-file'],imageDatasetFileIds=['photo-1'],correspondenceCount=1),correspondences=marks)
        navd_bytes=json.dumps(navd,ensure_ascii=False,separators=(',',':')).encode()
        navd_meta=dict(generated,byteSize=len(navd_file),sha256=hashlib.sha256(navd_file).hexdigest())
        with self.assertRaisesRegex(ValueError,'NAVD88 source height transform unavailable'):
            verify_gcp_semantics(evidence_bytes=navd_bytes,file_bytes=navd_file,generated_gcp=navd_meta,
                source_records=sources,attempt_id='attempt-1',selected_image_ids=['photo-1'])

    def test_selected_pages_bind_generated_gcp_object_and_viewer_input_digest(self):
        attempt='attempt-2';file_bytes=b'EPSG:4326\n-88.1 44.5 244 12 20 photo.jpg\n'
        generated=dict(byteSize=len(file_bytes),sha256=hashlib.sha256(file_bytes).hexdigest(),attemptId=attempt,
            correspondenceSha256='c'*64,sourceFiles=[dict(datasetFileId='gcp-file',relativePath='control.csv',sha256='a'*64)])
        records=[dict(type='dataset_file',ordinal=0,pageObjectId='photo-1',datasetFileId='photo-1'),
            dict(type='attempt_generated_gcp',ordinal=1,pageObjectId=f'gcp_{attempt}',attemptId=attempt,mediaType='text/plain')]
        page={'version':'ltds-viewer-selected-frames-v1','index':0,'records':records}
        raw,sha=frame(page,SELECTED_DOMAIN);wire=raw+b'\n'
        intent={'attemptId':attempt,'selectedPageHashes':[sha],'selectedFrameBytes':len(wire),'selectedCount':2,
            'inputManifestSha256':'0'*64,'gcp':{'generatedGcp':generated}}
        # Match the Viewer SHA-256 over JSON.stringify(inputFiles), including exact source ordering.
        from processing.direct_selected_gcp_frames import _js_stringify
        input_files=[dict(relativePath='flight/photo.jpg',byteSize=12,sha256='b'*64),dict(relativePath='gcp_list.txt',byteSize=len(file_bytes),sha256=generated['sha256'])]
        intent['inputManifestSha256']=hashlib.sha256(_js_stringify(input_files).encode()).hexdigest()
        sources=[dict(type='dataset_file',datasetFileId='gcp-file',relativePath='control.csv',processingRole='gcp_source',byteSize=10,sha256='a'*64,kind='auxiliary',mediaType='text/csv'),
            dict(type='dataset_file',datasetFileId='old-gcp-list',relativePath='gcp_list.txt',processingRole='provider_input',byteSize=3,sha256='d'*64,kind='auxiliary',mediaType='text/plain'),
            dict(type='dataset_file',datasetFileId='photo-1',relativePath='flight/photo.jpg',processingRole='image',byteSize=12,sha256='b'*64,kind='image',mediaType='image/jpeg')]
        manifest={'objects':[dict(id='photo-1',name='photo.jpg',byteSize=12,sha256='b'*64,kind='image',mediaType='image/jpeg'),
            dict(id=f'gcp_{attempt}',name='gcp_list.txt',byteSize=len(file_bytes),sha256=generated['sha256'],kind='auxiliary',mediaType='text/plain')]}
        self.assertEqual(verify_selected(intent,wire,source_records=sources,pipeline_manifest=manifest)['selectedCount'],2)
        intent['inputManifestSha256']='0'*64
        with self.assertRaisesRegex(ValueError,'input manifest digest changed'):
            verify_selected(intent,wire,source_records=sources,pipeline_manifest=manifest)

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

    def test_viewer_photo_first_selection_excludes_auto_auxiliary(self):
        def source(identifier,name,role,kind,media):
            return {'type':'dataset_file','datasetFileId':identifier,'relativePath':name,'processingRole':role,
                    'byteSize':3,'sha256':'a'*64,'kind':kind,'mediaType':media}
        records=[source('input','notes.txt','provider_input','auxiliary','text/plain'),
                 source('ignored','other.txt','auto','auxiliary','text/plain'),
                 source('photo','photo.jpg','auto','image','image/jpeg')]
        def check(ids):
            entries=[dict(type='dataset_file',ordinal=i,pageObjectId=ident,datasetFileId=ident) for i,ident in enumerate(ids)]
            raw,sha=frame(dict(version='ltds-viewer-selected-frames-v1',index=0,records=entries),SELECTED_DOMAIN)
            wire=raw+b'\n'; intent=dict(selectedPageHashes=[sha],selectedFrameBytes=len(wire),selectedCount=len(ids))
            by_id={r['datasetFileId']:r for r in records}
            objects=[dict(id=x,name=by_id[x]['relativePath'],byteSize=3,sha256='a'*64,
                          kind=by_id[x]['kind'],mediaType=by_id[x]['mediaType']) for x in ids]
            return verify_selected(intent,wire,source_records=records,pipeline_manifest={'objects':objects})
        self.assertEqual(check(['photo','input'])['selectedCount'],2)
        without_photo=records.pop()
        with self.assertRaises(ValueError):check(['input'])
        records.append(without_photo)
        for ids in (['input','photo'],['photo'],['photo','input','ignored']):
            with self.subTest(ids=ids),self.assertRaises(ValueError):check(ids)
