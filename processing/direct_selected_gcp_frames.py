"""Unwired held-snapshot frame checks; caller authenticates signed intent.

These routines do not validate GCP coordinate semantics, grant custody, or
allow release. They must not be used with unverified intent or manifests.
"""
import base64
import codecs
import decimal
import hashlib
import json
import math
import re
from pathlib import PurePosixPath
from .direct_v2_snapshot_verifier import _decode

SELECTED_DOMAIN=b'ltds-viewer-selected-frame-v1\0'
EVIDENCE_DOMAIN=b'ltds-viewer-gcp-evidence-frame-v2\0'
EVIDENCE_DOMAIN_V1=b'ltds-viewer-gcp-evidence-frame-v1\0'
FILE_DOMAIN=b'ltds-viewer-generated-gcp-file-frame-v1\0'
PHOTO=re.compile(r'\.(?:jpe?g|png|tiff?|dng|raw|heic)\Z',re.IGNORECASE|re.ASCII)
AUXILIARY=re.compile(r'\.(?:txt|geojson|json|zip|las|laz)\Z',re.IGNORECASE|re.ASCII)


def _lines(raw, hashes, *, total_bytes, limit, page_limit):
    if (type(raw) is not bytes or type(total_bytes) is not int or total_bytes!=len(raw)
            or not raw.endswith(b'\n') or not 0<len(raw)<=limit or type(hashes) is not list
            or not 1<=len(hashes)<=page_limit):raise ValueError('frame stream invalid')
    lines=raw[:-1].split(b'\n')
    if len(lines)!=len(hashes) or any(type(x) is not str or not re.fullmatch('[a-f0-9]{64}',x) for x in hashes):
        raise ValueError('frame inventory invalid')
    return lines


def verify_selected(intent, frames, *, source_records, pipeline_manifest):
    """Validate exact ordered selected IDs against signed sources and pipeline."""
    lines=_lines(frames,intent['selectedPageHashes'],total_bytes=intent['selectedFrameBytes'],
                 limit=12*1024*1024,page_limit=64)
    sources={r['datasetFileId']:r for r in source_records if r.get('type')=='dataset_file'}
    if len(sources)!=sum(r.get('type')=='dataset_file' for r in source_records):raise ValueError('duplicate source')
    objects=pipeline_manifest['objects']; selected=[]
    generated=intent.get('gcp')
    generated=generated.get('generatedGcp') if type(generated) is dict else None
    attempt_id=intent.get('attemptId')
    generated_id=f"gcp_{attempt_id}" if type(attempt_id) is str else None
    for index,line in enumerate(lines):
        page=_decode(line,256*1024)
        if (type(page) is not dict or set(page)!={'version','index','records'}
                or page['version']!='ltds-viewer-selected-frames-v1' or type(page['index']) is not int
                or page['index']!=index or type(page['records']) is not list or not 1<=len(page['records'])<=512
                or hashlib.sha256(SELECTED_DOMAIN+line).hexdigest()!=intent['selectedPageHashes'][index]):
            raise ValueError('selected page invalid')
        for record in page['records']:
            if type(record) is not dict or type(record.get('ordinal')) is not int or record['ordinal']!=len(selected):
                raise ValueError('selected record invalid')
            if set(record)=={'type','ordinal','pageObjectId','datasetFileId'} and record['type']=='dataset_file' and record['pageObjectId']==record['datasetFileId']:
                selected.append(('dataset_file',record['datasetFileId']))
            elif (set(record)=={'type','ordinal','pageObjectId','attemptId','mediaType'} and record['type']=='attempt_generated_gcp'
                    and record['pageObjectId']==generated_id and record['attemptId']==intent['attemptId'] and record['mediaType']=='text/plain'):
                if generated is None:raise ValueError('unbound generated GCP selection')
                selected.append(('attempt_generated_gcp',generated_id))
            else:raise ValueError('selected record invalid')
            if len(selected)>20000:raise ValueError('selected count invalid')
    ids=[identifier for _,identifier in selected]
    if (type(intent['selectedCount']) is not int or intent['selectedCount']!=len(selected)
            or len(ids)!=len(set(ids)) or len(selected)!=len(objects)):
        raise ValueError('selected closure invalid')
    # Match Viewer's photo-first selection, preserving source order within each group.
    photos=[];inputs=[]
    for source in source_records:
        if source.get('type')!='dataset_file':continue
        role=source['processingRole'];name=source['relativePath'];photo=bool(PHOTO.search(name))
        if generated is not None and PurePosixPath(name).name.lower()=='gcp_list.txt':continue
        if role in ('gcp_source','administrative') or (role=='auto' and not photo):continue
        if role=='image' and not photo:raise ValueError('unsupported selected image')
        if role=='provider_input' and not AUXILIARY.search(name):raise ValueError('unsupported provider input')
        (photos if photo else inputs).append(source['datasetFileId'])
    expected=[('dataset_file',identifier) for identifier in photos+inputs]
    if generated is not None:expected.append(('attempt_generated_gcp',generated_id))
    if selected!=expected or not photos:raise ValueError('selected source policy changed')
    input_files=[]
    for (kind,identifier),obj in zip(selected,objects):
        if kind=='attempt_generated_gcp':
            if (obj.get('id')!=generated_id or obj.get('name')!='gcp_list.txt'
                    or obj.get('byteSize')!=generated.get('byteSize') or obj.get('sha256')!=generated.get('sha256')
                    or obj.get('kind')!='auxiliary' or obj.get('mediaType')!='text/plain'):
                raise ValueError('selected generated GCP object mismatch')
            input_files.append({'relativePath':'gcp_list.txt','byteSize':generated['byteSize'],'sha256':generated['sha256']})
            continue
        source=sources.get(identifier)
        if (source is None or source['processingRole'] in ('gcp_source','administrative')
                or identifier!=obj['id'] or source['relativePath'].rsplit('/',1)[-1]!=obj['name']
                or source['byteSize']!=obj['byteSize'] or source['sha256']!=obj['sha256']
                or source['kind']!=obj['kind'] or source['mediaType']!=obj['mediaType']):
            raise ValueError('selected object mismatch')
        input_files.append({'relativePath':source['relativePath'],'byteSize':source['byteSize'],'sha256':source['sha256']})
    if 'inputManifestSha256' in intent and hashlib.sha256(_js_stringify(input_files).encode('utf-8')).hexdigest()!=intent['inputManifestSha256']:
        raise ValueError('Viewer input manifest digest changed')
    selected_image_ids=[identifier for kind,identifier in selected if kind=='dataset_file' and sources[identifier]['kind']=='image']
    return {'state':'selected_pages_verified_not_custody','selectedCount':len(selected),'selectedImageIds':selected_image_ids,
            'sourceCustodyAccepted':False,'processingAuthorized':False}


def _verify_gcp_chunks_payload(raw, *, hashes, page_bytes, stream_bytes, byte_count,
                               sha256, attempt_id, kind, evidence_sha256=None, version=None):
    """Check signed chunk bytes only; GCP semantic evidence remains held."""
    if kind=='evidence':
        version=version or 'ltds-viewer-gcp-evidence-chunks-v2'
        if version=='ltds-viewer-gcp-evidence-chunks-v2':domain=EVIDENCE_DOMAIN
        elif version=='ltds-viewer-gcp-evidence-chunks-v1':domain=EVIDENCE_DOMAIN_V1
        else:raise ValueError('chunk version invalid')
    elif kind=='file':
        if version not in (None,'ltds-viewer-generated-gcp-file-chunks-v1'):raise ValueError('chunk version invalid')
        version,domain='ltds-viewer-generated-gcp-file-chunks-v1',FILE_DOMAIN
    else:raise ValueError('chunk kind invalid')
    if (type(byte_count) is not int or not 1<=byte_count<=16*1024*1024
            or type(sha256) is not str or not re.fullmatch('[a-f0-9]{64}',sha256)
            or (kind=='file' and (type(evidence_sha256) is not str or not re.fullmatch('[a-f0-9]{64}',evidence_sha256)))):
        raise ValueError('chunk metadata invalid')
    lines=_lines(raw,hashes,total_bytes=stream_bytes,limit=24*1024*1024,page_limit=512)
    if type(page_bytes) is not list or len(page_bytes)!=len(lines) or len(lines)<2:raise ValueError('chunk inventory invalid')
    digest=hashlib.sha256(); offset=0; utf8=codecs.getincrementaldecoder('utf-8')('strict'); chunks=[]
    for index,line in enumerate(lines):
        frame=_decode(line,65535)
        common={'version','attemptId','index','offset','evidenceSha256'}
        if kind=='file':common.add('fileSha256')
        terminal=index==len(lines)-1
        required=common|({'complete','totalBytes'} if terminal else {'data','decodedByteCount'})
        if (type(frame) is not dict or set(frame)!=required or frame['version']!=version
                or frame['attemptId']!=attempt_id or type(frame['index']) is not int or frame['index']!=index
                or type(frame['offset']) is not int or frame['offset']!=offset
                or type(page_bytes[index]) is not int or page_bytes[index]!=len(line)
                or hashlib.sha256(domain+line).hexdigest()!=hashes[index]
                or frame['evidenceSha256']!=(sha256 if kind=='evidence' else evidence_sha256)
                or (kind=='file' and frame['fileSha256']!=sha256)):
            raise ValueError('chunk binding changed')
        if terminal:
            if frame['complete'] is not True or type(frame['totalBytes']) is not int or frame['totalBytes']!=offset:raise ValueError('chunk closure invalid')
            continue
        data=frame['data']
        if type(data) is not str or not re.fullmatch('[A-Za-z0-9_-]+',data) or len(data)>61440:
            raise ValueError('chunk data invalid')
        chunk=base64.urlsafe_b64decode(data+'='*((-len(data))%4))
        if (base64.urlsafe_b64encode(chunk).decode().rstrip('=')!=data or type(frame['decodedByteCount']) is not int
                or not 1<=len(chunk)<=46080 or frame['decodedByteCount']!=len(chunk)):
            raise ValueError('chunk data invalid')
        offset+=len(chunk)
        if offset>byte_count:raise ValueError('chunk size exceeded')
        utf8.decode(chunk,final=False);digest.update(chunk);chunks.append(chunk)
    utf8.decode(b'',final=True)
    if offset!=byte_count or digest.hexdigest()!=sha256:raise ValueError('chunk digest changed')
    return b''.join(chunks)


def verify_gcp_chunks(raw, *, hashes, page_bytes, stream_bytes, byte_count,
                      sha256, attempt_id, kind, evidence_sha256=None, version=None):
    _verify_gcp_chunks_payload(raw,hashes=hashes,page_bytes=page_bytes,stream_bytes=stream_bytes,byte_count=byte_count,
        sha256=sha256,attempt_id=attempt_id,kind=kind,evidence_sha256=evidence_sha256,version=version)
    return {'state':'gcp_chunk_bytes_verified_not_semantics','sourceCustodyAccepted':False,'processingAuthorized':False}


def _pairs(items):
    out={}
    for key,value in items:
        if key in out:raise ValueError('duplicate evidence key')
        out[key]=value
    return out


def _strict_json(raw):
    def invalid(_):raise ValueError('invalid JSON number')
    try:
        value=json.loads(raw.decode('utf-8'),object_pairs_hook=_pairs,parse_constant=invalid)
    except (ValueError,UnicodeError,RecursionError):raise ValueError('GCP evidence JSON invalid') from None
    def check(item,depth=0):
        if depth>24:raise ValueError('GCP evidence depth invalid')
        if type(item) is dict:
            for key,child in item.items():
                if type(key) is not str or any(ord(char)<32 for char in key):raise ValueError('GCP evidence key invalid')
                check(child,depth+1)
        elif type(item) is list:
            for child in item:check(child,depth+1)
        elif type(item) is float and not math.isfinite(item):raise ValueError('GCP evidence number invalid')
        elif item is not None and type(item) not in (str,bool,int,float):raise ValueError('GCP evidence value invalid')
    check(value)
    return value


def _js_number(value):
    if type(value) is int:return str(value)
    if type(value) is not float or not math.isfinite(value):raise ValueError('GCP numeric value invalid')
    if value==0:return '0'
    text=repr(value).lower()
    if 'e' not in text:
        if text.endswith('.0'):text=text[:-2]
        return text
    mantissa,exponent=text.split('e');exponent=int(exponent)
    if -6<=exponent<21:
        sign=''
        if mantissa.startswith('-'):sign,mantissa='-',mantissa[1:]
        digits=mantissa.replace('.','');point=(mantissa.find('.') if '.' in mantissa else len(mantissa))+exponent
        if point<=0:return sign+'0.'+'0'*(-point)+digits
        if point>=len(digits):return sign+digits+'0'*(point-len(digits))
        return sign+digits[:point]+'.'+digits[point:]
    if mantissa.endswith('.0'):mantissa=mantissa[:-2]
    return mantissa+'e'+('+' if exponent>=0 else '')+str(exponent)


def _js_stringify(value):
    if value is None:return 'null'
    if type(value) is bool:return 'true' if value else 'false'
    if type(value) in (int,float):return _js_number(value)
    if type(value) is str:return json.dumps(value,ensure_ascii=False,separators=(',',':'))
    if type(value) is list:return '['+','.join(_js_stringify(child) for child in value)+']'
    if type(value) is dict:return '{'+','.join(_js_stringify(key)+':'+_js_stringify(child) for key,child in value.items())+'}'
    raise ValueError('unsupported JSON value')


def _scalar(value):
    if type(value) not in (int,float) or not math.isfinite(value):raise ValueError('GCP coordinate invalid')
    if type(value) is int or value.is_integer():return str(int(value))
    rounded=decimal.Decimal.from_float(value).quantize(decimal.Decimal('0.00000001'),rounding=decimal.ROUND_HALF_UP)
    return format(rounded,'f').rstrip('0').rstrip('.')


def _coordinate_header(horizontal,vertical,height_source):
    normalized=re.sub(r'[\s_()-]','',vertical or '').lower()
    if re.fullmatch(r'navd88(?:geoid\d+[a-z]*)?height',normalized):
        if horizontal!='EPSG:6318':raise ValueError('unsupported NAVD88 horizontal CRS')
        if height_source=='source':raise ValueError('NAVD88 source height transform unavailable')
        return 'EPSG:6349'
    if re.fullmatch(r'(?:ellipsoidal?|ellipsoid)height',normalized):
        return 'EPSG:6319' if horizontal=='EPSG:6318' else 'EPSG:4979'
    if normalized=='egm96height':return horizontal+'+5773'
    if normalized=='egm2008height':return horizontal+'+3855'
    if normalized:raise ValueError('unsupported vertical coordinate system')
    return horizontal


def verify_gcp_semantics(*, evidence_bytes, file_bytes, generated_gcp, source_records, attempt_id, selected_image_ids):
    """Bind retained evidence and exact NodeODM coordinates to held source records."""
    evidence=_strict_json(evidence_bytes)
    if type(evidence) is not dict or type(evidence.get('schemaVersion')) is not int or evidence.get('schemaVersion') not in (1,2,3):
        raise ValueError('GCP evidence shape invalid')
    schema=evidence['schemaVersion']
    expected_evidence={'schemaVersion','snapshot','correspondences'}|({'coordinateContract'} if schema>=2 else set())
    if set(evidence)!=expected_evidence:raise ValueError('GCP evidence shape invalid')
    snapshot=evidence['snapshot'];marks=evidence['correspondences']
    if type(snapshot) is not dict or type(marks) is not list or not marks:
        raise ValueError('GCP evidence empty')
    required_snapshot={'snapshotId','byteSize','sha256','sourceGcpDatasetFileIds','imageDatasetFileIds','correspondenceCount'}
    if (set(snapshot)!=required_snapshot or snapshot['snapshotId']!=attempt_id or type(snapshot['byteSize']) is not int or snapshot['byteSize']!=len(file_bytes)
            or type(snapshot['correspondenceCount']) is not int
            or snapshot['sha256']!=hashlib.sha256(file_bytes).hexdigest() or snapshot['correspondenceCount']!=len(marks)):
        raise ValueError('GCP snapshot binding changed')
    contract=evidence.get('coordinateContract')
    if schema==1:
        if 'coordinateContract' in evidence:raise ValueError('legacy GCP contract changed')
        contract={'horizontalCrs':'EPSG:4326','verticalSystem':'','heightSource':'source','schemaVersion':1}
    else:
        if 'coordinateContract' not in evidence:raise ValueError('GCP coordinate contract unavailable')
        contract=evidence['coordinateContract']
        if (type(contract) is not dict or type(contract.get('schemaVersion')) is not int
                or contract.get('schemaVersion') not in (1,2)):raise ValueError('GCP coordinate contract invalid')
        keys={'schemaVersion','horizontalCrs','verticalSystem','zSource','sourceUnits','outputZUnit','sourceCoordinateSystem','parser'}
        if contract['schemaVersion']==2:keys|={'heightSource','outputVerticalSystem'}
        if (set(contract)!=keys or contract['horizontalCrs'] not in ('EPSG:4326','EPSG:6318') or contract['outputZUnit']!='m'
                or not all(type(contract.get(key)) is str and contract[key].strip() for key in ('verticalSystem','zSource','sourceUnits','sourceCoordinateSystem','parser'))):
            raise ValueError('GCP coordinate contract invalid')
    height_source=contract.get('heightSource','source')
    if schema==3 and (contract.get('schemaVersion')!=2 or height_source not in ('source','ellipsoidal')
            or (height_source=='ellipsoidal' and contract.get('outputVerticalSystem')!='ellipsoidal height')
            or (contract.get('outputVerticalSystem') is not None and type(contract.get('outputVerticalSystem')) is not str)):
        raise ValueError('GCP height contract invalid')
    if height_source not in ('source','ellipsoidal'):raise ValueError('GCP height source invalid')
    by_id={record['datasetFileId']:record for record in source_records if record.get('type')=='dataset_file'}
    if len(by_id)!=sum(record.get('type')=='dataset_file' for record in source_records):raise ValueError('duplicate source object')
    source_ids=set();image_ids=set();correspondence_ids=set();point_image_pairs=set();source_facts={};image_facts={};rows=[]
    mark_keys={'correspondenceId','gcpPointId','gcpSetId','externalId','label','latitude','longitude','elevationM','ellipsoidalHeightM',
        'sourceGcpDatasetFileId','sourceGcpSha256','sourceGcpRelativePath','imageDatasetFileId','imageRelativePath','imageWidth','imageHeight','pixelX','pixelY'}
    for mark in marks:
        if type(mark) is not dict or set(mark)!=mark_keys:raise ValueError('GCP correspondence shape invalid')
        if any(type(mark[key]) is not str or not mark[key] for key in ('correspondenceId','gcpPointId','gcpSetId','externalId','sourceGcpDatasetFileId','sourceGcpRelativePath','imageDatasetFileId','imageRelativePath')):
            raise ValueError('GCP correspondence identity invalid')
        if mark['label'] is not None and type(mark['label']) is not str:raise ValueError('GCP label invalid')
        numeric=('latitude','longitude','elevationM','pixelX','pixelY')
        if any(type(mark[key]) not in (int,float) or not math.isfinite(mark[key]) for key in numeric):raise ValueError('GCP coordinate invalid')
        if not -90<=mark['latitude']<=90 or not -180<=mark['longitude']<=180 or not -12000<=mark['elevationM']<=100000 or mark['pixelX']<0 or mark['pixelY']<0:raise ValueError('GCP coordinate out of range')
        for key in ('imageWidth','imageHeight'):
            value=mark[key]
            if value is not None and (type(value) is not int or value<=0):raise ValueError('GCP image dimensions invalid')
        if (mark['imageWidth'] is not None and mark['pixelX']>=mark['imageWidth']) or (mark['imageHeight'] is not None and mark['pixelY']>=mark['imageHeight']):raise ValueError('GCP pixel outside image')
        if height_source=='ellipsoidal' and (type(mark['ellipsoidalHeightM']) not in (int,float) or not math.isfinite(mark['ellipsoidalHeightM']) or not -12000<=mark['ellipsoidalHeightM']<=100000):raise ValueError('GCP ellipsoidal height missing')
        source=by_id.get(mark['sourceGcpDatasetFileId']);image=by_id.get(mark['imageDatasetFileId'])
        if (not source or source['processingRole']!='gcp_source' or source['sha256']!=mark['sourceGcpSha256']
                or source['relativePath']!=mark['sourceGcpRelativePath'] or not image or image['kind']!='image'
                or image['relativePath']!=mark['imageRelativePath'] or mark['imageDatasetFileId'] not in selected_image_ids):
            raise ValueError('GCP source or selected image binding changed')
        if mark['correspondenceId'] in correspondence_ids:raise ValueError('duplicate GCP correspondence')
        pair=(mark['gcpPointId'],mark['imageDatasetFileId'])
        if pair in point_image_pairs:raise ValueError('duplicate GCP point image pair')
        if mark['sourceGcpDatasetFileId'] in source_facts and source_facts[mark['sourceGcpDatasetFileId']]!=(mark['sourceGcpSha256'],mark['sourceGcpRelativePath']):raise ValueError('GCP source facts conflict')
        if mark['imageDatasetFileId'] in image_facts and image_facts[mark['imageDatasetFileId']]!=(mark['imageRelativePath'],mark['imageWidth'],mark['imageHeight']):raise ValueError('GCP image facts conflict')
        correspondence_ids.add(mark['correspondenceId']);point_image_pairs.add(pair);source_facts[mark['sourceGcpDatasetFileId']]=(mark['sourceGcpSha256'],mark['sourceGcpRelativePath']);image_facts[mark['imageDatasetFileId']]=(mark['imageRelativePath'],mark['imageWidth'],mark['imageHeight'])
        source_ids.add(mark['sourceGcpDatasetFileId']);image_ids.add(mark['imageDatasetFileId'])
        basename=PurePosixPath(mark['imageRelativePath']).name
        if not basename or re.search(r'\s',basename):raise ValueError('ODM image basename invalid')
        z=mark['ellipsoidalHeightM'] if height_source=='ellipsoidal' else mark['elevationM']
        rows.append(' '.join((_scalar(mark['longitude']),_scalar(mark['latitude']),_scalar(z),_scalar(mark['pixelX']),_scalar(mark['pixelY']),basename)))
    if (type(snapshot['sourceGcpDatasetFileIds']) is not list or type(snapshot['imageDatasetFileIds']) is not list
            or snapshot['sourceGcpDatasetFileIds']!=sorted(source_ids) or snapshot['imageDatasetFileIds']!=sorted(image_ids)):
        raise ValueError('GCP source and image inventory changed')
    source_files=sorted(({'datasetFileId':identifier,'relativePath':source_facts[identifier][1],'sha256':source_facts[identifier][0]} for identifier in source_ids),key=lambda row:row['datasetFileId'])
    correspondence_hash=hashlib.sha256(_js_stringify(marks).encode('utf-8')).hexdigest()
    if (type(generated_gcp) is not dict or set(generated_gcp)!={'byteSize','sha256','attemptId','correspondenceSha256','sourceFiles'}
            or generated_gcp['attemptId']!=attempt_id or generated_gcp['byteSize']!=len(file_bytes)
            or generated_gcp['sha256']!=hashlib.sha256(file_bytes).hexdigest() or generated_gcp['correspondenceSha256']!=correspondence_hash
            or generated_gcp['sourceFiles']!=source_files):raise ValueError('generated GCP metadata mismatch')
    vertical=contract.get('outputVerticalSystem') if height_source=='ellipsoidal' else contract.get('verticalSystem')
    expected=(_coordinate_header(contract['horizontalCrs'],vertical or '',height_source)+'\n'+'\n'.join(rows)+'\n').encode('utf-8')
    if file_bytes!=expected:raise ValueError('generated NodeODM GCP file does not match evidence')
    return {'state':'gcp_evidence_and_generated_file_verified_not_source_csv_reparsed','correspondenceCount':len(marks),
            'rawSourceCsvBytesReparsed':False,'sourceCustodyAccepted':False,'processingAuthorized':False}
