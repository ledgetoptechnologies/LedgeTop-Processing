"""Inactive semantic check of Viewer ODM snapshot; not a source authority."""
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, localcontext
import math
import re

from . import reference_protocol as p
from .source_inventory_candidate import MAX_CORRESPONDENCES, MAX_GENERATED_GCP_BYTES, reference_v1_names

_NUMBER=re.compile(r'-?(?:0|[1-9][0-9]*)(?:\.[0-9]{0,7}[1-9])?\Z')
_FIELDS=('correspondenceId','gcpPointId','gcpSetId','externalId','label','latitude','longitude','elevationM',
         'sourceGcpDatasetFileId','sourceGcpSha256','sourceGcpRelativePath',
         'imageDatasetFileId','imageRelativePath','imageWidth','imageHeight','pixelX','pixelY')


def _rounded(value, low, high):
    if type(value) not in (int,float) or not low <= value <= high or not math.isfinite(value):
        raise ValueError('invalid trusted GCP coordinate')
    # Viewer converts through JS binary64 and renders nonintegers with toFixed(8).
    number=float(value)
    if number.is_integer(): return Decimal(int(number))
    with localcontext() as ctx:
        ctx.prec=80
        return Decimal.from_float(number).quantize(Decimal('0.00000001'),rounding=ROUND_HALF_UP)


def validate_gcp_semantics(raw, *, processing, retained, authoritative_files, correspondences):
    """Match actual bytes to an independently authenticated immutable row list.

    This verifies neither snapshot authenticity nor source/correspondence
    immutability; callers need a transactional Viewer-supplied binding.
    """
    if type(raw) is not bytes or len(raw)>MAX_GENERATED_GCP_BYTES: raise ValueError('invalid GCP bytes')
    if type(correspondences) is not list or not correspondences or len(correspondences)>MAX_CORRESPONDENCES:
        raise ValueError('invalid trusted GCP rows')
    names=reference_v1_names(processing)
    if 'gcp_list.txt' in {name.lower() for name in names}: raise ValueError('GCP filename collision')
    files={item['datasetFileId']:item for item in processing if item['processingRole']=='image'}
    if len(files)!=sum(item['processingRole']=='image' for item in processing): raise ValueError('duplicate image identity')
    sources={item['datasetFileId']:item for item in retained if item['processingRole']=='gcp_source'}
    indexed={item['datasetFileId']:item for item in authoritative_files}
    if len(indexed)!=len(authoritative_files): raise ValueError('duplicate authoritative identity')
    try: text=raw.decode('utf8')
    except UnicodeError: raise ValueError('invalid GCP UTF-8') from None
    if not text.endswith('\n') or '\r' in text: raise ValueError('invalid GCP line endings')
    lines=text[:-1].split('\n')
    if len(lines)!=len(correspondences)+1 or lines[0]!='EPSG:4326': raise ValueError('GCP row count or CRS mismatch')
    seen=set(); seen_rows=set(); seen_ids=set()
    for line,row in zip(lines[1:],correspondences):
        p._closed(row,_FIELDS,'trusted GCP row')
        for field in ('sourceGcpDatasetFileId','imageDatasetFileId','gcpPointId','gcpSetId','correspondenceId'): p._slug(row[field],field)
        for field in ('externalId','label'):
            if type(row[field]) is not str or not row[field] or len(row[field].encode('utf8'))>1024:
                raise ValueError('invalid GCP point label')
        pair=(row['gcpPointId'],row['imageDatasetFileId'])
        if pair in seen or row['correspondenceId'] in seen_ids: raise ValueError('duplicate GCP correspondence')
        seen.add(pair);seen_ids.add(row['correspondenceId'])
        obj=files.get(row['imageDatasetFileId'])
        if obj is None: raise ValueError('unknown GCP image')
        source=sources.get(row['sourceGcpDatasetFileId']); source_index=indexed.get(row['sourceGcpDatasetFileId'])
        image_index=indexed.get(row['imageDatasetFileId'])
        if (source is None or source_index is None or image_index is None
                or (row['sourceGcpRelativePath'],row['sourceGcpSha256']) != (source['relativePath'],source['sha256'])
                or (row['sourceGcpRelativePath'],row['sourceGcpSha256']) != (source_index['relativePath'],source_index['sha256'])
                or row['imageRelativePath'] != obj['relativePath'] or row['imageRelativePath'] != image_index['relativePath']):
            raise ValueError('GCP dataset file binding mismatch')
        for dimension in ('imageWidth','imageHeight'):
            value=row[dimension]
            if value is not None and (type(value) is not int or not 0<value<=p.MAX_SAFE_INTEGER):
                raise ValueError('invalid image dimension')
            if value != image_index[dimension] or type(value) is not type(image_index[dimension]):
                raise ValueError('image dimension binding mismatch')
        _rounded(row['pixelX'],0,1e9); _rounded(row['pixelY'],0,1e9)
        if ((row['imageWidth'] is not None and row['pixelX'] >= row['imageWidth'])
                or (row['imageHeight'] is not None and row['pixelY'] >= row['imageHeight'])):
            raise ValueError('GCP pixel outside image')
        parts=line.split(' ')
        if len(parts)!=6 or any(not s for s in parts): raise ValueError('invalid GCP row')
        if parts[5] != obj['relativePath'].split('/')[-1] or any(ch.isspace() for ch in parts[5]):
            raise ValueError('GCP image name mismatch')
        limits=((-180,180),(-90,90),(-12000,100000),(0,1e9),(0,1e9))
        numeric=[]
        for token,field,(lo,hi) in zip(parts[:5],('longitude','latitude','elevationM','pixelX','pixelY'),limits):
            if len(token)>64 or not _NUMBER.fullmatch(token): raise ValueError('invalid GCP number')
            try: actual=Decimal(token)
            except InvalidOperation: raise ValueError('invalid GCP number') from None
            if not actual.is_finite() or actual != _rounded(row[field],lo,hi): raise ValueError('GCP coordinate mismatch')
            numeric.append(actual)
        physical=(*numeric,parts[5])
        if physical in seen_rows: raise ValueError('ambiguous duplicate GCP row')
        seen_rows.add(physical)
