"""Inactive semantic check of Viewer ODM snapshot; not a source authority."""
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, localcontext
import math
import re

from . import reference_protocol as p
from .source_inventory_candidate import MAX_CORRESPONDENCES, MAX_GENERATED_GCP_BYTES, reference_v1_names

_NUMBER=re.compile(r'-?(?:0|[1-9][0-9]*)(?:\.[0-9]{0,7}[1-9])?\Z')
_FIELDS=('sourceGcpDatasetFileId','imageDatasetFileId','pointId','longitude','latitude','elevationM','pixelX','pixelY')


def _rounded(value, low, high):
    if type(value) not in (int,float) or not low <= value <= high or not math.isfinite(value):
        raise ValueError('invalid trusted GCP coordinate')
    # Viewer converts through JS binary64 and renders nonintegers with toFixed(8).
    number=float(value)
    if number.is_integer(): return Decimal(int(number))
    with localcontext() as ctx:
        ctx.prec=80
        return Decimal.from_float(number).quantize(Decimal('0.00000001'),rounding=ROUND_HALF_UP)


def validate_gcp_semantics(raw, *, processing, correspondences):
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
    try: text=raw.decode('utf8')
    except UnicodeError: raise ValueError('invalid GCP UTF-8') from None
    if not text.endswith('\n') or '\r' in text: raise ValueError('invalid GCP line endings')
    lines=text[:-1].split('\n')
    if len(lines)!=len(correspondences)+1 or lines[0]!='EPSG:4326': raise ValueError('GCP row count or CRS mismatch')
    seen=set(); seen_rows=set()
    for line,row in zip(lines[1:],correspondences):
        p._closed(row,_FIELDS,'trusted GCP row')
        for field in ('sourceGcpDatasetFileId','imageDatasetFileId','pointId'): p._slug(row[field],field)
        pair=(row['pointId'],row['imageDatasetFileId'])
        if pair in seen: raise ValueError('duplicate GCP correspondence')
        seen.add(pair)
        obj=files.get(row['imageDatasetFileId'])
        if obj is None: raise ValueError('unknown GCP image')
        parts=line.split(' ')
        if len(parts)!=6 or any(not s for s in parts): raise ValueError('invalid GCP row')
        if parts[5] != obj['relativePath'].split('/')[-1] or any(ch.isspace() for ch in parts[5]):
            raise ValueError('GCP image name mismatch')
        limits=((-180,180),(-90,90),(-1e9,1e9),(0,1e9),(0,1e9))
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
