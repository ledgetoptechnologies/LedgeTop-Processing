"""Source-only proposed full input-v2 manifest validator.

No caller authentication, reservation, transfer, receipt, or runtime route.
The digest is a content commitment, not evidence of origin or disk custody.
"""
import hashlib
import hmac
import re
from . import reference_protocol as p
from .reference_input_budget_v2 import MAX_OBJECTS, MAX_METADATA_BYTES, decode_canonical_input_v2

DOMAIN = b'ledgetop.reference.manifest.v2\0'


def validate_manifest_v2(manifest):
    p._closed(manifest, ('id', 'revision', 'sha256', 'totalBytes', 'objectCount',
                         'inputCounts', 'objects'), 'v2 manifest')
    p._slug(manifest['id'], 'manifest id')
    p._integer(manifest['revision'], 'manifest revision', allow_zero=True)
    p._hash(manifest['sha256'], 'manifest hash')
    p._integer(manifest['totalBytes'], 'manifest bytes', allow_zero=True)
    p._integer(manifest['objectCount'], 'object count')
    objects = manifest['objects']
    p._require(type(objects) is list and 0 < len(objects) <= MAX_OBJECTS
               and manifest['objectCount'] == len(objects), 'invalid object count')
    counts = manifest['inputCounts']
    p._closed(counts, ('images', 'lidarObjects', 'points'), 'input counts')
    for key in counts:
        p._integer(counts[key], key, allow_zero=True)
    ids, names = set(), set()
    total = images = lidar = 0
    for item in objects:
        p._closed(item, ('id', 'name', 'byteSize', 'sha256', 'kind', 'mediaType'), 'manifest object')
        p._slug(item['id'], 'object id')
        name = item['name']
        p._require(type(name) is str and 1 <= len(name) <= 255 and all(32 <= ord(c) <= 126 for c in name)
                   and not any(c in name for c in '/\\:<>"|?*')
                   and not name.endswith(('.', ' ')) and not name.startswith('.')
                   and re.fullmatch(r'.+\.[^.]+', name) is not None
                   and re.match(r'^(?:con|prn|aux|nul|com[1-9]|lpt[1-9])(?:\.|$)', name, re.IGNORECASE) is None,
                   'invalid object name')
        p._hash(item['sha256'], 'object hash')
        p._integer(item['byteSize'], 'object bytes', allow_zero=True)
        kinds = {'image': ('image/jpeg', 'image/png', 'image/tiff'),
                 'lidar': ('application/vnd.las', 'application/vnd.laz'),
                 'auxiliary': ('application/octet-stream', 'application/json', 'text/plain', 'text/csv')}
        p._require(type(item['kind']) is str and item['kind'] in kinds
                   and type(item['mediaType']) is str and item['mediaType'] in kinds[item['kind']], 'invalid media type')
        p._require(item['id'] not in ids and item['name'].lower() not in names,
                   'duplicate object id or name')
        ids.add(item['id']); names.add(item['name'].lower())
        total += item['byteSize']
        p._require(total <= p.MAX_SAFE_INTEGER, 'manifest total overflow')
        images += item['kind'] == 'image'
        lidar += item['kind'] == 'lidar'
    p._require(total == manifest['totalBytes'] and images == counts['images']
               and lidar == counts['lidarObjects'] and (lidar or counts['points'] == 0), 'manifest totals mismatch')
    encoded = p.canonical_json(manifest)
    p._require(len(encoded) <= MAX_METADATA_BYTES, 'manifest metadata limit')
    content = p.canonical_json({key: value for key, value in manifest.items() if key != 'sha256'})
    digest = hashlib.sha256(DOMAIN + content).hexdigest()
    p._require(hmac.compare_digest(digest, manifest['sha256']), 'manifest digest mismatch')
    return manifest


def decode_manifest_v2(raw):
    """Validate canonical raw manifest; this is not an authenticated wire API."""
    return validate_manifest_v2(decode_canonical_input_v2(raw))
