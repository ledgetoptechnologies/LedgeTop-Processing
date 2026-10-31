"""Inert source-proof v2 validators; not an issuer, transport or runtime grant.

The authenticated caller supplies the expected root digest and original input
manifest. These functions do not authenticate either value, validate a grant,
or authorize filesystem access. All wire documents must already be canonical.
"""
import hashlib

from . import reference_protocol as p

VERSION = 2
CHUNK_BYTES = 4 * 1024 * 1024
PAGE_CHUNKS = 256
MAX_CHUNKS = 100_000
MAX_OBJECTS = 10_000
MAX_ROOT_BYTES = 4 * 1024 * 1024
MAX_DESCRIPTOR_BYTES = 64 * 1024
MAX_PAGE_BYTES = 64 * 1024
EMPTY_SHA256 = hashlib.sha256(b'').hexdigest()
ROOT_DOMAIN = b'ltds-source-root-v2\0'
OBJECT_DOMAIN = b'ltds-source-object-v2\0'
PAGE_DOMAIN = b'ltds-source-page-v2\0'


def _document(raw, limit):
    p._require(type(raw) is bytes and len(raw) <= limit, 'bounded canonical proof required')
    value = p.decode_submission(raw)
    p._require(p.canonical_json(value) == raw, 'noncanonical proof document')
    return value


def _hash_document(domain, raw, expected):
    p._hash(expected, 'expected proof hash')
    p._require(hashlib.sha256(domain + raw).hexdigest() == expected, 'proof hash mismatch')


def _version(value):
    p._require(type(value) is int and value == VERSION, 'unsupported proof version')


def validate_root(raw, *, expected_sha256, job_id, generation, environment, input_manifest):
    """Validate a root against an independently authenticated commitment."""
    value = _document(raw, MAX_ROOT_BYTES)
    _hash_document(ROOT_DOMAIN, raw, expected_sha256)
    p._closed(value, ('version', 'jobId', 'generation', 'environment', 'inputManifestSha256',
                      'chunkBytes', 'objects'), 'source proof root')
    _version(value['version'])
    p._slug(job_id, 'expected job ID'); p._integer(generation, 'expected generation')
    p._environment(environment)
    p._slug(value['jobId'], 'source proof job ID'); p._integer(value['generation'], 'source proof generation')
    p._environment(value['environment'])
    p._require((value['jobId'], value['generation'], value['environment']) ==
               (job_id, generation, environment), 'source proof identity mismatch')
    p._require(type(value['chunkBytes']) is int and value['chunkBytes'] == CHUNK_BYTES,
               'unsupported source proof chunk size')
    p._require(type(input_manifest) is dict and input_manifest.get('sha256') == p.manifest_sha256(input_manifest),
               'authenticated input manifest mismatch')
    p._hash(value['inputManifestSha256'], 'input manifest hash')
    p._require(value['inputManifestSha256'] == input_manifest['sha256'], 'input manifest digest mismatch')
    inputs = input_manifest.get('objects')
    p._require(type(inputs) is list and 0 < len(inputs) <= MAX_OBJECTS, 'invalid input closure')
    ids = []
    for item in inputs:
        p._require(type(item) is dict and {'id', 'byteSize', 'sha256'} <= set(item), 'invalid input object')
        p._slug(item['id'], 'input ID'); p._integer(item['byteSize'], 'input size', allow_zero=True)
        p._hash(item['sha256'], 'input hash'); ids.append(item['id'])
    p._require(len(ids) == len(set(ids)), 'duplicate input ID')
    objects = value['objects']
    p._require(type(objects) is list and len(objects) == len(ids), 'source proof closure mismatch')
    found = []
    for item in objects:
        p._closed(item, ('objectId', 'objectProofSha256'), 'source proof root object')
        p._slug(item['objectId'], 'source object ID'); p._hash(item['objectProofSha256'], 'object proof hash')
        found.append(item['objectId'])
    p._require(found == sorted(ids), 'source proof object order or closure mismatch')
    return value


def validate_object(raw, *, expected_sha256, input_object):
    """Validate a descriptor against the authenticated root and input object."""
    value = _document(raw, MAX_DESCRIPTOR_BYTES)
    _hash_document(OBJECT_DOMAIN, raw, expected_sha256)
    p._closed(value, ('version', 'objectId', 'byteSize', 'sha256', 'chunkCount', 'pageDigests'),
              'source proof object')
    _version(value['version']); p._slug(value['objectId'], 'source object ID')
    p._integer(value['byteSize'], 'source object size', allow_zero=True); p._hash(value['sha256'], 'source hash')
    p._require(type(input_object) is dict, 'validated input object required')
    p._slug(input_object.get('id'), 'input object ID')
    p._integer(input_object.get('byteSize'), 'input object size', allow_zero=True)
    p._hash(input_object.get('sha256'), 'input object hash')
    p._require(type(input_object) is dict and (value['objectId'], value['byteSize'], value['sha256']) ==
               (input_object.get('id'), input_object.get('byteSize'), input_object.get('sha256')),
               'source object binding mismatch')
    count = (value['byteSize'] + CHUNK_BYTES - 1) // CHUNK_BYTES
    p._require(type(value['chunkCount']) is int and value['chunkCount'] == count and count <= MAX_CHUNKS,
               'source chunk count mismatch')
    pages = value['pageDigests']
    p._require(type(pages) is list and len(pages) == (count + PAGE_CHUNKS - 1) // PAGE_CHUNKS,
               'source page count mismatch')
    for digest in pages: p._hash(digest, 'source page hash')
    p._require(value['byteSize'] != 0 or value['sha256'] == EMPTY_SHA256, 'empty source hash mismatch')
    return value


def validate_page(raw, *, object_descriptor, page_index):
    """Check one page against a previously validated object descriptor."""
    p._require(type(page_index) is int and 0 <= page_index < len(object_descriptor['pageDigests']),
               'source page index out of bounds')
    value = _document(raw, MAX_PAGE_BYTES)
    _hash_document(PAGE_DOMAIN, raw, object_descriptor['pageDigests'][page_index])
    p._closed(value, ('version', 'objectId', 'pageIndex', 'chunks'), 'source proof page')
    _version(value['version'])
    p._require(value['objectId'] == object_descriptor['objectId'] and
               type(value['pageIndex']) is int and value['pageIndex'] == page_index,
               'source page identity mismatch')
    first = page_index * PAGE_CHUNKS
    expected_count = min(PAGE_CHUNKS, object_descriptor['chunkCount'] - first)
    chunks = value['chunks']
    p._require(type(chunks) is list and len(chunks) == expected_count, 'source page chunk count mismatch')
    for index, chunk in enumerate(chunks, first):
        p._closed(chunk, ('byteOffset', 'byteSize', 'sha256'), 'source proof chunk')
        offset = index * CHUNK_BYTES
        length = min(CHUNK_BYTES, object_descriptor['byteSize'] - offset)
        p._require(type(chunk['byteOffset']) is int and chunk['byteOffset'] == offset and
                   type(chunk['byteSize']) is int and chunk['byteSize'] == length,
                   'source chunk layout mismatch')
        p._hash(chunk['sha256'], 'source chunk hash')
    return value
