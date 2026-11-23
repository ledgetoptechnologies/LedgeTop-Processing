"""Inspect external resource closure without reading geometry/texture payloads.

Only dependency JSON is buffered (32MiB maximum per metadata object). Larger or
unsupported metadata holds CLEANUP, never rejects/limits the processing job.
"""
import json
import math
import os
from pathlib import PurePosixPath
import shlex
import struct
import xml.etree.ElementTree as ET
from .intermediate_pdf import PdfHeld, inspect_pdf

MAX_JSON = 32 * 1024 * 1024
GLTF_EXTENSIONS = {'KHR_draco_mesh_compression','KHR_materials_unlit','KHR_texture_transform',
                   'KHR_mesh_quantization','EXT_texture_webp','KHR_texture_basisu',
                   'EXT_meshopt_compression','KHR_materials_pbrSpecularGlossiness',
                   'KHR_materials_specular','KHR_materials_ior','KHR_materials_transmission',
                   'KHR_materials_clearcoat','KHR_materials_volume','KHR_materials_sheen','CESIUM_RTC'}
TILES_EXTENSIONS = {'3DTILES_content_gltf','3DTILES_multiple_contents','3DTILES_metadata',
                    '3DTILES_batch_table_hierarchy'}


class DependencyHeld(ValueError):
    pass


class References(list):
    def __init__(self, values=(), buffers=(), nonbuffers=None):
        super().__init__(values);self.buffers=set(buffers)
        self.nonbuffers=set(values)-self.buffers if nonbuffers is None else set(nonbuffers)
    def extend(self, values):
        self.buffers.update(getattr(values,'buffers',()))
        self.nonbuffers.update(getattr(values,'nonbuffers',values))
        super().extend(values)


def need(ok, code):
    if not ok:
        raise DependencyHeld(code)


def read_at(fd, offset, size):
    os.lseek(fd, offset, os.SEEK_SET)
    data = os.read(fd, size)
    need(len(data) == size, 'truncated_dependency_container')
    return data


def json_at(fd, offset, length):
    need(0 <= length <= MAX_JSON, 'dependency_metadata_budget_exceeded')
    return json.loads(read_at(fd, offset, length).rstrip(b'\0 \t\r\n'))


def json_refs(value):
    refs = []
    def walk(item):
        if isinstance(item, dict):
            need('implicitTiling' not in item and 'subtrees' not in item,
                 'implicit_tiles_dependency_uninspected')
            for key, child in item.items():
                if key.lower() in ('uri','url','schemauri') or key.lower().endswith('uri'):
                    need(isinstance(child, str), 'invalid_resource_uri')
                    refs.append(child)
                elif key.lower() in ('uris','urls'):
                    need(isinstance(child, list) and all(isinstance(x,str) for x in child), 'invalid_resource_uris')
                    refs.extend(child)
                walk(child)
        elif isinstance(item, list):
            for child in item:
                walk(child)
    walk(value)
    return refs


def validate_extensions(value, allowed):
    need(set(value.get('extensionsUsed', [])) <= allowed
         and set(value.get('extensionsRequired', [])) <= allowed, 'unsupported_resource_extension')


def gltf_refs(value):
    need(isinstance(value, dict) and str(value.get('asset',{}).get('version')) == '2.0',
         'unsupported_gltf_schema')
    validate_extensions(value, GLTF_EXTENSIONS)
    rtc=value.get('extensions',{}).get('CESIUM_RTC')
    if 'CESIUM_RTC' in value.get('extensions',{}) or 'CESIUM_RTC' in value.get('extensionsUsed',[]) or 'CESIUM_RTC' in value.get('extensionsRequired',[]):
        need(isinstance(rtc,dict) and set(rtc)=={'center'}, 'unsupported_cesium_rtc_schema')
        center=rtc['center']
        need(isinstance(center,list) and len(center)==3 and all(
             type(x) in (int,float) and math.isfinite(x) for x in center), 'invalid_cesium_rtc_center')
    return References(json_refs(value),[b['uri'] for b in value.get('buffers',[])
                      if isinstance(b,dict) and isinstance(b.get('uri'),str)],
                      json_refs({k:v for k,v in value.items() if k!='buffers'}))


def glb_refs(fd, offset, length):
    magic, version, declared = struct.unpack('<4sII', read_at(fd, offset, 12))
    need(magic == b'glTF' and version == 2 and declared == length, 'unsupported_glb_container')
    position, end, refs, count = offset + 12, offset + length, References(), 0
    while position < end:
        size, kind = struct.unpack('<I4s', read_at(fd, position, 8))
        position += 8
        need(position + size <= end and size % 4 == 0, 'invalid_glb_chunk')
        if kind == b'JSON':
            count += 1
            refs.extend(gltf_refs(json_at(fd, position, size)))
        else:
            need(kind == b'BIN\0', 'unsupported_glb_chunk')
        position += size
    need(position == end and count == 1, 'invalid_glb_json_inventory')
    return refs


def tile_refs(fd, offset, length, depth=0):
    need(depth < 32, 'nested_tile_metadata_budget_exceeded')
    magic, version, declared = struct.unpack('<4sII', read_at(fd, offset, 12))
    need(version == 1 and declared == length, 'unsupported_tile_container')
    if magic == b'cmpt':
        count = struct.unpack('<I', read_at(fd, offset + 12, 4))[0]
        position, refs = offset + 16, References()
        for _ in range(count):
            child_len = struct.unpack('<I', read_at(fd, position + 8, 4))[0]
            need(child_len >= 12 and position + child_len <= offset + length, 'invalid_composite_child')
            refs.extend(tile_refs(fd, position, child_len, depth + 1))
            position += child_len
        need(position == offset + length, 'invalid_composite_length')
        return refs
    need(magic in (b'b3dm',b'i3dm',b'pnts') and length >= 28, 'unsupported_tile_content')
    a,b,c,d = struct.unpack('<IIII', read_at(fd, offset + 12, 16))
    position = offset + (32 if magic == b'i3dm' else 28)
    need(position + a + b + c + d <= offset + length, 'invalid_tile_tables')
    refs = References()
    if a:
        refs.extend(json_refs(json_at(fd, position, a)))
    if c:
        refs.extend(json_refs(json_at(fd, position + a + b, c)))
    position += a + b + c + d
    remaining = offset + length - position
    if magic == b'pnts':
        need(remaining == 0, 'unsupported_point_tile_payload')
    elif magic == b'i3dm':
        fmt = struct.unpack('<I', read_at(fd, offset + 28, 4))[0]
        need(fmt in (0,1), 'unsupported_instanced_gltf')
        if fmt == 0:
            need(remaining <= MAX_JSON, 'dependency_metadata_budget_exceeded')
            refs.append(read_at(fd, position, remaining).rstrip(b'\0 \t\r\n').decode('utf8'))
            refs.nonbuffers.add(refs[-1])
        else:
            refs.extend(glb_refs(fd, position, remaining))
    else:
        refs.extend(glb_refs(fd, position, remaining))
    return refs


def resource_references(fd, filename, *, opaque_buffer=False):
    suffix = PurePosixPath(filename).suffix.lower()
    length = os.fstat(fd).st_size
    if suffix == '.bin':
        magic=read_at(fd,0,min(length,4))
        if magic==b'glTF':return glb_refs(fd,0,length)
        if magic in (b'b3dm',b'i3dm',b'pnts',b'cmpt'):return tile_refs(fd,0,length)
        need(opaque_buffer, 'untyped_binary_resource')
        return []
    if suffix == '.glb':
        return glb_refs(fd, 0, length)
    if suffix in ('.b3dm','.i3dm','.pnts','.cmpt'):
        return tile_refs(fd, 0, length)
    if suffix in ('.subtree','.vctr','.geom','.dae','.fbx','.3tz','.zip','.html','.htm',
                  '.kml','.kmz','.xml','.svg','.x3d','.3ds','.osgb'):
        raise DependencyHeld('unsupported_external_resource_container')
    if suffix == '.vrt':
        need(length <= MAX_JSON, 'dependency_metadata_budget_exceeded')
        raw=read_at(fd,0,length)
        need(b'<!doctype' not in raw.lower(), 'unsupported_xml_doctype')
        tree=ET.fromstring(raw)
        return [node.text.strip() for node in tree.iter()
                if node.tag.rsplit('}',1)[-1] in ('SourceFilename','SourceDataset') and node.text]
    if suffix == '.ply':
        refs=[]
        os.lseek(fd,0,os.SEEK_SET)
        with os.fdopen(os.dup(fd),'rb') as stream:
            need(stream.readline(1024).strip()==b'ply','unsupported_ply_header')
            total=0
            while True:
                line=stream.readline(65537)
                total+=len(line)
                need(line and len(line)<=65536 and total<=MAX_JSON,'invalid_ply_metadata')
                if line.strip()==b'end_header':
                    break
                bits=line.decode('ascii').strip().split(None,2)
                if len(bits)>=2 and bits[0]=='comment' and bits[1].lower()=='texturefile':
                    need(len(bits)==3,'invalid_ply_texture_reference')
                    refs.append(bits[2])
        return refs
    if suffix in ('.gltf','.json','.geojson'):
        value = json_at(fd, 0, length)
        if suffix == '.gltf':
            return gltf_refs(value)
        if isinstance(value, dict) and ('root' in value or PurePosixPath(filename).name.lower() == 'tileset.json'):
            need('root' in value and str(value.get('asset',{}).get('version')) in ('1.0','1.1'),
                 'unsupported_tileset_schema')
            validate_extensions(value, TILES_EXTENSIONS)
        return json_refs(value)
    if suffix in ('.obj','.mtl'):
        refs=[]
        os.lseek(fd, 0, os.SEEK_SET)
        with os.fdopen(os.dup(fd), 'r', encoding='utf8', errors='strict') as stream:
            for line in stream:
                bits=shlex.split(line, comments=True)
                if bits and (bits[0]=='mtllib' or bits[0].lower().startswith('map_')
                             or bits[0].lower() in ('bump','disp','decal','refl')):
                    need(len(bits)==2, 'unsupported_output_dependency_syntax')
                    refs.append(bits[1])
        return refs
    # A suffix is not evidence of dependency freedom. These formats have no
    # external-resource mechanism in the reviewed NodeODM consumer contract.
    # Opaque BIN requires an inspected glTF-buffer role above.
    signatures = {'.png':(b'\x89PNG\r\n\x1a\n',), '.jpg':(b'\xff\xd8\xff',),
                  '.jpeg':(b'\xff\xd8\xff',), '.tif':(b'II*\0',b'MM\0*',b'II+\0',b'MM\0+'),
                  '.tiff':(b'II*\0',b'MM\0*',b'II+\0',b'MM\0+'),
                  '.las':(b'LASF',), '.laz':(b'LASF',)}
    if suffix in signatures:
        head=read_at(fd,0,min(length,8))
        need(any(head.startswith(x) for x in signatures[suffix]), 'invalid_leaf_format_header')
        return []
    if suffix in ('.txt','.log','.csv','.prj','.wkt','.bounds'):
        # These are data/report records, not interpreted resource containers.
        # Do not silently admit another format renamed to a text extension.
        need(length <= MAX_JSON, 'dependency_metadata_budget_exceeded')
        raw=read_at(fd,0,length)
        raw.decode('utf8')
        need(b'\0' not in raw and not raw.lstrip().startswith(
             (b'#VRML',b'<',b'{',b'[',b'%PDF',b'glTF')), 'unsupported_text_container')
        return []
    if suffix == '.pdf':
        need(length <= MAX_JSON, 'dependency_metadata_budget_exceeded')
        raw=read_at(fd,0,length)
        try:inspect_pdf(raw)
        except PdfHeld as error:raise DependencyHeld(str(error)) from None
        return []
    raise DependencyHeld('unsupported_final_resource_format')
