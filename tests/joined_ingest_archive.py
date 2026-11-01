"""Small deterministic ODM-shaped fixture, synthetic bytes only."""
import io
import zipfile

def archive():
    out=io.BytesIO()
    files={
        'odm_texturing/odm_textured_model_geo.glb':b'glb',
        'odm_texturing/odm_textured_model_geo.obj':b'mtllib model.mtl\nv 0 0 0\n',
        'odm_texturing/model.mtl':b'newmtl surface\nmap_Kd texture.jpg\n',
        'odm_texturing/texture.jpg':b'texture',
        'entwine_pointcloud/ept.json':b'{}',
        '3d_tiles/model/tileset.json':b'{}',
    }
    with zipfile.ZipFile(out,'w',compression=zipfile.ZIP_STORED) as z:
        for name,data in files.items():
            info=zipfile.ZipInfo(name,(2020,1,1,0,0,0));info.external_attr=0o600<<16
            z.writestr(info,data)
    return out.getvalue()

ARCHIVE=archive()
