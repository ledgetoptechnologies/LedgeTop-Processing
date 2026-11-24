"""Narrow offline GeoTIFF height conversion; no warp and no client defaults."""
import hashlib,json,math,os,tempfile
from pathlib import Path
import numpy as np
from osgeo import gdal,osr
from pyproj import CRS,Transformer
from pyproj.enums import TransformDirection
from .geodesy import GRID_SHA256,GRID_SIZE

def sha(path):
    d=hashlib.sha256()
    with open(path,'rb') as f:
        for b in iter(lambda:f.read(1024*1024),b''):d.update(b)
    return d.hexdigest()

def convert(source,destination,*,source_sha256,grid_path,horizontal_epsg,block_size=128):
    with gdal.ExceptionMgr(useExceptions=True):
        return _convert(source,destination,source_sha256=source_sha256,grid_path=grid_path,horizontal_epsg=horizontal_epsg,block_size=block_size)

def _convert(source,destination,*,source_sha256,grid_path,horizontal_epsg,block_size=128):
    source,destination,grid_path=map(lambda p:Path(p).resolve(),(source,destination,grid_path))
    if source==destination or destination.exists():raise ValueError('new_output_path_required')
    if block_size<1 or block_size>256:raise ValueError('bounded_blocks_required')
    if sha(source)!=source_sha256:raise ValueError('source_digest_mismatch')
    if grid_path.stat().st_size!=GRID_SIZE or sha(grid_path)!=GRID_SHA256:raise ValueError('verified_geoid18_grid_required')
    if any(c in str(grid_path) for c in [' ','+','\n','\r']):raise ValueError('unsupported_grid_path')
    ds=gdal.OpenEx(str(source),gdal.OF_RASTER|gdal.OF_READONLY)
    if ds is None or ds.GetDriver().ShortName!='GTiff':raise ValueError('physical_geotiff_required_no_vrt')
    if {Path(p).resolve() for p in ds.GetFileList()}!={source}:raise ValueError('self_contained_geotiff_required')
    if ds.RasterCount!=1:raise ValueError('single_band_required')
    sr=ds.GetSpatialRef()
    if sr is None or not CRS.from_wkt(sr.ExportToWkt()).equals(CRS(horizontal_epsg),ignore_axis_order=True):raise ValueError('declared_horizontal_required')
    if ds.GetMetadataItem('HEIGHT_REFERENCE')!='NAD83(2011)_ELLIPSOID_METRE' or ds.GetMetadataItem('HEIGHT_TRANSFORM_COUNT')!='0':raise ValueError('unconverted_ellipsoid_source_required')
    band=ds.GetRasterBand(1)
    if band.DataType!=gdal.GDT_Float64 or band.GetUnitType()!='m':raise ValueError('float64_metre_band_required')
    if band.GetScale() not in [None,1] or band.GetOffset() not in [None,0]:raise ValueError('unscaled_physical_heights_required')
    gt=ds.GetGeoTransform(can_return_null=True)
    if gt is None or not all(math.isfinite(x) for x in gt):raise ValueError('finite_geotransform_required')
    if ds.GetGCPCount():raise ValueError('affine_georeference_only')
    xy=Transformer.from_crs(horizontal_epsg,6318,always_xy=True,allow_ballpark=False)
    vertical=Transformer.from_pipeline(f'+proj=pipeline +step +proj=unitconvert +xy_in=deg +xy_out=rad +step +inv +proj=vgridshift +grids={grid_path.as_posix()} +multiplier=1 +step +proj=unitconvert +xy_in=rad +xy_out=deg')
    nodata=band.GetNoDataValue();mask=band.GetMaskBand()
    destination.parent.mkdir(parents=True,exist_ok=True)
    fd,temp=tempfile.mkstemp(prefix='.navd88-',suffix='.tif',dir=destination.parent);os.close(fd)
    out=None; valid_count=0;delta_min=None;delta_max=None
    old=gdal.GetThreadLocalConfigOption('GDAL_TIFF_INTERNAL_MASK');gdal.SetThreadLocalConfigOption('GDAL_TIFF_INTERNAL_MASK','YES')
    try:
        out=gdal.GetDriverByName('GTiff').Create(temp,ds.RasterXSize,ds.RasterYSize,1,gdal.GDT_Float64,options=['TILED=YES','COMPRESS=DEFLATE'])
        out.SetGeoTransform(gt);target=osr.SpatialReference();target.ImportFromWkt(CRS.from_user_input(f'EPSG:{horizontal_epsg}+5703').to_wkt());out.SetSpatialRef(target)
        metadata=ds.GetMetadata().copy();metadata.update({'HEIGHT_REFERENCE':'NAVD88_GEOID18_METRE','HEIGHT_TRANSFORM_COUNT':'1','HEIGHT_SOURCE_SHA256':source_sha256,'HEIGHT_GEOID18_SHA256':GRID_SHA256,'HEIGHT_OPERATION':'EPSG:9229 inverse geoid shift h->H; native pixel centers; no resampling'})
        out.SetMetadata(metadata);ob=out.GetRasterBand(1);ob.SetUnitType('m')
        if nodata is not None:ob.SetNoDataValue(nodata)
        ob.CreateMaskBand(gdal.GMF_PER_DATASET);om=ob.GetMaskBand()
        for y in range(0,ds.RasterYSize,block_size):
            for x in range(0,ds.RasterXSize,block_size):
                w,h=min(block_size,ds.RasterXSize-x),min(block_size,ds.RasterYSize-y)
                values=band.ReadAsArray(x,y,w,h);m=mask.ReadAsArray(x,y,w,h);valid=m!=0
                if nodata is not None:valid &= ~np.isnan(values) if math.isnan(nodata) else values!=nodata
                if np.any(valid & ~np.isfinite(values)):raise ValueError('nonfinite_valid_height')
                result=values.copy()
                if np.any(valid):
                    row,col=np.indices((h,w));px=col+x+0.5;py=row+y+0.5
                    east=gt[0]+px*gt[1]+py*gt[2];north=gt[3]+px*gt[4]+py*gt[5]
                    lon,lat=xy.transform(east[valid],north[valid],errcheck=True)
                    _,_,converted=vertical.transform(lon,lat,values[valid],errcheck=True)
                    converted=np.asarray(converted)
                    if not np.all(np.isfinite(converted)):raise ValueError('nonfinite_converted_height')
                    if nodata is not None and np.any(converted == nodata):
                        raise ValueError('converted_height_conflicts_with_nodata')
                    delta=converted-values[valid];result[valid]=converted;valid_count+=int(valid.sum())
                    delta_min=float(delta.min()) if delta_min is None else min(delta_min,float(delta.min()))
                    delta_max=float(delta.max()) if delta_max is None else max(delta_max,float(delta.max()))
                ob.WriteArray(result,x,y);om.WriteArray(np.where(valid,m,0),x,y)
        out.FlushCache();om=None;ob=None;out=None;mask=None;band=None;ds=None
        if not valid_count:
            raise ValueError('no_valid_height_pixels')
        if sha(source)!=source_sha256:raise ValueError('source_changed_during_conversion')
        if sha(grid_path)!=GRID_SHA256:raise ValueError('grid_changed_during_conversion')
        # Hard-link publication is exclusive: never replace an existing destination.
        os.link(temp,destination)
        return {'source_sha256':source_sha256,'output_sha256':sha(destination),'grid_sha256':GRID_SHA256,'valid_pixels':valid_count,'height_shift_range_m':[delta_min,delta_max],'target':f'EPSG:{horizontal_epsg}+5703; NAVD88 GEOID18; metres','block_size':block_size}
    finally:
        om=None;ob=None;out=None;ds=None;gdal.SetThreadLocalConfigOption('GDAL_TIFF_INTERNAL_MASK',old)
        if os.path.exists(temp):os.unlink(temp)
