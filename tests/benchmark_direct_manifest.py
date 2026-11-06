"""Synthetic local manifest index measurement; never a throughput claim."""
import platform
import time
import tracemalloc
from processing import direct_limits as limits, direct_worker_receiver as receiver
from processing import reference_protocol as p

def main():
    objects=[dict(id=f'photo-{n:05}',name=f'{n:05}.jpg',byteSize=1,sha256='a'*64,
                  kind='image',mediaType='image/jpeg') for n in range(10000)]
    objects.append(dict(id='derived-1',name='gcp.txt',byteSize=1,sha256='b'*64,
                        kind='auxiliary',mediaType='text/plain'))
    manifest=dict(id='dataset',revision=1,totalBytes=len(objects),objectCount=len(objects),
                  inputCounts=dict(images=10000,lidarObjects=0,points=0),objects=objects)
    manifest['sha256']=p.manifest_sha256(manifest)
    item=dict(manifestSha256=manifest['sha256'],derivedManifestSha256=receiver.derived_digest(manifest,['derived-1']),
              inputBytes=len(objects),limitProfile=limits.PROFILE)
    tracemalloc.start()
    start=time.perf_counter()
    index=receiver._PreparedManifest(manifest,item,['derived-1'])
    built=time.perf_counter()
    for obj in objects:
        found=index.object(item,obj['id'])
        assert found==obj
    end=time.perf_counter()
    current,peak=tracemalloc.get_traced_memory()
    print('python='+platform.python_version(), 'platform='+platform.system(), 'arch='+platform.machine(),
          'objects='+str(len(objects)), 'canonical_manifest_bytes='+str(len(p.canonical_json(manifest))),
          'index_seconds='+format(built-start,'.3f'), 'lookups_seconds='+format(end-built,'.3f'),
          'traced_current_bytes='+str(current), 'traced_peak_bytes='+str(peak),
          'measurement=synthetic-index-only')

if __name__=='__main__':main()
