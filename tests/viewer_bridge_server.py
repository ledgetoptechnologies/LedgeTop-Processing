"""Bounded synthetic PR149 fixture for the cross-language Viewer rehearsal."""
import json
import os
from pathlib import Path
import time
import zipfile
from unittest.mock import patch

from ops.reference_worker_daemon import DaemonStateLock
from processing.viewer_local_result import send_completion
from tests.test_viewer_completion_projection import ViewerCompletionProjectionTests


def main():
    original=zipfile.ZipFile.writestr
    entries=[('odm_texturing/odm_textured_model_geo.glb','glb'),
             ('odm_texturing/odm_textured_model_geo.obj','mtllib model.mtl\nv 0 0 0\n'),
             ('odm_texturing/model.mtl','newmtl surface\nmap_Kd texture.jpg\n'),
             ('odm_texturing/texture.jpg','texture'),('entwine_pointcloud/ept.json','{}'),
             ('3d_tiles/model/tileset.json','{}')]
    def write(archive,name,data,*args,**kwargs):
        if name=='synthetic.txt':
            for entry,body in entries:original(archive,entry,body)
            return
        return original(archive,name,data,*args,**kwargs)
    fixture=ViewerCompletionProjectionTests('test_completion_status_requires_exact_durable_attestation_and_spool')
    try:
        with patch.object(zipfile.ZipFile,'writestr',write):fixture.setUp()
        with DaemonStateLock(fixture.node.worker) as owner:
            send_completion(fixture.worker(owner),synthetic_fixture=True)
        result={'port':fixture.base.server.server_port,'task':'22222222-2222-4222-8222-222222222222',
                'archiveSha256':fixture.base.base.base.record['archiveSha256'],'token':'viewer-production-token'}
        Path(os.environ['BRIDGE_READY']).write_text(json.dumps(result))
        deadline=time.monotonic()+120
        while not Path(os.environ['BRIDGE_STOP']).exists() and time.monotonic()<deadline:time.sleep(.1)
        retained=dict(nodeInputCopies=len(list(fixture.base.base.base.base.base.inputs.glob('*.alloc'))),
                      nodeResultCopies=len(list(fixture.node.output.glob('*.alloc'))),
                      coordinatorSpoolCopies=len(list(fixture.base.base.root.glob('*.alloc'))),
                      coordinatorJobStatus=fixture.fixture.queue.get(fixture.fixture.job)['status'])
        if retained!={'nodeInputCopies':1,'nodeResultCopies':1,'coordinatorSpoolCopies':1,'coordinatorJobStatus':'running'}:
            raise AssertionError('synthetic processing retention changed')
        Path(os.environ['BRIDGE_STOP']+'.result').write_text(json.dumps(retained))
    finally:fixture.doCleanups()


if __name__=='__main__':main()
