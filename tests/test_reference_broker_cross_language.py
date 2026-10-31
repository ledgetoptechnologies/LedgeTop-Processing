"""Opt-in synthetic integration with an explicitly selected Viewer checkout.

This uses loopback HTTP with an injected test transport, never a real NAS or
production credential. Production TLS remains covered separately.
"""
import json
import os
from pathlib import Path
import queue
import subprocess
import tempfile
import threading
import unittest
from urllib.request import ProxyHandler, Request, build_opener

from processing.reference_transfer import BrokerClient, validate_grant


SERVER = r'''
const path = require('node:path'), fs = require('node:fs'), http = require('node:http');
const {ReferenceTransferBroker} = require(path.join(process.argv[1], 'referenceTransferBroker.js'));
const {createReferenceBrokerHandler} = require(path.join(process.argv[1], 'referenceHttp.js'));
const p = require(path.join(process.argv[1], 'referenceProtocol.js'));
const root = process.argv[2], file = path.join(root, 'image.jpg');
const bytes = Buffer.alloc(16 * 1024 * 1024 + 1, 120); fs.writeFileSync(file, bytes);
const broker = new ReferenceTransferBroker({databasePath:path.join(root,'broker.sqlite'), managedRoot:root, environment:'staging'});
const manifest = {id:'manifest-1',revision:1,sha256:'0'.repeat(64),totalBytes:bytes.length,objectCount:1,
 inputCounts:{images:1,lidarObjects:0,points:0},objects:[{id:'image-1',name:'image.jpg',byteSize:bytes.length,sha256:p.sha256(bytes),kind:'image',mediaType:'image/jpeg'}]};
manifest.sha256=p.manifestSha256(manifest);
const capabilities=p.createCapabilities('staging');
const reservation={reservationId:'reservation-1',generation:1,expiresAt:new Date(Date.now()+120000).toISOString(),
 manifestSha256:manifest.sha256,byteBudget:bytes.length*3,nonce:'nonce-1'};
const minted=broker.mintGrant({jobId:'job-1',generation:1,manifest,reservationAvailable:true,reservation,
 objectFiles:{'image-1':file},capabilities,capabilityFingerprint:capabilities.capabilityFingerprint});
const handler=createReferenceBrokerHandler({broker,enabled:true,isProtectedTransport:()=>true});
const server=http.createServer((req,res)=>{handler(req,res).then(ok=>{if(!ok){res.statusCode=404;res.end();}}).catch(()=>res.destroy());});
server.listen(0,'127.0.0.1',()=>console.log(JSON.stringify({port:server.address().port,manifest,reservation,grant:minted.grant})));
process.stdin.on('data',()=>{server.closeAllConnections();server.close(()=>{broker.close();process.exit(0);});});
'''


@unittest.skipUnless(os.environ.get('LT_VIEWER_REFERENCE_SERVER'), 'set LT_VIEWER_REFERENCE_SERVER for synthetic cross-language test')
class ActualViewerBrokerRangeTests(unittest.TestCase):
    def test_large_object_ranges_from_actual_viewer_broker(self):
        viewer = Path(os.environ['LT_VIEWER_REFERENCE_SERVER']).resolve(strict=True)
        with tempfile.TemporaryDirectory() as directory:
            proc = subprocess.Popen(['node', '-e', SERVER, str(viewer), directory],
                                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            try:
                lines = queue.Queue(maxsize=1)
                reader = threading.Thread(target=lambda: lines.put(proc.stdout.readline()), daemon=True)
                reader.start()
                info = json.loads(lines.get(timeout=10))
                job = dict(jobId='job-1', generation=1, environment='staging',
                           manifestSha256=info['manifest']['sha256'],
                           submission={'manifest': info['manifest']}, transferRequest=info['reservation'])
                grant = validate_grant(info['grant'], job=job)
                ranges = []
                local_open = build_opener(ProxyHandler({})).open
                def opener(request, *, timeout):
                    ranges.append(request.get_header('Range'))
                    local = Request('http://127.0.0.1:%d%s' % (info['port'], request.selector),
                                    headers=dict(request.header_items()))
                    response = local_open(local, timeout=timeout)
                    response.geturl = lambda: request.full_url
                    return response
                target = Path(directory) / 'download.input'
                BrokerClient('https://synthetic.invalid', opener=opener).download_object(grant, 'image-1', target)
                self.assertEqual(ranges, ['bytes=0-16777215', 'bytes=16777216-16777216'])
                self.assertEqual(target.stat().st_size, 16 * 1024 * 1024 + 1)
            finally:
                if proc.poll() is None:
                    try:
                        proc.stdin.write(b'close\n'); proc.stdin.flush(); proc.wait(timeout=5)
                    except (BrokenPipeError, subprocess.TimeoutExpired):
                        proc.kill(); proc.wait(timeout=5)
                proc.stdin.close(); proc.stdout.close()
                if 'reader' in locals():
                    reader.join(timeout=5)
