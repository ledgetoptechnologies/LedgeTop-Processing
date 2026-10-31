"""Opt-in integration: real reference driver and Viewer v1 broker over loopback.

The Viewer checkout is an explicit external test input. Synthetic data and a
test-only HTTP transport are used; coordinator and NodeODM remain fixtures.
"""
import hashlib
import json
import os
from pathlib import Path
import queue
import subprocess
import threading
import unittest
from urllib.request import ProxyHandler, Request, build_opener

from tests import test_reference_driver as fixtures


SERVER = r'''
const path = require('node:path'), fs = require('node:fs'), http = require('node:http');
const readline = require('node:readline');
const {ReferenceTransferBroker} = require(path.join(process.argv[1], 'referenceTransferBroker.js'));
const {createReferenceBrokerHandler} = require(path.join(process.argv[1], 'referenceHttp.js'));
const p = require(path.join(process.argv[1], 'referenceProtocol.js'));
const root = process.argv[2];
const broker = new ReferenceTransferBroker({databasePath:path.join(root,'viewer-broker.sqlite'),managedRoot:root,environment:'staging',allowCreate:true});
const handler = createReferenceBrokerHandler({broker,enabled:true,isProtectedTransport:()=>true});
const server = http.createServer((req,res)=>{handler(req,res).then(ok=>{if(!ok){res.statusCode=404;res.end();}}).catch(()=>res.destroy());});
server.listen(0,'127.0.0.1',()=>console.log(JSON.stringify({port:server.address().port})));
readline.createInterface({input:process.stdin}).on('line',line=>{
 if(line==='close'){server.closeAllConnections();server.close(()=>{broker.close();process.exit(0);});return;}
 try { const job=JSON.parse(line), capabilities=p.createCapabilities('staging'), files={};
   for(const obj of job.submission.manifest.objects) files[obj.id]=path.join(root,obj.id+'.source');
   const result=broker.mintGrant({jobId:job.jobId,generation:job.generation,manifest:job.submission.manifest,
      reservationAvailable:true,reservation:job.transferRequest,objectFiles:files,capabilities,
      capabilityFingerprint:capabilities.capabilityFingerprint});
   console.log(JSON.stringify({grant:result.grant}));
 } catch(error){console.log(JSON.stringify({error:String(error.code||error.message)}));}
});
'''

# Viewer commit 91dc17682558ebe1d6681195cc7243a653427403. Read the
# supplied checkout once, normalize CRLF to LF, verify each source file, then run copies
# inside the test-owned temporary directory. This does not qualify the later
# Viewer release candidate or any build artifact.
VIEWER_FILES = {
    'referenceTransferBroker.js': 'c3d129067db4c6ae57fa7bc0a63080f3065fc43268db28bc97a7e5fe99aeef08',
    'referenceHttp.js': '1e2f6cda90a50866ca4e1937a9044023e78cffbe4e1eff18abaabc0b36b17f87',
    'referenceProtocol.js': '17d505c268b53ff5e17c61bd6f89fff8081b9089d35ca00cef6d96906d934159',
}


@unittest.skipUnless(os.environ.get('LT_VIEWER_REFERENCE_SERVER'), 'explicit Viewer source checkout required')
class ActualViewerBrokerWorkerTests(unittest.TestCase):
    def test_driver_fetches_ranges_from_actual_viewer_broker_and_restarts(self):
        viewer = Path(os.environ['LT_VIEWER_REFERENCE_SERVER']).resolve(strict=True)
        fixture = fixtures.ReferenceDriverTests('test_arbitrary_images_production_and_next_job_before_viewer_acceptance')
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        pinned = fixture.root / 'pinned-viewer'
        pinned.mkdir(mode=0o700)
        for name, expected in VIEWER_FILES.items():
            source = (viewer / name).read_bytes().replace(b'\r\n', b'\n')
            self.assertEqual(hashlib.sha256(source).hexdigest(), expected, 'Viewer test source changed: '+name)
            (pinned / name).write_bytes(source)
        # Remove only this fixture's fake broker method; exercise BrokerClient.
        fixture.patches.pop().stop()
        content = b'x' * (16 * 1024 * 1024 + 1)
        job = fixture.submit(contents=(content, b'second-synthetic-photo'))
        for object_id, payload in fixture.contents.items():
            (fixture.root / (object_id + '.source')).write_bytes(payload)
        proc = subprocess.Popen(['node', '-e', SERVER, str(pinned), str(fixture.root)],
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        def cleanup():
            if proc.poll() is None:
                try:
                    proc.stdin.write(b'close\n'); proc.stdin.flush(); proc.wait(timeout=5)
                except (BrokenPipeError, subprocess.TimeoutExpired):
                    proc.kill(); proc.wait(timeout=5)
            proc.stdin.close(); proc.stdout.close()
            reader.join(timeout=5)
        self.addCleanup(cleanup)
        lines = queue.Queue()
        def read_lines():
            for line in proc.stdout: lines.put(line)
        reader = threading.Thread(target=read_lines, daemon=True); reader.start()
        info = json.loads(lines.get(timeout=10))
        opener = build_opener(ProxyHandler({})).open
        requests = []
        def open_loopback(request, *, timeout):
            requests.append((request.selector, request.get_header('Range')))
            local = Request('http://127.0.0.1:%d%s' % (info['port'],request.selector),
                            headers=dict(request.header_items()))
            response = opener(local,timeout=timeout)
            response.geturl = lambda: request.full_url
            return response
        fixture.broker._open = open_loopback
        fixture.broker._custom_opener = True
        fixture.broker.enable_ranges = True
        # Advance via the actual durable coordinator/worker until the Viewer
        # can mint a grant bound to the worker-created reservation.
        for _ in range(20):
            result = fixture.driver.step()
            if result['state'] == 'awaiting_grant': break
            self.assertNotEqual(result['state'], 'held', result)
        else: self.fail('worker did not request a reservation')
        private_job = fixture.store.get(job['jobId'], private=True)
        proc.stdin.write((json.dumps(private_job)+'\n').encode()); proc.stdin.flush()
        response = json.loads(lines.get(timeout=10))
        self.assertNotIn('error',response)
        fixture.store.receive_transfer_grant(job['jobId'], response['grant'])
        self.assertEqual(fixture.driver.step()['state'], 'fetching_pending')
        self.assertEqual(fixture.driver.step()['state'], 'fetching')
        self.assertEqual(fixture.driver.step()['state'], 'fetching')
        # Restart the real durable driver after the first verified object.
        fixture.driver = fixture.make_driver()
        self.assertEqual(fixture.driver.step()['state'], 'fetching')
        self.assertEqual(fixture.driver.step()['state'], 'processing_pending')
        self.assertEqual([value for _,value in requests],
                         ['bytes=0-16777215','bytes=16777216-16777216',None])
        with fixture.driver._db() as db:
            state = json.loads(db.execute('SELECT data FROM driver_jobs WHERE job=?',(job['jobId'],)).fetchone()[0])
        self.assertEqual(set(state['inputPaths']),set(fixture.contents))
        for object_id, relative in state['inputPaths'].items():
            self.assertEqual((fixture.scratch / relative).read_bytes(),fixture.contents[object_id])
