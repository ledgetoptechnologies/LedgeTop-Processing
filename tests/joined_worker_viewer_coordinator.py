"""NOT REVIEWED. Synthetic coordinator child; never connect to production."""
import copy
import hashlib
import json
import os
from pathlib import Path
import ssl
import sys
import threading
import time
from contextlib import nullcontext

from processing.prototype import Queue
from processing.reference_control import ReferenceControlAPI
from processing.reference_http import create_reference_http_server
from processing.reference_protocol import manifest_sha256, validate_submission
from processing.reference_result_proxy import ResultProxy
from processing.reference_store import ReferenceStore


def publish(path, value):
    temp=path.with_suffix('.tmp')
    temp.write_text(json.dumps(value),encoding='utf-8')
    os.replace(temp,path)


def main():
    root=Path(sys.argv[1]).resolve()
    config=json.loads((root/'coordinator-config.json').read_text())
    q=Queue(root/'queue',reference_enabled=True)
    server=None;thread=None
    try:
        store=ReferenceStore(q)
        fixture=json.loads(Path('contracts/reference-v1.contract.json').read_text());cap=fixture['capabilities']
        api=ReferenceControlAPI(store,viewer_bearers={config['viewerToken']:'staging'},
            worker_bearers={config['workerToken']:dict(node_id='cluster',worker_id='worker',worker_instance_id='instance')},
            capability_fingerprint=cap['capabilityFingerprint'],options_fingerprint=cap['engines']['odm']['optionsFingerprint'])
        client_tls=ssl.create_default_context(cafile=str(root/'cert.pem'))
        api.viewer.result_proxy=ResultProxy({'cluster':'https://127.0.0.1:'+str(config['workerPort'])},
            worker_bearers={'cluster':config['readerToken']},admit_read=lambda _binding:nullcontext(),ssl_context=client_tls)
        tls=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);tls.load_cert_chain(root/'cert.pem',root/'key.pem')
        server=create_reference_http_server(api,'127.0.0.1',config['coordinatorPort'],ssl_context=tls)
        thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.05});thread.start()
        identity_path=root/'coordinator-job.json'
        if identity_path.exists():
            identity=json.loads(identity_path.read_text())
            if identity['authorityId']!=q.authority_id:raise RuntimeError('coordinator authority changed')
        else:
            raw=copy.deepcopy(fixture['submission']);raw['submissionId']=config['submissionId']
            obj=dict(id='image-'+raw['submissionId'],name='test.jpg',byteSize=3,
                     sha256=hashlib.sha256(b'abc').hexdigest(),kind='image',mediaType='image/jpeg')
            raw['manifest'].update(objects=[obj],objectCount=1,totalBytes=3,inputCounts=dict(images=1,lidarObjects=0,points=0))
            raw['manifest']['sha256']=manifest_sha256(raw['manifest'])
            job=store.submit(validate_submission(raw,environment='staging',capability_fingerprint=raw['capabilityFingerprint']))[0]
            identity=dict(jobId=job['jobId'],objectId=obj['id'],authorityId=q.authority_id)
            publish(identity_path,identity)
        publish(root/('coordinator-ready-'+config['epoch']+'.json'),identity)
        while not (root/'coordinator-stop').exists():
            state=store.get(identity['jobId'],private=True)
            viewer_complete=root/'viewer-complete.json'
            if viewer_complete.exists():
                viewer=json.loads(viewer_complete.read_text())
                with q._db() as db:
                    rows=db.execute('SELECT receipt_id,job_id,receipt_json FROM reference_acceptances').fetchall()
                if len(rows)!=1 or rows[0]['job_id']!=identity['jobId'] or json.loads(rows[0]['receipt_json'])!=viewer['receipt']:
                    raise RuntimeError('Viewer acceptance missing or mismatched')
                publish(root/'coordinator-viewer-verified.json',dict(jobId=identity['jobId'],receiptId=rows[0]['receipt_id']))
            if state['phase']=='awaiting_transfer' and not (root/'coordinator-granted').exists():
                req=state['transferRequest']
                store.receive_transfer_grant(identity['jobId'],dict(req,jobId=identity['jobId'],environment='staging',
                    objectIds=[identity['objectId']],token=config['brokerToken']))
                (root/'coordinator-granted').write_text('grant persisted')
            if state['phase']=='awaiting_acceptance':
                with q._db() as db:
                    rows=db.execute('SELECT outbox_id,payload_sha,job_id,receipt_json FROM reference_worker_receipts').fetchall()
                if len(rows)==1:
                    row=rows[0]
                    publish(root/'coordinator-accepted.json',dict(outboxId=row['outbox_id'],payloadSha256=row['payload_sha'],
                        jobId=row['job_id'],receipt=json.loads(row['receipt_json']),manifest=state['resultManifest']))
            time.sleep(.1)
    finally:
        if server is not None:
            if thread is not None and thread.is_alive():server.shutdown();thread.join(5)
            server.server_close()
        q.close()


if __name__=='__main__':main()
