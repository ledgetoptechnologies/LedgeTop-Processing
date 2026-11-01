"""NOT REVIEWED. Synthetic coordinator child; never connect to production."""
import copy
import hashlib
import json
import os
from pathlib import Path
import sys
import time
from processing.app import make_handler
from processing.config import Config
from processing.reference_protocol import manifest_sha256, validate_submission


def publish(path, value):
    temp=path.with_suffix('.tmp')
    temp.write_text(json.dumps(value),encoding='utf-8')
    os.replace(temp,path)


def main():
    root=Path(sys.argv[1]).resolve()
    config=json.loads((root/'coordinator-config.json').read_text())
    runtime_config=dict(schemaVersion=1,listener=dict(host='127.0.0.1',port=config['coordinatorPort'],
        certificate=str(root/'cert.pem'),privateKey=str(root/'key.pem'),clientCA=str(root/'cert.pem')),
        viewerBearers={'staging':config['viewerToken']},workers={
            'cluster':dict(bearer=config['workerToken'],workerId='worker',instanceId='instance',
                resultOrigin='https://127.0.0.1:'+str(config['workerPort']),resultBearer=config['readerToken']),
            'main':dict(bearer='SYNTHETIC-MAIN-WORKER-'+'m'*36,workerId='main-worker',instanceId='main-instance',
                resultOrigin='https://127.0.0.1:19999',resultBearer='SYNTHETIC-MAIN-READER-'+'n'*36)},
        streamBudget=dict(maxConcurrent=1,maxObjectBytes=1048576))
    private=root/'reference-runtime.json';private.write_text(json.dumps(runtime_config));private.chmod(0o600)
    app_config=Config(api_token='SYNTHETIC-LEGACY-API-'+'a'*36,local_node_url='http://127.0.0.1:18080',
        main_node_url='http://127.0.0.1:18081',local_max_photos=1000,local_max_input_bytes=20*1024**3,
        prototype_state_dir=str(root/'queue'),prototype_node_ids=('cluster','main'),treedar_node_ids=('main',),
        reference_config_path=str(private))
    handler=make_handler(app_config);q=handler.prototype_queue;runtime=handler.reference_listener
    try:
        store=runtime.store
        fixture=json.loads(Path('contracts/reference-v1.contract.json').read_text());cap=fixture['capabilities']
        runtime.start()
        print('ACTUAL_MAKE_HANDLER_REFERENCE_RUNTIME_STARTED',flush=True)
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
        runtime.close()
        q.close()


if __name__=='__main__':main()
