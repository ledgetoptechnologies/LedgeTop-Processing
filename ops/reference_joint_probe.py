"""Run only against the synthetic internal Docker fixture, never production."""
import argparse
import json
import ssl
import time
from urllib.error import HTTPError
from urllib.request import HTTPSHandler, ProxyHandler, Request, build_opener

TOKEN = 'synthetic-reference-fixture-token'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--synthetic', action='store_true', required=True)
    parser.add_argument('--mode', choices=('success', 'prepare', 'crash-before-ack', 'recover'), default='success')
    args = parser.parse_args()
    context = ssl.create_default_context(cafile='/tls/cert.pem')
    opener = build_opener(ProxyHandler({}), HTTPSHandler(context=context))
    def call(node, action, *, method='POST'):
        origin = 'https://viewer:9444' if node == 'viewer' else 'https://processing:9443'
        request = Request(origin + '/fixture/' + action, data=b'{}' if method == 'POST' else None,
            method=method, headers={'Authorization':'Bearer ' + TOKEN, 'Content-Type':'application/json'})
        try:
            with opener.open(request, timeout=20) as response:
                result = json.loads(response.read(65537))
        except HTTPError as error:
            result = json.loads(error.read(65537))
            raise RuntimeError(f'{node}/{action}: {result}') from None
        print(json.dumps({'endpoint':node+'/'+action,'result':result}), flush=True)
        return result
    if args.mode != 'recover':
        submitted = call('viewer','submit')
        if args.mode in ('success', 'prepare'):
            assert submitted['status'] == 'queued'
        for _ in range(60):
            step = call('processing','step')
            call('viewer','pump')
            if args.mode == 'prepare' and step['state'] == 'delivering':
                state = call('processing','state',method='GET')
                assert state['retainedArchiveCount'] == 1 and state['cleanupCandidates'] == 0
                print(json.dumps({'prepared':True,'metadataDelivered':False,'engineCalls':state['engineCalls']}))
                return
            if step['state'] == 'awaiting_acceptance':
                break
            time.sleep(0.1)
        else:
            raise RuntimeError('synthetic driver did not finish delivery')
        if args.mode == 'crash-before-ack':
            try:
                call('viewer','ingest')
            except (OSError, ValueError):
                state = call('processing','state',method='GET')
                assert state['retainedArchiveCount'] == 1 and state['cleanupCandidates'] == 0
                assert state['jobs'][0]['phase'] == 'awaiting_acceptance'
                print(json.dumps({'interruptedBeforeAck':True,'workerRetained':True,'cleanupBlocked':True}))
                return
            raise RuntimeError('Viewer did not exit at its durable commit failpoint')
        accepted = call('viewer','ingest')
        assert accepted['durableCopy'] and accepted['acknowledged'] and accepted['published'] is False
    else:
        replay = call('viewer','replay-acceptance')
        assert replay['replayed'] == 1 and replay['receipts'][0]['acknowledged']
    for _ in range(4):
        step = call('processing','step')
        if step['state'] == 'completed':
            break
    assert step['state'] == 'completed'
    cleaned = call('processing','cleanup')
    assert cleaned['cleanedObjects'] == 1
    final = call('processing','state',method='GET')
    assert final['engineCalls'] == {'initialize':1,'upload':1,'commit':1,'download':1}
    assert final['retainedArchiveCount'] == 0
    assert final['jobs'][0]['phase'] == 'completed' and final['jobs'][0]['status'] == 'succeeded'
    viewer = call('viewer','state',method='GET')
    assert viewer['pendingOutboxCount'] == 0 and viewer['acceptedObjectIds'] == ['odm-archive']
    print(json.dumps({'passed':True,'realTLS':True,'payloadTransferred':True,'workerResultStreamed':True,
        'durableViewerAcceptance':True,'workerCleanup':True,'engineExecutedOnce':True,
        'syntheticEngine':True,'geometryValidated':False,'productionPublished':False}),flush=True)


if __name__ == '__main__':
    main()
