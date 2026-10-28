"""Synthetic-only TLS metadata fixture for the actual Viewer provider probe.

No engine, worker, live token, registry or deployment is contacted. Run only in
an isolated disposable container with generated test certificates.
"""
import argparse
import json
from pathlib import Path
import ssl
import tempfile

from processing.prototype import Queue
from processing.reference_store import ReferenceStore
from processing.reference_control import ReferenceControlAPI
from processing.reference_http import create_reference_http_server


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--synthetic', action='store_true', required=True)
    parser.add_argument('--cert-dir', required=True)
    args = parser.parse_args()
    cert = Path(args.cert_dir)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert / 'cert.pem', cert / 'key.pem')
    fixture = json.loads((Path(__file__).resolve().parents[1] /
                          'contracts/reference-v1.contract.json').read_text())
    with tempfile.TemporaryDirectory(prefix='reference-tls-fixture-') as state:
        queue = Queue(state, reference_enabled=True, node_ids=('cluster', 'main'))
        try:
            api = ReferenceControlAPI(ReferenceStore(queue),
                viewer_bearers={'synthetic-reference-staging-token': 'staging',
                                'synthetic-reference-production-token': 'production'},
                worker_bearers={'synthetic-reference-worker-token': dict(node_id='cluster',
                    worker_id='worker', worker_instance_id='fixture-instance')},
                capability_fingerprint=fixture['capabilities']['capabilityFingerprint'],
                options_fingerprint=fixture['capabilities']['engines']['odm']['optionsFingerprint'])
            server = create_reference_http_server(api, '127.0.0.1', 9443, ssl_context=context)
            print('synthetic TLS fixture ready', flush=True)
            try:
                server.serve_forever(poll_interval=0.1)
            finally:
                server.server_close()
        finally:
            queue.close()


if __name__ == '__main__':
    main()
