"""Read-only scheduling preview using a separate observer credential.

This command performs GETs only, never imports a worker or invokes Docker.
Snapshots are advisory and may change between reads; they reserve no capacity.
"""
import argparse
import json
import os
from urllib.request import Request, build_opener, ProxyHandler, HTTPRedirectHandler
from urllib.parse import urlsplit


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ValueError('redirect refused')


def preview(jobs, nodes):
    if any(job['status'] in ('running', 'held') for job in jobs):
        return {'executesWork': False, 'reason': 'occupied_or_uncertain', 'proposedJobId': None}
    pending = [(index, job) for index, job in enumerate(jobs) if job['status'] == 'queued']
    head = min(pending, key=lambda item: (item[1]['environment'] != 'production', item[0]))[1] if pending else None
    return {'executesWork': False, 'reason': 'advisory_snapshot' if head else 'empty',
            'proposedJobId': head['id'] if head else None,
            'environment': head['environment'] if head else None,
            'eligibleNodeIds': [node['id'] for node in nodes if node['state'] in ('active', 'scheduled')]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--coordinator', required=True)
    parser.add_argument('--verified-origin')
    args = parser.parse_args()
    try:
        base = args.coordinator.rstrip('/')
        parsed = urlsplit(base)
        if (parsed.scheme not in ('http', 'https') or not parsed.hostname or parsed.username
                or parsed.password or parsed.path or parsed.query or parsed.fragment
                or parsed.port == 0 or (parsed.hostname not in ('localhost', '127.0.0.1', '::1')
                                       and args.verified_origin != base)):
            raise ValueError()
        token = os.environ.get('PROCESSING_OBSERVER_TOKEN', '')
        if not token or any(ord(c) < 33 or ord(c) > 126 for c in token):
            raise ValueError()
        opener = build_opener(ProxyHandler({}), NoRedirect())

        def get(route):
            request = Request(base + route, headers={'Authorization': 'Bearer ' + token})
            with opener.open(request, timeout=10) as response:
                raw = response.read(8 * 1024 * 1024 + 1)
                if len(raw) > 8 * 1024 * 1024:
                    raise ValueError()
                return json.loads(raw)

        authority = get('/v1/prototype/authority')['authorityId']
        jobs = get('/v1/prototype/jobs')['jobs']
        nodes = get('/v1/prototype/nodes')['nodes']
        print(json.dumps({'authorityId': authority, **preview(jobs, nodes)}, sort_keys=True))
        return 0
    except Exception:
        print('Shadow snapshot unavailable; no work assigned.')
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
