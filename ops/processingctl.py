"""Private cluster prototype commands. Never prints credentials or claim tokens."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from urllib.request import Request, urlopen
import uuid

root = Path(__file__).resolve().parent
installation = json.loads((root / 'installation.json').read_text())
token = next(line.split('=', 1)[1] for line in (root / 'runtime.env').read_text().splitlines()
             if line.startswith('PROCESSING_API_TOKEN='))
parser = argparse.ArgumentParser(description=__doc__)
commands = parser.add_subparsers(dest='command', required=True)
commands.add_parser('status')
submit = commands.add_parser('submit-synthetic')
submit.add_argument('--environment', choices=('production','staging'), default='staging')
dispatch = commands.add_parser('dispatch')
dispatch.add_argument('--max-jobs', type=int, choices=range(1,11), default=2)
args = parser.parse_args()

def api(path, payload=None):
    request = Request(installation['origin'] + path,
        data=None if payload is None else json.dumps(payload).encode(),
        headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'})
    with urlopen(request,timeout=10) as response:
        return json.load(response)

try:
    if args.command == 'status':
        jobs = api('/v1/prototype/jobs')['jobs']
        nodes = api('/v1/prototype/nodes')['nodes']
        print(json.dumps({'health':api('/healthz')['status'],
            'authorityId':api('/v1/prototype/authority')['authorityId'],
            'jobCounts':{state:sum(j['status']==state for j in jobs)
                         for state in ('queued','running','held','succeeded','failed')},
            'nodes':[{key:n[key] for key in ('id','state')} for n in nodes]}))
    elif args.command == 'submit-synthetic':
        job=api('/v1/prototype/jobs',{'requestId':'manual-synthetic-'+uuid.uuid4().hex,
            'environment':args.environment,'profile':'treedar-synthetic-v1'})['job']
        print(json.dumps({'id':job['id'],'environment':job['environment'],'status':job['status']}))
    else:
        environment={key:value for key,value in os.environ.items()
                     if key not in ('PROCESSING_PROTOTYPE_OPERATOR_TOKEN','PROCESSING_PROTOTYPE_OBSERVER_TOKEN')}
        environment['PROCESSING_API_TOKEN']=token
        engine='sha256:f22ac13e70dbbabbc7d59f56d4eca8ec04476e711e2dd96354ba273011b3e029'
        command=[sys.executable,str(root/'source/scripts/prototype_dispatcher.py'),
            '--coordinator',installation['origin'],'--image',engine,'--scratch',str(root/'work'),
            '--state',str(root/'dispatch-state'),'--worker-id','cluster-synthetic',
            '--docker-engine-id',installation['engineId'],'--node-generation',str(installation['generation']),
            '--max-jobs',str(args.max_jobs)]
        raise SystemExit(subprocess.run(command,env=environment).returncode)
except Exception:
    print('Prototype command failed; inspect coordinator status and private receipts.',file=sys.stderr)
    raise SystemExit(2)
