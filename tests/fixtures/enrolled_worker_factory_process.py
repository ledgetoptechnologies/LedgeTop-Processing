"""Local qualification child seams; never systemd/SSH/installed activation."""
import hashlib
import json
import os
from pathlib import Path
import signal
import sys
import threading


def main(mode):
    from ops.enrolled_coordinator_transport import canonical,decode,require
    raw=sys.stdin.buffer.read(65537);require(len(raw)<=65536,'bounded_fixture_child_input_required')
    packet=decode(raw)
    if mode=='identity':
        import importlib.metadata
        from processing.reference_worker_readiness import source_digest
        root=Path(__file__).resolve().parents[2]
        print(json.dumps(dict(pythonSha256=hashlib.sha256(Path(sys.executable).read_bytes()).hexdigest(),
            pythonVersion=sys.version.split()[0],versions={name:importlib.metadata.version(name)
                for name in ('cryptography','cffi','pycparser','rfc8785')},sourceSha256=source_digest(root),
            requirementsSha256=hashlib.sha256((root/'requirements.txt').read_bytes()).hexdigest())))
        return
    if mode=='gate':
        from processing.node_gate import NodeGate,canonical_gate_path
        require(os.environ['HOME']==packet['home'] and os.environ['XDG_STATE_HOME']==packet['home']+'/.local/state',
                'canonical_fixture_home_required')
        with NodeGate(packet['engineId']) as gate:
            gate.provision(packet['nodeId'],packet['coordinatorOrigin'],packet['authorityId'])
        path=canonical_gate_path(packet['engineId'])/'gate.json';value=decode(path.read_bytes())
        print(json.dumps(dict(path=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            **{key:value[key] for key in ('nodeId','authorityId','generation','coordinatorOrigin')})))
        return
    if mode=='probe':
        from ops.enrolled_worker_readiness_probe import probe
        from ops.qualify_enrolled_worker_preparation import process_identity
        class Control:
            def probe(self):
                require(process_identity(packet['pid'])==packet['startTime'],'actual_fixture_child_changed')
                return dict(pid=packet['pid'],active='active',substate='running',cgroup='/fixture-child/'+str(packet['pid']),
                            invocationId=packet['worker']['nodeId'],startTime=packet['startTime'])
        from urllib.error import HTTPError,URLError
        try:value=probe(packet['worker'],packet['challenge'],control=Control())
        except HTTPError as error:
            status=error.code;error.close()
            if status!=503:raise
            value={'pending':True}
        except URLError as error:
            # Local fixture startup only: production probe keeps its masking.
            reason=error.reason
            if not isinstance(reason,ConnectionRefusedError) or reason.errno!=111:raise
            value={'pending':True}
        print(json.dumps(value,sort_keys=True,separators=(',',':')))
        return
    require(mode=='worker','closed_fixture_child_mode_required')
    from ops import reference_worker_daemon as daemon
    config=daemon.load_config(packet['configPath'])
    require(config['authorityId']==packet['authorityId'] and config['engineId']==packet['engineId'],
            'fixture_worker_physical_binding_changed')
    driver,server=daemon.build_worker(config)
    driver.worker.engine_probe=lambda deadline:packet['engineId']
    # Explicit synthetic coordinator seam: no job is claimed or processed.
    def request(method,route,payload=None,**kwargs):
        if method=='GET' and route=='/authority':return 200,dict(authorityId=packet['authorityId'])
        if method=='GET' and route=='/next':return 200,dict(job=None)
        raise RuntimeError('unexpected_fixture_coordinator_route')
    driver._request=request
    stop=threading.Event()
    signal.signal(signal.SIGTERM,lambda *_:stop.set())
    service=daemon.WorkerService(driver,server,poll_seconds=0.1)
    result=service.run(stop)
    if result:
        # Closed service state/reason only; exceptions/configuration never enter logs.
        health=service.health()
        allowed={'listener_thread_exited','listener_thread_failed','driver_thread_failed',
                 'worker_service_failed','listener_drain_failed'}
        reason=health.get('reason')
        print(json.dumps(dict(scope='local-fixture-service-failure',
            reason=reason if reason in allowed else 'unclassified_service_failure',
            driverAlive=health.get('driverAlive') is True,listenerAlive=health.get('listenerAlive') is True)),
            file=sys.stderr,flush=True)
    raise SystemExit(result)


if __name__=='__main__':main(sys.argv[1])
