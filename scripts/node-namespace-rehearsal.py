"""REVIEW CANDIDATE ONLY: creates owned Docker resources if executed.

No deletion. The runner attempts to stop only containers whose recorded ID,
exact name and per-run label are reverified. Stopped containers and the owned
bridge are retained. Review before execution; not a production fence.
"""
import argparse
import ipaddress
import json
import re
import secrets
import socket
import subprocess
import sys
import time

LABEL = 'ledgetop.rehearsal'
IMAGE_ID = re.compile(r'sha256:[a-f0-9]{64}\Z')
FULL_ID = re.compile(r'[a-f0-9]{64}\Z')


def docker(*args, timeout=12):
    completed = subprocess.run(['docker', *args], capture_output=True, text=True,
                               timeout=timeout, check=False)
    if completed.returncode:
        raise RuntimeError('Docker command failed: ' + args[0])
    return completed.stdout.strip()


def inspect_container(identifier):
    fmt = '{{json .Id}}|{{json .Name}}|{{json (index .Config.Labels "' + LABEL + '")}}|{{json .HostConfig.NetworkMode}}|{{json .HostConfig.PortBindings}}|{{json .State.Status}}'
    fields = docker('inspect', '--format', fmt, identifier).split('|')
    if len(fields) != 6:
        raise RuntimeError('invalid container inspection')
    return tuple(json.loads(field) for field in fields)


def verified_container(identifier, name, run, network=None):
    obj, observed_name, observed_label, mode, ports, state = inspect_container(identifier)
    if (not FULL_ID.fullmatch(obj) or obj != identifier or observed_name != '/' + name
            or observed_label != run or (network is not None and mode != network)
            or ports not in (None, {})):
        raise RuntimeError('container ownership or isolation differs')
    return state


def network_inspect(identifier):
    fmt = '{{json .Id}}|{{json .Name}}|{{json (index .Labels "' + LABEL + '")}}|{{json .Internal}}|{{json .IPAM.Config}}'
    fields = docker('network', 'inspect', '--format', fmt, identifier).split('|')
    if len(fields) != 5:
        raise RuntimeError('invalid network inspection')
    return tuple(json.loads(field) for field in fields)


def create_or_reconcile(kind, name, run, args):
    # Daemon may finish after CLI timeout. Reconcile by exact generated name;
    # if it cannot be immediately verified, leave it alone and report it.
    try:
        output = docker(*args)
    except (RuntimeError, subprocess.TimeoutExpired):
        try:
            observed = network_inspect(name) if kind == 'network' else inspect_container(name)
            identifier = observed[0]
            observed_name = observed[1].lstrip('/')
            if not FULL_ID.fullmatch(identifier) or observed_name != name or observed[2] != run:
                raise RuntimeError('unverified resource after create failure')
            print('RETAINED after create failure:', kind, identifier, flush=True)
        except Exception:
            print('UNKNOWN create outcome, inspect manually by exact name:', kind, name, flush=True)
        raise
    if not FULL_ID.fullmatch(output):
        raise RuntimeError('Docker did not return a full resource ID')
    return output


def probe_program(port, host):
    # Only connect and close; do not send data to unrelated host services.
    return ("import socket; s=socket.socket(); s.settimeout(1); "
            "r=s.connect_ex((%r,%d)); s.close(); print(r)" % (host, port))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--image-id', required=True, help='pre-existing immutable sha256 image ID with python3')
    parser.add_argument('--run-id', required=True, help='24 lowercase hex characters, supplied by outer watchdog')
    args = parser.parse_args()
    if not IMAGE_ID.fullmatch(args.image_id):
        parser.error('an immutable full sha256 image ID is required')
    if not re.fullmatch('[a-f0-9]{24}', args.run_id):
        parser.error('invalid watchdog run ID')
    run = args.run_id
    prefix = 'lt-ns-' + run
    print('run:', run, 'image:', args.image_id, flush=True)
    if docker('image', 'inspect', '--format', '{{.Id}}', args.image_id) != args.image_id:
        raise RuntimeError('local image identity mismatch')
    containers = []
    network_id = None
    test_passed = False
    cleanup_failures = []
    host_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    host_socket.bind(('127.0.0.1', 0)); host_socket.listen(1)
    port = host_socket.getsockname()[1]
    marker = 'synthetic-' + run
    sleeper = 'import time; time.sleep(180)'
    server = ("from http.server import BaseHTTPRequestHandler,HTTPServer\n"
              "class H(BaseHTTPRequestHandler):\n"
              " def log_message(self,*a): pass\n"
              " def do_GET(self):\n"
              "  b=%r.encode(); self.send_response(200); self.end_headers(); self.wfile.write(b)\n"
              "HTTPServer(('127.0.0.1',%d),H).serve_forever()" % (marker, port))
    common = ['--label', LABEL + '=' + run, '--restart=no', '--read-only', '--user', '65534:65534',
              '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges', '--memory', '96m',
              '--cpus', '0.25', '--pids-limit', '32', '--entrypoint', 'python3']
    try:
        network_name = prefix + '-bridge'
        network_id = create_or_reconcile('network', network_name, run,
            ['network', 'create', '--internal', '--driver', 'bridge', '--label', LABEL + '=' + run, network_name])
        observed = network_inspect(network_id)
        if observed[:4] != (network_id, network_name, run, True):
            raise RuntimeError('network ownership or internal setting differs')
        print('owned network retained:', network_id, flush=True)
        def container(name, network, code):
            full_name = prefix + '-' + name
            identifier = create_or_reconcile('container', full_name, run,
                ['create', '--name', full_name, '--network', network, *common, args.image_id, '-I', '-u', '-c', code])
            # Record before any later verification so failures do not lose ownership data.
            containers.append((identifier, full_name))
            verified_container(identifier, full_name, run, network)
            docker('start', identifier)
            if verified_container(identifier, full_name, run, network) != 'running':
                raise RuntimeError('owned container did not start')
            print('owned container:', identifier, full_name, flush=True)
            return identifier
        broker = container('broker', 'none', sleeper)
        engine = container('engine', 'container:' + broker, server)
        none_probe = container('none-probe', 'none', sleeper)
        bridge_probe = container('bridge-probe', network_id, sleeper)
        def exec_py(identifier, code):
            return docker('exec', identifier, 'python3', '-I', '-c', code, timeout=5)
        namespaces = [exec_py(c, "import os;print(os.readlink('/proc/self/ns/net'))")
                      for c in (broker, engine, none_probe, bridge_probe)]
        if namespaces[0] != namespaces[1] or len({namespaces[0], namespaces[2], namespaces[3]}) != 3:
            raise RuntimeError('actual network namespaces differ from topology')
        interfaces = json.loads(exec_py(broker, "import os,json;print(json.dumps(sorted(os.listdir('/sys/class/net'))))"))
        if interfaces != ['lo']:
            raise RuntimeError('broker namespace exposes non-loopback interfaces')
        positive = "import urllib.request; o=urllib.request.build_opener(urllib.request.ProxyHandler({})); print(o.open('http://127.0.0.1:%d/',timeout=2).read(128).decode())" % port
        deadline = time.monotonic() + 5
        while True:
            try:
                if exec_py(broker, positive) != marker:
                    raise RuntimeError('broker received unexpected response')
                break
            except RuntimeError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(.1)
        code = ("import pathlib,json; print(json.dumps([p.read_text() for p in "
                "(pathlib.Path('/proc/net/tcp'),pathlib.Path('/proc/net/tcp6'))]))")
        tcp, tcp6 = json.loads(exec_py(broker, code))
        listeners = []
        for family, table in ((4, tcp), (6, tcp6)):
            for line in table.splitlines()[1:]:
                fields = line.split(); addr, hexadecimal_port = fields[1].split(':')
                if int(hexadecimal_port, 16) == port and fields[3] == '0A':
                    listeners.append((family, addr))
        if listeners != [(4, '0100007F')]:
            raise RuntimeError('synthetic engine listener is not exclusive loopback')
        if exec_py(none_probe, probe_program(port, '127.0.0.1')) == '0':
            raise RuntimeError('unrelated none-network probe connected')
        gateways = [item.get('Gateway') for item in observed[4] if isinstance(item, dict) and item.get('Gateway')]
        if len(gateways) != 1:
            raise RuntimeError('bridge gateway inconclusive')
        gateway = str(ipaddress.IPv4Address(gateways[0]))
        bridge_address = docker('inspect', '--format',
            '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}', bridge_probe)
        probe_address = ipaddress.IPv4Address(bridge_address)
        if probe_address.is_loopback or probe_address.is_unspecified:
            raise RuntimeError('bridge probe address inconclusive')
        for destination in (gateway, str(probe_address)):
            if exec_py(bridge_probe, probe_program(port, destination)) == '0':
                raise RuntimeError('bridge probe connected to an unrelated endpoint: inconclusive')
        test_passed = True
        print('HOST INCONCLUSIVE: test port reserved locally; no direct host-to-engine probe possible.', flush=True)
        print('LIMIT: no packet routed to the broker namespace; no host-root/Docker-admin fence.', flush=True)
    finally:
        host_socket.close()
        # No delete. Never stop an unverified resource.
        for identifier, name in reversed(containers):
            try:
                state = verified_container(identifier, name, run)
                if state == 'running':
                    docker('stop', '--time', '2', identifier, timeout=8)
                if verified_container(identifier, name, run) not in ('created','exited'):
                    raise RuntimeError('owned container is not confirmed stopped')
                print('verified stopped and retained:', identifier, flush=True)
            except Exception as error:
                cleanup_failures.append(identifier)
                print('could not safely stop owned candidate:', identifier, type(error).__name__, flush=True)
        if network_id:
            try:
                final = network_inspect(network_id)
                if final[:4] != (network_id, network_name, run, True):
                    raise RuntimeError('network identity changed')
                print('verified retained network:', network_id, flush=True)
            except Exception as error:
                cleanup_failures.append(network_id)
                print('network verification failed:', network_id, type(error).__name__, flush=True)
        if cleanup_failures:
            raise RuntimeError('owned resource stop or verification failed; manual review required')
    if test_passed:
        print('PASS: namespace probes completed and owned containers verified stopped.', flush=True)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print('INCONCLUSIVE:', type(error).__name__, str(error), file=sys.stderr)
        raise SystemExit(1)
