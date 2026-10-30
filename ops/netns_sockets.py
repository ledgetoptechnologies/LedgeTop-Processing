"""Summarize TCP sockets in the current network namespace; prints no addresses.

Run inside a container with ``docker exec -i <id> python - < netns_sockets.py``.
This covers listeners and peers that reach the container over a Docker bridge,
loopback or published ports alike, which host ``ss`` cannot see.
"""

import json
import sys

STATES = {'01': 'established', '02': 'syn_sent', '03': 'syn_recv', '04': 'fin_wait1',
          '05': 'fin_wait2', '06': 'time_wait', '07': 'close', '08': 'close_wait',
          '09': 'last_ack', '0A': 'listen', '0B': 'closing'}


def summarize(lines):
    listening, connections = set(), {}
    for line in lines:
        fields = line.split()
        if len(fields) < 4 or fields[0] == 'sl':
            continue
        port = int(fields[1].rsplit(':', 1)[1], 16)
        state = STATES.get(fields[3].upper(), 'unknown')
        if state == 'listen':
            listening.add(port)
        else:
            connections.setdefault(port, {}).setdefault(state, 0)
            connections[port][state] += 1
    inbound = {str(port): states for port, states in sorted(connections.items()) if port in listening}
    outbound = sum(sum(states.values()) for port, states in connections.items() if port not in listening)
    return {'listening': sorted(listening), 'inboundByLocalPort': inbound, 'otherConnections': outbound}


def main():
    lines = []
    for name in ('/proc/net/tcp', '/proc/net/tcp6'):
        try:
            with open(name) as stream:
                lines.extend(stream.read().splitlines())
        except FileNotFoundError:
            pass
    json.dump(summarize(lines), sys.stdout, sort_keys=True)
    sys.stdout.write('\n')


if __name__ == '__main__':
    main()
