# Disabled streaming gateway and network-namespace fixtures

This is a synthetic research candidate. It is not integrated into the live
coordinator, NodeODM, Docker lifecycle or an authenticated operations service.
Do not deploy it or use real credentials or datasets. `fixture=True` is a
caller-controlled test switch, not a security boundary.

The loopback gateway streams fixed-Content-Length requests and responses in
bounded chunks, starts closed and tracks connections through handler cleanup.
The isolated marked connection sets SO_MARK before connecting to an exact
loopback endpoint in the checked current network namespace. The fixture uses an
nftables rule in a disposable user and network namespace; its test-only client
has CAP_NET_ADMIN and CAP_NET_RAW removed with setpriv. It checks a synthetic
upload, drain, backend restart, and denial of direct connections by that client.

The prototype uses a plaintext synthetic control/data token and a preliminary
header allowlist. It does not establish the complete Viewer/NodeODM HTTP
contract, TLS authentication, enforceable Docker ownership, host firewall
fencing, safe restart after host failure, or a durable maintenance protocol.
A host-root user, Docker administrator, a process with network capabilities, or
a route outside the test namespace can bypass its isolation. Namespace and
file identity checks assume a trusted local harness and immutable tools during
the test; they are not a hardened privileged execution boundary.

Tests use Linux user namespaces, `unshare`, `ip`, `setpriv` and a locally
extracted nftables tool and dependent libraries. No third-party binaries are
committed. The integration fixture currently uses the test-owned path
`/mnt/c/Users/fstor/.codex/worktrees/nonempty-cutover-guard/nft-fixture-packages/extracted`
and exact SHA-256 pins inside `tests/netfilter_gateway_fixture.py`. The earlier
experiment downloaded Debian packages and compared each SHA-256 against the
local APT index before extraction; this does not establish ongoing provenance or
protect against replacement between verification and execution. Do not run the
fixture if those assumptions or namespace safeguards cannot be established.

The socket-level and malformed framing tests can run independently of nft:

```
python3 -m unittest tests.test_linux_marked_connection tests.test_loopback_streaming_gateway -v
```

Only after trusted-harness, tool and resource review, the namespace fixture can
be run under an external watchdog:

```
timeout 40s python3 -m unittest tests.test_netfilter_gateway -v
```

The measured synthetic run passed those seven named tests. It does not prove
actual photogrammetry, Viewer ingestion, unattended updates, or the safety of
cutting over a nonempty live coordinator.
