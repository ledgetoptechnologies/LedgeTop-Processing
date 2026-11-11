# Viewer TLS connection limits

The Viewer TLS listener admits at most 32 concurrent connections, including
handshakes and active requests. A connection occupies one slot until its handler
finishes, including the full duration of a result download. When all slots are
occupied, additional TCP connections are closed without starting a handler;
clients must retry. No active job or download is preempted to admit another.

TLS handshakes run in per-connection workers and are limited to five seconds.
After a successful handshake, the existing HTTP handler's socket timeout applies.
The fixed limit bounds concurrent Viewer TLS request workers; it is not a
fairness, bandwidth or job scheduling policy. Plan capacity and monitor rejects
before changing the limit. The native and reference listener limits are not
changed by this Viewer-specific guard.
