# Viewer TLS connection limits

The Viewer TLS listener admits at most 32 concurrent connections, including
handshakes and active requests. A connection occupies one slot until its handler
finishes, including the full duration of a result download. When all slots are
occupied, additional TCP connections are closed without starting a handler;
clients must retry. No active job or download is preempted to admit another.

Each connection passes through these limits in order:

| Phase | Limit | Default |
| --- | --- | --- |
| TLS handshake | Total deadline for the whole handshake, in the request worker | 5 s |
| Request line and headers | Absolute deadline from handshake completion to the blank line ending the headers | 15 s |
| Request body and response (uploads, ACK bodies, downloads) | At least 16 KiB must move for every 30 s spent waiting on the socket | 16 KiB / 30 s |
| Any single socket wait | The HTTP handler's per-operation timeout | 10 s |

The transfer rule counts only time spent blocked in socket reads or writes. Time
the coordinator spends hashing, writing temporary files or committing state is
not counted. A transfer of any size or duration stays valid while it keeps
moving. There is no fixed connection lifetime, and no photo-count or byte cap is
applied here. Writes are sent in 16 KiB slices, so in practice a download reader
must accept at least 16 KiB within each 10 s wait.

These limits bound how long a slot can be held by a client that sends or reads
too slowly, or not at all. A trickled header is cut at the header deadline. A
trickled body or a stalled download reader is cut when the transfer rule is
missed. As a result, coordinator shutdown, which waits for request workers
without abandoning them, can't be held open indefinitely by a slow client. It
still waits for honest transfers that are making progress. A connection that
misses a limit is closed without logging peer or request details.

The values are class attributes of `ViewerTLSHTTPServer` and are not
configurable at runtime. The fixed limits bound concurrent Viewer TLS request
workers. They are not a fairness, bandwidth or job scheduling policy. Plan
capacity and monitor rejects before changing them. The native and reference
listener limits are not changed by this Viewer-specific guard.
