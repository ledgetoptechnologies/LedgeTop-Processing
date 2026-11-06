# Direct worker preparation runtime candidate

The reference worker can optionally construct a separate loopback mTLS control
listener from a closed, owner-private `directPreparation` configuration. It
maps pinned client certificate fingerprints to coordinator principals and
environments and uses separately pinned Viewer source keys and a worker signer.
The worker service owns its startup, health observation and drain.
Enabled direct configuration also requires an independently enrolled config
SHA-256 supplied to the loader and a protected descriptor reread of those exact
bytes. The candidate daemon can obtain that digest through an explicit
`--direct-enrollment` record whose file and every ancestor are root-owned and
not writable by group or other users. It validates the enrolled worker identity
and exact certificate, private-key, client-CA and signer bytes before
constructing the service. The v3 record also pins the coordinator CA bytes;
older v1/v2 records are refused without reviewed migration. Direct-enabled
worker policy connections use only that CA in a separately constructed TLS
context, with hostname verification, rather than system trust roots.
Enrollment creation, unit installation, mandatory
service-unit enrollment, old-process/old-binary rollback fencing and host ownership
verification are not implemented. Removing the enrollment flag or running an old
binary is not fenced by this candidate. Without
the explicit record the daemon still refuses enabled direct configuration.

The current coordinator policy always returns `directPreparationEnabled: false`;
the endpoint therefore returns unavailable before touching the node gate or
scratch. There is no enabled direct scheduler or per-job upload-listener
handoff, and this is not ready for deployment. The candidate reads protected
Linux-owned files through no-follow nonblocking descriptors and loads TLS from
sealed in-memory copies. Credential enrollment, installed mount/owner verification,
proof of policy freshness across the
gate operation, real process restart, cleanup and supported recovery remain to
be independently implemented and tested. Missing configuration leaves the
existing worker service unchanged.
