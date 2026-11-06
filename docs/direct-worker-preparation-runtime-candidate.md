# Direct worker preparation runtime candidate

The reference worker can optionally construct a separate loopback mTLS control
listener from a closed, owner-private `directPreparation` configuration. It
maps pinned client certificate fingerprints to coordinator principals and
environments and uses separately pinned Viewer source keys and a worker signer.
The worker service owns its startup, health observation and drain.

The current coordinator policy always returns `directPreparationEnabled: false`;
the endpoint therefore returns unavailable before touching the node gate or
scratch. There is no enabled direct scheduler or per-job upload-listener
handoff, and this is not ready for deployment. Credential enrollment, protected
CA ownership and immutable key loading, proof of policy freshness across the
gate operation, real process restart, cleanup and supported recovery remain to
be independently implemented and tested. Missing configuration leaves the
existing worker service unchanged.
