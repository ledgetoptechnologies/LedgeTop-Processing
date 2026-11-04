# Unagreed disabled service identity registry fixture

The public constructor refuses operation. No API route or runtime component
uses this fixture; it proves neither peer authentication nor durable custody.

This experiment assumes one service per `(owner, environment, authority)`
scope and a complete, server-owned registry spanning all scopes. Neither is an
agreed contract or established property of the current system. Global key and
issuer reuse checks only cover the supplied entries; filtering the registry
before validation invalidates that assumption. Rotation may overlap keys only
within one complete identity, and a revoked key is rejected rather than falling
back to another. Revocation freshness, atomic reload, registry persistence,
authenticated owner/service/authority binding and key compromise handling
require a separate design and qualification before any runtime use.
