# Synthetic chunk journal scaling observation

The v1 fixture journal still permits at most 4,096 chunks of at most 256 KiB each (1 GiB). It stores two durable files per chunk, plus a header and seal. Reopening an existing, verified header no longer enumerates all prior chunk files; when a header is absent, the full ambiguity scan remains mandatory, and legacy or partial state remains held.

A bounded network-none test wrote and sealed 4,096 one-byte chunks, re-instantiating the journal for every write while forbidding directory enumeration after the header was created. It produced 8,194 journal records, used about 38 MiB peak Python RSS in that run and preserved crash/tamper regression behavior. This exercises maximum record count, not high byte volume, large archive allocation, network throughput or power-loss durability.

A larger-than-1-GiB protocol remains unimplemented. It needs an explicitly versioned journal and negotiated geometry, bounded admission and storage, mixed-version fail-closed behavior, and meaningful crash/tamper tests before changing either limit. No 10,000-photo, 50-GB or production-readiness claim follows from this optimization.
