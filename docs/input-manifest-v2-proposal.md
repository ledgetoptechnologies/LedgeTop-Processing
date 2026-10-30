# Provisional full input-v2 manifest

The source-only manifest validator commits to the complete ordered manifest using SHA-256 over ASCII `ledgetop.reference.manifest.v2`, a NUL byte, and compact sorted-key UTF-8 JSON of the manifest without `sha256`. The full canonical manifest, including that field, is limited to 16 MiB and 32,768 objects. The raw entrypoint checks canonical bytes; the object entrypoint cannot enforce raw lexical rules.

This digest commits to asserted metadata only. It does not authenticate a producer, prove files or point counts, or bind a durable reservation or charge receipt. No endpoint, transfer path or existing v1 code is enabled by this proposal. Broker accounting, renewal, recovery and persisted binding still need a jointly agreed protocol.

The current MIME lists are intentionally restricted and do not support common unlisted HEIC, DNG or RAW media types. Leading-space names and a mismatch between extension and MIME are accepted by the proposal; compatibility with downstream NodeODM needs qualification before activation. Zero-byte objects are accepted; consumers must independently determine whether they are usable. These choices do not change live admission.
