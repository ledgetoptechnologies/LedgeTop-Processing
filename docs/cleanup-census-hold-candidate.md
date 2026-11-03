# Cleanup census hold candidate

`ScratchLedger.arm_cleanup_census()` installs a durable, one-way hold on
cleanup planning and intent acquisition or recovery in this candidate. It
reports existing non-cleaned intents and remains armed after restart. There is
no automatic release or production startup integration in this candidate.

The hold transaction serializes against new cleanup intents written by this
version. It cannot stop an executor that has already received an intent or is
already deleting a file. A nonzero intent count requires those executors to be
quiesced and their intents reconciled. Older binaries do not recognize the
marker and can bypass it, so external old-writer exclusion is required before
the hold can serve as a startup boundary. The marker alone is not a census:
driver and worker records, malformed identities, and reservation bindings
still need coherent enumeration and review before any release design exists.
