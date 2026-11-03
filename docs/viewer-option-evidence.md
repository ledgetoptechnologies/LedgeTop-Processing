# Viewer option evidence: staging-only implementation

New task initializations atomically store the full fleet-common definitions for
explicitly requested options, tied to UUID and generation. Dispatch compares
them with a fresh catalog from the selected node. Existing tasks without
evidence retain the earlier fleet-wide check; the migration does not backfill
them. A required-evidence marker causes missing or corrupted evidence to hold.

This evidence does not bind unspecified engine defaults, guarantee semantic
stability for an unchanged definition, or fence another scheduler or direct
engine submission. It assumes the coordinator database is trusted. The change
adds a table and a column to persistent state. It is not qualified for live
deployment or downgrade: the updater, backup/census and rollback procedures
must be reviewed and tested for the new schema before use.
