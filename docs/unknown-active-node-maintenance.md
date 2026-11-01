# Unknown active-job node ownership

This candidate preserves a missing job node when the coordinator restarts.
Maintenance begin and resume refuse any node while running or held work has an
unknown node, or while an active Viewer job and its Viewer task disagree on the
node or cannot be matched uniquely. Matching work still blocks its own node.
The existing global execution slot continues to block new claims while any job
is running or held.

Older coordinator versions may already have assigned `cluster` to a job with
previously unknown ownership. That assignment cannot be distinguished from a
real one by this change. Review such work against the actual worker before
maintenance or operator resolution. Consistent database fields alone cannot
prove a physical worker is stopped. This change does not inspect or stop worker
containers, repair jobs, or authorize a coordinator cutover.
