# Canonical worker recipe capture

Use `worker_recipe_sha256(image, config, host, mounts)` from
`ops.reference_worker_maintenance` both when constructing a new host-owned
`WorkerPolicy` and when checking its actual Docker inspection. The digest binds
all image, Config, HostConfig and effective-mount fields. Only the effective
four-mount list is sorted by its unique absolute Destination. Environment,
command and HostConfig arrays retain their order. Missing, extra, duplicate or
untyped destinations are refused before hashing; the existing exact source,
destination, bind type and writable checks still apply afterward.

Actual empty-lab Docker observations returned different effective mount orders
without changing their fields. Hashing that enumeration order caused a false
identity failure. A domain-separated projection identifies this new capture
rule. Former raw-array policy digests are refused; this is not automatic policy
migration or permission to recapture an unexplained mismatch.

The same lab exposed an additional pre-start versus later policy mismatch that
mount permutation alone did not explain. Its full pre-start recipe was not
retained, so the cause remains unproven. Preserve that failure. Future capture
must retain complete safe private pre-start and post-start projections, compare
all fields, and establish a reviewed initial policy from known owned resources.
Do not ignore changed fields or adopt an unknown stopped worker.

This source fix does not restart the failed lab worker, update its policy,
change worker code, establish cold stopped-mapping authority or qualify an
actual maintenance operation. Real Docker stop/start and image-transition
acceptance remain separate requirements.
