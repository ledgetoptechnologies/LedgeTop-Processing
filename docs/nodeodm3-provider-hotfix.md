# NodeODM 3 provider compatibility hotfix

This patch is based on deployed coordinator commit `6bbd468e1177746cb695e43195e75355b0681f23`. It changes only native backend discovery in `processing/provider.py`; it does not introduce the newer reference-worker, cache, runtime-publication, or cleanup code.

The live cluster GET `/info` returned API version `3.6.2`, engine `odm`, matching engine version `3.6.2`, queue count zero, and maxImages 1000. Its dynamic options included all required Viewer outputs. The old classifier accepted only 2.x NodeODM signatures, so the small-first dispatcher waited despite that healthy backend.

The additional compatibility rule accepts a syntactically valid 3.x version only when the engine is exactly lowercase `odm`, the engine version exactly matches the API version, and the ClusterODM memory/CPU sentinel is absent. Existing 2.x and sentinel-based 1.x classification is unchanged. Unknown engines, inconsistent versions and malformed capacity fields remain rejected. Dynamic option discovery and required-output checks still run; this rule is not Docker/image attestation or permission to execute a task.

The new real-HTTP tests use the relevant redacted installed `/info` fields and the observed required option names (not a frozen copy of the engine's full catalog). They exercise authenticated GET discovery through `probe_node` and the unchanged routing policy: 10, 50 and 1000 photos select local, 1001 selects main; busy or draining local waits without enabling fallback. Other tests preserve legacy signatures and reject malformed/unknown inputs and missing dynamic outputs.

Qualification: six new tests passed on Windows, and 77 tests passed in cached Linux Python image `83028785a336b420f42fd4c6a2219f289437e6406255b555aa7b5c70dcdf24fb` with network disabled, source mounted read-only and ResourceWarnings treated as errors. The Linux selection includes existing synthetic native Viewer upload/commit/status/cancel/download, priority/global-slot, uncertain response and recovery checks. This is transport/routing qualification, not a real photogrammetry run.

No image build, deployment, live submission, configuration change, disk cleanup or NAS access is part of this patch. A reviewed immutable image and separately qualified coordinator cutover are required before a user-selected real 10–50-photo trial. The installed source remains unchanged until that cutover. Newer source55 images remain inactive and retain their separate activation gates.
