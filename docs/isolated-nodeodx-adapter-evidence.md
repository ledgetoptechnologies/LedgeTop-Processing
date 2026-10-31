# Isolated NodeODX adapter compatibility evidence

A one-time synthetic probe invoked the staged `initialize` and `upload_locked` adapters against image `sha256:b9d9a1a4603db9f092b790a6e58586fcf944758b7ca1149daefb2bd55dc6bdde`, the identity observed on the main processing node. Both calls completed successfully. The input was synthetic non-photo bytes in an allocated temporary slot; the task was never committed and no processing was started.

The container used `--network none`, 0.5 CPU, 512 MiB memory, no GPU or shared host mounts, and temporary filesystems. The task-owned container was removed and its absence verified. This evidence does not establish commit compatibility, real-photo acceptance, result delivery, restart safety, or production readiness.

The original executable has been removed because it could be pointed at an arbitrary local service and could leave an uncommitted task there. A future executable harness must independently verify its unique owned container and exact image, network isolation, absence of shared mounts and GPU, bounded execution, and identity-based cleanup before invoking either adapter. Documentation alone is not an isolation control.
