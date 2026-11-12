# Viewer transfer fence qualification — 2026-11-11

Exact runtime source commit `6996f43f` was archived with SHA-256 `3594669cfecee215aec87fa607d2b34e8898682f14f706d70947b72f20afe46f`. The cached coordinator image was `sha256:2922ba81f3758b0faa92716123eb54b1de6b7ea1488d3846dde41bce9bda119a`; candidate source was mounted read-only for testing, so this was not an installed candidate-image test.

One task-owned Linux container ran `python -m unittest tests.test_viewer_tasks tests.test_viewer_service tests.test_viewer_http -v`: 112 tests passed, zero skips. It used a read-only root and source mount, network disabled, a 128 MiB tmpfs, 1 CPU, 512 MiB memory, and 64 PID limit. No production data, credentials, runtime mounts, ports, or remote hosts were used. The `--rm` container was absent afterward.

An automated policy review blocked recursive removal of the task-owned frozen source directory `C:/Users/fstor/.codex/tmp/transfer-fence-6996f43f-qualification-20261111` without a more specific reason. It remains for manual cleanup; no alternate deletion was attempted.

This test does not authorize cleanup or prove production deployment. Durable orphan-pin reconciliation, verified Viewer outputs ready for at least 24 hours, canonical source custody, all external readers and writers, and a separately reviewed deletion path remain required.
