# LedgeTop Processing agent instructions

This repository owns the processing coordinator and its engine implementations.
Read `README.md` and `docs/architecture.md` before coordinator work. TreeDar's
engine-specific instructions and research notes live in `engines/treedar`.

- Keep the coordinator in `processing/` and TreeDar in `engines/treedar/`.
- Keep their runtime dependencies, Docker build contexts and tests separate.
- A local engine command is not a coordinator adapter or live Viewer integration.
- Do not change production, TrueNAS, Viewer provider settings or deployment
  credentials as part of ordinary code changes.
- Keep raw data, images, coordinates, generated outputs, credentials and local
  environments out of Git. Preserve existing private dataset/run locations.
- Use synthetic fixtures for committed tests. Retain dataset, model and code
  license notes separately, and do not assign a project license without Beau.
- Run coordinator tests at the repository root with
  `python -m unittest discover -v`. Run TreeDar setup/tests from
  `engines/treedar`; see that directory's README.
- Preserve package and CLI name `treedar`. New TreeDar development belongs here,
  not in the legacy TreeDar repository. Historical PRs remain there for reference.
