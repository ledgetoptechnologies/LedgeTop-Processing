# TreeDar: Claude Code entry instructions

## Read first

- `docs/original-handoff.md`: original Claude cloud handoff, preserved unchanged.
- `docs/hermes-review.md`: Hermes input, proposed scope refinements, and validation safeguards.
- `README.md`: current project status.

## Current owner direction

On 2026-10-22 Beau authorized migrating TreeDar into LedgeTop Processing.
Work in `engines/treedar` within that repository. The old TreeDar repo is for
historical PRs and will be archived by Beau after verification. See the root
`CLAUDE.md` and `docs/treedar-migration.md` for repository boundaries and provenance.

Keep shared implementation and research notes as Markdown in this engine.
Use the existing linked MCP discussion for coordination when available. The
original handoff is historical context, not a request to redo completed work.

## First work

The structural prototype, classifier, benchmarks and aligned-photo commands are
implemented. Continue from the current milestone documents and the latest
owner decisions; do not restart the initial tool survey. Synthetic tests do not
establish field accuracy.

Treat the Hermes review as recommended refinements to resolve in that report, not as proof of feasibility or approval of commercial claims. Establish measurable success criteria and bounded resource budgets before implementation or large downloads.

## Evaluation priorities

Evaluate tree segmentation, field-stem-to-crown matching, structural estimates, and species classification separately. Initial candidate scope is individually resolvable canopy trees, not a complete forest inventory.

Use field-confirmed labels and LiDAR-plus-RGB baselines comparable to the intended equipment. Keep trees and repeat dates together in spatially independent evaluation partitions. Separate calibration data from final test data. Include unknown / insufficient evidence outputs and report confidence together with coverage.

One property demonstrates local feasibility, not Wisconsin-wide generalization. Do not substitute model predictions for ground truth or hyperspectral results for RGB feasibility.

## Boundaries

- TreeDar work belongs in this engine directory. Coordinator changes require
  their own task and checks. Do not edit Project Alpha, its website,
  LedgeTop-Ops, CRM, or production infrastructure as part of engine work.
- No client contact, outbound messages, private-data uploads, paid services, purchases, or remote inference on project data without Beau's approval.
- Public web research is allowed. Agree on resource limits before large public-data downloads or compute-intensive runs.
- Stop and ask before accessing client data, touching production, or spending money.
- Jev availability is not permission to send sensitive data or incur costs.
- No client or hunting-land data in Git history, including precise coordinates, imagery, point clouds, field records, or identifying logs.
- Check and record code, model-weight, and dataset licenses separately before use. Do not assign a project license without Beau's direction.
- Prefer free, self-hosted tools and minimal Docker. Verify vendor processing/export dependencies rather than assuming the pipeline is free.
- Require reproducible scripts and actual end-to-end tests with fixtures when implementation is approved. Syntax checks alone are insufficient.
- Keep sensitive datasets outside the repository. Track only explicitly reviewed, licensed, non-sensitive small fixtures if needed later.
- Use Markdown and no mid-sentence em dashes in owner-facing writing.

## Honest status

Local processing and synthetic end-to-end tests are implemented. Field-confirmed
accuracy and broader generalization remain unvalidated. Do not invent findings
or treat hypotheses, synthetic recovery or model output as ground truth.
