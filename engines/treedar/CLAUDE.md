# TreeDar: Claude Code entry instructions

## Read first

- `docs/original-handoff.md`: original Claude cloud handoff, preserved unchanged.
- `docs/hermes-review.md`: Hermes input, proposed scope refinements, and validation safeguards.
- `README.md`: current project status.

## Current owner direction

Beau requested that the handoff and review be pushed to https://github.com/ledgetoptechnologies/TreeDar so Claude Code can pick them up when Beau gets home. The repository replaces MCP as the shared handoff channel for now. Do not assume Claude is watching MCP and do not require it for progress.

Keep shared notes, architecture decisions, findings, and open questions in this repository as `.md` files. The original handoff's MCP instruction is superseded by this direction.

## First work

Begin Task 1 (open-source tool survey) and Task 3 (sensor and data feasibility) in parallel if practical. Include matched-label availability and raw-data processing costs in the feasibility assessment. Report back to Beau before building a prototype.

Treat the Hermes review as recommended refinements to resolve in that report, not as proof of feasibility or approval of commercial claims. Establish measurable success criteria and bounded resource budgets before implementation or large downloads.

## Evaluation priorities

Evaluate tree segmentation, field-stem-to-crown matching, structural estimates, and species classification separately. Initial candidate scope is individually resolvable canopy trees, not a complete forest inventory.

Use field-confirmed labels and LiDAR-plus-RGB baselines comparable to the intended equipment. Keep trees and repeat dates together in spatially independent evaluation partitions. Separate calibration data from final test data. Include unknown / insufficient evidence outputs and report confidence together with coverage.

One property demonstrates local feasibility, not Wisconsin-wide generalization. Do not substitute model predictions for ground truth or hyperspectral results for RGB feasibility.

## Boundaries

- Work only in this separate repository. Do not edit Project Alpha, its website, LedgeTop-Ops, CRM, or production infrastructure.
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

This repository currently contains planning material only. Tool names, sensor specifications, licenses, flight settings, model performance, and regional market uniqueness still require verification. Do not invent findings or treat hypotheses as facts.
