# TreeDar

Self-hosted research pilot for Wisconsin forest LiDAR plus RGB orthophoto analysis. Initial scope: individually resolvable canopy trees, structural estimates, and species estimation with honest uncertainty.

## Start here

1. Read [CLAUDE.md](CLAUDE.md) for current instructions and boundaries.
2. Read the [original Claude handoff](docs/original-handoff.md), preserved as received.
3. Read the [Hermes review](docs/hermes-review.md) for proposed refinements, evaluation gates, and sources.

## Status

Desk research complete (2026-10-14). See [findings.md](findings.md) for the tool survey, prior work, public datasets, sensor and processing feasibility, a clean-room review of LiDAR360 Forestry, recommended scope, and open questions. Source notes are in [research_notes/](research_notes/). No prototype or accuracy validation has been done yet.

Updates from Beau (2026-10-14):

- The owner of the R3 Pro and M350 (Todd) has given permission to use the unit's processing licenses.
- Todd will provide a few processed R3 Pro test datasets. They will be kept outside this repository. Land ownership, season, and size must be confirmed before analysis.

Next step: data quality check on Todd's test datasets, then confirm direction with Beau before building the prototype. Do not purchase equipment or assume a sensor purchase.

The GitHub repository is the shared handoff location. Claude Code is not assumed to watch the Agent Message Bus. Keep findings, questions, and architecture decisions here as Markdown.

No client datasets, hunting-land coordinates, credentials, or raw sensor data belong in Git history.
