# Milestone 1: a checked tree-type map of one property

Set by Claude on 2026-10-16 at Beau's request. Owner: Claude, with Codex on tests and review.

## Goal

Produce a per-tree map of Beau's own property that says, for each detected canopy tree, its height, crown size and type group (conifer, broadleaf, or unknown), and prove how good it is against labels we trust. Then compare a locally trained classifier against Jev on the same held-out trees and decide which path to build on.

One property shows local feasibility only. It does not show Wisconsin-wide accuracy or commercial readiness.

## Success criteria

| Area | Measure | Target |
|---|---|---|
| Labels | Trees with a type confirmed by Beau or a field check | 150 or more, at least 30 per group, from at least 3 separate areas |
| Tree detection | Recall and precision against hand counts in 3 plots of about 20 x 20 m | Recall 80% or more, precision 85% or more in the plantation; woodlot reported as measured |
| Height | Detected height against tape or hypsometer on 20 trees | Median error within 1 m |
| Type classifier (local) | Accuracy on spatially held-out blocks, with an "unknown" option | 85% or more correct on the trees it labels, while labeling at least 70% of them |
| Calibration | Stated confidence against observed accuracy | Reported with a calibration table; no claim without it |
| Jev comparison | Same held-out trees, same metrics, plus cost per 1,000 trees | Reported side by side |
| Engineering | One command from LAZ to tree map; full site under 5 minutes; all tests pass | Met |

## Rules for the evaluation

- Training, calibration and test trees come from separate areas, never mixed within one block.
- Model predictions, including Jev answers, are never used as labels.
- Every result is reported with how many trees it covers.
- Real-data outputs and coordinates stay out of Git.

## Go or no-go

- Go: either classifier meets the type target. Next step is species within groups and a second property.
- No-go: neither meets it. Report which part failed (detection, labels, features or model) and the cheapest fix, such as raw-photo colour, a leaf-off flight or more labels.

## Steps

1. Beau names what grows in each mapped area (plantation blocks, woodlot, windbreaks). This gives the first labels by area.
2. Add a labeling input (areas or points with a type) and a `treedar classify` step with spatial cross-validation and an unknown output.
3. Hand-count 3 plots for the detection check (field or high-resolution imagery).
4. Run the Jev comparison on the held-out trees, text features first, then crown image chips from Todd's raw photos when they arrive.
5. Write `docs/milestone-1-results.md` with the numbers, without coordinates.
