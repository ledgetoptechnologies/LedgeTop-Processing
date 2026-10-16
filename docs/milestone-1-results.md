# Milestone 1 results (in progress)

Status as of 2026-10-16. Targets are in [milestone-1.md](milestone-1.md). Numbers come from Beau's own property (one R3 Pro flight, 2026-07-01, leaf-on). No coordinates or imagery are included here.

## Scorecard

| Area | Target | Result | Status |
|---|---|---|---|
| Labels | 150+ trusted trees, 30+ per group, 3+ areas | None yet. Area naming map and template are ready (`treedar areas`). | Waiting on Beau |
| Tree detection | Recall 80%+, precision 85%+ in plantation plots | Not measured. Detection count is highly sensitive to settings (table below), so it needs hand counts. | Waiting on plot counts |
| Height | Median error within 1 m on 20 trees | Not measured in the field. Synthetic recovery within 0.06 m. | Waiting on field heights |
| Type classifier (local) | 85%+ correct while labeling 70%+, held-out areas | Built and tested on synthetic data; no real labels yet. | Waiting on labels |
| Calibration | Calibration table reported | Produced automatically by `treedar classify`. | Ready |
| Jev comparison | Same held-out trees, same metrics, cost per 1,000 trees | Built (`treedar jev --split-from`). About 620 input tokens per tree, about $0.03 per 1,000 trees at list price. | Waiting on labels |
| Engineering | One command, full site under 5 min, tests pass | `treedar pipeline` runs the 200M-point site in 95 s (peak memory 24 GB, so much larger sites will need tiling). Tests: see the latest PR. | Met |

## Data quality (measured)

- 199.9 million points over about 22 ha; median density 1,040 points per m2 (all returns), 937 first returns.
- 11% of points are second returns; the R3 Pro records at most two.
- All 32 flight lines agree within 5 mm median and 2.3 cm at the 95th percentile over smooth open ground (relative offsets, not absolute accuracy).
- Heights are WGS 84 ellipsoidal. Tree heights are above the ground model, so they are unaffected.
- 14 single-cell height spikes (35 to 42 m) were removed from the canopy model on one test tile.

## Detection sensitivity (no ground truth yet)

Treetop counts in four 20 x 20 m audit plots (A1 woodlot, A2 to A4 plantation block) and for the whole site, by detection setting. The search window diameter is `a + b x height`.

| Setting | A1 | A2 | A3 | A4 | Whole site |
|---|---|---|---|---|---|
| 3 + 0.1h, 0.5 m smoothing (current default) | 8 | 19 | 14 | 19 | 1,067 |
| 2 + 0.08h, 0.5 m smoothing | 8 | 25 | 18 | 27 | 1,370 |
| 2 + 0.05h, 0.25 m smoothing | 11 | 35 | 30 | 41 | 2,165 |
| 1.5 + 0.05h, no smoothing | 32 | 45 | 33 | 45 | 3,234 |

Visual review of the plot overlays suggests the default merges some neighbouring crowns in the plantation, but the right setting cannot be chosen without counts. Hand counts in these plots decide it.

## Unsupervised check (not evidence of accuracy)

Clustering the 1,067 default detections on colour and structure, with no labels, produced spatially coherent groups: one group filled the regular plantation block, another the taller woodlot, another the edges and windbreaks. This suggests the features carry type information. It is not an accuracy result.

## Shape features (added after Hermes's prior-art review)

Six literature features were added per tree: skewness and kurtosis of relative heights, the Clark-Evans aggregation index of upper-crown points in plan view, and eigenvalue linearity, planarity and sphericity. On this property, comparing the woodlot zone (A1, 108 trees) with the plantation zones (A2 to A4, 619 trees) as a rough proxy, not labels: Clark-Evans did not differ (median 0.895 vs 0.892), while height skew (-1.08 vs -0.71), sphericity (0.24 vs 0.30) and green fraction (0.62 vs 0.54) did. The classifier will show which features matter once labels exist; the 94 to 95% conifer/broadleaf accuracy reported in the literature is not evidence for this site.

## Tools ready for the comparison

- `treedar classify` holds out whole named areas: development areas for cross-validation and threshold choice, and a separate final test set of areas scored once.
- `treedar jev --split-from` scores Jev on the same final test trees with a threshold chosen on the development trees. `--chips` adds a top-down crown image per tree (experimental TypeSafe image support).
- `treedar ortho` and `treedar chips` build a true orthophoto from point colour (aligned with the LiDAR crowns) and per-crown images. Todd's raw photos can replace this colour source when they arrive.

## What is needed to finish

1. Beau names what grows in each lettered area (`area_types.csv`), ideally with at least 3 areas per group.
2. Hand counts and types for the 4 audit plots, plus 20 measured heights.
3. Then run `treedar pipeline ... --area-types area_types.csv` and `treedar jev --split-from classify/predictions.csv`, and fill in this scorecard.
