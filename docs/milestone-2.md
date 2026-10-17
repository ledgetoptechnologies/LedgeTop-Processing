# Milestone 2 (proposed): processing and acquisition settings

Proposed by Claude on 2026-10-16 from Beau's notes. Starts after Milestone 1 has its labels and plot counts, so every change below can be measured against the same trusted trees.

## Goal

Find which processing and flight settings actually change TreeDar's results on Beau's property, one setting at a time, and record the best defaults for future flights.

## Experiments

| # | Change | What it tests | Measured by |
|---|---|---|---|
| 1 | Todd re-exports the same flight in NAD83(2011) UTM 16N (EPSG:6344) with NAVD88 heights (GEOID18) | Pipeline handles a second CRS and vertical datum; absolute elevations match county LiDAR | Same tree count and heights as the WGS 84 export; ground elevations against Wisconsin statewide LiDAR at open spots |
| 2 | ROCK range gate and angle gate settings (current export described by Todd as "50/50") | Whether near-range or narrow-angle filtering improves crown edges or removes useful under-canopy returns | Ground model error in the woodlot, second-return share, detection recall in the audit plots |
| 3 | Crown colour from Todd's raw photos instead of point colour | Better colour for type classification (the point cloud has colour on about 83% of upper-crown points) | Classifier accuracy and coverage on the Milestone 1 final test areas |
| 4 | Flight trajectory from Todd | Check flight-line alignment and scan angle against the trajectory directly | Strip offsets by line, density by scan angle |
| 5 | A leaf-off flight of the same property (spring) | Ground model and stem detection under the hardwoods | Ground model change in the woodlot, detection recall, broadleaf vs conifer separation |

## Results so far

### Experiment 2 (first look): ROCK export with and without the "50/50" range gate

Same flight of Beau's property, two ROCK exports, same TreeDar settings (2026-10-17):

| Measure | With 50/50 range gate | Without (original, narrower) |
|---|---|---|
| Points | 199.9 million | 155.9 million (22% fewer) |
| Median density, all returns | 1,040 per m2 | 874 per m2 |
| Second-return share | 10.8% | 10.8% |
| Flight-line agreement (median / 95th percentile) | 0.5 / 2.3 cm | 0.4 / 2.3 cm |
| Trees detected | 1,067 | 988 (7% fewer) |
| Tree height median / 95th percentile | 10.5 / 16.5 m | 10.6 / 16.6 m |
| Single-cell spikes removed from the canopy model | 954 | 535 |

88% of trees found in the 50/50 export have a tree within 1.5 m in the other export, and matched heights agree (median difference 0.00 m, 95% within 0.21 m). The 50/50 export adds points and detections and also more isolated high spikes. Which export gives the more correct tree count cannot be decided until the audit plots are hand counted.

## Notes

- ROCK documents a range gate (minimum and maximum distance from the sensor kept in the output; R3 Pro defaults 8 m and 65 m) and an angle gate. What "50/50" means in Todd's export is not yet confirmed. It may refer to settings or to the flight plan (the file name also contains "5050"). Ask Todd for the exact range gate, angle gate, altitude and overlap values.
- Experiments change one thing at a time, and each result is reported with the number of trees it covers.
- Example datasets are organised by location: `Example Datasets/<location>/<flight or data type>/`. They stay local and out of Git.

## Sources

- [ROCK: How to use the range gate in ROCK Desktop](https://learn.rockrobotic.com/how-to-use-the-range-gate-in-rock-desktop)
- [ROCK: Trajectory processing, range and angle gate](https://learn.rockrobotic.com/rock-desktop-trajectory-processing-range-and-angle-gate)
