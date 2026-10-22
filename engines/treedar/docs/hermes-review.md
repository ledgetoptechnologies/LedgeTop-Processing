# Hermes review of the Forest LiDAR + Ortho pilot

This is a strong handoff for a research pilot, but it should be tightened before implementation. The privacy boundaries, separate workspace, public-data prototype, and explicit permission to conclude that this does not work are right.

The biggest weakness is treating finding trees, identifying species, and producing useful habitat information as almost one problem. They need separate success criteria.

This is a review, not completed sensor research or validated feasibility findings.

## 1. Narrow the initial promise to visible canopy trees

Replace the broad per-tree promise with:

> Produce estimates for individually resolvable canopy trees, with explicit coverage limits for understory trees, overlapping crowns, and ambiguous detections.

An accurate species prediction for a detected crown does not establish that every tree was detected. Report missed trees, merged crowns, and split crowns separately from classification accuracy.

Define canopy density explicitly. Crown cover, return density, stem density, and understory density are different measurements.

## 2. Soften the commercial-readiness hypothesis

Replace “reliable and can be sold now” with:

> Terrain and canopy structure are the first candidate deliverables. Validate their accuracy and coverage under the actual acquisition conditions before making commercial claims.

Bare-earth quality depends on ground returns. Per-tree height depends on correct tree separation, ground normalization, and capturing the treetop. A dense point cloud alone does not prove either is good.

## 3. Make crown matching a first-class task

Associate the field-identified stem, its actual crown, the LiDAR instance, and the same crown in each imagery date.

Nearest stem or nearest treetop is not automatically the right match, especially with leaning trees and overlapping crowns. Require manually checked matches for the initial training set and record ambiguous matches rather than forcing them.

Otherwise, a model can be trained on the wrong species label while everything appears technically correct.

## 4. Keep public-data inputs comparable to the intended equipment

Distinguish field-confirmed labels from model-predicted species maps. Predictions must not become ground truth.

Require the main baseline to use LiDAR plus RGB only. The published NEON canopy-species study below uses hyperspectral imagery to differentiate species, alongside RGB and canopy-height data. Its results do not establish what the intended LiDAR-plus-RGB setup can achieve.

NEON vegetation structure provides field measurements, species identifiers, and mapped positions for qualifying individuals, but associating those stems with visible crowns still requires work.

Sources consulted for this review:

- https://journals.plos.org/plosbiology/article?id=10.1371/journal.pbio.3002700
- https://data.neonscience.org/data-products/DP1.10098.001

## 5. Treat seasonal acquisition as an experiment

Leaf-off LiDAR and fall imagery are sensible hypotheses, not a settled prescription. Evaluate what leaf-off data gains in terrain and woody structure, what it loses in foliage-derived crown features, whether proposed segmentation methods work on leaf-off airborne data, and whether additional imagery dates justify repeat flights.

Measure fall color locally rather than scheduling around “maples before oaks” as a universal rule. Weather, individual trees, illumination, and leaf loss can complicate the pattern.

Use a small repeatable test area for acquisition comparisons. Change one meaningful factor at a time instead of combining lower altitude, slower speed, tighter spacing, and crosshatch in the first test.

## 6. Separate confidence from coverage

Require:

- Spatially separated training, calibration, and final test data.
- The same tree kept in one partition across every date.
- Separate segmentation quality and species confidence.
- An unknown / insufficient evidence output.
- Precision and coverage at the chosen confidence threshold.
- No claim that calibration transfers automatically to a new property or sensor.

A system that correctly labels a limited subset of crowns may be useful. A system that confidently labels every crown may be misleading.

## 7. Replace plot count with a sampling design

Roughly 20 to 30 plots is a planning placeholder, not evidence that the dataset will be adequate.

Specify plot size, sampling across forest conditions, target species, canopy visibility, positioning method, and who verifies identification. Tree counts per species and distribution across independent plots matter more than plot count alone.

One property can demonstrate feasibility on that property. It cannot establish regional generalization.

## 8. Put the business question ahead of fine-grained species identification

Ask what decisions Thomas needs this layer to support. Would broadleaf versus conifer, oak-group likelihood, canopy openings, height classes, and field-check locations be useful even if exact species identification remains weak?

These are candidate outputs, not capabilities to promise. They offer a useful fallback rather than making exact species identification the only win.

## Additional safeguards

- Verify required vendor software, subscriptions, export rights, and trajectory-data access. Free downstream tools do not guarantee a free path from raw sensor recordings to georeferenced LAS/LAZ.
- Treat intensity and return features as acquisition-dependent until stability across range, scan angle, dates, and sensors is tested.
- Record code, model-weight, and dataset licenses separately, including commercial-use restrictions.
- Set bounded download, disk, compute, and research budgets before the prototype.
- Require portable crown polygons, attributes, and raster layers before custom viewer integration. Keep viewer work out of the first feasibility test.
- Replace the ambiguous outbound ban with: no outbound messages, private-data uploads, paid services, or remote inference on project data without approval. Public web research and approved public-data downloads are distinct from those actions. Jev is subject to the same private-data and cost boundaries.

## Recommendation and first gates

Proceed with staged research and approval before building. Task 1 and Task 3 remain a reasonable first step. Their report should also answer:

1. Can matched, field-confirmed labels be obtained for data comparable to the intended sensor setup?
2. Can acquisition and raw-data processing fit the actual budget?

Recommended scope paragraph:

> Evaluate segmentation, crown-to-field-label matching, structural measurements, and species classification separately. Scope the first prototype to individually resolvable canopy trees using LiDAR and RGB inputs comparable to the intended drone workflow. Use field-confirmed labels, spatially independent evaluation, and an explicit unknown class. Public-data success demonstrates pipeline feasibility, not Wisconsin accuracy or commercial readiness. Before expanding acquisition or building viewer integration, agree with Beau on useful outputs, measurable acceptance criteria, and a bounded resource budget.

Bottom line: pursue it, but make tree separation and trustworthy labels the first technical gate. A polished species layer is not valuable if it assigns plausible labels to the wrong crowns.
