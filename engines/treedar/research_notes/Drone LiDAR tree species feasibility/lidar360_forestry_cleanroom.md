# LiDAR360 Forestry (ALS Forest and TLS Forest): clean-room functional notes

Prepared 2026-10-13 from public sources only. No software was downloaded, installed, run, or decompiled. No accounts, trials, or forms were used. Text is summarized in our own words.

## Legend and method

- **[V]** verified from a primary public source (GreenValley's online user guide, vendor store page, or the original paper's publisher listing).
- **[P]** partial or secondary: inferred from parameter names, a search-engine summary of a page we could not open directly, or a third-party page.
- **[U]** unverified: our hypothesis or an open question.

Primary documentation base: the public online "LiDAR360 User Guide" at `https://www.greenvalleyintl.com/docs/lidar360/`. Tool pages below sit under `https://www.greenvalleyintl.com/docs/lidar360/ToolReference/ALSForest/` (abbreviated **ALS/**) and `https://www.greenvalleyintl.com/docs/lidar360/ToolReference/TLSForest/` (abbreviated **TLS/**). Page content was read with an automated fetch tool that summarizes pages; formulas shown as images on GreenValley pages were not readable, so those are marked [P].

Caveat on doc version: the online guide does not print a version number on its index page. Its changelog tops out at v9.0 (2024-08-26), but the navigation contains tools (LLM species identification, panoramic species identification) that are not in the visible release notes, so the online guide likely reflects a release newer than v9.0 [P].

---

## 0. Product, pricing, versions

| Item | Finding | Tag | Source |
|---|---|---|---|
| LiDAR360 Forestry module price | USD 2,360 excl. tax at SPH Engineering store; requires LiDAR360 Framework | [V] | https://shop.sphengineering.com/products/lidar360-forestry |
| What Forestry license includes | ALS Forest (stats/metrics extraction, regression, individual tree segmentation, batch, reporting) and TLS Forest (ground classification, trunk-based tree segmentation, individual tree editing) | [V] | same |
| LiDAR360 Framework price | USD 4,730 excl. VAT (base license: data management, strip alignment, classification) | [V] | https://shop.sphengineering.com/products/lidar360-framework |
| Minimum list cost for Forestry on this reseller | Framework + Forestry = USD 7,090 before tax, perpetual vs subscription not stated | [P] (arithmetic from two pages) | both above |
| Other module prices (Terrain, Mine, 3D Building, Photo, Spectrum) | Not listed on the pages fetched. A Terrain store page exists but price was not retrieved | [U] | https://shop.sphengineering.com/products/lidar360-terrain |
| Module list in V9 | Framework, Terrain, ALS Forestry, TLS Forestry, Mine, 3D Building, Photo, Spectrum, Distributed Computing (third-party article; page returned 403 to direct fetch) | [P] | https://gnss.ae/?p=18147 |
| Licensing mechanics | USB dongle or software license code (online/offline activation); single-use and concurrent (LAN server, default port 8273); one-day LiCloud trial sign-in | [V] | https://www.greenvalleyintl.com/docs/lidar360/Introduction/Install.html |
| System requirements | Windows 7 to 11 64-bit (10+ recommended), >=32 GB RAM, NVIDIA GPU above GTX 970 with >=4 GB VRAM | [V] | same |
| Version history (forestry items) | v5.4 (2022-07-25) manual species marking, selectable attribute methods; v6.0 (dated 2022-08-10) ITS matching, tree 3D models, ITS report, TLS trunk extraction, CBH, ALS tree editor; v7.0 (dated 2022-06-10, date ordering looks inconsistent) biomass estimation, DL individual tree segmentation; v8.0 (2024-08-15) stand delineation, 70-species tree model library, stem curvature, stem sections/volume, forked-DBH, stand report, CHM pit filling; v9.0 (2024-08-26) DL crown segmentation (general and palm), ALS trunk-based segmentation, TLS spatial structure, species recognition merged into TLS seed editor, ALS DBH cylinder-fitting priority | [V] (dates as printed; ordering anomaly noted) | https://www.greenvalleyintl.com/docs/lidar360/CHANGELOG.html |
| v9.1 | Referenced by third parties in 2026; no official 9.1 notes found | [P] | https://gnss.ae/?p=18147 |
| Company / founder link | Dr. Qinghua Guo identified as founder and CEO of GreenValley International in 2022 Velodyne releases; Guo is second author of Li et al. 2012 | [V] (press release + paper) | https://www.businesswire.com/news/home/20221117005179/en ; https://www.ingentaconnect.com/content/10.14358/PERS.78.1.75 |

---

## 1 and 2. Feature inventory with documented inputs, outputs, parameters, and underlying algorithm

Confidence column refers to the **algorithm** attribution: Doc = named on the GreenValley page; Inferred = our reading of parameters; Unknown = not disclosed.

### 1A. ALS Forest: individual tree segmentation (ITS)

**CHM Segmentation** (ALS/CHMSegmentation.html) [V]
- Input: one or more CHM rasters loaded in LiDAR360.
- Params (defaults): Max tree height 80 m; Min tree height 2 m; Buffer 50 px (CHM tiled into 1500x1500 blocks); Crown base height threshold 0.8 m (pixels above define crown extent); optional Gaussian smoothing, Sigma 1, Radius 5 px (odd).
- Output: CSV (ID, X, Y, height, crown diameter, crown area) plus SHP crown polygons with same attributes.
- Algorithm: marker/watershed segmentation of the CHM; cites Chen, Baldocchi, Gong, Kelly (2006) PE&RS 72(8):923-932. Confidence: **Doc** [V]. Tuning guidance: lower Sigma (0.5) for under-segmentation, raise (1.5) for over-segmentation; CHM resolution finer than 1 m, FAQ says 0.3 to 1 m with 0.5 to 0.6 m often best, and not more than one-third of crown width [V] (ALS/CHMSegmentation.html; https://www.greenvalleyintl.com/docs/lidar360/FAQ/Forestry.html).
- FAQ recommends CHM Segmentation for mixed conifer/broadleaf forest [V].

**Point Cloud Segmentation (PCS)** (ALS/PointCloudSegmentation.html) [V]
- Input: normalized point cloud (by DEM or ground class); From Class filter.
- Params (defaults): Grid size 0.6 m (about one-fifth of mean crown diameter, 0.3 to 0.6 m typical); Buffer 50 px; Min tree height 0 m; Height above ground 2 m; Gaussian smoothing on, Sigma 1, Radius 5 px; optimize color rendering; output CSV or tree attribute file.
- Output per tree: ID, X, Y, height, crown diameter, crown area, crown volume.
- Algorithm: page states it follows Li et al. (2012): start at the highest point as the first tree top, then assign lower points by a spacing threshold (about crown radius) and nearest-neighbor comparison. Confidence: **Doc** [V]. The page also lists Tao et al. (2015) as the tool's reference entry, which looks like a doc copy/paste inconsistency [V for what the page says, U for which is correct].
- Observation: the grid-size and Gaussian parameters suggest a raster step (probably locating tree tops or setting the spacing threshold on a smoothed surface) in front of the Li 2012 region growing. This is a hybrid, not plain Li 2012 [P / Inferred].
- Full citation (verified on publisher listing): Li W., Guo Q., Jakubowski M.K., Kelly M. (2012). A new method for segmenting individual trees from the lidar point cloud. PE&RS 78(1):75-84. https://www.ingentaconnect.com/content/10.14358/PERS.78.1.75 [V]. Original reported accuracy in Sierra Nevada mixed conifer: recall 0.86, precision 0.94, F 0.90 [P] (abstract via search summary; https://czo-archive.criticalzone.org/sierra/publications/pub/li-et-al-2012-a-new-method-for-segmenting-individual-trees-from-the-lidar-p/).

**Generate Seed Points from CHM** (ALS/GenerateSeedbyCHM.html) [V]
- Same interface and defaults as CHM Segmentation (max 80 m, min 2 m, buffer 50, Sigma 1, Radius 5). Output: seed CSV (ID, X, Y, Z), no SHP.
- Algorithm: not described. Most likely local-maximum detection on the smoothed CHM, the same front end as CHM Segmentation. Confidence: **Inferred** [P].

**Generate Seeds from Layer Stacking** (ALS/GenerateSeedbyLayerStacking.html) [V]
- Input: normalized point cloud(s).
- Params (defaults): XSize/YSize 1 m (0.3 to 2 m); Height above ground 2 m; Layer thickness 1 m (0.5 to 2 m); Min spacing between trees 1 m; Buffer 50 px; Gaussian Sigma 1, Radius 5 px. Output: seed CSV (ID, X, Y, Z).
- Algorithm: "layer stacking" named, no citation. Name and parameters match the layer-stacking idea of Ayrey et al. (2017, Canadian Journal of Remote Sensing), which slices the cloud into height layers, clusters each layer, and stacks them to find tree centers. Confidence: **Inferred**; the GreenValley page does not cite Ayrey [P].

**Point Cloud Segmentation from Seed Points** (ALS/PCSwithSeeds.html) [V]
- Input: normalized point cloud plus matching seed CSV; up to five pairs per run; From Class.
- Params: Height above ground 2 m; optimize color rendering; output CSV or tree attribute file.
- Output per tree: ID, X, Y, height, DBH, crown diameter, crown area, crown volume. IDs may be renumbered.
- Algorithm: not described on the tool page. FAQ says seeded segmentation uses a "competitive" strategy, so tree count can differ from seed count and poorly shaped trees can be misassigned. Confidence: **Unknown / partially Doc** (competitive region growing from seeds) [V for FAQ statement]. How DBH is obtained from ALS points here is not documented [U].

**Trunk-based Tree Segmentation (ALS)** (ALS/ASLTrunkBasedSegmentation.html) [V], added v9.0
- Input: normalized point cloud or dataset.
- Params: Initial class all; Trunk class 23; Crown/Foliage class 23 or 24 (page shows both); Prefer GPU; Height above ground 0.3 m; optimize colors.
- Output: CSV (ID, X, Y, height, DBH, crown diameter, crown projection area, crown volume) and per-point trunk/crown classes.
- Algorithm: deep-learning trunk extraction, then trunks as seeds and branches/foliage grown outward. Cites López Serrano et al. (2022) AID-FOREST, IJAEOG 113:103014. Intended for high-penetration sensors that capture trunks. Confidence: **Doc** for the two-step pipeline; network architecture **Unknown** [V/U].

**Universal Tree Segmentation (ALS)** (ALS/UniversalPointCloudSegmentation.html) [V]
- Params: From class, Stem class, Branch-and-leaf class, Min tree height, color optimization. No defaults on ALS page. Output: segmented point cloud.
- Algorithm: stem and seed extraction, then crown growing. The TLS version of the page says the stem step is deep learning. No citation. Confidence: **Doc (pipeline), Unknown (model)** [V/U].

**Stem Extraction (ALS)** (ALS/StemExtraction.html) [V]
- Input normalized cloud; params From class, Stem class, Min tree height, Prefer GPU. Output: stem-classified points usable as seeds. Algorithm not stated; GPU option and FAQ imply deep learning [P].

**Individual Tree Crown Segmentation (deep learning, imagery)** (ALS/ExtractCrownByDeepLearning.html) [V], added v9.0
- Input: RGB (>=3-band, 8-bit TIFF) orthophoto at 1 to 10 cm GSD; optional mask. Not a point cloud.
- Params: Overlap (IoU) ratio 0.7 for NMS; Confidence threshold 0.3; model choice General or Palm.
- Output: crown polygon vector plus tree attribute file (position = mean of contour vertices).
- Algorithm: YOLOv8 (Ultralytics) instance segmentation; page claims mAP 95% with no dataset or protocol given. Confidence: **Doc** for architecture; accuracy claim **unverified** [V/U].

**Editing and utilities** [V]
- ALS Seed Point Editor (ALS/ALSEditor.html): add seeds at tree tops, select/delete seeds, profile view, tree filter by height/crown area, rerun seeded PCS, save seeds as new CSV; undo/redo 20 steps.
- Individual Tree Editor (ALS/IndividualTreeEditor.html): v6.0 notes describe semi-automatic segmentation and create/delete/merge operations [V via changelog].
- Clear Tree ID, Clear Tree ID by File, Mosaic ITS (merge tiled results), Extract by Tree ID, View CHM / Point Cloud Segmentation Results (ALS/ index) [V for existence; behaviour inferred from names, P].

**Batch** (ALS/BatchProcessIndex.html): batch Forest Metrics, batch Point Cloud Segmentation, batch CHM Segmentation [V existence].

### 1B. ALS Forest: tree attributes

| Function | Inputs | Outputs / params | Algorithm | Tag |
|---|---|---|---|---|
| Individual Tree Attributes Statistics (ALS/StatisticIndividualTreeAttributes.html) | ITS-segmented cloud; optional DEM, slope TIFF, reference-point CSV (X, Y, Radius) | New or updated attribute file; elevation, slope, azimuth/distance to reference points; Height-above-ground and Min tree height filters | Not described. v5.4 notes say position and crown-width methods are selectable; v9.0 adds "DBH position priority" and "cylinder fitting priority" for ALS DBH | [V] page; methods [P] |
| Extract Tree Crown Boundaries (ALS/ExtractTreeCrownContour.html) | LiData cloud with Tree IDs | Vector polygons; Convex hull (default) or Concave hull with max edge length 2 m (0 = 2x mean spacing) | 2D convex/concave hull, XY only | [V] Doc |
| Generate DBH Inversion Model (ALS/GenerateDBHInversionModel.html) | .treedb sample with height, crown width, measured DBH | .dbhim model; 16 selectable forms (linear, log, power, polynomial, exponential, logistic in H and CW) | Least-squares fit of allometric-style equations | [V] Doc |
| DBH Inversion (ALS/DBHInversion.html) | .treedb + .dbhim | DBH written back plus HTML report | Applies the stored equation; DBH is modelled, not measured, for ALS | [V] Doc |
| Tree Species Library (ALS/TreeSpeciesLibrary.html) | Species records, import/export | Stores species, linked biomass models, tree models, species recognition models | Storage only; no ALS classifier on this page | [V] |
| View Azimuth and Distance, Tree Attribute Format Conversion | attribute files | CSV <-> treedb etc. | Utility | [V] existence |
| Generate Tree Model / Tree Model Management | attribute file | 3D tree models (library of 70 species since v8.0) for visualization | Template instancing, not reconstruction | [P] |

### 1C. ALS Forest: area-based metrics and regression

**Calculate Forest Metrics by Grid / Polygon / Forest Stands** (ALS/CalculateForestMetricsByGrid.html etc.) [V]
- Input: normalized cloud; optional region file.
- Params: XSize/YSize 15 m (should exceed crown size); Height break 2 m; Extinction coefficient 0.5 (spherical/elliptical leaf angle assumption).
- Output: CSV or TIFF per metric family; vector of retained cells if region used.
- Families: elevation, intensity, canopy cover, LAI, gap fraction.

**Elevation metrics** (ALS/TheoryOfElevationMetrics.html) [V]: 46 elevation metrics (mean, median, SD, variance, CV, AAD, MAD median, min, max, skewness, kurtosis, quadratic and cubic means, 15 height percentiles 1 to 99 plus IQR, 15 cumulative height percentiles (AIH) plus IQR, canopy relief ratio) and 10 density metrics (proportion of returns in ten equal height slices). No citation; these mirror the FUSION/lidR standard metric set. Confidence: **Doc** (definitions), lineage **Inferred** [V/P].

**Intensity metrics** (ALS/TheoryOfIntensityMetrics.html): exists [V]; contents not fetched [U].

**Canopy cover** (ALS/TheoryOfCanopyCover.html) [V]: vegetation first returns / all first returns per cell when return numbers exist, otherwise vegetation points / all points above a height threshold. Cites Jennings, Brown, Sheil (1999) Forestry 72(1):59-73 and Ma, Su, Guo (2017) IEEE JSTARS 10(9):4225-4236 (Guo group). Confidence: **Doc**.

**Gap fraction and LAI** (ALS/TheoryOfGapFraction.html, ALS/TheoryOfLeafAreaIndex.html) [V for variables, P for exact formula]: gap fraction uses ground-return count over total return count; LAI is a function of mean scan angle, gap fraction, and extinction coefficient k. Cites Chen and Black (1991? page lists Chen et al. 1991 Agric. For. Meteorol. 57:1-12) and Richardson et al. (2009) Agric. For. Meteorol. 149(6):1152-1160. The formula images were unreadable; the variable list is consistent with the Beer-Lambert form LAI = -cos(mean scan angle) * ln(GF) / k used by Richardson et al. [P, Inferred].

**CHM Pits Filling** (ALS/CHMPitsFilling.html): exists since v8.0 [V]; algorithm not fetched [U] (likely Khosravipour-style pit-free CHM or morphological fill).

**Regression Analysis** (ALS/RegressionAnalysisIndex.html) [V]
- Linear, SVM, FANN (Fast Artificial Neural Network library), Random Forest regression; Run Existing Model; Regression Predict.
- Random Forest page: built on scikit-learn and NumPy; params Tree num, Max depth, Min split, Min leaf, K-fold CV (K>1); outputs HTML report with residuals, predicted TIFF, optional saved model and training CSV [V] (ALS/RandomForest.html).
- FAQ: at least 30 random, representative plots recommended; exports metrics for SPSS/R [V].
- Algorithm: standard library regressors. Confidence: **Doc**.

**Forest Stands Delineating** (ALS/ForestStandDelineation.html) [V]: CHM input; Region Growing or Cellular Automata; cell size, min stand area, min/max height, initial stand area (CA). No defaults or citation. Algorithm family **Doc**, details **Unknown**.

**Forest Structure Change Detection** (ALS/ForestChangeDetection.html), **Automatic Registration of Forest** (ALS/ForestPointCloudRegistration.html): exist [V]; contents not fetched [U].

### 1D. TLS Forest

| Function | Inputs | Key params (defaults) | Outputs | Algorithm and confidence | Source |
|---|---|---|---|---|---|
| Filter Ground Points | TLS cloud(s) | Grid 0.5 m; ground thickness 0.3 m; optional window smoothing, window 3 | Ground class | Lowest point per cell plus thickness band, neighborhood consistency. Doc, no citation [V] | TLS/FilterGroundPoints.html |
| Point Cloud Segmentation (TLS) | Normalized cloud | Clustering threshold 0.2 m; min cluster size 500; height above ground 0.3 m; min tree height 2 m; trunk height 1.6 m | CSV or attribute file: ID, X, Y, H, DBH, crown dia, area, volume | Trunk detection in a 0.3 to 1.6 m band, trunks as seeds, cluster growth. Cites Tao et al. (2015) ISPRS J. 110:66-76 (comparative shortest-path, Guo group). Doc [V] | TLS/PointCloudSegmentation.html |
| Trunk-based Tree Segmentation (TLS) | Normalized cloud | Trunk class 23; HAG 0.3 m; stem segment up to 5 m; foliage class; cluster threshold 0.2 m; segment-from 1.0 m | CSV as above plus trunk/foliage classes | DL stem extraction then crown growth; cites AID-FOREST (López Serrano et al. 2022). Best for plantations DBH > 10 cm. Doc (pipeline), Unknown (net) [V] | TLS/StemBasedPointCloudSegmentation.html |
| Universal Tree Segmentation (TLS) | Normalized cloud | Stem class 23; branch/leaf 24; min height 2 m; Prefer GPU | CSV as above | DL stem step, then seeds and crown growth. No citation [V] | TLS/UniversialPointCloudSegmentation.html |
| Tree Branch and Segmentation | Normalized cloud | As trunk-based, but trunk class includes branches | CSV; trunk+branch class and foliage class | Extracts trunk and major branches for species without a clear leader; AID-FOREST cited. No QSM described [V] | TLS/TreeBranchAndSegmentation.html |
| PCS from Seed Points (TLS) | Cloud + seed file | HAG | CSV | Same as ALS seeded PCS [V] | TLS/PCSwithSeeds.html |
| TLS Seed Point Editor | Normalized cloud, seeds | Min points 10; DBH range 0.05 to 1.00 m; max lean 30 deg; confidence color classes | Seed CSV or .tatt; DBH per tree; species labels | DBH by least-squares 2D circle (default), 3D cylinder (leaning stems), or direct least-squares ellipse (DBH = 2*sqrt(a*b)); batch fit with confidence grades; species marking via panoramic overlay. Doc [V] | TLS/TLSEditor.html |
| DBH Measure (manual) | Cloud, click at tree base | Measuring height, buffer height, buffer radius | DBH table, export txt/csv etc. | Points in a cylinder buffer fitted with planar circle. Doc [V] | TLS/DBHMeasure.html |
| Extract CBH | Normalized cloud + attribute CSV | Stem volume to CBH or to tree top | CBH column, StemVolume column | Not described [V/U] | TLS/TLSCBHExtraction.html |
| Compute Volume with Branches | Trunk-based result with branch class + attribute file | Min trunk diameter 12.1 cm | Trunk volume, branch length, length/start-DBH/end-DBH columns | Not described; output suggests stem tapering in sections, not a full QSM [V/P] | TLS/ComputeBranchVolume.html |
| Stem curvature, stem sections/volume, forked-DBH | Segmented trees | n/a | Attributes | Added v8.0; not fetched [V existence, U method] | CHANGELOG; TLS/StemCurvature.html |
| Estimate Tree Biomass / Biomass Model Manager | Attribute file (species, DBH, H) | Database: Chinese, North American (NBEL, about 7,000 USDA-sourced equations), Custom; filter by region, species, author, part | Biomass columns appended | Lookup and apply published allometric equations. Doc [V] | TLS/TreeBiomassEstimation.html |
| Quantify Spatial Structure | Normalized cloud | XY 5 m; vertical step 0.2 m; Area or Point-count method | TIFFs or CSV | Per-slice vegetation occupancy summed/averaged. No citation [V] | TLS/QuantityMetrics.html |
| Canopy cover, gap fraction, LAI (TLS) | TLS cloud | n/a | Rasters | Exist; not fetched [U] | TLS/TLSCanopyCover.html etc. |
| Forest metrics by grid/polygon/stand (TLS) | Normalized cloud | n/a | n/a | Exist [V existence] | TLS/TLSForestMetricsIndex.html |
| Match Tree Locations; Forest Registration | Two tree maps / clouds | n/a | Matched IDs / transform | Exist; method not fetched [U] | TLS/MatchTreeLocations.html |
| Individual Tree Report | Segmented cloud + attribute file | ID range, colour mode, views, attributes, description | HTML report with per-tree renders and attributes | Doc; TLS only because ALS too sparse for images [V] | TLS/IndividualTreeReport.html |
| Stand Analysis and Reporting, Stand Tree Volume, Forest Tending Planning (+ SDK guide), Carbon Sequestration | Attribute files | n/a | Reports, plans | Exist [V existence]; not fetched [U] | TLS/ index |
| Gaussian Mixture Model Classification | Cloud | n/a | Classes | Exists, probably wood/leaf or ground/vegetation GMM [U] | TLS/GaussianMixtureModelClassification.html |

---

## 3. Open-source equivalents

Licenses checked 2026-10-13 via GitHub API or license file. Record again before any use, per CLAUDE.md.

| LiDAR360 function | Open-source equivalent(s) | License | Gap / notes |
|---|---|---|---|
| CHM generation, pit filling | lidR `rasterize_canopy()` with `p2r`, `dsmtin`, `pitfree` (Khosravipour 2014) | GPL-3.0 [V] | None material |
| Seeds from CHM | lidR `locate_trees(lmf(ws, hmin))` | GPL-3.0 [V] | Gaussian pre-smoothing via `terra::focal` or lidR examples |
| CHM Segmentation (watershed, Chen 2006) | lidR `segment_trees(watershed())` (EBImage), `dalponte2016()`, `silva2016()`; also `ForestTools::mcws` (marker-controlled watershed) | lidR GPL-3.0 [V]; ForestTools license not checked [U] | Chen 2006 uses a variable-window marker watershed; `mcws` is closest |
| Point Cloud Segmentation (Li 2012) | lidR `segment_trees(li2012())` | GPL-3.0 [V] | lidR implementation is O(n^2) and slow on dense UAV data; LiDAR360 adds raster pre-processing we cannot replicate exactly |
| Layer stacking seeds | No mainstream maintained implementation found; Ayrey et al. 2017 is published and reimplementable | n/a | Gap: needs our own implementation if wanted |
| Seeded segmentation | lidR `dalponte2016(chm, ttops)` / `silva2016(chm, ttops)` accept user tree tops; PDAL has no tree segmentation stage | lidR GPL-3.0; PDAL BSD [V] | Point-based seeded competitive growth is not a one-liner in lidR; can be scripted |
| Trunk/stem-based segmentation (ALS and TLS) | 3DFin (stem detection, DBH, heights; CloudCompare plugin and standalone); FSCT (semantic + instance); SegmentAnyTree (sensor-agnostic DL); TreeLearn; ForAINet | 3DFin GPL-3.0 [V]; FSCT GPL-3.0 [V]; SegmentAnyTree MIT [V]; TreeLearn (ecker-lab) MIT [V]; ForAINet custom license file, terms not reviewed [U] | Model-weight licenses must be checked separately from code [U] |
| DL crown delineation from RGB (YOLOv8) | DeepForest (RetinaNet boxes), detectree2 (Mask R-CNN polygons), SAM-based tools; Ultralytics YOLOv8 itself | DeepForest MIT [U, not re-checked]; detectree2 MIT [U]; Ultralytics is AGPL-3.0 [U, not re-checked] | Note: LiDAR360 bundles an AGPL library; licensing of their use is their concern, but any TreeDar reuse of Ultralytics would carry AGPL obligations [U] |
| Crown boundaries (convex/concave hull) | lidR `crown_metrics(geom = "convex" or "concave")`; `concaveman` | GPL-3.0 [V] | None |
| Tree attributes (H, crown area, volume) | lidR `crown_metrics()`, `tree_metrics()` | GPL-3.0 [V] | None |
| DBH from TLS/MLS (circle, cylinder, ellipse fit) | 3DFin; FSCT; CloudCompare 3DFin plugin; TreeLS (R, archived); `CircleFit` routines in Python | 3DFin GPL-3.0; CloudCompare GPL-2.0-or-later [V] | None material |
| DBH inversion from H and CW | Any regression in R/Python (nls, statsmodels) | n/a | Trivial to replicate; needs field DBH |
| Area-based metrics (46 elevation + 10 density, intensity) | lidR `pixel_metrics(.stdmetrics)`, `plot_metrics()`; FUSION GridMetrics (USFS, free but not OSI) | lidR GPL-3.0 [V] | Metric names differ slightly; mapping table needed |
| Canopy cover, gap fraction, LAI | lidR `pixel_metrics` with custom functions; `lidR::LAD()`; leafR package | lidR GPL-3.0 [V]; leafR not checked [U] | Easy to reimplement from cited formulas |
| Regression (linear, SVM, ANN, RF) | scikit-learn, R `randomForest`/`ranger`, `e1071` | BSD-3 (sklearn) [U, not re-checked] | None |
| Ground filtering | PDAL `filters.smrf`, `filters.csf`; CSF library (Zhang et al. 2016, Guo group) | PDAL BSD [V]; CSF Apache-2.0 [V] | None |
| Normalization | lidR `normalize_height()`; PDAL `filters.hag_*` | GPL-3.0 / BSD [V] | None |
| Stand delineation | Generic: `terra`/`scikit-image` region growing, `SAGA` segmentation | various | No dedicated forestry tool; easy |
| Biomass equations (NBEL USDA) | Published equations (Jenkins et al.; NSVB) applied in code; `allodb` R package | allodb license not checked [U] | Equations public; DB packaging is the value-add |
| Branch volume / QSM | TreeQSM (MATLAB), AdTree, SimpleForest (CloudCompare plugin), aRchi | TreeQSM GPL-3.0-or-later but needs MATLAB [V]; AdTree GPL-3.0 [V] | Open tools exceed LiDAR360's documented branch features for TLS; none suit sparse UAV ALS |
| Editors (seed edit, tree edit) | CloudCompare manual segmentation, lidR `tree_metrics` + QGIS editing | GPL-2.0+ [V] | Gap: no polished open seed/tree editor with re-segmentation loop. This is the biggest usability gap |
| Batch processing | lidR `LAScatalog` engine; PDAL pipelines | GPL-3.0 / BSD [V] | None (open tooling is stronger for scripting) |
| HTML reports | R Markdown / Quarto | n/a | None |
| Species recognition (DL on per-tree points or panoramas) | Research code only (e.g., PointNet++ / DGCNN baselines, FOR-species20K benchmark code); no turnkey tool | various [U] | Gap: no maintained, general open-source species classifier; must train our own with field labels |
| LLM species identification | None needed; general multimodal LLM API | n/a | Remote inference on project data would require Beau's approval |

---

## 4. Species classification, deep learning, QSM / branches

**Species classification: yes, but documented in TLS Forest only.** [V]
- Point Cloud Tree Species Recognition (TLS/TreeSpeciesRecognition.html): deep-learning model on individually segmented trees; Prefer GPU; name priority (Latin, Chinese, common, genus); optional panoramic mode if the model was trained with panoramas. No architecture, built-in species list, training data, or accuracy is published [V for what is stated, U for everything else].
- Custom Tree Species Classification Model Training (TLS/DefineTreeSpeciesRecognitionModel.html): user trains on segmented clouds with TreeID plus CSV mapping TreeID to species, or on panoramic images plus CSV labels. Architecture and hyperparameters not given [V].
- Tree Species Identification Based on Point Cloud and Panoramic Images (TLS/TreeSpeciesIdentification-PanoramicImages.html): modes Point Structure, Panoramic Images (optional LLM refinement), LLM; writes species and confidence to .treedb [V].
- LLM Tree Species Identification (TLS/LLMTreeSpeciesIdentification.html): reasons over tree attributes and optional panoramas; requires network connection and LiCloud login; provider not named; data sent off-device not documented [V for requirements; data flow U]. Any TreeDar use would count as remote processing needing Beau's approval.
- ALS Forest has only a Tree Species Library (storage) and manual species marking (v5.4). No documented ALS or RGB-ortho species classifier [V absence in the ALS nav; P that none exists in the binary].
- Implication for TreeDar: LiDAR360 does not offer a documented, validated UAV-ALS-plus-RGB species classifier. Its species tools target TLS/MLS plus panoramic camera data (GreenValley backpack/handheld products) [P].

**Deep learning features** [V]
- ALS: DL trunk extraction (Trunk-based Tree Segmentation, Stem Extraction, Universal Tree Segmentation), YOLOv8 RGB crown segmentation (general, palm).
- TLS: DL stem extraction, DL species recognition, custom species training.
- Framework (not Forestry): DL point classification, DL ground classification, DL road extraction, custom DL classification, and a separately downloaded Deep Learning Server (https://www.greenvalleyintl.com/docs/lidar360/DeepLearningServer/LiDAR360DLServerUserGuide-EN.html, not fetched) [V existence].
- FAQ: DL stem extraction needs an NVIDIA GPU with current drivers; CPU fallback by unchecking Prefer GPU [V].

**QSM / branch features** [V/P]
- No documented quantitative structure model (cylinder tree reconstruction) of the TreeQSM type. Closest: Tree Branch and Segmentation (trunk plus major branch points), Compute Volume with Branches (trunk volume, branch length, branch start/end diameter), stem curvature, stem sections/volume, forked-DBH (v8.0). Methods are not described, so whether a cylinder model is used internally is [U].
- "Generate Tree Model" produces visualization models from a 70-species library, not reconstructions [P].

---

## 5. Published accuracy evaluations involving LiDAR360

| Study | What was compared | Result relevant to LiDAR360 | Tag |
|---|---|---|---|
| Li et al. 2012 PE&RS 78(1):75-84 | The algorithm behind LiDAR360 PCS, Sierra Nevada mixed conifer, airborne | Recall 0.86, precision 0.94, F 0.90 (original paper, not the commercial build) | [P] (abstract via search summary) |
| Marcello et al. 2024, Drones 8(12):772, doi:10.3390/drones8120772 | UAV Zenmuse L1, two forest types in Spain; Dalponte2016, Silva2016, Watershed, Li2012; ground filtering PMF (lidR) vs CSF (LiDAR360) | Li2012 best (F1 91%, IoU 83%) per abstract. Search summary of full text: best local-maximum tree detection F 88.89% in lidR (ws 6) vs 91.67% in LiDAR360 (sigma 7). Full text returned 403 to us, so the LiDAR360 numbers are second-hand | Abstract [V] (https://upcommons.upc.edu/entities/publication/b3e5f34a-d44f-4a50-bbc6-399436c584e6); LiDAR360 numbers [P] (https://www.mdpi.com/2504-446X/8/12/772) |
| Yan et al. 2024 (eucalyptus, cited in the Marcello review table) | LiDAR360 and lidR used for ground classification and segmentation | Not retrieved | [U] |
| YOLOv8 crown model (vendor) | Vendor claim on tool page | mAP 95%, no dataset, split, or IoU threshold | [V that the claim exists; U accuracy] |

We found no independent, peer-reviewed head-to-head benchmark of LiDAR360's full ITS pipeline (CHM, PCS, seeded PCS, trunk-based DL) against lidR, 3DFin, FSCT, or SegmentAnyTree on shared field-validated plots [P, absence after several searches]. GreenValley's FAQ recommends the standard Li 2012 detection metrics (TP, FN, FP, recall, precision, F) against field data [V], which matches what TreeDar should use regardless.

---

## Key takeaways for TreeDar

1. Most ALS Forest functions map onto published, open algorithms already in lidR (Li 2012, watershed, local maxima, standard metrics, convex hulls, regression). The commercial value is the GUI, seed/tree editors, batch tiling, and reporting, not unique algorithms [P].
2. Newer value-add is closed: DL trunk extraction (AID-FOREST-inspired), YOLOv8 RGB crowns, TLS species DL, LLM species. Architectures, training data, and accuracy are not published [V/U].
3. LiDAR360 does not document a UAV-ALS or RGB-ortho species classifier. Buying it would not answer TreeDar's species-classification feasibility question [V/P].
4. ALS DBH is modeled from height and crown width via user-fitted equations, so it needs field DBH; it is not a measurement [V].
5. Minimum reseller cost seen: USD 7,090 (Framework plus Forestry), excluding tax, maintenance terms unknown; Windows, NVIDIA GPU, 32 GB RAM [V/P].

## Not done / open items

- Did not fetch: Intensity metrics theory, Gap Fraction theory page, CHM Pits Filling, Forest Change Detection, Registration, Individual Tree Editor, stand reporting, TLS canopy/LAI, Deep Learning Server guide, Appendix file formats.
- Did not verify licenses for ForestTools, DeepForest, detectree2, Ultralytics, leafR, allodb, scikit-learn, or any model weights.
- Did not watch GreenValley webinars/YouTube.
- Version of the online guide beyond v9.0 is unconfirmed.
