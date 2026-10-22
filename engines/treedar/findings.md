# Drone LiDAR maps crowns, not branch architecture

**Short answer:** Partly. With the borrowed R3 Pro and RGB imagery, a small Wisconsin operator can plausibly deliver per-tree structure layers for individually resolvable canopy trees: position, height, crown area and volume. Coarse type layers also look achievable, such as conifer vs broadleaf and some distinct groups like white pine, aspen and paper birch. Full species maps for northern hardwoods are a different matter. The best regional drone RGB study reached only 0.61 to 0.72 mean F1 across 14 classes, and red vs sugar maple stayed confused. Branch architecture is unlikely to rescue species ID. Above-canopy drone LiDAR has reliably reconstructed only trunks and limbs of about 30 cm diameter or more, and only with survey-grade RIEGL sensors. I found no peer-reviewed stem or DBH results for the Hesai sensor in the R3 Pro or for the DJI L-series. Crown and branch form also change strongly with competition within a single species, which confounds any architecture-based classifier. The cheapest decisive step is one leaf-off test flight on non-client land, checked against tape-measured stems. The precondition is confirming that the borrowed unit's ROCK Desktop licence is still active, because without it the raw data cannot be turned into LAS/LAZ.

**Evidence labels used throughout:** **[V]** verified (I read the primary source or official page: paper full text, manufacturer spec page, LICENSE file, dataset landing page). **[P]** partially verified (abstract, search snippet, reseller, vendor blog or forum only). **[U]** unverified, or an estimate computed for this report. Vendor marketing is flagged as such even when the page itself was read. Airborne results are labelled **AIRBORNE** and hyperspectral results **HSI**. Neither is evidence for drone RGB feasibility.

## Above-canopy drone LiDAR reliably resolves only trunks and limbs over about 30 cm

**Trunks, large limbs and DBH.** The positive evidence comes almost entirely from survey-grade RIEGL sensors. Brede et al. (2019) compared drone and terrestrial LiDAR quantitative structure models (QSMs) on 200 trees. Drone LiDAR modelled volume reliably for trunks and branches **at least 30 cm in diameter** in a mature beech/oak stand (CCC 0.85). Including smaller branches caused large overestimation (CCC 0.51). Trunks were essentially lost in dense Norway spruce (CCC 0.36) and in small young trees (CCC 0.01) [P] ([Tampere record](https://trepo.tuni.fi/handle/10024/215373)). In a leaf-off RIEGL VUX-1 flight, stems with **DBH over 20 cm reconstructed almost 100%** of the time, with 9% relative DBH error at 20 to 30 cm, but trunk sections were selected by hand [P] ([Wieser et al. 2017](https://www.mdpi.com/2072-4292/9/11/1154)). Automated stem detection in a follow-up reached 91.0% with a miniVUX-1DL and 77.6% with a VUX-1UAV [P] ([EGU2020-7378](https://meetingorganizer.copernicus.org/EGU2020/EGU2020-7378.html)). Halving the pulse rate measurably worsened DBH error [P] ([Bruggisser et al. 2020](https://archiv.ub.uni-heidelberg.de/volltextserver/37194/1/bruggisser_et_al_2020_forestdynamics.pdf)). So point coverage on the stem itself matters more than nominal ground density.

Even leaf-off, point density near 1.3 m stays low in closed canopy [P] ([Remote Sensing 13(3):352](https://api.crossref.org/works/10.3390%2FRS13030352)). A Finnish group judged above-canopy drone LiDAR not yet accurate enough for individual-tree stem attributes, with DBH RMSE of 10 to 20%. Under-canopy flight reached about 1 cm DBH RMSE [P] ([UTU](https://www.utupub.fi/handle/11111/35379)).

**Forks and branches.** Branch skeletons from drone LiDAR break into disconnected pieces where branches are occluded [P] ([Cárdenas et al. 2022](https://www.frontiersin.org/journals/environmental-science/articles/10.3389/fenvs.2022.960083/pdf)). I found **no study that validates drone-derived fork heights, branch angles or branch orders** against field or terrestrial data for temperate hardwoods.

**The sensor gap.** I found no peer-reviewed stem or DBH study for the DJI L1, L2 or L3, and none for any Hesai-based drone system, which includes the XT32 in the R3 Pro. The only peer-reviewed L1 accuracy work measured about 3.5 cm on survey targets, not stems [V] ([Štroner et al. 2021](https://www.mdpi.com/2072-4292/13/23/4811)). A peer-reviewed comparison of Livox, Hesai Pandar40, Velodyne and RIEGL scanners found broadleaf tree heights much less accurate than conifer heights (RMSE 1.63 m vs 0.59 m) [P] ([Remote Sensing 13(1):77](https://www.mdpi.com/2072-4292/13/1/77)).

My inference [U]: on Wisconsin hardwoods, realistic architecture targets for the R3 Pro are main-stem position, first fork height and major scaffold limbs on large, open-grown, leaf-off trees. Small-branch geometry should not be promised. Hemlock, spruce, fir, cedar and dense young aspen will probably behave like the Brede spruce case, with trunks largely invisible from above.

**Does architecture help species ID?** The evidence says only weakly, and competition undermines it. Point-cloud species classifiers reach 70 to 95% overall accuracy across studies, but none I found isolates branching architecture as the driver or uses drone-derived branch skeletons as features. The best drone LiDAR result in a mixed temperate forest found **intensity features, not structure, most useful**: 75 to 76% overall accuracy for one season and 80% for leaf-on plus leaf-off, with beech and hornbeam confused [P] ([EGU26-14442](https://meetingorganizer.copernicus.org/EGU26/EGU26-14442.html); [Forestry 2026](https://academic.oup.com/forestry/article/99/4/cpag069/8789523)). Leaf-off data clearly helps conifer vs deciduous separation. One airborne full-waveform study (AIRBORNE) reported 85% leaf-on vs 96% leaf-off for that split [P] ([ISPRS laserscanning09](https://www.isprs.org/proceedings/XXXVIII/3-W8/papers/158_laserscanning09.pdf)). The same literature notes that branch and foliage arrangement is highly plastic within a species and even within one tree [P] ([Remote Sensing 2021 review](https://doaj.org/article/d44cd140ae8d44a9bab89e8eb2eed0fc)).

**Open-grown vs crowded trees.** Your open-grown vs crowded concern is supported by terrestrial LiDAR work. **Sugar maple foliage shifted toward the crown base in mixed stands compared with pure stands** [P] ([Martin-Ducup et al. 2018](https://mdpi-res.com/d_attachment/forests/forests-09-00673/article_deploy/forests-09-00673.pdf)). Beech in mixture had flatter branch angles and larger crowns than in pure stands [P] ([Barbeito et al. 2017](https://waldwachstum.wzw.tum.de/fileadmin/publications/Barbeito_2017.pdf)). I found no study that reports species accuracy separately for open-grown vs closed-canopy trees, or that uses a true neighbour-competition index as a classification input.

The implication [U]: an open-grown sugar maple and a suppressed sugar maple can differ more in form than a sugar maple and a red maple of the same crown class. Any architecture classifier trained mostly on one crown class should be expected to degrade on the other. Suppressed trees are also lost first at segmentation, before classification even starts. In a mixed-conifer study, detection recall fell from 0.67 to 0.36 as canopy cover rose [P] ([Can. J. Remote Sensing 2018](https://doaj.org/article/546ce589aca748a1805c841d80d51116)).

## Prior work tops out near 0.6 to 0.8 accuracy and leaves maples confused

**Closest regional analog (drone RGB).** Cloutier, Germain and Laliberté flew a DJI Phantom 4 RTK 60 m above a mixed temperate forest in Quebec on seven dates in 2021. They labelled **23,000 crowns in 14 classes** using a submeter GNSS tablet in the field. **Mean F1 was 0.72 at the best date (2 September, the start of colouring) and 0.61 at peak October colour** [V] ([bioRxiv preprint](https://www.biorxiv.org/content/10.1101/2023.08.03.548604.full.pdf)). Results by group:

| Group | Result |
|---|---|
| White pine and tamarack | Best classes, F1 0.77 to 0.87 |
| Striped maple, beech, yellow birch | Weakest classes |
| Red maple | Frequently predicted as sugar maple at every date |

The train/validation split was random by tile, and the test set was only partly spatially independent, so these numbers likely overstate accuracy on a new property [V] (same source). In New Hampshire, multi-date drone RGB beat single-date imagery, gains plateaued at three dates, and spring imagery was best [P] ([Grybas and Congalton 2021](https://www.mdpi.com/2072-4292/13/13/2631/htm)). In Germany, drone RGB reached mean F1 0.73 for nine species, and adding canopy height gave only a slight gain [P] ([Schiefer et al. 2020](https://rsc4earth.de/publication/schiefer-mapping-2020/)).

**Drone LiDAR alone.** In a mixed temperate forest, drone LiDAR alone reached the 75 to 80% overall accuracy described above for about six species [P]. Conifer vs broadleaf from drone LiDAR reached about **95% overall accuracy** on 1,045 trees [P] ([Slavík et al. 2023](https://www.mdpi.com/1999-4907/14/8/1581)). On the mostly European FOR-species20K benchmark, models that classify 2D views rendered from the point cloud averaged 0.77 overall accuracy, ahead of 3D point models at 0.72. Those trees were pre-segmented, so segmentation error is excluded [P] ([arXiv 2408.06507](https://export.arxiv.org/pdf/2408.06507)).

**Fusion results are mostly hyperspectral (HSI), not RGB.** In Northeast China, hyperspectral plus LiDAR gave 89.2%, hyperspectral alone 86.1% and LiDAR alone 76.4% (HSI) [P] ([PMC9539217](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC9539217/)). I found **no peer-reviewed drone LiDAR plus drone RGB species study in Great Lakes or northeastern northern hardwoods**. That gap is an inference from search, not proof that none exists.

**NEON species maps (AIRBORNE, HSI, not comparable to drone RGB).** Weinstein et al. (2024) produced per-tree species maps at the three Domain 05 sites [V] ([PMC11251727](https://pmc.ncbi.nlm.nih.gov/articles/PMC11251727)). Micro / macro accuracy by site:

| Site | Accuracy (micro / macro) | Species |
|---|---|---|
| UNDE | 0.79 / 0.79 | 13 |
| TREE | 0.78 / 0.72 | 15 |
| STEI | 0.80 / 0.83 | 6 |

The species input was **airborne hyperspectral**, and the authors found RGB did not significantly improve classification. The paper also gives two useful lessons on labels. At UNDE, **157 targeted extra field stems raised macro accuracy from 61.6% to 79.1%**. The authors list stem-to-crown mismatch as an accuracy limit, and crown detection was only about 70 to 75% accurate [V] (same source).

**Commercial landscape.** A limited search found no company advertising drone per-tree species maps in Wisconsin or the Upper Midwest [P]. Adjacent players exist, including Digital Timber in Ontario [P] ([CBC](https://amp.cbc.ca/news/canada/sudbury/northbay-drones-counting-trees-1.5827160)). One is directly relevant: Tree Stand Forestry, an Ohio consultancy serving deer hunters [P] ([DroneOn Show Ep. 35](https://the-droneon-show.castos.com/episodes/from-forestry-to-drone-tech-habitat-lidar-public-hunts-the-droneon-show-episode-35)). "Only one in the region" is not established.

## A permissive open-source stack exists, with several commercial-use traps

All licences below were read from LICENSE files or landing pages on 2026-10-13 [V] unless marked otherwise. This is a technical reading, not legal advice.

| Component | Role | Licence | Commercial-use note |
|---|---|---|---|
| PDAL | I/O, filtering, ground, tiling | BSD | OK |
| lidR | CHM-based tree segmentation, metrics | GPL-3.0 | OK for internal use; GPL applies if you distribute derived code |
| CloudCompare, 3D Forest, 3DFin | QA, labelling, stem tools | GPL | Same as lidR |
| SegmentAnyTree | Deep-learning tree segmentation | MIT; weights inside the MIT repo, no separate weight licence | Best permissive candidate. Needs NVIDIA GPU, Docker, Linux |
| ForAINet | Deep-learning segmentation | BSD-3; weight licence not verified [U] | Check weights |
| ForestFormer3D | Top-scoring segmentation | **CC BY-NC 4.0 code**; weights on Zenodo labelled GPL-3.0 | **Non-commercial: blocker** |
| 3Dtrees_ForestMamba | Segmentation | **CC BY-NC 4.0** | **Non-commercial** |
| DetailView | Point-cloud species classifier (1st in FOR-species20K, OA 0.79) | GPL-3.0 code, CC BY 4.0 weights | Trained on European species; Wisconsin needs retraining [U] |
| DeepForest | RGB crown detection | MIT code, MIT weights | Good RGB baseline, trained on US NEON data |
| TreeQSM | Branch/volume models | GPL-3.0 | Needs paid MATLAB; built for terrestrial scans |
| AdQSM | Branch models | **No licence; Windows binaries only** | Avoid |
| LAStools | LiDAR tools | Mixed | **Closed tools require a paid licence for commercial use**; PDAL covers most needs |
| COPC, Potree, PotreeConverter | Formats and web viewing | MIT / BSD | OK |

Sources: [SegmentAnyTree](https://github.com/SmartForest-no/SegmentAnyTree), [ForAINet](https://github.com/prs-eth/ForAINet), [ForestFormer3D](https://github.com/SmartForest-no/ForestFormer3D), [DetailView](https://github.com/JulFrey/DetailView), [DeepForest weights](https://huggingface.co/weecology/deepforest-tree), [LAStools LICENSE](https://github.com/LAStools/LAStools), [AdQSM](https://github.com/GuangpengFan/AdQSM), [lidR](https://github.com/r-lidar/lidR), [PDAL](https://github.com/PDAL/PDAL). I could not find a tool named "Pc2Tree".

**Benchmarks on deciduous trees are thin.** On FOR-instance, the only deciduous-dominated plot (TUWIEN) was leaf-off, with **150 trees**. On that plot, tree-level F1 was 76.7 for ForestFormer3D, 69.4 for ForAINet, 50.1 for SegmentAnyTree and only 30.0 for a canopy-height-model baseline. Most of those scores are author-reported by the same research group [V] ([ForestFormer3D paper](https://arxiv.org/html/2506.16991v2)). Leaf-on North American hardwood segmentation is essentially untested in public benchmarks.

**3Dtrees.earth** is a University of Freiburg research platform with Leipzig and NIBIO co-authors [P] ([EGU26-20434](https://meetingorganizer.copernicus.org/EGU26/EGU26-20434.html)). It runs these steps as Galaxy workflows on usegalaxy.eu [V] ([3dtrees-api config](https://github.com/3dTrees-earth/3dtrees-api/blob/main/trees_api/core/config.py)):

1. Standardisation of the uploaded LAS/LAZ.
2. Smart tiling and cross-tile merging.
3. Terrain model (DTM).
4. SegmentAnyTree segmentation.
5. DetailView species prediction.
6. Structure metrics.
7. Potree and 3D Tiles viewing.

It stores data in Supabase and in S3/MinIO buckets [V] (same source). Several of its repositories have **no licence file**, including the SegmentAnyTree wrapper, Smart Tile and DTM. Without a licence they can be read but not legally reused. Others are MIT, GPL-3.0 or CC BY-NC [V] ([3dTrees-earth org](https://github.com/3dTrees-earth)). I found no public statement on data licence, retention or storage location. **Uploading any client or hunting-land data to 3Dtrees.earth, ROCK Cloud or LP360 cloud requires Beau's approval.** A self-hosted copy of the same pipeline shape is feasible from the licensed parts.

**Datasets.**

| Dataset | Licence | Notes |
|---|---|---|
| NEON STEI, TREE, UNDE (AIRBORNE LiDAR, 10 cm RGB, field-mapped stems with species) | **CC BY 4.0 since June 2026**; commercial use allowed with attribution; account or token now required [V] ([NEON blog](https://www.neonscience.org/impact/observatory-blog/logins-required-and-data-licensing-updated)) | In-region field truth. Airborne LiDAR at "several pts/m2" is a lower bound, not a match for drone data. Mapped stems at DBH 10 cm or more [V] ([NEON API](https://data.neonscience.org/api/v0/products/DP1.10098.001)) |
| Quebec Trees Dataset | CC BY 4.0 [V] ([Zenodo 8148479](https://zenodo.org/records/8148479)) | Best species overlap with northern Wisconsin. RGB and photogrammetry only. 149 GB. Label-verification method not stated on the record |
| FOR-instance | CC BY 4.0 [V] ([Zenodo 8287792](https://zenodo.org/records/8287792)) | Segmentation only |
| FOR-species20K | **GPL-3.0 / AGPL-3.0 on a dataset** [V] ([Zenodo 13255198](https://zenodo.org/records/13255198)) | Ambiguous for commercial model training; needs legal review. Mostly European, 70% terrestrial scans |
| TEMPL (eastern US hardwoods; drone LiDAR plus field species, 231 trees) | Dataset licence [U] ([ESSD preprint](https://essd.copernicus.org/preprints/essd-2026-632/)) | Under review |
| Dubrovin et al. (Russia, drone LiDAR plus RGB) | Licence not stated [U] | |
| NEON crown and species *prediction* products | n/a | Model outputs. **Must not be used as ground truth** |

No public dataset combines drone LiDAR, drone RGB and field-confirmed species in Great Lakes northern hardwoods. Field labels from the property itself are unavoidable.

## The R3 Pro is the weakest-penetrating option, and raw processing is vendor-locked

**Sensor specs.**

| Spec | ROCK R3 Pro V2 | DJI Zenmuse L3 | DJI Zenmuse L2 (M350 alternative) |
|---|---|---|---|
| Laser | Hesai XT32, 905 nm, 32-channel spinning | 1535 nm | 905 nm |
| Returns | **2 (dual)** | **4 / 8 / 16** | 5 |
| Pulse / point rate | 640k pts/s single, 1.28M dual | 100 kHz to 2,000 kHz | 240k single, 1.2M multi |
| Range | 120 m at 60% reflectivity | 700 m at 10% (350 kHz) | 250 m at 10% (100 klx) |
| Precision / accuracy | 5 mm precision, 3 cm system accuracy | ±10 mm ranging, 3 cm V / 4 cm H at 120 m | 4 cm V / 5 cm H at 150 m |
| Beam | Divergence not published | 0.25 mrad, 41 mm spot at 120 m | 4 x 12 cm spot at 100 m |
| RGB | 26 MP | 100 MP or 25 MP | 20 MP |
| Positioning | PPK | RTK, optional PPK | RTK, optional PPK |
| Aircraft | M300/M350 | **Listed for Matrice 400 only; M350 not listed** | M400, M350, M300 |

Sources [V]: [ROCK R3 Pro V2 datasheet](https://learn.rockrobotic.com/r3-pro-v2-specifications-download-datasheet), [Hesai XT32](https://www.hesaitech.com/product/xt32/), [DJI L3 specs](https://enterprise.dji.com/zenmuse-l3/specs), [DJI L2 specs](https://enterprise.dji.com/zenmuse-l2/specs).

The XT32 spins through 360 degrees, so only part of each revolution reaches the ground swath. ROCK does not publish the usable aerial field of view. ROCK markets detail "captured at 60m AGL" [V] ([ROCK R3 Pro](https://www.rockrobotic.com/r3-pro)). From the specs I infer [U] that the R3 Pro should fly at roughly 60 to 80 m over forest and will be the weakest of the three for ground and understory under leaf-on canopy. The V2 upgraded the IMU, GNSS and computer, so whether the borrowed unit is V1 or V2 matters.

**Density.** These values are **[U] estimates**, computed with swath = 2 x height x tan(FOV/2), 50% side overlap, flat terrain and every pulse returning. Units are pulses/m2.

| Sensor (assumption) | 60 m, 5 m/s | 80 m, 5 m/s | 80 m, 10 m/s |
|---|---|---|---|
| R3 Pro, assumed 90 deg usable FOV | ~530 | ~400 | ~200 |
| R3 Pro, assumed 120 deg usable FOV | n/a | ~310 | ~150 |
| L2, 240 kHz, 70 deg | ~1,140 | ~860 | ~430 |
| L3, 350 kHz, 80 deg | ~1,390 | ~1,040 | ~520 |
| L3, 1,000 kHz, 80 deg | ~3,970 | ~2,980 | ~1,490 |

The only field report found came from an L3 reseller flying at 120 m and 50% overlap. It measured **L2 at 106 pulses/m2 and L3 at 46 pulses/m2 (278 returns/m2)**, far below what the formula predicts [P] ([Interpine](https://interpine.nz/analysis-of-lidar-dji-l2-vs-l3-lidar-sensors/)). Treat the table as an upper bound until one flight is measured. Storage estimates [U]: at 500 to 1,000 pts/m2, roughly 2 to 4 M points per acre, about 73 to 146 MB per acre as uncompressed LAS and about 6 to 20 MB per acre as LAZ. Raw sensor files and RGB images (especially 100 MP) will be several times larger. Interpine also reported the L2 tenting ground models under dense canopy, while the L3 gave better ground and branch detail. That result is qualitative and comes from conifers [P] (same source).

**Raw-to-LAZ dependency and cost.** **No open-source raw-to-point-cloud path was found for either ecosystem.** Trajectory processing and boresight calibration stay in vendor software.

| Path | Cost | Source |
|---|---|---|
| ROCK Desktop | Included for **1 year** with purchase; after that, an annual renewal priced by quote [V] (vendor marketing) | [ROCK costs article](https://www.rockrobotic.com/articles/rock-software-costs-truth-vs-dji-2026/) |
| ROCK Desktop renewal | One user reported **about $2,000** [P, anecdotal] | [ROCK forum](https://community.rockrobotic.com/t/licence-renewal/1869) |
| ROCK Cloud | Token pricing; users report opaque, rising quotes [P, anecdotal] | [ROCK forum](https://community.rockrobotic.com/t/rock-cloud-processing-credit-structure-change/1653) |
| DJI Terra, L2 point clouds | **Free**, including accuracy optimisation [V] | [DJI Terra FAQ](https://enterprise.dji.com/dji-terra/faq) |
| DJI Terra, L3 point clouds | Free from V5.1 per a reseller; not stated by DJI [P] | [DroneBundle](https://dronebundle.com/blog/dji-terra) |
| DJI Terra Standard | $1,850/yr or $5,280 permanent [P] | [DroneBundle](https://dronebundle.com/blog/dji-terra) |
| LP360 DJI add-on | Paid and cloud-based [V] | [LP360](https://support.lp360.com/hc/en-us/articles/41424651491603-DJI-Tab) |

ROCK states it puts no restrictions on output LAS use [V] (vendor marketing). DJI exports LAS/LAZ and trajectory files [V]. Offline processing with ROCK Desktop or DJI Terra is the only path that complies with current boundaries without approval.

## LiDAR360 Forestry: commercial reference, mostly published methods

Clean-room review of GreenValley International's LiDAR360 Forestry module from its public online user guide and published papers only. Nothing was purchased, installed, run, or decompiled. Full notes: `research_notes/Drone LiDAR tree species feasibility/lidar360_forestry_cleanroom.md`.

- **Cost.** Forestry module USD 2,360 plus required Framework USD 4,730 at SPH Engineering, so about USD 7,090 before tax. License term not stated. Windows only, NVIDIA GPU with 4 GB or more, 32 GB RAM, dongle or license code [V]. Last official changelog entry is v9.0 (2024-08-26) [P].
- **Airborne segmentation uses published, openly implemented algorithms.**
  - CHM watershed segmentation cites Chen et al. 2006 [V].
  - Point cloud segmentation is Li et al. 2012, co-authored by GreenValley's founder, with extra smoothing steps in front [V/P]. lidR implements Li 2012 as `li2012()`.
  - Seed-based segmentation uses a "layer stacking" step whose parameters match Ayrey et al. 2017; the method is not cited [P].
- **Airborne metrics and regression.**
  - Area metrics: height percentiles, density, intensity, canopy cover, gap fraction, LAI [V].
  - Regression: linear, SVM, neural net, random forest (scikit-learn) [V].
  - Airborne DBH is predicted from height and crown width using user-fitted equations, so it needs field DBH samples [V].
- **Closed deep learning features with no public accuracy data.**
  - Airborne trunk-based segmentation cites the AID-FOREST paper [V].
  - Crown outlines from RGB orthophotos use YOLOv8. The vendor claims 95% mAP with no dataset or method [U].
- **Species classification is TLS-only.**
  - The ground-scan module has a deep learning classifier and an "LLM mode" that needs a LiCloud login and a network connection [V]. Using that mode on project data would be remote inference and needs Beau's approval.
  - The airborne/drone module has only manual species tagging [V/P]. **Buying LiDAR360 would not answer TreeDar's species question.**
- **Branch features.**
  - The ground-scan module reports trunk volume, branch length and branch start/end diameters [V].
  - No QSM-style cylinder reconstruction is documented [V].
- **Accuracy.** No independent head-to-head benchmark of the full pipeline was found. One drone study (Marcello et al. 2024, Zenmuse L1) reports Li 2012 as the best method. A secondhand summary gives treetop F-scores of 91.7% in LiDAR360 and 88.9% in lidR [P, full text not read].
- **Open-source parity.** lidR (GPL-3.0), PDAL (BSD), CSF (Apache-2.0), SegmentAnyTree (MIT), 3DFin and FSCT (GPL-3.0) cover segmentation, metrics, and ground-scan stem fitting. Real gaps are a polished interactive tree and seed editor, a ready-made layer-stacking implementation, and a maintained open species classifier.

**Implication.** LiDAR360's value is a single Windows interface and editing tools, not unique algorithms. TreeDar can match its airborne segmentation and metrics with free tools, and neither LiDAR360 nor any open tool solves drone species classification for Wisconsin hardwoods. A fair side-by-side test is possible if someone with an existing license runs the same public dataset through it.

## Recommended scope, the earliest kill test, and what Beau needs to decide

**Recommended scope (proposal).** Restrict the first product to **individually resolvable canopy trees** and evaluate four things separately:

1. **Segmentation.** Compare lidR CHM methods against SegmentAnyTree.
2. **Stem-to-crown matching.** Match field-mapped stems to segmented crowns.
3. **Structure.** Height, crown area and crown volume. Treat DBH as experimental, and only for leaf-off hardwoods with DBH over 20 cm.
4. **Classification.** Use a hierarchy (outline below) with an explicit "unknown / insufficient evidence" output, and report confidence together with coverage.

Proposed classification hierarchy:

1. Conifer vs broadleaf.
2. Habitat-relevant groups where evidence supports them: white pine, aspen, paper birch, and oak vs non-oak. Oak accuracy is unestablished and must be measured.
3. "Maple (Acer)" at genus level only.

Do not offer branch architecture, red vs white oak, or red vs sugar maple as deliverables. Plan for two to three flight dates if species matter: leaf-off for LiDAR structure and conifers, late spring, and early September colour onset. Do not use peak colour. Record crown class and a neighbour-competition index for every field tree, and report accuracy per crown class. Keep repeat dates of the same trees in one spatial partition, and hold calibration data apart from the final test.

**Earliest cheap test that could prove the idea wrong.**

| Step | What | Cost |
|---|---|---|
| 0 | Confirm the borrowed R3 Pro's version and that its ROCK Desktop licence is active and usable by Beau. If it has lapsed, the R3 Pro path stops until someone pays. | Zero |
| 1 | One leaf-off flight over about 5 to 10 acres of **non-client** northern hardwood (proposed settings: 60 m AGL, 5 m/s, 50 to 70% overlap, plus a crosshatch pass), processed offline. In the field, tape DBH and record GNSS position, crown class and species for 50 or so trees. Then measure the items below with PDAL and lidR. | Borrowed gear, labour, no software spend if the licence is active |

What to measure in step 1:

1. Actual pulse and return density.
2. The share of stems with DBH over 20 cm that have enough points at 1.3 m to fit a circle.
3. Segmentation recall against field-mapped canopy trees.
4. The RGB-to-LiDAR crown offset on isolated trees.

Proposed kill criteria [U] (to be agreed, not derived from literature):

| Condition | Consequence |
|---|---|
| Fewer than half of large stems are resolvable | Drop stem and architecture claims for this sensor |
| Canopy-tree segmentation F1 lands near the 30 to 50 range seen on the deciduous benchmark plot | Rethink per-tree products for leaf-on hardwoods |

A zero-flight desk check on NEON TREE/STEI data (CC BY 4.0, AIRBORNE) can rehearse stem-to-crown matching in parallel. It cannot test drone sensor feasibility.

**Open questions for Beau.**

| # | Question |
|---|---|
| 1 | Is the borrowed R3 Pro a V1 or V2, is its ROCK Desktop licence current, and may Beau process data under it? |
| 2 | Is non-client land available for the test flight and field tape survey, and who does the field labelling? |
| 3 | Are you willing to spend on software if needed (ROCK renewal, about $2,000 reported; or Terra Standard), and on a Linux machine with a roughly 24 GB NVIDIA GPU for deep-learning tools? |
| 4 | Is an M400 plus L3 purchase realistic, given the L3 is not listed for the M350? Would an L2 on the M350 be worth borrowing or renting for comparison? |
| 5 | Which layers do hunting and habitat clients actually value: oak mast, conifer thermal cover, height and structure, or full species? Are genus-level and "unknown" outputs commercially acceptable? |
| 6 | Will you approve, or rule out, any cloud processing (3Dtrees.earth, ROCK Cloud, LP360)? |
| 7 | Should GPL/AGPL-licensed datasets (FOR-species20K) and GPL-labelled weights get a legal review before use, or be excluded? |
| 8 | Do you accept two to three flight dates per property as the cost of species work? |
| 9 | Should the "only provider in the region" positioning be shelved until a deeper competitor scan is done? |

## Conclusion

The idea's weakest link is not the species model. It is the sensor's view of the wood. The most attractive part of the pitch, reading species from branch architecture, rests on capabilities shown only with RIEGL-class sensors on large trunks, and those capabilities are confounded by competition-driven plasticity. The defensible product is a crown-level structure layer plus coarse, confidence-gated type groups. For species, RGB timing and field labels matter more than LiDAR.

The project's real dependencies are a vendor software licence, field-label labour and per-crown-class honest reporting, not open-source tooling, which is largely available under permissive terms. One cheap leaf-off flight with a tape survey will settle the architecture question for this specific sensor before any money is spent.
