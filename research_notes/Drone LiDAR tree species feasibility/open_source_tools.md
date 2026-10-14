# Open-source software for forest point-cloud processing (status as of 2026-10-13)

Method note: Licenses were read from each repository's LICENSE file through the GitHub API (or GitLab API / Zenodo API / Hugging Face API / FreiData API for weights and datasets) on 2026-10-13. "Last push" is the GitHub `pushed_at` timestamp, which reflects the most recent push to any branch and is not guaranteed to be a default-branch commit. Nothing was downloaded or run. This is not legal advice; license conclusions are a technical reading and should be confirmed before commercial use.

## Survey table: what exists, license, maintenance, hardware, and fit

### Takeaway
Most of the named tools exist and are maintained in 2026. Classical tools (lidR, PDAL, CloudCompare, 3DFin, TreeIso, raycloudtools, 3D Forest) run on CPU workstations; the deep-learning segmenters (SegmentAnyTree, ForAINet, TreeLearn, ForestFormer3D) and the DetailView species classifier need an NVIDIA GPU and Linux/Docker in practice. "pc2tree / Pc2Tree" could not be found. AdQSM is a Windows binary with no license and no source.

### Cited Findings

**Segmentation (deep learning, point cloud)**

| Tool | Purpose | Code license (verified) | Weights license if separate | Last push / release | Training region and data | Self-hosted | Hardware | Fit for drone LiDAR, Wisconsin hardwood/mixed |
|---|---|---|---|---|---|---|---|---|
| SegmentAnyTree (SmartForest-no) | Sensor-agnostic tree instance + semantic segmentation | MIT ([LICENSE via repo](https://github.com/SmartForest-no/SegmentAnyTree)) | Weights `model_file/PointGroup-PAPER.pt` are committed inside the MIT repo; no separate weight license stated ([repo tree](https://github.com/SmartForest-no/SegmentAnyTree/tree/main/model_file)) | Last push 2025-08-22; no GitHub releases | Trained on FOR-instance (original); per ForestFormer3D supplement, not on NIBIO_MLS, BlueCat, Yuchen ([ForestFormer3D paper](https://arxiv.org/html/2506.16991v2)) | Yes (Docker image) | NVIDIA GPU, Docker `--gpus all`; tested on Linux only, Minkowski Engine may not work on Windows ([README](https://github.com/SmartForest-no/SegmentAnyTree)) | Strong candidate: permissive license, already used by 3Dtrees.earth, mid-range ULS scores; weak on the leaf-off deciduous TUWIEN plot (F1 50.1) |
| ForAINet (prs-eth) | Panoptic segmentation for high-density ALS/ULS | BSD 3-Clause (LICENSE.md; based on torch-points3d, retains Principia Labs notice) ([repo](https://github.com/prs-eth/ForAINet)) | Training data on Zenodo 10828636 is CC BY 4.0 ([Zenodo](https://zenodo.org/records/10828636)); separate pretrained-weight license not verified | Last push 2026-07-07; no releases | FOR-instance ULS (Riegl) data ([ForestFormer3D paper](https://arxiv.org/html/2506.16991v2)) | Yes | NVIDIA GPU (CUDA training/test commands) ([README](https://github.com/prs-eth/ForAINet)) | Candidate; permissive; high precision but lower recall on mixed benchmark |
| TreeLearn (ecker-lab) | Tree instance segmentation, ground-based LiDAR | MIT ([repo](https://github.com/ecker-lab/TreeLearn)) | Weights downloaded via script (model_weights_20241213 and a "with_small" variant); separate weight license not verified ([pipeline doc](https://github.com/ecker-lab/TreeLearn/blob/main/docs/segmentation_pipeline.md)) | Last push 2026-02-17 | Ground-based (TLS/MLS) data; default weights detect only trees at least 10 m tall ([pipeline doc](https://github.com/ecker-lab/TreeLearn/blob/main/docs/segmentation_pipeline.md); [paper](https://arxiv.org/html/2309.08471v3)) | Yes | GPU with spconv; Colab notebook currently non-functional due to CUDA 12.2/spconv incompatibility ([README](https://github.com/ecker-lab/TreeLearn)) | Poor fit: designed for ground-based scans, low recall (36.6%) on FOR-instanceV2 |
| ForestFormer3D (SmartForest-no) | End-to-end semantic + instance segmentation (ICCV 2025) | **CC BY-NC 4.0** (inherited from OneFormer3D) ([LICENSE / README](https://github.com/SmartForest-no/ForestFormer3D)) | Zenodo 16742708 (FOR-instanceV2 data + pretrained model `clean_forestformer.zip`) is labeled GPL-3.0-or-later ([Zenodo](https://zenodo.org/records/16742708)) | Last push 2026-03-03 | FOR-instanceV2 (ULS Riegl VUX-1/miniVUX-1, plus MLS GeoSLAM, TLS Leica P20) ([paper](https://arxiv.org/html/2506.16991v2)) | Yes | Trained on single A100 80 GB; Docker with `--shm-size=128g`; reduce cylinder radius to avoid OOM on smaller GPUs ([README](https://github.com/SmartForest-no/ForestFormer3D)) | Best benchmark scores, but non-commercial code license is a blocker for commercial use |
| 3Dtrees_ForestMamba (3dTrees-earth) | Sparse Mamba forest segmentation | CC BY-NC 4.0 (LICENSE file) ([repo](https://github.com/3dTrees-earth/3Dtrees_ForestMamba)) | Not verified | Last push 2026-09-17 | Not verified | Presumably yes | Not verified | Research only; non-commercial |
| SegmentAnyTreeV2 (paper) | PTv3-based segmentation, FOR-instance v3 benchmark (427 scenes, 26,496 trees) | No code link found on arXiv page ([arXiv 2606.08206](https://arxiv.org/abs/2606.08206)) | Unknown | Preprint June 2026 | Multi-sensor | Unknown | Unknown | Watch item; unverified availability |

**Classical / non-DL segmentation and inventory**

| Tool | Purpose | Code license (verified) | Last push / release | Self-hosted | Hardware | Fit |
|---|---|---|---|---|---|---|
| lidR (R) | ALS/ULS processing; ITD/ITS (`segment_trees`, li2012, dalponte2016, silva2016, watershed) | GPL-3.0 ([repo](https://github.com/r-lidar/lidR)); R files `algorithm-its.R`, `algorithm-itd.R`, `segment_trees.R` present | Last push 2026-09-21; latest GitHub release v4.0.3 (2023-03-16), CRAN releases not checked | Yes | CPU | Good baseline for canopy-tree segmentation (CHM-based); GPL only matters if redistributing derivative code |
| PDAL | Point cloud I/O, filtering, ground, tiling, COPC writing | BSD ([LICENSE.txt](https://github.com/PDAL/PDAL)) | 2.10.2 (2026-06-12); last push 2026-10-09 | Yes | CPU | Core pipeline backbone |
| CloudCompare | Desktop viewer/editor, plugin host | GPL v2 or later ([license.txt](https://github.com/CloudCompare/CloudCompare)) | v2.13.2 (2024-07-11); last push 2026-10-13 | Yes | CPU, desktop GPU for display | Manual QA, labeling, visualization |
| 3DFin | DBH/height/stem detection; CloudCompare (2.13) and QGIS plugin | GPL-3.0 ([repo](https://github.com/3DFin/3DFin)); plugin source in CloudCompare tree as `cc3DFin` ([CC plugins dir](https://github.com/CloudCompare/CloudCompare/tree/master/plugins/core/Standard)) | v0.6.0 (2025-12-05) | Yes | CPU | Stem-focused; sub-canopy stems from drone LiDAR are often sparse, so likely limited use |
| TreeIso (artemis_treeiso, CC plugin qTreeIso) | Cut-pursuit tree isolation | Mixed: treeiso + cutpursuit MIT; `matlas_tools` part LGPL 2.1 ([LICENSE](https://github.com/truebelief/artemis_treeiso)); plugin in CloudCompare as `qTreeIso` ([CC plugins dir](https://github.com/CloudCompare/CloudCompare/tree/master/plugins/core/Standard)) | Last push 2025-03-21 | Yes | CPU | Described as TLS tree isolation; ULS fit unverified |
| FSCT | Semantic seg + stem measurement | GPL-3.0 ([repo](https://github.com/SKrisanski/FSCT)) | Last push 2024-03-03 (appears unmaintained) | Yes | GPU strongly recommended (dev system Titan RTX 24 GB, 128 GB RAM); CPU gives worse results per author ([README](https://github.com/SKrisanski/FSCT)) | README says very high resolution ALS is "borderline"; designed for TLS/MLS/below-canopy; poor fit |
| 3D Forest (VUKOZ-OEL) | Desktop forest point-cloud processing, plugins | GPL-3.0 ([repo](https://github.com/VUKOZ-OEL/3d-forest)) | release-2026-03-24; last push 2026-10-11 | Yes | CPU | Possible QA/desktop tool; ULS segmentation quality unverified |
| CSP_StandSegmentation (3dTrees-earth fork) | R package single-tree delineation from TLS/MLS/ULS | GPL-3.0 ([repo](https://github.com/3dTrees-earth/3dtrees_CSP_StandSegmentation)) | Last push 2026-09-25 | Yes | Not verified | Worth testing as non-DL ULS baseline |
| raycloudtools / rayextract | Ray-cloud tools; `rayextract trees` segments forest and extracts tree structure | CSIRO Open Source Software Licence (BSD/MIT variation) ([licence.txt](https://github.com/csiro-robotics/raycloudtools)) | Last push 2026-09-24; RayExtract paper Devereux et al. 2026 | Yes (Docker for Linux/macOS/Windows) | CPU | Requires sensor trajectory per point; useful if drone vendor exports trajectory |
| LAStools | LiDAR processing suite | Split: LASzip, LASlib, lasinfo, las2las, lasmerge, lascopcindex etc. open source; tools such as las2dem, blast2dem require a license for commercial, government, or production use; unlicensed output can be "slightly distorted" past point limits ([LICENSE.txt](https://github.com/LAStools/LAStools)) | v2.0.5 (2026-05-05) | Yes | CPU | Avoid closed tools for commercial work without a paid license; PDAL covers most needs |

**Quantitative structure models (QSM)**

| Tool | License | Status | Hardware | Fit |
|---|---|---|---|---|
| TreeQSM | GPL-3.0-or-later (LICENSE.md, Raumonen) ([repo](https://github.com/InverseTampere/TreeQSM)) | 2.4.1 (2022-05-11), last push 2023-05-11 | MATLAB required (proprietary runtime cost) | Designed for TLS; branch reconstruction from drone LiDAR is generally unreliable (inference) |
| AdQSM | **No license file; repository contains only Windows .exe binaries and DLLs** ([repo contents](https://github.com/GuangpengFan/AdQSM)); README calls it a "Test Version" validated on 29 destructively sampled trees | Last push 2023-07-20 | Windows | Not open source; not reproducible; avoid |
| SimpleForest | GPL-3.0-or-later on GitLab ([computree/ct_pluginsimpleforest](https://gitlab.com/computree/ct_pluginsimpleforest); [SimpleForest/pluginSimpleForest](https://gitlab.com/SimpleForest/pluginSimpleForest)) | Computree plugin active 2026-10-07; standalone repo last active 2021 | Runs inside Computree (desktop) | TLS-oriented QSM; low fit |
| rayextract trees | CSIRO BSD/MIT variant (above) | Active | CPU | Fast tree volume reconstruction; ULS performance unverified |

**Species classification (point cloud)**

| Tool | Code license | Weights license | Training data | Hardware | Fit |
|---|---|---|---|---|---|
| DetailView (Frey and Schindler, Freiburg) | GPL-3.0 ([JulFrey/DetailView](https://github.com/JulFrey/DetailView); 3Dtrees fork [3dTrees_DetailView](https://github.com/3dTrees-earth/3dTrees_DetailView)) | CC BY 4.0 on FreiData ([record](https://doi.org/10.60493/f850a-bb152)); Zenodo software record CC BY 4.0 ([Zenodo 14204431](https://zenodo.org/records/14204431)) | FOR-species20K (TLS/MLS/ULS, mostly European temperate/boreal species) | GPU via Docker `--gpus all` | Ranked 1st in FOR-species20K competition (OA 0.79) ([FOR-species20K README](https://github.com/stefp/FOR-species20K)); species list is European, so Wisconsin species (e.g. sugar maple, red oak, basswood) would need retraining (inference) |
| FOR-species20K benchmark entries | YOLOv5 images (rank 2, OA 0.78), SimpleView (0.76), PointNet++ (0.76), MinkNet (0.74), MLP-Mixer (0.71), PointAugment+DGCNN (0.68) ([leaderboard](https://github.com/stefp/FOR-species20K)) | Individual licenses not checked | FOR-species20K | GPU | Reference baselines |
| FOR-species20K dataset | n/a | **Dataset labeled GPL-3.0-or-later on Zenodo**; dev.zip is 25.35 GB, test.zip 1.84 GB ([Zenodo 13255198](https://zenodo.org/records/13255198)) | Approx. 20K trees, 33 species with more than 50 trees; tree_H is bounding height, not true height ([README](https://github.com/stefp/FOR-species20K)) | n/a | Pretraining source only; species mismatch with Wisconsin |

**RGB crown detection**

| Tool | Code license | Weights license | Training region | Hardware | Fit |
|---|---|---|---|---|---|
| DeepForest (weecology) | MIT ([repo](https://github.com/weecology/DeepForest)) | MIT on Hugging Face for `deepforest-tree`, `cropmodel-tree-species`, `cropmodel-neon-resnet18-species` and others ([HF API](https://huggingface.co/weecology/deepforest-tree)) | NEON (US) sites per project docs; exact species/region coverage of species crop models not verified | CPU inference possible; GPU for training | Good fit for RGB crown baseline in US forests |
| detectree2 | MIT ([repo](https://github.com/PatBall1/detectree2)) | Pretrained models in `model_garden`; license of weights not separately verified | Designed for dense tropical forests (Mask R-CNN / Detectron2) | GPU recommended for training | Usable but would need local retraining |

**Tiling, formats, viewing**

| Tool | License | Status | Notes |
|---|---|---|---|
| COPC spec (copcio) | MIT ([repo](https://github.com/copcio/copcio.github.io)) | Last push 2026-08-18 | LAZ-compatible, range-readable format |
| Entwine (EPT) | LGPL 2 (embedded libs BSD/public domain) ([LICENSE](https://github.com/connormanning/entwine)) | 3.2.1 (2025-10-15) | EPT octree builder |
| untwine | GPL-3.0 ([repo](https://github.com/hobuinc/untwine)) | 1.5.1 (2025-07-17) | COPC builder, used by QGIS |
| Potree | BSD 2-clause style ([LICENSE](https://github.com/potree/potree)) | 1.8.2 (2023-12-12); last push 2026-01-08 | WebGL viewer |
| PotreeConverter | BSD-2-Clause ([repo](https://github.com/potree/PotreeConverter)) | Last push 2026-09-23 | Used by 3Dtrees (v2.1.1 binary) |
| 3Dtrees_py3dtiles | MIT ([repo](https://github.com/3dTrees-earth/3Dtrees_py3dtiles)) | Last push 2026-01-09 | LAS/LAZ to Cesium 3D Tiles |

**Not found / unverified**
- "pc2tree / Pc2Tree": no GitHub repository found by name search and no web result describing a tool with that name; treat as unverified. Related alternative found: treeseg (MIT, C++/PCL, TLS-oriented) ([Burt et al. 2018](https://discovery-pp.ucl.ac.uk/10065105/1/Burt_et_al-2018-Methods_in_Ecology_and_Evolution.pdf)).
- Other GitHub species classifiers found: hellwue/TreeSpeciesClassification (MIT, PointCNN/3DmFV, last push 2022), l333wang AACB (GPL-3.0, 2025) ([GitHub search](https://github.com/search?q=tree+species+point+cloud+classification&type=repositories)); not evaluated.

### Inferences
- For a commercial-capable stack, the permissive core is PDAL (BSD) + COPC (MIT) + Potree (BSD) + SegmentAnyTree (MIT) or ForAINet (BSD-3) + DeepForest (MIT, MIT weights), with lidR (GPL-3.0) and CloudCompare (GPL) as internal tools.
- None of the point-cloud species classifiers found is trained on North American hardwood species, so Wisconsin species classification would need local field-labeled training data regardless of tool choice.
- QSM tools are TLS-oriented; structural estimates from drone LiDAR should likely be limited to height, crown area, and crown volume until tested.

### Gaps
- Separate license statements for ForAINet, TreeLearn, and detectree2 pretrained weights were not verified.
- DeepForest species crop model training region/species list not verified.
- lidR CRAN release date not checked (GitHub release tag is old).
- SegmentAnyTreeV2 code/weights availability unknown.
- Pc2Tree could not be identified.

## Which segmentation tools have been benchmarked on ULS data (FOR-instance) and how did they score?

### Takeaway
On FOR-instance ULS regions, ForestFormer3D led in the authors' own comparison, with SegmentAnyTree and ForAINet close on conifer plots, but every method dropped sharply on TUWIEN, the only deciduous-dominated plot, which was scanned leaf-off. That plot is the closest analog to Wisconsin hardwoods, and it has only 150 trees.

### Cited Findings
- FOR-instance is a ULS benchmark (Riegl sensors), 1,130 manually segmented trees, five regions, CC BY 4.0 on Zenodo (1.64 GB) ([Zenodo 8287792](https://zenodo.org/records/8287792); [paper](https://arxiv.org/pdf/2309.01279)).
- TUWIEN is a deciduous-dominated alluvial forest, Riegl VUX-1 UAV, about 1717 pts/m², 150 trees, 0.55 ha; it is the only subset not acquired leaf-on ([FOR-instance paper](https://arxiv.org/pdf/2309.01279)).
- FOR-instanceV2 test split, individual tree F1: ForestFormer3D 82.8 (P 92.4, R 75.0); OneFormer3D 73.1; ForAINetV2_R8 72.4; ForAINetV2_R16 70.8; TreeLearn 50.6 (recall 36.6, Yuchen excluded) ([ForestFormer3D paper, Table 1](https://arxiv.org/html/2506.16991v2)).
- Per-region ULS F1 (Table 5): TUWIEN: ForestFormer3D 76.7, ForAINet 69.4, ForAINetV2_R16 52.6, SegmentAnyTree 50.1, OneFormer3D 50.0, CHM-based YOLOv5 30.0. RMIT: ForestFormer3D 82.2, SegmentAnyTree 75.4, ForAINet 69.5, CHM YOLOv5 65.0. NIBIO: ForestFormer3D 96.7, ForAINetV2 93.4, ForAINet 92.4, SegmentAnyTree 89.5, CHM YOLOv5 79.0. SCION: ForAINetV2 95.0, ForestFormer3D 94.7, SegmentAnyTree 92.5, ForAINet 91.5, CHM YOLOv5 91.0. CULS: most methods 100 ([ForestFormer3D paper](https://arxiv.org/html/2506.16991v2)).
- Caveat: ForAINet, SegmentAnyTree, and CHM YOLOv5 were trained on the original FOR-instance, not on the extra V2 regions ([ForestFormer3D paper](https://arxiv.org/html/2506.16991v2)).
- SegmentAnyTreeV2 (Wielgosz, Puliti, Astrup; arXiv June 2026) reports FOR-instanceV2 test F1 85.0 (P 90.5, R 80.2) and introduces FOR-instance v3 (427 scenes, 26,496 trees) ([arXiv](https://arxiv.org/abs/2606.08206)). Not peer reviewed; no code link found.
- On the ground-based TreeLearn benchmark, ForAINet scored F1 78.3 and SegmentAnyTree 64.8, with TreeLearn best; results depend on benchmark ([TreeLearn paper](https://arxiv.org/html/2309.08471v3)).

### Inferences
- All published scores are author-reported, mostly from the same research group (NIBIO/SmartForest), and evaluated on European/Oceanian conifer-heavy data. Deciduous leaf-on hardwood performance is essentially untested in these benchmarks.
- The CHM-based baseline's collapse on TUWIEN (F1 30) suggests CHM-only methods (e.g. lidR dalponte/silva) may also struggle in interlocking hardwood crowns; this needs local testing.

### Gaps
- No benchmark found specific to North American leaf-on hardwood ULS.
- SegmentAnyTreeV2 per-region scores and comparison numbers not seen (only abstract page read).
- lidR li2012/dalponte/silva were not found in these FOR-instance tables.

## Which tools require GPUs, and which are practical on a single workstation?

### Takeaway
All DL segmenters and the DetailView species model expect an NVIDIA GPU with CUDA and Docker, effectively Linux. Inference is likely workstation-feasible with tiling; ForestFormer3D training used an 80 GB A100. CPU-only tools (lidR, PDAL, CloudCompare, 3DFin, raycloudtools, 3D Forest) run on any workstation.

### Cited Findings
- SegmentAnyTree: Docker with `--gpus all`; tested on Linux only; Minkowski Engine may not be available for Windows ([README](https://github.com/SmartForest-no/SegmentAnyTree)).
- 3Dtrees' SAT wrapper requires NVIDIA GPU + drivers and NVIDIA Container Toolkit, and logs CPU/memory/GPU usage ([3dTrees_SAT](https://github.com/3dTrees-earth/3dTrees_SAT)).
- ForestFormer3D trained on a single A100 80 GB for 3000 epochs; inference processes cylinders sequentially and suggests reducing radius for OOM ([README](https://github.com/SmartForest-no/ForestFormer3D); [paper](https://arxiv.org/html/2506.16991v2)).
- FSCT strongly recommends CUDA GPU; dev system Titan RTX 24 GB, 128 GB RAM ([README](https://github.com/SKrisanski/FSCT)).
- TreeLearn requires spconv matched to GPU/CUDA version ([README](https://github.com/ecker-lab/TreeLearn)).
- DetailView runs via Docker `--gpus all` ([README](https://github.com/JulFrey/DetailView)).
- 3Dtrees Smart Tile pipeline exists to tile large clouds with buffers, run external segmentation (ForAINet, SegmentAnyTree), and merge instances across tiles, because DL models are memory-limited ([3dtrees_Smart_Tile](https://github.com/3dTrees-earth/3dtrees_Smart_Tile)).
- TreeQSM requires MATLAB ([repo](https://github.com/InverseTampere/TreeQSM)).

### Inferences
- A single Linux workstation with a 24 GB class NVIDIA GPU is probably sufficient for inference with tiling; this is not verified by any measured run.
- The owner's Windows environment would need WSL2 or a Linux box for DL tools.

### Gaps
- No published inference VRAM/time per hectare for SegmentAnyTree or ForAINet found.

## Any license traps (non-commercial, GPL implications, model weights under CC BY-NC)?

### Takeaway
Yes. ForestFormer3D code and 3Dtrees_ForestMamba are CC BY-NC 4.0 (non-commercial). LAStools' processing tools need a paid license for commercial use. AdQSM has no license at all. Several datasets and weights are GPL-3.0 on Zenodo, which is unusual and ambiguous for data/weights.

### Cited Findings
- ForestFormer3D: CC BY-NC 4.0 because it derives from OneFormer3D ([README License section](https://github.com/SmartForest-no/ForestFormer3D)). Its Zenodo data + pretrained model record is labeled GPL-3.0-or-later ([Zenodo 16742708](https://zenodo.org/records/16742708)), conflicting with the code license.
- 3Dtrees_ForestMamba LICENSE is CC BY-NC 4.0 ([repo](https://github.com/3dTrees-earth/3Dtrees_ForestMamba)).
- LAStools: closed tools "require licensing for any commercial, government, or production use"; open part limited to LASzip/LASlib and utilities ([LICENSE.txt](https://github.com/LAStools/LAStools)).
- AdQSM: binaries only, no LICENSE ([repo](https://github.com/GuangpengFan/AdQSM)). No license means no granted reuse rights.
- FOR-species20K dataset is GPL-3.0-or-later on Zenodo ([Zenodo 13255198](https://zenodo.org/records/13255198)).
- GPL-3.0 tools: lidR, 3DFin, FSCT, 3D Forest, TreeQSM, untwine, SimpleForest, DetailView code; GPL v2+: CloudCompare; LGPL: Entwine, treeiso matlas_tools ([sources in table above](https://github.com/r-lidar/lidR)).
- CSIRO raycloudtools licence is a BSD/MIT variation with Australian Consumer Law clauses ([licence.txt](https://github.com/csiro-robotics/raycloudtools)).
- Several 3dTrees-earth repos have no license file (3dTrees_SAT, 3dtrees-pc-standard, 3dtrees_potree, 3Dtrees_DTM, 3dtrees_metadata, 3dtrees_Smart_Tile, tool_3dtiles) ([org repo list](https://github.com/3dTrees-earth)).

### Inferences
- GPL tools used internally as a service (not distributed) generally do not force releasing proprietary code; distributing modified binaries would. Confirm with counsel.
- Whether models fine-tuned from GPL-labeled weights or trained on GPL-labeled datasets inherit GPL obligations is legally unsettled; treat ForestFormer3D weights and FOR-species20K-derived models as needing review before commercial use.
- No-license repos (including 3Dtrees wrappers) can be read but not legally reused; reimplement or ask authors.

### Gaps
- No explicit license terms found for TreeLearn, ForAINet, or detectree2 weight downloads.

## 3Dtrees.earth: who runs it, public repos, pipeline, infrastructure dependencies

### Takeaway
3Dtrees.earth is a University of Freiburg research platform (with NIBIO and Leipzig coauthors) that wraps SegmentAnyTree, DetailView, Potree, 3D Tiles, and a structure-metrics tool as Galaxy tools on Galaxy Europe (usegalaxy.eu), with Supabase as database/auth and S3/MinIO buckets for raw, product, and visualization storage. Its public code is a mix of MIT, GPL-3.0, CC BY-NC, and unlicensed repos. No public data-license statement was found.

### Cited Findings
- EGU 2026 abstract EGU26-20434: authors from University of Freiburg (Faculty of Environment and Natural Resources; Institute of Computer Science, Björn Grüning), University of Leipzig (Benjamin Brede), and NIBIO (Stefano Puliti). Describes an open, cloud-based platform for TLS, ULS, MLS with harmonization, instance/semantic segmentation, species prediction, structural trait extraction, and tree-level inventory; workflows containerized and deployed via Galaxy Europe; derived products FAIR ([EGU26-20434](https://meetingorganizer.copernicus.org/EGU26/EGU26-20434.html)).
- Galaxy Community Hub: platform "Built on the expertise and computational resources of the Galaxy project"; presented at EGU 2026 by the Freiburg team ([Galaxy Hub](https://artifact.galaxyproject.org/news/2026-05-13-egu/)).
- A 2026 conference paper (Gerberding, Grüning, Kattenborn, Brede et al.) lists standardization, tiling, tree instance segmentation, species prediction, structural metrics, and web 3D visualization (search snippet only; the Fraunhofer page was bot-blocked) ([publica.fraunhofer.de](https://publica.fraunhofer.de/handle/publica/513875)).
- The 3dtrees.earth homepage is a JavaScript app; fetching it returned only the title "Explore your forests in 3D" ([3dtrees.earth](https://3dtrees.earth/)).
- GitHub org `3dTrees-earth` has 21 public repos ([org](https://github.com/3dTrees-earth)). Pipeline mapping from READMEs:
  - Ingestion/standardization: `3dtrees-pc-standard` (LAS/LAZ validation and standardization, Docker; no license) ([repo](https://github.com/3dTrees-earth/3dtrees-pc-standard)); `3dtrees_metadata` (no license).
  - Tiling/merge: `3dtrees_Smart_Tile` (fork of kgerb/3dtrees_Smart_Tiles; COPC tiles, buffers, cross-tile instance merging; no license), `3Dtrees_Tile_Merge` (no license) ([repo](https://github.com/3dTrees-earth/3dtrees_Smart_Tile)).
  - Terrain: `3Dtrees_DTM` (no license).
  - Segmentation: `3dTrees_SAT` (SegmentAnyTree GPU Docker, Galaxy tool, runnable on usegalaxy.eu; no license) ([repo](https://github.com/3dTrees-earth/3dTrees_SAT)); `3dtrees_CSP_StandSegmentation` (GPL-3.0); `3Dtrees_ForestMamba` (CC BY-NC 4.0).
  - Species: `3dTrees_DetailView` (GPL-3.0 fork of JulFrey/DetailView) ([repo](https://github.com/3dTrees-earth/3dTrees_DetailView)).
  - Traits: `3dtrees_foreststructure` (GPL-3.0; Dockerized forest-structure indices, tile metrics, DTM, CHM, COPC-based) ([repo](https://github.com/3dTrees-earth/3dtrees_foreststructure)).
  - Visualization: `3dtrees_potree` (PotreeConverter 2.1.1 wrapper, no license), `3Dtrees_py3dtiles` (MIT), `tool_3dtiles`, `tool_overviews` (MIT).
  - Orchestration: `3dtrees-api` (MIT), `3dtrees-status` (GPL-3.0), `galaxy`, `galaxytools` (MIT fork), `usegalaxy-eu-tools` and `infrastructure-playbook` (MIT forks of UseGalaxy.eu repos), `temporary-tools` (MIT).
- `3dtrees-api` invokes named Galaxy workflows via BioBlend and polls invocation state by cron; upload URL generation handled by a separate cloud function ([README](https://github.com/3dTrees-earth/3dtrees-api)).
- `3dtrees-api` config defines `GalaxyConfig` (default Galaxy EU detection via `usegalaxy.eu` URL; file sources for raw, products, visualization storage), `SupabaseConfig` (datasets and `galaxy_workflow_invocations` tables, auth users), and an S3/MinIO storage config with buckets `3dtrees-raw`, `3dtrees-products`, `3dtrees-dev` ([config.py](https://github.com/3dTrees-earth/3dtrees-api/blob/main/trees_api/core/config.py)). Tests reference Supabase auth/RLS and MinIO ([repo tree](https://github.com/3dTrees-earth/3dtrees-api/tree/main/tests)).
- SciStarter listing invites users to upload terrestrial, drone, or airborne LiDAR for automatic AI analysis ([SciStarter](https://scistarter.org/3dtreesearth)).

### Inferences
- Galaxy Europe compute is hosted by the University of Freiburg (inference from Björn Grüning's role and usegalaxy.eu infrastructure forks); the S3/MinIO host location is not stated publicly.
- Uploading client or hunting-land point clouds to 3Dtrees.earth would send data to a third-party German research service; under TreeDar boundaries that requires Beau's approval. The open components (SAT wrapper, Smart Tile, foreststructure) show a self-hosted pipeline shape that could be replicated locally, subject to licenses (several are unlicensed).

### Gaps
- No public statement found on data license, retention, privacy, or physical storage location for uploaded data.
- Operator/legal entity and funding not confirmed (homepage not readable without JavaScript; Fraunhofer paper page blocked).
- Exact Galaxy workflow definitions (which tools in which order) not inspected.
