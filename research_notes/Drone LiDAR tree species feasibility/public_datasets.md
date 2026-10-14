# Public labeled datasets for individual-tree segmentation and species classification (LiDAR + RGB), with Wisconsin / Great Lakes emphasis

Status as of 2026-10-13. No data were downloaded. Licenses were checked on the dataset landing pages where noted; anything not confirmed on a primary page is marked **UNVERIFIED**. Sizes are as listed on landing pages.

## Summary table

| Dataset | Point cloud | RGB | Labels | Region / species | Labeled trees | License (verified?) | Access | Size | Fit for WI drone LiDAR+RGB |
|---|---|---|---|---|---|---|---|---|---|
| NEON AOP + Vegetation Structure, STEI / TREE (WI), UNDE (MI UP) | ALS discrete return, "several pts/m2", 1 km tiles LAZ | 10 cm or finer orthomosaic, 1 km tiles | Field-mapped stems with taxonID (DBH >= 10 cm mapped); no crown polygons | Northern WI and MI UP mixed / northern hardwood forest | Not counted here (plot-level; GAP) | CC BY 4.0 since June 2026 (verified via NEON blog and API page); earlier downloads were CC0 | NEON portal / API, account or token now required | Per 1 km tile (GAP; not stated) | Best in-region field-truth source; airborne not drone; stem-to-crown matching required |
| NeonTreeEvaluation (Weinstein et al.) | ALS (NEON) | NEON 10 cm | Image-annotated bounding-box crowns; no species stated on repo page | 22 NEON sites (site list not confirmed to include STEI/TREE/UNDE) | 30,975 annotations | GitHub CC0-1.0; Zenodo data release CC BY 4.0 (both verified) | GitHub + Zenodo 5914554 | 8.4 GB (Zenodo 0.2.2) | Good segmentation benchmark, not species |
| NEON Tree Crowns (DeepForest predictions) | n/a (derived) | n/a | Model-predicted crowns (~100M trees, 37 sites) | US NEON sites | Predictions, not labels | Not verified (GAP) | Zenodo 3765872 | GAP | Do NOT use as ground truth |
| NEON species predictions (Weinstein lab, 81 species) | n/a | n/a | Model-predicted species crowns | NEON sites | Predictions | Not verified | Zenodo / Google Earth Engine | GAP | Do NOT use as ground truth |
| IDTReeS 2020 competition | NEON ALS | NEON RGB (+ hyperspectral per memory, UNVERIFIED) | Crown polygons linked to NEON field data | NEON sites (sites UNVERIFIED on page; believed OSBS, MLBS, TALL) | GAP | CC BY 4.0 (verified, Zenodo 3934932); underlying NEON data under NEON terms | Zenodo | 572 MB | Methodology reference; southeastern / Appalachian sites, not Great Lakes |
| FOR-species20K (Puliti et al.) | TLS 70%, ULS 22%, MLS 8% | None | Species per individual tree point cloud | Mainly Europe plus scattered other continents; 33 species | 20,158 trees (dev 90%, test 10% labels withheld) | GPL-3.0-or-later and AGPL-3.0-or-later listed on Zenodo 13255198 (verified) | Zenodo + Codabench | 27.2 GB | Strong pretraining / benchmark for 3D species; species mismatch with WI |
| FOR-instance | ULS (Riegl) | None | Manual individual-tree and semantic segmentation | Norway, Czech Republic, Austria, New Zealand, Australia | 1,130 trees | CC BY 4.0 (verified, Zenodo 8287792) | Zenodo | 1.6 GB | Segmentation benchmark only, no species |
| FOR-instanceV2 | ULS, MLS, TLS | GAP | Instance segmentation | Several countries | 11,028 instances (reported in later paper) | UNVERIFIED (release/license not found) | GAP | GAP | Segmentation only; confirm release |
| Quebec Trees Dataset (Cloutier, Germain, Laliberte) | Photogrammetric (SfM) point clouds only, no LiDAR | High-res UAV RGB orthomosaics, 7 dates May-Oct 2021 | Crown polygons, 14 classes | Temperate Quebec; ABBA, ACPE, ACRU, ACSA, BEAL, BEPA, FAGR, LALA, Picea, PIST, Populus, THOC, TSCA, dead | 22,260 annotations in table (abstract says 23,000) | CC BY 4.0 (verified, Zenodo 8148479) | Zenodo | 149.3 GB total (about 20-23 GB per date) | Closest species list to northern WI; RGB only; label verification method not stated |
| TEMPL (Park et al., Purdue; ESSD preprint) | ALS + ULS + 2 backpack LiDAR | Not mentioned | Individual tree point clouds, 16 species + snag, field-measured attributes | Central Hardwood region, eastern US (state not named on page) | 231 trees (838 tree point clouds), 9 CFI plots | UNVERIFIED for dataset (preprint is CC BY 4.0) | doi.org/10.4231/5DGX-DP81 | GAP | Closest North American "drone LiDAR + field species in hardwoods"; small; under review |
| Dubrovin et al. 2024 (Sci Rep) | UAV LiDAR, AGM-MS3, 37 pts/m2 | 7 cm orthophoto (Sony A6000) | Field inventory, 7 species | Perm Krai, Russia (spruce, birch, fir, aspen, tilia, alder, willow) | 3,600 trees, 10 plots | Not stated for Kaggle data (UNVERIFIED) | Kaggle | GAP | Format analog for LiDAR+RGB+field species; boreal-mixed genera overlap |
| USGS 3DEP / Wisconsin county LiDAR | ALS QL2 (>= 2 pts/m2) or QL1 (>= 8 pts/m2) | Separate (not part of 3DEP) | Ground/class labels only, no trees | Statewide WI, all 72 counties | None | Public domain per secondary sources (USGS primary page not fetched) | WisconsinView, GeoData@Wisconsin, USGS downloader, AWS (EPT public; LAZ requester-pays) | Large | Context / regional CHM only, no tree labels |
| USFS FIA / WI DNR WisCFI plots | n/a | n/a | Tree-level species and DBH | WI | Many, but locations hidden | Public tables; exact coordinates restricted | FIA DataMart; WisCFI via access committee | n/a | Not usable for tree-level matching |

## NEON sites in Wisconsin / Great Lakes and available products

### Takeaway
NEON Domain 05 has two Wisconsin terrestrial sites (STEI and TREE, near Tomahawk) and UNDE in the Michigan Upper Peninsula, each with repeated AOP LiDAR and RGB flights from 2016 to 2025 and annual-to-5-year field vegetation structure with mapped stems and taxon IDs. NEON data are now CC BY 4.0 (changed from CC0 in June 2026) and require an account or API token to download.

### Cited Findings
- TREE (Treehaven) is a terrestrial site in Lincoln County, WI, about 4.7 km2, hosted by UW-Stevens Point, approx. 45.49 N, 89.59 W. Source: [DEIMS Treehaven](https://deims.org/c42b2e3e-598f-4048-822b-56c64748542b)
- STEI tower plots are on Steigerwaldt Land Services land east of Tomahawk about 1 mile north of Treehaven; distributed plots are in the Park Falls District of Chequamegon-Nicolet National Forest about 30 miles NW, about 23.4 km2. Source: [DEIMS STEI](https://deims.org/d9e0fbf0-9f84-4dc6-bb17-2adbfb714849)
- UNDE (UNDERC, Michigan) is in Domain 05; vegetation is mostly second-growth mixed forest with red and sugar maple, aspen, and birch. Source: [NEON UNDE](https://www.neonscience.org/field-sites/unde)
- Discrete-return LiDAR DP1.30003.001: classified point clouds in 1 km LAZ tiles, "several points per square meter"; available months STEI 2016-09, 2017-09, 2019-06, 2020-08, 2022-06, 2024-07, 2025-07 (provisional); TREE same through 2024-07; UNDE same as STEI. Source: [NEON API DP1.30003.001](https://data.neonscience.org/api/v0/products/DP1.30003.001)
- RGB orthomosaic DP3.30010.001: 1 km tiles, resolution at least 0.1 m; STEI 2016-09, 2017-09, 2020-08, 2022-06, 2024-07, 2025-07 (prov.); TREE 2016-09, 2017-09, 2020-08, 2022-06, 2024-07; UNDE 2017-09, 2020-08, 2022-06, 2024-07, 2025-07 (prov.). Source: [NEON API DP3.30010.001](https://data.neonscience.org/api/v0/products/DP3.30010.001)
- Vegetation structure DP1.10098.001 includes "mapped positions of qualifying plants"; woody individuals with DBH >= 10 cm are mapped and measured; taxon IDs are in the mapping and tagging table with identification history archived; distributed plots every 5 years (2026 suspended), tower subset (n=5) annually. Source: [NEON API DP1.10098.001](https://data.neonscience.org/api/v0/products/DP1.10098.001)
- VST available months: STEI 2015-08 through 2026-08 (many months); TREE 2015-07 through 2026-08; UNDE 2016-08 through 2026-08. Source: [NEON API DP1.10098.001](https://data.neonscience.org/api/v0/products/DP1.10098.001)
- License: NEON data products moved from CC0 to CC BY 4.0 and users must create an account and sign in or use an API token (blog dated June 30, 2026). Source: [NEON blog](https://www.neonscience.org/impact/observatory-blog/logins-required-and-data-licensing-updated); API page states CC BY 4.0. Source: [NEON Data Use Policy](https://data.neonscience.org/data-api/data-use-policy/)
- Older usage-policy pages still describe CC0 with commercial use allowed and rare/threatened species data excluded. Source: [NEON Usage Policies](https://neonscience.org/data-samples/guidelines-policies/usage-policies) (conflicts with the newer notices above; newer notices take precedence)

### Inferences
- CC BY 4.0 permits commercial use with attribution, so NEON remains usable for a commercial prototype; attribution must be maintained (CC BY 4.0 standard terms, not a NEON-specific statement).
- STEI/TREE/UNDE all share flights in Sept 2016, Sept 2017, Aug 2020, June 2022, July 2024, which pair with VST surveys in the same or adjacent seasons. This gives repeat-date stacks that must be kept in the same spatial partition per CLAUDE.md.
- Hyperspectral (DP1.30006.001) and CHM (DP3.30015.001) are also AOP products, but their site/year availability was not checked here. Hyperspectral must not substitute for RGB feasibility.

### Gaps
- Field names for stem offsets (pointID, stemDistance, stemAzimuth) were not shown in the fetched description; from prior knowledge they exist in vst_mappingandtagging, but this is **UNVERIFIED** in this session. Mapping may be only for a subset of plots or years.
- Number of mapped, species-identified trees at STEI/TREE/UNDE was not counted.
- Per-tile file sizes for LiDAR and RGB not stated (prior estimate of tens to a few hundred MB per 1 km tile is **UNVERIFIED**).
- Whether data downloaded before June 2026 stay CC0 is reported by a search summary but not confirmed on the fetched blog page.
- Hyperspectral and CHM year availability not checked.

## NeonTreeEvaluation and NEON crown/species datasets

### Takeaway
NeonTreeEvaluation is a CC0 (code repo) / CC BY 4.0 (Zenodo data) crown-delineation benchmark with about 31k image-annotated crowns across 22 NEON sites; it has no species labels on its repo page. The large NEON crown and species map products are model predictions and must not be used as ground truth.

### Cited Findings
- NeonTreeEvaluation: image-annotated bounding boxes on RGB, LiDAR annotations from boxes draped on point clouds, 22 NEON sites, 30,975 annotations, license CC0-1.0. Source: [GitHub NeonTreeEvaluation](https://github.com/weecology/NeonTreeEvaluation)
- Zenodo data release 0.2.2: CC BY 4.0; RGB, LiDAR, 426-band hyperspectral, 1 m CHM per site; annotations.zip 561 kB, evaluation.zip 3.9 GB, training.zip 4.5 GB (8.4 GB total). Source: [Zenodo 5914554](https://zenodo.org/record/5914554)
- A derived Hugging Face copy (CanopyRS) declares CC BY 4.0. Source: [HF CanopyRS README](https://huggingface.co/datasets/CanopyRS/NeonTreeEvaluation/blob/main/README.md)
- NEON Tree Crowns: DeepForest-predicted crowns for about 100 million trees across 37 NEON sites, 1 km tiles. Source: [Zenodo 3765872](https://zenodo.org/records/3765872)
- Weinstein-lab species predictions: 81 canopy species, average 79% accuracy, about 6 species per site, on Zenodo and Google Earth Engine, summarized from [NSF PAR 10453016](https://par.nsf.gov/biblio/10453016) via search; primary record not fetched

### Inferences
- Use NeonTreeEvaluation for crown detection/segmentation evaluation only. Species must come from NEON VST field records linked to crowns by the project's own stem-to-crown matching.

### Gaps
- Whether STEI, TREE, or UNDE are among the 22 benchmark sites was not confirmed.
- Prior knowledge that the benchmark paper also includes field-collected stems and field-delineated crowns at some sites is **UNVERIFIED** here.
- License of NEON Tree Crowns and the species prediction product not verified; exact title/year of the species paper not confirmed.

## FOR-species20K, FOR-instance, FOR-instanceV2

### Takeaway
FOR-species20K (20,158 trees, 33 species, mostly European TLS) is the largest 3D species benchmark but is GPL/AGPL licensed on Zenodo and has little or no North American species overlap. FOR-instance is a small CC BY 4.0 ULS segmentation benchmark without species. FOR-instanceV2 license and release status are unconfirmed.

### Cited Findings
- FOR-species20K: over 20,000 trees, 33 species, TLS/MLS/ULS. Source: [arXiv 2408.06507](https://arxiv.org/abs/2408.06507)
- Composition: TLS 70%, ULS 22%, MLS 8%; mainly European forests (Mediterranean, temperate, boreal) plus scattered trees from other continents; 12 TLS sensors, one sensor each for MLS and ULS. Source: [arXiv PDF](https://arxiv.org/pdf/2408.06507)
- Zenodo record: licenses listed "GNU General Public License v3.0 or later" and "GNU Affero General Public License v3.0 or later"; dev.zip 25.4 GB, test.zip 1.8 GB, tree_metadata_dev.csv 1.2 MB (27.2 GB total); test labels withheld for Codabench. Source: [Zenodo 13255198](https://zenodo.org/records/13255198)
- A separate "for review" record is restricted; use 13255198. Source: [Zenodo 13307764](https://zenodo.org/records/13307764)
- FOR-instance: CC BY 4.0, 1.6 GB, Riegl ULS, Norway, Czech Republic, Austria, New Zealand, Australia, 1,130 manually segmented trees, no species mentioned. Source: [Zenodo 8287792](https://zenodo.org/records/8287792)
- FOR-instanceV2: ULS, MLS, TLS from several countries, 48 train / 17 val / 29 test plots, 11,028 instances, reported in [ForestFormer3D arXiv 2506.16991](https://arxiv.org/pdf/2506.16991) and [arXiv 2609.24787](https://arxiv.org/pdf/2609.24787) (via search summary)

### Inferences
- GPL/AGPL is a software license applied to data; its implications for training a commercial model on the data are unclear and need Beau's or counsel's review before use. Treat as a risk flag.
- Most FOR-species20K trees are TLS, so its point density and occlusion differ from drone LiDAR; the ULS subset (22%) is the relevant slice.

### Gaps
- Full species list and whether any North American species (e.g., Acer saccharum, Pinus strobus) are included were not retrieved (would be in tree_metadata_dev.csv or paper Table 2).
- FOR-instanceV2 download location and license not found.

## USGS 3DEP, Wisconsin LiDAR, FIA and WisCFI

### Takeaway
Wisconsin has statewide public QL2-or-better airborne LiDAR (all 72 counties), generally public domain, useful as regional context but with no tree labels. Exact FIA and WisCFI plot locations are not public, so they cannot provide tree-level matched labels.

### Cited Findings
- By 2022 all 72 WI counties expected to have QL2 or better LiDAR; QL2 >= 2 pts/m2, QL1 >= 8 pts/m2, about 10 cm vertical RMSE. Source: [WI SCO](https://sco.wisc.edu/2023/04/21/wisconsin-to-achieve-statewide-publicly-available-lidar-coverage-in-2023)
- Download through WisconsinView (Robinson Map Library and SCO), GeoData@Wisconsin, SCO elevation map, USGS National Map downloader. Source: [WI SCO lidar tag](https://www.sco.wisc.edu/tag/lidar); [2024 WLIP Report](https://doa.wi.gov/DIR/2024_WLIP_Report.pdf)
- 3DEP products are described as public domain; credit requested; AWS has a public EPT collection and a requester-pays LAZ bucket. Source: [Data.gov 3DEP](https://catalog.data.gov/dataset/lidar-point-cloud-usgs-national-map-3dep-downloadable-data-collection); [USGS AWS notice](https://www.usgs.gov/news/usgs-3dep-lidar-point-cloud-now-available-amazon-public-dataset)
- FIA public plot locations are fuzzed (study describes perturbation by 805 m) and some plots swapped; exact locations protected under the Food Security Act of 1985. Source: [USFS Treesearch 14404](https://research.fs.usda.gov/treesearch/14404); [FIA Spatial Data Services](https://research.fs.usda.gov/programs/fia/sds)
- FIA SDS currently states it cannot process confidential-location requests due to workforce reductions. Source: [FIA SDS](https://research.fs.usda.gov/programs/fia/sds)
- WisCFI: permanent plots about 1 per 200 acres on 10 state forests, one-fifth sampled yearly; exact locations reviewed by an access committee and described as not available to the general public. Source: [WI DNR WisCFI](https://dnr.wisconsin.gov/topic/ForestPlanning/forestInventory); [Council on Forestry list](https://councilonforestry.wi.gov/Documents/PracticesStudy/PublicallyAvailableDatasets.pdf)

### Inferences
- "Requester pays" LAZ on AWS is a cost item; WisconsinView/USGS downloader are free routes.
- County LiDAR can provide a pre-flight CHM for the property and a lower-density ALS baseline.

### Gaps
- USGS primary licensing page not fetched; public-domain status from secondary catalogs.
- Per-county acquisition years/densities for the specific property not checked.
- No public university plot dataset with Wisconsin tree positions found in this session (e.g., UW-Madison or UWSP research forests); not searched exhaustively.

## Other North American labeled individual-tree datasets with species

### Takeaway
The Quebec Trees Dataset has the best species overlap with northern Wisconsin (maples, birches, beech, aspen, white pine, hemlock, cedar, balsam fir) but is RGB/photogrammetry only. TEMPL is the only found North American drone LiDAR dataset with field species in hardwoods, but it is small and under review. IDTReeS is CC BY 4.0 but uses NEON airborne data from non-Great-Lakes sites.

### Cited Findings
- Quebec Trees: Cloutier, Germain, Laliberte; seven UAV dates May-Oct 2021; RGB orthomosaics and photogrammetric point clouds; 14 classes, 22,260 annotations in Table 1 (ACRU 5,857, BEPA 5,894, ABBA 2,895, THOC 1,510, Populus 1,114, Picea 1,022, ACSA 1,014, ACPE 751, PIST 569, BEAL 290, FAGR 222, LALA 185, TSCA 59, dead 878); CC BY 4.0; 149.3 GB. Source: [Zenodo 8148479](https://zenodo.org/records/8148479)
- Later work notes Populus and Picea are genus-level due to annotator uncertainty. Source: [arXiv 2603.23669](https://arxiv.org/pdf/2603.23669) (via search summary)
- TEMPL: ALS, ULS, two backpack LiDAR; nine CFI plots; 231 trees, 838 tree point clouds, 16 species plus snag; field-measured attributes; dataset DOI 10.4231/5DGX-DP81; preprint CC BY 4.0, under ESSD review. Source: [ESSD preprint essd-2026-632](https://essd.copernicus.org/preprints/essd-2026-632/)
- IDTReeS 2020: UW-Madison and UF; NEON RGB, LiDAR, VST, ecosystem structure; crown polygons; CC BY 4.0; 572.3 MB. Source: [Zenodo 3934932](https://zenodo.org/records/3934932); [DOI 10.5281/zenodo.3700196](https://dx.doi.org/10.5281/zenodo.3700196)
- Dubrovin et al. 2024: Perm Krai, Russia; AGM-MS3 LiDAR 37 pts/m2; 7 cm RGB; 3,600 trees, 10 plots, 7 species; Kaggle; data license not stated. Source: [PMC11415378](https://pmc.ncbi.nlm.nih.gov/articles/PMC11415378/)
- CedarCypress3D: UAV LiDAR, 1,627 census trees in Japanese cedar/cypress plantations, Zenodo. Source: [arXiv 2608.30149](https://arxiv.org/pdf/2608.30149) (via search summary)
- Quebec Plantations (FRDR): UAV photogrammetry, UAV LiDAR, crown polygons, georeferenced height and DBH in carbon plantations. Source: [FRDR record](https://www.frdr-dfdr.ca/repo/dataset/9f10a155-c89f-43ee-9864-da28ca436af6?mode=full) (via search summary; license not checked)

### Inferences
- Quebec Trees is useful for RGB species classification transfer and phenology (multi-date) testing, but whether labels are field-verified is not stated, so treat them as photo-interpreted until confirmed.
- TreeSatAI (Germany) and Harvard Forest datasets were not researched in this session.

### Gaps
- TEMPL dataset license, size, state, ULS model, and RGB availability not confirmed.
- Quebec Trees label-verification method and site name not stated on the record.
- IDTReeS site list and tree/species counts not confirmed on the record page.
- Harvard Forest (HARV NEON plus ForestGEO) and TreeSatAI not checked.

## Closest match to "drone LiDAR + RGB + field-confirmed species in northern hardwoods"

### Takeaway
No public dataset found combines drone LiDAR, drone RGB, and field-confirmed species in Great Lakes northern hardwoods. The practical combination is NEON STEI/TREE/UNDE (airborne LiDAR + 10 cm RGB + field-mapped stems with species, in-region, CC BY 4.0) for in-region labels, plus TEMPL (drone LiDAR, field species, eastern US hardwoods) and Quebec Trees (drone RGB, matching species) as sensor-analog supplements, with the project's own field-confirmed property data as the real test.

### Cited Findings
- NEON in-region field stems with species and repeated AOP flights. Source: [NEON API DP1.10098.001](https://data.neonscience.org/api/v0/products/DP1.10098.001); [NEON API DP1.30003.001](https://data.neonscience.org/api/v0/products/DP1.30003.001)
- TEMPL drone LiDAR with field-measured species in hardwoods. Source: [ESSD preprint](https://essd.copernicus.org/preprints/essd-2026-632/)
- Quebec Trees drone RGB with northern-hardwood species. Source: [Zenodo 8148479](https://zenodo.org/records/8148479)

### Inferences
- NEON ALS density ("several pts/m2") is far below typical drone LiDAR, so NEON is a lower-bound baseline, not a sensor-matched one.
- Any model trained on these sources still needs spatially independent, field-confirmed test data from the target property; one property shows local feasibility only.

### Gaps
- Unmatched: a public Great Lakes drone LiDAR + RGB + field species dataset; none found. Searching university repositories (UW-Madison, UWSP, Michigan Tech, U Minnesota) could be a follow-up.
