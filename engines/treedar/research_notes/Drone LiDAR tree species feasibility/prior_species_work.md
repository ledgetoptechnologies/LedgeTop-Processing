# Prior Work: Drone LiDAR + RGB Individual-Tree Species Classification in Temperate Deciduous and Mixed Forests

Research date: 2026-10-13. Scope: published projects most similar to a small Wisconsin drone operator using UAV LiDAR plus UAV RGB (including multi-date / fall-color RGB) for per-tree species maps in northern hardwood and mixed forest.

Labeling conventions used in these notes:
- **[UAV-RGB]**, **[UAV-LiDAR]**, **[UAV-LiDAR+RGB]**, **[UAV-HSI]** (hyperspectral), **[UAV-MS]** (multispectral), **[AIRBORNE]** (crewed plane).
- **[VERIFIED-FULLTEXT]** means I read the relevant passage of the primary source. **[ABSTRACT/SNIPPET ONLY]** means the number came from an abstract or search summary and should be checked against the full paper before it is quoted to Beau or clients.
- Accuracy metrics differ across studies (pixel F1, crown-level overall accuracy, micro vs macro accuracy). They are not directly comparable.

---

## Q1. Which studies classified individual-tree species from UAV RGB alone, UAV LiDAR alone, and fused LiDAR+RGB in comparable forests, and what did they achieve?

### Takeaway
The closest regional analog is the Quebec multi-date UAV RGB study (Cloutier, Germain, Laliberté), which reached only a mean F1 of 0.61 to 0.72 across 14 classes with 23,000 field-labelled crowns, with maples the weakest group. I found no peer-reviewed study that fuses UAV LiDAR with UAV RGB for species classification in a Great Lakes / Northeastern US / eastern Canadian northern hardwood forest; the fused-sensor results that exist are mostly from China and mostly use hyperspectral, not RGB.

### Cited Findings

**UAV RGB only (most relevant)**
- **Cloutier, Germain, Laliberté, Quebec [UAV-RGB] [VERIFIED-FULLTEXT, preprint v1]:** Site is the Université de Montréal research station at Saint-Hippolyte, Quebec, an unmanaged mixed temperate forest whose most common canopy species are paper birch, red maple, bigtooth aspen and sugar maple. Sensor was a DJI Phantom 4 RTK flown 60 m above canopy with terrain follow, 25 GCPs, georeferencing generally under 3 cm. Seven acquisitions May to October 2021. 23,000 crowns in 14 classes (including Abies balsamea, Acer pensylvanicum, A. rubrum, A. saccharum, Betula alleghaniensis, B. papyrifera, Fagus grandifolia, Larix laricina, Picea spp., Pinus strobus, Populus spp., Thuja occidentalis, Tsuga canadensis, dead trees). Method: U-Net semantic segmentation (pixel-level), one model per date. Mean F1 was highest (0.72) at the start of leaf colouring on 2 Sept 2021, second best August (0.66), others 0.61 to 0.65, lowest 0.61 at peak colour in early October. Best classes: Larix laricina, Pinus strobus (F1 0.77 to 0.87 across dates), Populus spp. Worst: Acer pensylvanicum (F1 0.36 to 0.61), Fagus grandifolia, Betula alleghaniensis.. Source: [bioRxiv preprint 10.1101/2023.08.03.548604](https://www.biorxiv.org/content/10.1101/2023.08.03.548604.full.pdf); dataset [Quebec Trees Dataset, Zenodo 8148479](https://zenodo.org/records/8148479)
- Same study, evaluation caveat: tiles in the training site were split randomly 80/20 into train/validation, and the test set combined a separate test site with some held-out annotations from the training site "to balance the classes." This is only partly spatially independent and should not be treated as a strict out-of-area result.. Source: [bioRxiv preprint](https://www.biorxiv.org/content/10.1101/2023.08.03.548604.full.pdf)
- A 2026 ISPRS Annals paper cites the dataset as "Cloutier et al., 2024," implying a peer-reviewed version exists, but I could not confirm the journal or whether reported numbers changed.. Source: [ISPRS Annals XI-3-2026-19](https://isprs-annals.copernicus.org/articles/XI-3-2026/19/2026/isprs-annals-XI-3-2026-19-2026.pdf)
- **Grybas and Congalton 2021, New Hampshire [UAV-RGB vs UAV-MS, multi-date] [ABSTRACT/SNIPPET ONLY]:** 14 species in heterogeneous New Hampshire forests. Multi-temporal classification significantly improved accuracy over single date, but more than three dates gave no significant benefit. Mid to late spring imagery gave the highest accuracies. Consumer RGB significantly outperformed the multispectral sensor, attributed to the blue band. Overall accuracy numbers were not available to me (MDPI returned 403).. Source: [Remote Sensing 13(13):2631](https://www.mdpi.com/2072-4292/13/13/2631/htm); [UNH Scholars record](https://scholars.unh.edu/faculty_pubs/1316)
- **Schiefer et al. 2020, Germany (Black Forest and Hainich) [UAV-RGB] [ABSTRACT/SNIPPET ONLY]:** U-Net on under 2 cm RGB over 51 ha; nine species, three genus-level classes, deadwood and forest floor; mean F1 0.73. Adding nDSM height gave only a slight gain. Coarsening to 32 cm dropped mean F1 to 0.26. Data released as the CC BY 4.0 "Fortress" dataset.. Source: [ISPRS J. Photogramm. 170:205-215 summary](https://rsc4earth.de/publication/schiefer-mapping-2020/); [KIT record](https://publikationen.bibliothek.kit.edu/1000125945); [Fortress dataset](https://service.tib.eu/ldmservice/vdataset/6f47c669-450e-4dfb-87b2-0e8b4b1fd5d5)
- **Winter UAV RGB, eight temperate species [UAV-RGB] [ABSTRACT/SNIPPET ONLY; region not confirmed]:** reported average F1 around 0.9 from leaf-off winter imagery; a related 2024 study of eight species reported best performance on summer imagery (average F1 0.96) and studied cross-season transfer. Region and labeling method not confirmed by me.. Source: [Remote Sensing Applications: Society and Environment, S2352938524002866](https://www.sciencedirect.com/science/article/abs/pii/S2352938524002866); [DOAJ Remote Sensing Oct 2024](https://doaj.org/article/5654d6cf7ba540acabe6db33e8c78e87)
- **Eastern Ontario hardwood, 2017 [UAV-MS, OBIA + Random Forest] [ABSTRACT/SNIPPET ONLY]:** white birch, aspen and two maple species; about 78% overall correct on only 23 validation crowns; the two maples were confused with each other while aspen and birch separated easily.. Source: [CCLM portal record](https://www.cclmportal.ca/resource/deciduous-tree-species-classification-using-object-based-analysis-and-machine-learning)

**UAV LiDAR only**
- **Mixed temperate forest, Germany (Forestry 2026; also EGU26 abstract) [UAV-LiDAR, Random Forest] [ABSTRACT/SNIPPET ONLY]:** six species (including Douglas-fir, Norway spruce, European beech, hornbeam, sessile oak). Leaf-on only 75% OA, leaf-off only 76%, leaf-on plus leaf-off 80%. Intensity features were the strongest discriminators; beech and hornbeam were confused.. Source: [Forestry 99(4) cpag069](https://academic.oup.com/forestry/article/99/4/cpag069/8789523); [EGU26-14442](https://meetingorganizer.copernicus.org/EGU26/EGU26-14442.html)
- **Slavík et al. 2023, Czech Republic [UAV-LiDAR] [ABSTRACT/SNIPPET ONLY]:** 1,045 trees, conifer vs broadleaf only; 94.8% (GLM) and 95.1% (RF) OA.. Source: [Forests 14(8):1581](https://www.mdpi.com/1999-4907/14/8/1581)
- **FOR-species20K benchmark [mixed TLS/MLS/ULS, mostly Europe] [ABSTRACT/SNIPPET ONLY]:** over 20,000 single-tree point clouds, 33 species; 2D multi-view models averaged 0.77 OA vs 0.72 for 3D point models; best model DetailView. Results are pooled across platforms; drone-only performance not separated in what I read. Uses pre-segmented trees, so it does not include segmentation error.. Source: [Methods Ecol. Evol. 2025, DOAJ](https://doaj.org/article/9ae6af6106f143678762a30c01730e62); [arXiv 2408.06507](https://export.arxiv.org/pdf/2408.06507)

**UAV LiDAR fused with imagery**
- **Northeast China mixed conifer-broadleaf [UAV-HSI + UAV-LiDAR] [ABSTRACT/SNIPPET ONLY]:** fused 89.20%, hyperspectral alone 86.08%, LiDAR alone 76.42%; individual tree segmentation 84.62%. Hyperspectral, not RGB.. Source: [Frontiers in Plant Science 2022, PMC9539217](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC9539217/)
- **Maoershan, NE China [UAV-HSI + UAV-LiDAR] [ABSTRACT/SNIPPET ONLY]:** birch and larch above 60% with LiDAR alone; birch best under fusion. Hyperspectral.. Source: [Remote Sensing 15(4):1000](https://doi.org/10.3390/rs15041000)
- **Lee, Baek, Jung 2023 [UAV-LiDAR + visible imagery] [ABSTRACT/SNIPPET ONLY; region and species not confirmed, likely not North American]:** 83% and 82% OA in two study areas; no LiDAR-only baseline seen., cited via [extended search result, Frontiers Forests Glob. Change 2025 review](https://www.frontiersin.org/journals/forests-and-global-change/articles/10.3389/ffgc.2025.1431603/full)
- **Subtropical broadleaf (RSE 2022) [UAV-LiDAR + UAV-HSI + ultrahigh-res RGB]:** exists as a three-sensor fusion reference but is subtropical and hyperspectral-led.. Source: [Remote Sensing of Environment S0034425722002577](https://www.sciencedirect.com/science/article/abs/pii/S0034425722002577)
- **Northeast China, improved YOLOv8 on UAV RGB + LiDAR [UAV-LiDAR+RGB] [ABSTRACT/SNIPPET ONLY]:** dual-branch fusion architecture; accuracy not captured.. Source: [Forests 15:293](https://mdpi-res.com/d_attachment/forests/forests-15-00293/article_deploy/forests-15-00293.pdf)
- A 2025 ISPRS Archives paper notes UAV LiDAR adds cost and usually needs a separate flight from image acquisition.. Source: [ISPRS Archives XLVIII-2-W11-2025-219](https://isprs-archives.copernicus.org/articles/XLVIII-2-W11-2025/219/2025/)

**Airborne (plane) reference, NEON [AIRBORNE RGB + HSI + LiDAR] [VERIFIED-FULLTEXT via PMC]:** see Q2.

### Inferences
- For RGB-only drone work in northern hardwood / mixed forest, a realistic starting expectation is roughly 0.6 to 0.75 mean F1 at species level across 10+ classes, with conifers, aspen and paper birch well above average and maples, beech and yellow birch well below. Single headline numbers above 0.9 usually come from fewer species, pre-segmented trees, or non-spatial splits.
- LiDAR alone in temperate broadleaf forest gives about 75 to 80% OA for around six species in the one European study found; its clearest value is conifer vs broadleaf and structural features, not separating maples.
- The evidence that LiDAR adds much on top of good RGB for species is weak: Schiefer found height gave only slight gains, and the strong fusion gains reported are with hyperspectral. A LiDAR+RGB fusion study in Great Lakes northern hardwoods appears to be an open gap (inference from not finding one, not proof of absence).

### Gaps
- Could not retrieve full text for Grybas and Congalton 2021 (overall accuracy values, per-species confusions), the Forestry 2026 UAV LiDAR paper, or Lee et al. 2023.
- Per-species F1 values in the Cloutier preprint Table 1 did not extract cleanly from the PDF; only the values stated in the text are reported here.
- Did not confirm the peer-reviewed journal version of Cloutier et al.

---

## Q2. Specific verifications: Cloutier/Germain/Laliberté; Weinstein NEON; NEON Wisconsin sites STEI, TREE, UNDE; Wisconsin DNR / UW / Michigan Tech / SUNY ESF drone species work

### Takeaway
Cloutier et al. is real and verified (details in Q1). Weinstein et al. 2024 (PLoS Biology) produced per-tree species maps for 24 NEON sites including the three Wisconsin-area sites, at about 0.78 to 0.80 micro accuracy there, but this is airborne hyperspectral-driven and is not evidence for drone RGB feasibility. I found no drone species-classification publication from Wisconsin DNR, UW, Michigan Tech, or SUNY ESF in this search.

### Cited Findings
- **Weinstein et al. 2024, "Individual canopy tree species maps for the National Ecological Observatory Network," PLoS Biology 22(7): e3002700 [AIRBORNE] [VERIFIED-FULLTEXT via PMC]:** 24 sites, 81 species; site-specific models averaged 78.8% micro and 75.8% macro accuracy, average six species per site (range 3 to 15).. Source: [PMC11251727](https://pmc.ncbi.nlm.nih.gov/articles/PMC11251727)
- Sensors: NEON AOP airborne 10 cm RGB (crown detection with DeepForest, alive/dead, stem matching), 1 m LiDAR CHM (height filter, 3 m minimum), and ~420 to 2,500 nm hyperspectral at 1 m (primary species input). The authors found RGB did not significantly improve species classification.. Source: [PMC11251727](https://pmc.ncbi.nlm.nih.gov/articles/PMC11251727)
- Per-site results from Table 2 (micro / macro, species, train / test trees):
  - UNDE: 0.79 / 0.79, 13 species, 547 / 178. An earlier version was 67.8% / 61.6% with 12 species; adding 157 targeted stems raised it to 77.7% / 79.1%.
  - TREE: 0.78 / 0.72, 15 species, 643 / 168.
  - STEI: 0.80 / 0.83, 6 species, 283 / 82.
  - HARV (Massachusetts): 0.76 / 0.57, 15 species, 9,782 / 1,194.
  - BART (New Hampshire): 0.68 / 0.66, 7 species, 514 / 125.. Source: [PMC11251727](https://pmc.ncbi.nlm.nih.gov/articles/PMC11251727)
- Site-location note: the summarizing tool labeled TREE and STEI as Michigan and UNDE as Wisconsin. My understanding is that STEI (Steigerwaldt-Chequamegon) and TREE (Treehaven) are in Wisconsin and UNDE (UNDERC) is on the Wisconsin / Michigan Upper Peninsula border; this is unverified here and should be checked on the NEON field-site pages before citing.
- The public prediction dataset lists 23 site codes including TREE and UNDE but not STEI, even though the paper's table reports STEI.. Source: [Zenodo 10067302](https://zenodo.org/records/10067302)
- Limitations stated by the authors: crown detection roughly 70 to 75% accurate; only sunlit canopy crowns; rare species dropped (minimum 10 train and 10 test per species per site); no unknown-class detector (listed as future work); confidence calibration not feasible because of limited held-out data; site-specific models do not transfer well and a single NEON-wide model performed worse.. Source: [PMC11251727](https://pmc.ncbi.nlm.nih.gov/articles/PMC11251727)
- Earlier continental hyperspectral model (Marconi, Weinstein et al. 2022, RSE) covered about 20% of US forest tree species, representing roughly 90% of upper canopy at studied sites, and reported useful uncertainty estimates. [AIRBORNE] [ABSTRACT/SNIPPET ONLY]. Source: [UF Machine Learning Lab summary](https://faculty.eng.ufl.edu/machine-learning/?p=11432)
- NEON Tree Crowns dataset (DeepForest crowns) covers 37 sites; crowns only, no species.. Source: [NSF PAR 10453016](https://par.nsf.gov/biblio/10453016-neon-tree-crowns-dataset)
- Wisconsin Extension presentation states drone studies map individual species, oak wilt mortality and biomass, and that drones are not expected to replace fieldwork. It is a general overview, not a species-accuracy study.. Source: [UW Extension Florence County, Kleiman.pdf](https://florence.extension.wisc.edu/files/2021/05/Kleiman.pdf)
- Searches for Michigan Tech or UW drone species classification returned no matching paper., (negative result; no source)

### Inferences
- NEON is the most useful public benchmark close to Wisconsin, and its field stems at STEI, TREE and UNDE could serve as an airborne-scale comparison, but the 0.78 to 0.80 numbers are a hyperspectral ceiling, not an RGB expectation. The CLAUDE.md rule "Do not substitute hyperspectral results for RGB feasibility" applies directly.
- The UNDE example (157 targeted extra stems moved macro accuracy from 62% to 79%) is a useful data point on label budgeting: targeted labels for weak classes paid off substantially.

### Gaps
- No SUNY ESF, Michigan Tech, UW-Madison or Wisconsin DNR drone species classification paper found in the time budget. A targeted Google Scholar author search is still needed before claiming none exists.
- Did not verify NEON site state locations directly.

---

## Q3. How much does multi-date phenology (fall color) help? How many dates? Which species pairs stay confused?

### Takeaway
Multi-date imagery helps, but peak fall color is not the best single date: in Quebec the best single date was early September (start of coloring), and peak-color October was the worst. In New Hampshire, gains plateaued at three dates, with spring best. Red vs sugar maple remains the persistent confusion in every northern hardwood study found.

### Cited Findings
- Quebec: best single date F1 0.72 at start of colouring (2 Sept), lowest 0.61 at peak colour (early Oct). Authors attribute the October drop largely to leaf fall and high within-species variation in colour and leaf-drop timing.. Source: [Cloutier et al. preprint](https://www.biorxiv.org/content/10.1101/2023.08.03.548604.full.pdf)
- Quebec confusions: maples (A. pensylvanicum, A. rubrum, A. saccharum) were the main misclassified classes, with A. rubrum often predicted as A. saccharum at all dates; B. papyrifera predicted as B. alleghaniensis; A. rubrum confused with F. grandifolia; A. rubrum predicted as "dead" rose from 1-2% before September to 10% late Sept and 20% October.. Source: [Cloutier et al. preprint](https://www.biorxiv.org/content/10.1101/2023.08.03.548604.full.pdf)
- Quebec models were trained and tested separately per date; the study did not test a fused multi-date model, so it does not measure the gain from stacking dates.. Source: [Cloutier et al. preprint](https://www.biorxiv.org/content/10.1101/2023.08.03.548604.full.pdf)
- New Hampshire: multi-temporal significantly improved accuracy, no significant benefit beyond three dates, mid to late spring best.. Source: [Grybas and Congalton 2021](https://www.mdpi.com/2072-4292/13/13/2631/htm)
- Cool-temperate mountain forest (Avtar et al. 2024) [UAV, multi-season] [ABSTRACT/SNIPPET ONLY]: RF best at 83.98% OA, kappa 0.80; using summer, winter, autumn and spring together improved accuracy by 14 to 18%. Region outside North America.. Source: [DOAJ Remote Sensing Oct 2024](https://doaj.org/article/63a185627ccc4aff949b1a7ff2e2b763)
- Germany UAV LiDAR: leaf-on plus leaf-off raised OA from 75-76% to 80%; beech and hornbeam confused.. Source: [Forestry 2026](https://academic.oup.com/forestry/article/99/4/cpag069/8789523)
- Ontario: two maples confused with each other.. Source: [CCLM portal](https://www.cclmportal.ca/resource/deciduous-tree-species-classification-using-object-based-analysis-and-machine-learning)
- Weinstein NEON used multi-temporal (multi-year airborne) hierarchical models.. Source: [Zenodo 10067302](https://zenodo.org/records/10067302)

### Inferences
- A practical Wisconsin plan is likely two to three dates (for example late spring flush, early fall color onset, possibly leaf-off for conifer/LiDAR structure), not repeated peak-color flights. This is an inference and should be tested locally.
- Red vs sugar maple should be expected to remain confused; reporting "maple (Acer)" at genus level with an unknown/insufficient-evidence option is likely needed.
- Red vs white oak: no study found that reports this pair from drone RGB. Treat as unknown.

### Gaps
- No drone study found reporting red oak vs white oak separation or Quercus per-class accuracy in North America.
- No study quantifying the gain of a fused multi-date model specifically including fall color in Great Lakes forests.

---

## Q4. Genus/group-level vs species-level accuracy; oak vs non-oak and conifer vs deciduous mapping relevant to wildlife habitat

### Takeaway
Coarser groupings are consistently much easier: conifer vs broadleaf from UAV LiDAR reached about 95% OA in the Czech study, versus 0.6 to 0.8 at species level in mixed temperate forests. I found no drone study explicitly framed around mast (oak) or thermal-cover mapping for wildlife.

### Cited Findings
- Conifer vs broadleaf from UAV LiDAR, 1,045 trees: 94.8% (GLM) and 95.1% (RF) OA.. Source: [Slavík et al. 2023, Forests 14:1581](https://www.mdpi.com/1999-4907/14/8/1581)
- In Quebec, evergreen classes showed less variability across dates and Pinus strobus and Larix laricina scored highest.. Source: [Cloutier et al. preprint](https://www.biorxiv.org/content/10.1101/2023.08.03.548604.full.pdf)
- Schiefer et al. used three genus-level classes alongside nine species when species-level labels were impractical.. Source: [Schiefer et al. summary](https://rsc4earth.de/publication/schiefer-mapping-2020/)
- 2018 UAV RGB CNN reached 92.7% on four types including deciduous vs coniferous plus a few species.. Source: [arXiv 1804.10390](https://arxiv.org/pdf/1804.10390)
- Poland 10-band multispectral UAV: species accuracies 70.73 to 91.77% in one area and 36.51 to 72.33% in another, with relatively high accuracy for oak. [UAV-MS] [ABSTRACT/SNIPPET ONLY]. Source: [ISPRS Annals XI-3-2026-351](https://isprs-annals.copernicus.org/articles/XI-3-2026/351/2026/)
- Spain (Valldaura) oak vs pine monitoring project exists as a student research abstract; not peer reviewed.. Source: [IAAC blog](https://www.iaacblog.com/programs/reading-forest-valldaura-forest-inventory-oak-vs-pine-species-monitoring-sustainable-forest-management/)

### Inferences
- A hunting-land product built on conifer vs deciduous (thermal cover) and Quercus vs non-Quercus (mast) is likely more defensible than full species maps, but oak-genus accuracy from drone RGB in Wisconsin is not established by anything found here and must be measured.
- The large drop in the second Polish study area (down to 36.51%) shows site transfer risk, consistent with CLAUDE.md's warning that one property does not demonstrate regional generalization.

### Gaps
- No peer-reviewed drone study found that targets wildlife mast or thermal cover mapping in the Great Lakes.
- No oak genus-level drone accuracy for North American oaks found.

---

## Q5. Is anyone commercially offering per-tree species maps from drones in the US Midwest?

### Takeaway
I found no company publicly advertising drone-based per-tree species maps for Wisconsin or the Upper Midwest. This does not prove the operator would be the only one: the search was limited, and adjacent providers (Canadian drone inventory firms, under-canopy LiDAR startups, forestry consultants using LiDAR, and state airborne LiDAR programs) exist.

### Cited Findings
- **Digital Timber (North Bay, Ontario):** described as the only North American licence holder for a Finnish tree-inventory software, providing forestry companies with data on what species are growing in an area.. Source: [CBC News](https://amp.cbc.ca/news/canada/sudbury/northbay-drones-counting-trees-1.5827160)
- **I2S Ltd. (Canada):** UAV-based individual tree locations and species identification in dense stands (Sudbury-area example).. Source: [Esri UC 2014 abstract](https://proceedings.esri.com/library/userconf/proc14/abstracts/a731.html)
- **Candrone (Canada):** markets aerial LiDAR inventory with a few ground-truth plots, contrasting with cruises that sample 2 to 5% of a forest. Species outputs not confirmed.. Source: [Candrone LinkedIn](https://linkedin.com/company/candrone)
- **Treeswift (Philadelphia):** under-canopy autonomous LiDAR drones; technology developer, not a landowner species-map service as far as found.. Source: [DroneDJ 2022](https://dronedj.com/2022/03/29/treeswift-forest-data-lidar-drones/)
- **Deep Forestry (Sweden):** below-canopy drone with Ouster LiDAR targeting species type, wood quality and volume. Not US-based.. Source: [USDA NAL](https://nal.usda.gov/research-tools/food-safety-research-projects/deep-forestry-drones-revolutionising-forestry-mapping); [Ouster blog](https://ouster.com/insights/blog/forest-biodiversity-powered-by-three-dimensional-deep-learning-and-the-ouster-os0)
- **Tree Stand Forestry (Ohio):** small consultancy working mainly with deer hunters on habitat-focused timber work, mentioning thermal drones and emerging LiDAR; species mapping as a service not confirmed. Directly relevant as a potential competitor in the hunting-habitat niche.. Source: [The DroneOn Show Ep. 35](https://the-droneon-show.castos.com/episodes/from-forestry-to-drone-tech-habitat-lidar-public-hunts-the-droneon-show-episode-35)
- **Minnesota DNR:** statewide inventory using crewed-aircraft LiDAR and photography over about 10 million acres in the north, paired with field sampling. Public program, not a drone vendor.. Source: [CBS Minnesota](https://www.cbsnews.com/minnesota/news/minnesota-tree-tracking)
- **Open Forest Observatory:** open-source drone forest mapping tools including taxonomic classification, developed on western US forests.. Source: [FOSS4G 2024 talk](https://talks.osgeo.org/foss4g-2024/talk/ACVK7R/)
- **Purdue Digital Forestry:** university drone/aerial tree inventory projects in Indiana.. Source: [Purdue](https://ag.purdue.edu/digital-forestry/projects/tree-inventory-with-aerial-remote-sensing.html)

### Inferences
- The "only one in the region" claim should be reframed as "no public competitor found in a limited web search" and should not be used commercially without a deeper scan (Wisconsin consulting forester directories, Michigan and Minnesota drone service listings, utility vegetation-management LiDAR vendors, and carbon-market inventory startups).
- Tree Stand Forestry is a notable analog for the hunting-habitat customer segment.

### Gaps
- No pricing, accuracy claims or validation protocols found for any commercial provider.
- Did not search Wisconsin consulting forester directories, carbon-credit inventory firms (for example NCX), or utility vegetation-management vendors.

---

## Q6. How were labels collected and matched to crowns, and how many labels were needed?

### Takeaway
The strongest studies used field visits with GNSS to assign species to crowns on an orthomosaic, then hand-drew crown polygons; NEON used field stem points automatically matched to detected crowns with height-consistency filters. Label counts range from hundreds of trees per site (NEON Wisconsin sites, roughly 280 to 640 training trees) to 23,000 crowns (Quebec).

### Cited Findings
- Quebec: field survey in the same summer as imaging, using an Arrow 100 submeter GNSS receiver and a Surface Pro tablet in ArcGIS Pro to place a species point on each crown directly over the orthomosaic in the field; crowns then hand-delineated as polygons in ArcGIS Pro. One polygon layer from the 2 Sept orthomosaic was reused across all dates (orthomosaics georeferenced to generally under 3 cm), without per-date adjustment. Annotation was described as very time-consuming. 23,000 crowns total.. Source: [Cloutier et al. preprint](https://www.biorxiv.org/content/10.1101/2023.08.03.548604.full.pdf)
- NEON: started from 41,036 raw stems (NEON Vegetation Structure, stems over 10 cm diameter, plus ForestGEO and other public data); removed dead/broken stems, unlabeled stems, stems under 3 m, and stems where field height differed from LiDAR height by more than 4 m; overlaid stems on DeepForest crown boxes, keeping the tallest stem when several fell in one box; stems outside boxes got a 1 m fixed box used only in training, never testing. Final 31,736 training points. Minimum 10 train and 10 test per species per site.. Source: [PMC11251727](https://pmc.ncbi.nlm.nih.gov/articles/PMC11251727)
- NEON authors list stem-to-crown mismatches and spectral mixing from neighboring crowns as accuracy limits.. Source: [PMC11251727](https://pmc.ncbi.nlm.nih.gov/articles/PMC11251727)
- UNDE: 157 targeted additional stems raised macro accuracy from 61.6% to 79.1%.. Source: [PMC11251727](https://pmc.ncbi.nlm.nih.gov/articles/PMC11251727)
- Weinstein dataset guidance recommends at least 20 records per species to reduce taxonomic and data cleaning errors.. Source: [Zenodo 10067302](https://zenodo.org/records/10067302)
- Ontario 2017 study validated on only 23 crowns, a sample too small for reliable per-class estimates.. Source: [CCLM portal](https://www.cclmportal.ca/resource/deciduous-tree-species-classification-using-object-based-analysis-and-machine-learning)
- Slavík et al. used 1,045 trees for conifer/broadleaf.. Source: [Forests 14:1581](https://www.mdpi.com/1999-4907/14/8/1581)

### Inferences
- For a single Wisconsin property, field-in-the-loop labeling on a tablet over a fresh RTK orthomosaic (the Quebec approach) is the most transferable method and avoids the stem-to-crown matching errors NEON reports. Budget at least tens of verified crowns per class for testing alone, more for training, and plan targeted top-up labels for weak classes.
- The Quebec random-tile split shows why TreeDar's spatially independent partitions matter: published numbers from random splits likely overstate accuracy on a new property.

### Gaps
- No study found that reports a learning curve (accuracy vs number of labels per species) for drone RGB in northern hardwoods.
- Labeling methods for Grybas and Congalton 2021 and the Forestry 2026 LiDAR study were not retrieved.
