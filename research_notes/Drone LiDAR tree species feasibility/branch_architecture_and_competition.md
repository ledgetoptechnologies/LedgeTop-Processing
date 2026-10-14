# Drone LiDAR: woody structure (stems, forks, branches), architecture-based species classification, and competition effects

Research date: 2026-10-13. Public web research only, about 18 search/fetch calls. Most figures below come from abstracts, repository pages, or search-result excerpts, not full-text reads. Anything recalled from prior knowledge but not confirmed in this session is listed under Gaps and marked UNVERIFIED.

## Q1. At what point densities and sensors has ULS reconstructed stems, DBH, forks, and branches, and how does it compare with TLS for QSMs (leaf-off vs leaf-on)?

### Takeaway
Survey-grade RIEGL ULS (VUX-1UAV) has modelled trunks and large branches (diameter at least 30 cm) of mature broadleaf trees about as well as TLS, but agreement falls sharply once smaller branches, dense stands, conifers, or small trees are included. DBH from above-canopy ULS works for large stems in leaf-off broadleaf stands. It degrades for stems under about 20 cm and wherever the lower stem is occluded. Fine branch architecture (forks above the first order, branch angles of small limbs) remains a TLS or under-canopy capability, not a demonstrated above-canopy ULS capability.

### Cited Findings
- Brede et al. (2019, Remote Sensing of Environment 233), "Non-destructive tree volume estimation through quantitative structure modelling: comparing UAV laser scanning with terrestrial LIDAR": 200 trees (DBH 6 to 91 cm) in 5 coniferous and deciduous stands were scanned and modelled with TreeQSM. ULS reliably modelled volume of trunks and branches with diameter of at least 30 cm in the mature beech and oak stand (CCC 0.85, RMSE 1.12 m3). Including smaller branches caused considerable overestimation (CCC 0.51, RMSE 6.59 m3). Dense Norway spruce prevented trunk sensing (CCC 0.36). Small young trees prevented proper trunk circumference depiction (CCC 0.01).. Source: [Tampere repository record](https://trepo.tuni.fi/handle/10024/215373)
- Brede et al. (2017, Sensors), RIEGL RiCOPTER with VUX-1UAV vs RIEGL VZ-400 TLS: of 58 trees whose trunk segments were extracted by both systems, 39 could be used to model DBH; ULS DBH vs TLS DBH correlation 0.98, RMSE 4.24 cm. ULS canopy height models were on average 11.5 cm higher than TLS, indicating TLS misses some treetops.. Source: [PMC5677400](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5677400/)
- Wieser et al. (2017, Remote Sensing 9(11):1154): RIEGL VUX-1 on RiCOPTER, leaf-off (26 Feb 2015), stands 10 to 35 m tall with DBH up to 1 m. Stems with DBH > 20 cm had almost 100% successful reconstruction, with relative DBH differences of 9% (DBH 20 to 30 cm) down to 1.8% (DBH > 40 cm). Trunk sections were selected manually, so this is not an automated stem detection result.. Source: [MDPI](https://www.mdpi.com/2072-4292/9/11/1154); manual-selection caveat per [a later MDPI paper](https://www.mdpi.com/2072-4292/12/8/1236/xml)
- Follow-on work (EGU 2020 abstract) reported automatic detection of 91.0% of stems with a RIEGL miniVUX-1DL and 77.6% with a VUX-1UAV in the study area.. Source: [EGU2020-7378](https://meetingorganizer.copernicus.org/EGU2020/EGU2020-7378.html)
- Bruggisser et al. (2020): DBH median absolute deviations to reference increased (0.1 to 1.5 cm for VUX-1; 0.2 to 2.2 cm for miniVUX-1DL) when pulse repetition frequency was artificially halved, i.e., stem accuracy depends on stem point coverage.. Source: [Heidelberg repository PDF](https://archiv.ub.uni-heidelberg.de/volltextserver/37194/1/bruggisser_et_al_2020_forestdynamics.pdf)
- Leaf-off ULS stem detection with HDBSCAN plus PCA (Remote Sensing 13(3):352): up to 82% of stems detected at 98% precision in leaf-off season, but point density at about 1.3 m height stays low in closed-canopy stands even leaf-off, limiting DBH accuracy.. Source: [Crossref record](https://api.crossref.org/works/10.3390%2FRS13030352)
- Hardwood forest ULS vs ALS/TLS (INRAE): best individual-tree results from ULS were under leaf-off conditions with a point-cloud bottom-up individual tree detection method, improving detection from 50% to 71% relative to a raster method, versus TLS reference.. Source: [HAL hal-03346149](https://hal.inrae.fr/hal-03346149)
- Finnish under-canopy ULS (Kaarta Stencil-1, manually flown below canopy, boreal): 93% stems detected in a sparse plot and 84% in an obstructed plot; DBH RMSE 0.60 cm (2.2%) and 0.92 cm (3.1%). Above-canopy ULS from the same group gave DBH RMSE of 10 to 20% via random forest prediction, and authors judged above-canopy ULS not yet accurate enough for individual-tree stem attributes.. Source: [Under-canopy UAV laser scanning, UTU](https://www.utupub.fi/handle/11111/35379); [UTU publication record](https://research.utu.fi/converis/portal/detail/Publication/47290217)
- Radiata pine ULS (Forests 2025): DBH estimated by PLS had lowest RMSE 1.624 cm (R2 0.756) at 12,200 points/m2.. Source: [Forests 15/00899 or related, MDPI PDF](https://mdpi-res.com/d_attachment/forests/forests-15-00899/article_deploy/forests-15-00899.pdf) (note: the search snippet attributing this figure was not checked against full text)
- Eisenschink et al. (Ecological Informatics 2025): flight parameters had a large effect on DBH accuracy; recommends low flight height above trees, relatively high speed, and high strip overlap.. Source: [LMU repository](https://epub.ub.uni-muenchen.de/128275/)
- Brede et al. (2022, RSE 280:113180) compared TLS-QSM AGB with ULS at four sites in temperate, wet tropical, and savanna biomes (at least 170 trees per site), segmenting ULS trees with a shortest-path algorithm. Abstract notes ULS estimation has been difficult in dense tropical canopies.. Source: [WUR eDepot](https://edepot.wur.nl/576459); [HAL](https://hal.inrae.fr/hal-03764393v1)
- Branch reconstruction from ULS (Cárdenas et al., Frontiers 2022): main obstacle is partial or complete occlusion of branch segments, producing disconnected components in graph-based skeletons; method uses radius search plus minimum spanning tree. No species classification.. Source: [Frontiers in Environmental Science](https://www.frontiersin.org/journals/environmental-science/articles/10.3389/fenvs.2022.960083/pdf)

### Inferences
- For the Wisconsin use case, above-canopy ULS can plausibly give a main-stem position, a coarse trunk/major-limb skeleton, and DBH for larger, leaf-off hardwoods. First fork height and major scaffold limbs of large open-grown hardwoods are the most realistic "architecture" targets. Small-branch angles and higher-order branching should not be promised.
- Conifer-dominated or dense stands (hemlock, spruce, fir, cedar, dense young aspen) will behave like the Brede 2019 Norway spruce case: trunks largely invisible from above, even leaf-off.
- Point density on stems, not nominal ground density, is the operative variable. Halving pulse rate measurably worsened DBH accuracy with RIEGL sensors.

### Gaps
- UNVERIFIED (from prior knowledge, not confirmed this session): Kükenbrink et al. (2022, likely International Journal of Applied Earth Observation and Geoinformation or RSE) compared ULS sensors including RIEGL miniVUX and DJI Zenmuse L1 against TLS for tree structure. Two searches failed to locate it. Exact title, sensors, and numbers must be checked before citing.
- UNVERIFIED: Terryn et al. (2022, RSE) TLS-ULS fusion/comparison for tropical QSMs. Search only surfaced Brede 2022 with Terryn as co-author; specific Terryn findings not confirmed.
- No full-text check of Brede 2019 to confirm leaf condition at each site (the beech/oak result is widely described as leaf-off, but not confirmed here).
- No study found that quantitatively validates ULS-derived branch angles, fork heights, or branch order counts against TLS or field data for temperate hardwoods.
- Minimum resolvable branch diameter for ULS is not stated as a single number in any source found; the Brede 2019 30 cm threshold is the best quantitative anchor.

## Q2. Which consumer or survey-grade drone LiDAR (Livox-based DJI L1/L2/L3, Hesai, RIEGL miniVUX/VUX) has been used for under-canopy stem detection, and how well?

### Takeaway
Nearly all positive stem/QSM evidence uses RIEGL VUX-1 or miniVUX-1 sensors. I found no peer-reviewed study in this session reporting DBH or stem detection accuracy for the DJI Zenmuse L1, L2, or L3 in forests, and no Hesai-based ULS forestry study. The only peer-reviewed L1 accuracy work found concerns geometric accuracy on targets, not forest stems.

### Cited Findings
- RIEGL VUX-1UAV and miniVUX-1DL: stem detection 77.6% and 91.0% respectively in one study area; DBH deviations as in Q1.. Source: [EGU2020-7378](https://meetingorganizer.copernicus.org/EGU2020/EGU2020-7378.html); [Bruggisser et al. 2020](https://archiv.ub.uni-heidelberg.de/volltextserver/37194/1/bruggisser_et_al_2020_forestdynamics.pdf)
- Bretten ULS/TLS dataset: RIEGL miniVUX-1UAV on DJI Matrice 600 Pro, 2019 to 2020 multi-campaign, with RIEGL VZ-400 TLS reference aligned by ICP.. Source: [Kükenbrink-related search result, Bruggisser et al. 2020](https://archiv.ub.uni-heidelberg.de/volltextserver/37194/1/bruggisser_et_al_2020_forestdynamics.pdf) (dataset description surfaced via search snippet; source attribution to be confirmed)
- Štroner, Urban, Línková (2021, Remote Sensing 13(23):4811) evaluated DJI Zenmuse L1: after removing the global georeferencing shift, accuracy about 3.5 cm in all directions, better than manufacturer's 10 cm horizontal / 5 cm vertical; RGB colorization showed about 0.2 m systematic shift. Test was on targets and an SfM reference, not forest stems.. Source: [MDPI](https://www.mdpi.com/2072-4292/13/23/4811)
- Vendor claim (not peer-reviewed): L1 data can be "fuzzy" by 10 to 50 cm, attributed mainly to IMU. Treat as unverified marketing material.. Source: [ROCK Robotic blog](https://www.rockrobotic.com/post/improving-accuracy-dji-zenmuse-l1-rock-cloud)
- Livox MID-360 low-cost wearable MLS prototype (Kükenbrink co-author, Frontiers in Remote Sensing 2026) compared against two commercial handheld MLS systems for urban trees.. Source: [Frontiers](https://www.frontiersin.org/journals/remote-sensing/articles/10.3389/frsen.2026.1774149/full)
- A low-cost mobile scanner achieved tree detection up to 78% and DBH rRMSE 19.7%.. Source: [Remote Sensing 17/00229 PDF](https://mdpi-res.com/d_attachment/remotesensing/remotesensing-17-00229/article_deploy/remotesensing-17-00229.pdf) (device identity not confirmed in snippet)
- Under-canopy flight with a mobile-mapping-class scanner (Kaarta Stencil-1) achieved centimetre DBH where above-canopy ULS did not.. Source: [UTU](https://www.utupub.fi/handle/11111/35379)

### Inferences
- L1/L2-class Livox sensors have lower range precision and fewer returns per pulse than RIEGL survey sensors (spec-level claim, not verified here). Combined with the 3.5 cm geometric accuracy floor, centimetre-level DBH on 20 to 40 cm Wisconsin hardwood stems from above-canopy L1/L2 data is unproven and should be treated as a hypothesis requiring a local TLS or tape-measured validation.
- If stem architecture matters commercially, the realistic options are (a) a RIEGL-class sensor (paid hardware or rental, requires Beau approval), (b) under-canopy or handheld MLS on sample plots, or (c) restricting claims to crown-level structure.

### Gaps
- No peer-reviewed DJI L1, L2, or L3 forest DBH/stem detection result found. Searches for "Zenmuse L1 DBH", "Livox noise forest" returned nothing directly applicable.
- No Hesai-based ULS forestry study found.
- No quantitative Livox range-noise figure on vegetation or stems from a primary source. Štroner 2021 full text may contain noise/roughness values; not read.
- DJI L2/L3 specification claims (returns, range precision) not verified in this session.

## Q3. Has leaf-off ULS (or point-cloud deep learning) been used to classify deciduous species, and do branching/architecture features help?

### Takeaway
Point-cloud species classification works at 70 to 95% overall accuracy in many studies, but almost none isolate branching architecture as the driver, and none found used ULS-derived branch skeletons as features. Leaf-off data clearly helps conifer vs deciduous separation. For species within deciduous groups, the best UAV evidence (a 2026 abstract) found intensity features more useful than structure, and combining leaf-on and leaf-off gave the best result (80% OA).

### Cited Findings
- FOR-species20K benchmark (Puliti et al., Methods in Ecology and Evolution 2025): over 20,000 individual-tree point clouds of 33 species from TLS, MLS and ULS, mostly European. Seven deep learning models tested; multi-view 2D image models averaged 0.77 OA vs 0.72 for 3D point models (PointNet++, MinkNet, MLP-Mixer, DGCNN). Top model DetailView. Paper reports accuracy above 0.8 across platforms/sensors (per-platform breakdown not seen).. Source: [arXiv 2408.06507](https://export.arxiv.org/pdf/2408.06507); [Zenodo 13255198](https://zenodo.org/records/13255198); [DOAJ](https://doaj.org/article/9ae6af6106f143678762a30c01730e62)
- Seidel et al. (2021, Frontiers in Plant Science): CNN on 2D images rendered from laser scanning point clouds classified seven species at 86% OA; augmentation added 6 points (ash +13%, oak +14%, pine +24%); faster and more accurate than PointNet.. Source: [PMC7902704](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC7902704/)
- Umar et al. (EGU 2026 abstract, Wageningen): leaf-on and leaf-off ULS in a German mixed temperate forest; 69 features reduced to 24; random forest. Intensity features (mean first/single-return intensity, median intensity) most effective. Leaf-on plus leaf-off 80% OA; single season 75 to 76%. Abstract only.. Source: [EGU26-14442](https://meetingorganizer.copernicus.org/EGU26/EGU26-14442.html)
- PointNet++ on ULS plus pulse width and multispectral features (pine, birch, alder): 90.2% OA vs 85.3% for random forest on handcrafted features.. Source: [ISPRS Annals V-2-2020-203](https://isprs-annals.copernicus.org/articles/V-2-2020/203/2020/isprs-annals-V-2-2020-203-2020.pdf)
- Hamraz et al. (airborne LiDAR, CNN on rasterized projections): about 82% for conifers and 90% for deciduous; leaf-off LiDAR was the primary information source for conifer/deciduous separation.. Source: [arXiv 1802.08872](https://arxiv.org/pdf/1802.08872)
- Bavarian Forest full-waveform airborne: conifer/deciduous OA 85% leaf-on vs 96% leaf-off., cited via [ISPRS laserscanning09 paper](https://www.isprs.org/proceedings/XXXVIII/3-W8/papers/158_laserscanning09.pdf) (search snippet; attribution not checked in full text)
- Brandtberg (cited in a review) proposed that leaf-off point statistics describing branch distribution may be species-dependent. The same literature notes branch and foliage arrangement is extremely plastic within species and even within individuals, making morphology-based identification difficult.. Source: [Remote Sensing 2021 review, DOAJ](https://doaj.org/article/d44cd140ae8d44a9bab89e8eb2eed0fc); [ISPRS laserscanning09](https://www.isprs.org/proceedings/XXXVIII/3-W8/papers/158_laserscanning09.pdf)
- Boreal study using Hallé architectural models: 85.0% accuracy for four species.. Source: [ISPRS Archives XLII-3/2629/2018](https://isprs-archives.copernicus.org/articles/XLII-3/2629/2018/)
- Upper-crown morphology from UAS lidar distinguishing Douglas-fir and western hemlock (McGaughey and colleagues, 2024).. Source: [WA DNR PDF](https://dnr.wa.gov/publications/lm_oesf_lidar_rm.pdf)
- Li et al. high-density lidar, two conifers and two deciduous, structural metrics: 76% OA. Lidar-only 79.86% vs lidar plus hyperspectral 85.12% in another crown study. Manual crown segments kappa 0.86 vs lidar segments 0.74 (Dalponte et al.).. Source: [Frontiers in Plant Science 2018](https://www.frontiersin.org/journals/plant-science/articles/10.3389/fpls.2018.00220/pdf)
- Transformer-based UAV LiDAR individual tree species classification exists (Forests 14(3):484) and a 2025 Frontiers paper compared two ML and two DL methods for poplar and pine. Data conditions not confirmed.. Source: [Forests 14/484](https://0-www-mdpi-com.brum.beds.ac.uk/1999-4907/14/3/484); [Frontiers 2025](https://www.frontiersin.org/journals/forests-and-global-change/articles/10.3389/ffgc.2025.1431603/pdf)
- LayerNet (cited in a GRSL paper) classified birch and larch from UAV point clouds whose density represented stems and branches. Original not checked.. Source: [GRSL preprint](https://rslab.disi.unitn.it/web/wp-content/uploads/2022/06/GRSLLiDARDL.pdf)

### Inferences
- For the 13 Wisconsin target species, structure alone is most likely to separate conifers from hardwoods and some distinct crown forms (white pine supercanopy, paper birch/aspen). Separating sugar maple vs red maple or red oak vs white oak from ULS structure is not supported by any evidence found.
- Leaf-off plus leaf-on multi-date acquisition is the best-supported LiDAR design, but it doubles flight cost and introduces co-registration and repeat-date partitioning requirements (repeat dates must stay in the same evaluation partition per CLAUDE.md).
- The 2D-projection approach outperforming raw-point deep learning (FOR-species20K, Seidel 2021) suggests TreeDar should benchmark a multi-view baseline before investing in PointNet++/DGCNN.
- FOR-species20K is the obvious public pretraining and benchmark source, but it is European-dominated; transfer to North American hardwoods is untested and its license must be checked separately.

### Gaps
- No study found that adds explicit QSM or skeleton-derived branching features (branch angle, fork height, bifurcation ratio) to ULS species classification and measures the gain.
- No North American northern-hardwood ULS species classification study found in this session.
- Per-platform (ULS vs TLS) accuracy breakdown for FOR-species20K not confirmed.
- FOR-species20K dataset license not checked.

## Q4. How does competition change crown shape and branching, and does species classification accuracy differ for open-grown vs crowded trees?

### Takeaway
TLS studies consistently show that neighbourhood competition and mixture change crown size, asymmetry, vertical distribution of crown material, and branch angles within a species, including sugar maple. This plasticity is a direct confound for architecture-based species classification. Only one study found tested a competition-type covariate in ULS classification, and it used a point-clustering index on a two-class problem, giving a small gain (about 1 to 2 points).

### Cited Findings
- Bayer et al. (2013, Trees), TLS plus skeletonization: European beech in mixture had flatter branch angles, more distinct ramification, larger crown volumes, and a lower share of single-branch space occupation than in pure stands. Norway spruce in mixture had longer branches and larger crowns.. Source: [search summary of Bayer et al., Göttingen record](https://dspacecris7.sub.uni-goettingen.de/items/6f61c9e1-5566-4dc3-b7d1-67ee37efa6ee) (attribution of this record to Bayer 2013 not confirmed)
- Barbeito et al. (2017): beech in beech/Scots pine mixtures had larger crowns regardless of site and a higher proportion of crown volume in the lower canopy than in pure stands.. Source: [TUM PDF](https://waldwachstum.wzw.tum.de/fileadmin/publications/Barbeito_2017.pdf)
- Martin-Ducup et al. (SilviLaser 2015): 36 co-dominant sugar maples scanned with a Faro Focus 3D in eastern Canada (half pure, half mixed stands), voxelized to reduce occlusion bias, to quantify crown plasticity by stand composition.. Source: [UQAR record](https://semaphore.uqar.ca/id/eprint/1569/)
- Martin-Ducup, Schneider, Fournier (2018, Forests 9:673): sugar maple foliage distribution shifted toward the crown base in mixed stands compared with pure stands; sugar maple benefits from mixture, balsam fir does not.. Source: [UQAR record](https://semaphore.uqar.ca/id/eprint/1574/); [MDPI PDF](https://mdpi-res.com/d_attachment/forests/forests-09-00673/article_deploy/forests-09-00673.pdf)
- TLS study of 15 broadleaf trees (beech, ash, hornbeam): competitive pressure model predicted direction of crown asymmetry to within 36 degrees on average; trees avoid competition through crown plasticity.. Source: [search summary; Pitkänen et al. 2021 TU Wien PDF discusses crown plasticity](https://repositum.tuwien.at/bitstream/20.500.12708/19072/3/Pitkaenen-2021-Quantifying%20Tree%20Crown%20Plasticity%20with%20TLS%20Data%20for%20Improv...-vor.pdf) (original 2011 paper not directly retrieved)
- Pine/oak mixed forests (Seidel co-author, 2021): pines had higher maximum crown width height and crown base height than oaks but lower crown volume and projection area, interpreted as complementarity.. Source: [DOAJ, Remote Sensing Dec 2021](https://doaj.org/article/3048e775e7ec4c4caf7166674c96d0b5)
- A 2021 Remote Sensing paper states knowledge of crown morphology changes from density, competition, and species mixture is still limited.. Source: [DOAJ](https://doaj.org/article/3048e775e7ec4c4caf7166674c96d0b5)
- Slavík, Kučelka, Modlinger, Surová (2023, Forests 14:1581): 1,045 ULS trees, conifer vs broadleaf, GLM and random forest. Adding the Clark-Evans aggregation index of the point cloud raised OA from 92.6% to 94.8% (GLM) and 93.8% to 95.1% (RF). Note: CE here measures point clustering within the tree cloud, not a neighbour competition index.. Source: [MDPI PDF](https://mdpi-res.com/d_attachment/forests/forests-14-01581/article_deploy/forests-14-01581.pdf)
- ForestView ITD in mixed conifer forest: recall 0.67 / precision 0.8 at lower canopy cover vs recall 0.36 / precision 0.67 at higher cover, mainly from omission of suppressed trees; well-represented species had producer/user accuracies above about 60%.. Source: [Canadian Journal of Remote Sensing 2018, DOAJ](https://doaj.org/article/546ce589aca748a1805c841d80d51116) (attribution from search snippet)
- Romanian ULS vs MLS: MLS gave near-inventory DBH (R2 up to 0.98) and reliable heights for intermediate and suppressed trees; ULS consistently underestimated height in dense multi-layered stands.. Source: [Forests 16(9):1481](https://www.mdpi.com/1999-4907/16/9/1481) (attribution from snippet)
- Multispectral lidar classification: removing height-correlated features lowered accuracy; stratifying by height gave only minor improvement.. Source: [Remote Sensing 15(2):406](https://www.mdpi.com/2072-4292/15/2/406/htm) (attribution from snippet)

### Inferences
- An open-grown sugar maple and a suppressed sugar maple can differ more in crown ratio, branch angle, and crown asymmetry than a sugar maple and a red maple of the same social position. Any architectural classifier trained mainly on one social class should be expected to degrade on the other.
- Practical controls for TreeDar evaluation: record social/crown class (dominant, codominant, intermediate, suppressed, open-grown) and a neighbour-based competition index (e.g., distance-dependent Hegyi-type or relative height to local canopy) for every field-labelled tree; stratify the test set and report accuracy and coverage per stratum; consider these as covariates or as a gate for "insufficient evidence" outputs.
- Suppressed and intermediate trees are first lost at detection/segmentation, before classification. Per CLAUDE.md, segmentation and classification should be scored separately so this is visible.

### Gaps
- No study found that directly reports species classification accuracy separately for open-grown vs closed-canopy trees, or that uses a true neighbour competition index as a classification covariate.
- Branch angle results for sugar maple pure vs mixed stands (Martin-Ducup 2015) were not retrieved; PDF fetch failed.
- No northern-hardwood (red oak, basswood, aspen, birch) TLS crown plasticity study found in this session.

## Q5. Practical limits: lower-stem occlusion, minimum branch diameter, beam footprint, Livox noise

### Takeaway
The binding constraints are occlusion of lower stems in dense or coniferous stands, low point density near 1.3 m even leaf-off, and sensor ranging precision. Quantitative anchors: ULS QSMs reliable only for components of at least about 30 cm diameter in a mature broadleaf stand; DBH reconstruction near-complete only for stems over 20 cm; L1 geometric accuracy about 3.5 cm after correction.

### Cited Findings
- Branches under 30 cm diameter caused volume overestimation in ULS QSMs; dense spruce and young trees broke trunk modelling.. Source: [Tampere record of Brede et al. 2019](https://trepo.tuni.fi/handle/10024/215373)
- DBH success near 100% only above 20 cm; 9% relative error at 20 to 30 cm.. Source: [Wieser et al. 2017](https://www.mdpi.com/2072-4292/9/11/1154)
- Point density at about 1.3 m remains low in closed canopy even leaf-off.. Source: [Remote Sensing 13(3):352](https://api.crossref.org/works/10.3390%2FRS13030352)
- Above-canopy ULS stem detection completeness significantly lower than under-canopy, especially in difficult forest. Many severely occluded spruces could not be detected even under canopy.. Source: [UTU](https://www.utupub.fi/handle/11111/35379); [UTU record](https://research.utu.fi/converis/portal/detail/Publication/51125812)
- Occlusion yields disconnected branch components in ULS skeletonization.. Source: [Cárdenas et al. 2022](https://www.frontiersin.org/journals/environmental-science/articles/10.3389/fenvs.2022.960083/pdf)
- DJI L1 accuracy 3.5 cm after removing georeferencing shift; colour shift about 0.2 m.. Source: [Štroner et al. 2021](https://www.mdpi.com/2072-4292/13/23/4811)
- Halving pulse rate increased DBH error for both VUX-1 and miniVUX-1DL.. Source: [Bruggisser et al. 2020](https://archiv.ub.uni-heidelberg.de/volltextserver/37194/1/bruggisser_et_al_2020_forestdynamics.pdf)

### Inferences
- With a 3.5 cm accuracy floor and additional range noise on vegetation, a Livox-based sensor is unlikely to resolve branches smaller than roughly 10 cm as cylinders; this is an inference, not a measured threshold.
- The 0.2 m L1 RGB colorization shift matters for LiDAR-plus-RGB fusion: per-tree RGB attribution near crown edges could be misassigned unless co-registration is verified.

### Gaps
- No primary source found giving beam divergence or footprint effects on stem/branch diameter bias for Livox or RIEGL ULS.
- No primary measurement of Livox L1/L2 range noise on tree stems or branches.
- UNVERIFIED (prior knowledge, not checked): Livox non-repetitive scanning gives uneven point distribution across a flight line, which may affect stem sampling; one search result suggested this as a search term but provided no evidence.
