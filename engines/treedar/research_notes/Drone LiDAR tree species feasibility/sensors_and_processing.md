# Drone LiDAR sensors and processing: ROCK R3 Pro, DJI Zenmuse L3, DJI Zenmuse L2

Status: researched 2026-10-13. Primary sources are manufacturer pages (rockrobotic.com, learn.rockrobotic.com, enterprise.dji.com, hesaitech.com). Everything else is marked as secondary, vendor marketing, reseller, or anecdotal. Every density and storage figure computed here is an estimate, not a measurement. Nothing was purchased or signed up for.

## 1. Published sensor specifications

### Takeaway
The R3 Pro V2 uses a Hesai XT32. It is a 905 nm spinning 32-channel unit with dual returns, 640k pts/s single return, 120 m range at 60% reflectivity, and 5 mm precision. It is a short-range, low-return-count sensor next to the DJI L3, which uses a 1535 nm laser with up to 16 returns, 0.25 mrad divergence and much longer range. The L3 is listed for the Matrice 400 only, not the M350. The L2 (905 nm, 5 returns, 240 kHz) sits between them and fits the M350.

### Cited Findings

ROCK R3 Pro V2 (primary: ROCK pages)
- LiDAR engine: Hesai XT32, 32 laser channels, 1.28M pts/s, 5 mm range precision, 2-3 cm post-processed accuracy, 360 degree FOV, 26 MP RGB camera, 1.26 kg with camera. Pricing is quote only ("request a quote"). Source: [ROCK R3 Pro page](https://www.rockrobotic.com/r3-pro)
- Datasheet: 905 nm, Class I, dual return, 1,280,000 pts/s in dual return mode, range 120 m at 60% reflectivity, precision 5 mm, system accuracy 0.1 ft (3 cm). IMU: pitch/roll 0.006 deg and heading 0.03 deg (PPK). The IMU model is not named. GNSS: BDS, GPS, GLONASS, Galileo. Camera: 26 MP, 23.5 x 15.7 mm sensor, 6252 x 4168 px, 73 deg FOV. Storage: 256 GB USB disk. 25 W, 10-30 V DC, DJI Skyport mount, M300/M350 attachment guides. The datasheet does not list the LiDAR horizontal/vertical FOV used in aerial mode. Source: [ROCK R3 Pro V2 datasheet, learn.rockrobotic.com](https://learn.rockrobotic.com/r3-pro-v2-specifications-download-datasheet)
- ROCK's comparison page lists XT32 at 905 nm, 2 echoes, 1.28M pts/s. It also shows a 320 m at 80% range row that appears to describe the sensor class rather than this datasheet figure (secondary summary of ROCK page, not independently re-read). Source: [ROCK compare page](https://www.rockrobotic.com/compare-products/)
- Hesai XT32 (sensor maker): range 0.05-120 m, range precision 0.5 cm (1 sigma), accuracy +/-1 cm, horizontal FOV 360 deg, vertical FOV 31 deg, horizontal resolution 0.18 deg, vertical resolution 1 deg, 640,000 pts/s single return. Wavelength and beam divergence are not on the product page. Source: [Hesai XT32 product page](https://www.hesaitech.com/product/xt32/)
- ROCK has also announced a "ROCK Ultra" with 1000 m range (homepage headline only, not researched). Source: [rockrobotic.com](https://rockrobotic.com/)

DJI Zenmuse L3 (primary: DJI spec page)
- Wavelength 1535 nm. Beam divergence 0.25 mrad (1/e^2). Spot 41 mm at 120 m and 86 mm at 300 m. Source: [DJI L3 specs](https://enterprise.dji.com/zenmuse-l3/specs)
- Range: 700 m at 10% reflectivity (350 kHz), 950 m at 10% (100 kHz), 2000 m at 80% (100 kHz). Source: [DJI L3 specs](https://enterprise.dji.com/zenmuse-l3/specs)
- Returns: 4/8/16 at 100 and 350 kHz, 4/8 at 1000 kHz, 4 at 2000 kHz. Pulse frequency 100 kHz (recommended below 500 m), 350 kHz (below 300 m), 1000 kHz (below 100 m), 2000 kHz (below 50 m). Source: [DJI L3 specs](https://enterprise.dji.com/zenmuse-l3/specs)
- Ranging: absolute accuracy +/-10 mm, repeatability below 5 mm (1 sigma). System accuracy at 120 m is 3 cm vertical and 4 cm horizontal RMSE; at 300 m it is 5 cm V and 7.5 cm H. Source: [DJI L3 specs](https://enterprise.dji.com/zenmuse-l3/specs)
- FOV: linear scan 80 x 3 deg; star-shaped and non-repetitive 80 x 80 deg. POS: 200 Hz, GNSS 5 Hz, post-processed attitude error 0.02 deg yaw, 0.01 deg pitch/roll (1 sigma). The IMU model is not given. Source: [DJI L3 specs](https://enterprise.dji.com/zenmuse-l3/specs)
- RGB: 100 MP (12288 x 8192) or 25 MP (6144 x 4096). Source: [DJI L3 specs](https://enterprise.dji.com/zenmuse-l3/specs)
- Supported aircraft: "DJI Matrice 400 (requires the Zenmuse L3 single gimbal connector)". The M350 RTK is not listed. Source: [DJI L3 specs](https://enterprise.dji.com/zenmuse-l3/specs)

DJI Zenmuse L2 (primary: DJI spec page)
- 905 nm. Divergence 0.2 mrad H x 0.6 mrad V (FWHM). Spot 4 x 12 cm at 100 m. Range 450 m at 50% (0 klx) and 250 m at 10% (100 klx), with a 3 m minimum range. Source: [DJI L2 specs](https://enterprise.dji.com/zenmuse-l2/specs)
- Up to 5 returns. 240,000 pts/s single return, max 1,200,000 pts/s multi-return. Ranging accuracy 2 cm at 150 m. System accuracy 5 cm H and 4 cm V at 150 m. Source: [DJI L2 specs](https://enterprise.dji.com/zenmuse-l2/specs)
- FOV: repetitive 70 x 3 deg; non-repetitive 70 x 75 deg. IMU 200 Hz. RGB 4/3 CMOS, 20 MP, 84 deg FOV. Aircraft: M400, M350 RTK, M300 RTK (M300 needs DJI RC Plus). Source: [DJI L2 specs](https://enterprise.dji.com/zenmuse-l2/specs)

### Inferences
- The XT32 is a 360 degree spinning automotive/robotics sensor. On a drone, most of each revolution points sideways or upward, so only a fraction of the 640k pts/s reaches the ground swath. The usable aerial swath angle is not published in the ROCK datasheet, so per-sensor density comparisons with the L2/L3 headline rates are not like-for-like.
- 120 m range at 60% reflectivity is a bright-target figure. Forest canopy and dark ground are often well below 60% at 905 nm, and the slant range grows off nadir. Flying the R3 Pro much above about 60-80 m AGL over forest is likely to lose returns at swath edges and from the ground. ROCK's own marketing references "60m AGL". This is an inference to verify in a test flight.
- Dual return (R3 Pro) against 5 (L2) and up to 16 (L3) returns per pulse is the most forestry-relevant spec difference for understory and ground capture under canopy.
- The L3 cannot be assumed to fit the borrowed M350. The upgrade path implies an M400.

### Gaps
- The XT32 dual-return mode combinations (for example last+strongest or first+last) as configured by ROCK were not confirmed. The Hesai product page lists single-return rate only.
- XT32 beam divergence and spot size: not found on primary pages.
- R3 Pro IMU make/model and GNSS receiver model: not published on the pages read.
- Whether the borrowed unit is V1 or V2 matters, because ROCK says the V2 upgraded the IMU, GNSS and computer. Confirm the serial or model with the owner.
- The "XT32M2X" variant naming was not confirmed in any source.

## 2. Point density versus altitude and speed, and storage per acre

### Takeaway
DJI and ROCK do not publish density tables on the pages reviewed. DJI Pilot 2 shows an estimated density when you plan a mission (not verified here). Using a standard swath formula, the L2 at 80 m and 5-10 m/s with 50% side overlap gives roughly 430-860 pulses/m2 (estimate). The L3 at 350 kHz gives roughly 520-1,040 and the R3 Pro roughly 150-400 (estimate, with an assumed usable FOV). The only field report found (Interpine, 120 m) measured far lower pulse densities than the formula predicts. Treat every value here as an upper bound until calibrated against a real flight.

### Cited Findings
- Interpine (L3 reseller/operator, Feb 2026) flew a young *Pinus radiata* stand at 120 m AGL, 50% overlap, terrain follow, repetitive/linear scan, L2 at 240 kHz and L3 at 350 kHz. Measured results: L2 106 pulses/m2, 112 returns/m2. L3 46 pulses/m2, 278 returns/m2. Speed was not stated. Source: [Interpine L2 vs L3](https://interpine.nz/analysis-of-lidar-dji-l2-vs-l3-lidar-sensors/)
- DJI's L2 accuracy and coverage benchmark used 150 m, 15 m/s, 20% side overlap, repetitive scan and RTK FIX. This is a mapping-efficiency setting, not a forestry one (secondary summary of DJI FAQ/press). Source: [DJI L2 FAQ](https://enterprise.dji.com/zenmuse-l2/faq)
- Heliguy's L2 vegetation trial used 100 m, 8 m/s, 50% overlap, penta return, 240 kHz (vendor blog). Source: [Heliguy L2 vs L1](https://us.heliguy.com/blogs/posts/dji-zenmuse-l2-vs-dji-zenmuse-l1-datasets-and-accuracy)
- An agricultural (non-forest) UAV LiDAR survey at 80 m reported 196 pts/m2 average (secondary search summary; sensor not checked). Source: [Land Reclamation Journal 2023](https://www.landreclamationjournal.usamv.ro/pdf/2023/Art13.pdf)

### Estimates (computed here, not sourced)

Formula: swath W = 2 x H x tan(FOV/2). Single-pass pulse density = PRR / (v x W). With side overlap o, average density = PRR / (v x W x (1 - o)). Assumptions: o = 50% (each point covered about twice), flat terrain, every pulse returns, no crosshatch, no edge cropping, first/single return only. Multi-returns in vegetation add points on top of this. Crosshatch roughly doubles it again.

| Sensor and mode (assumptions) | AGL | Swath | 5 m/s | 10 m/s |
|---|---|---|---|---|
| L2 repetitive, 240 kHz, 70 deg | 60 m | 84 m | ~1,140 | ~570 |
| | 80 m | 112 m | ~860 | ~430 |
| | 100 m | 140 m | ~690 | ~340 |
| L3 linear, 350 kHz, 80 deg | 60 m | 101 m | ~1,390 | ~700 |
| | 80 m | 134 m | ~1,040 | ~520 |
| | 100 m | 168 m | ~830 | ~420 |
| L3 linear, 1000 kHz, 80 deg (DJI: use below 100 m) | 60 m | 101 m | ~3,970 | ~1,990 |
| | 80 m | 134 m | ~2,980 | ~1,490 |
| R3 Pro XT32, 640k pts/s, ASSUMED 90 deg usable of 360 | 60 m | 120 m | ~530 | ~270 |
| | 80 m | 160 m | ~400 | ~200 |
| | 100 m | 200 m | ~320 | ~160 |
| R3 Pro XT32, ASSUMED 120 deg usable | 80 m | 277 m | ~310 | ~150 |

Units: pulses/m2 (estimate).

Storage per acre (1 acre = 4,046.86 m2), estimate:
- Points per acre = density x 4,046.86. At 500 pts/m2 that is about 2.0 M points/acre. At 1,000 pts/m2 it is about 4.0 M. At 3,000 pts/m2 it is about 12 M.
- Uncompressed LAS at 36 bytes/point (point format 7, with RGB and GPS time; record size from the LAS 1.4 format, not re-fetched here) is roughly 73 MB/acre at 500 pts/m2, 146 MB at 1,000 and 435 MB at 3,000.
- LAZ assumed at 3-5 bytes/point (a typical LASzip ratio of about 7-12x; unverified, not sourced) is roughly 6-10 MB/acre at 500 pts/m2, 12-20 MB at 1,000 and 36-60 MB at 3,000.
- With multiple returns in forest, the point count can be 1.5-3x the pulse count. Raw sensor files, RGB images (the L3 100 MP option especially) and processing intermediates will be several times larger than the final LAZ. Raw volumes were not found in primary sources.

### Inferences
- The Interpine L3 figure (46 pulses/m2 at 120 m and 350 kHz) is roughly 4-7x below the formula, and the L2 figure is roughly 1.5-3x below. Possible causes are fast speed, edge cropping, different definitions of "pulse", or reporting only gridded last returns. Formula values should be treated as optimistic, and the operator should calibrate with one test flight plus `lasinfo`/PDAL density stats.
- For individual-crown segmentation, all three sensors can plausibly exceed 100-200 pts/m2 at 60-80 m and 5-8 m/s. Ground and understory capture under deciduous canopy depends more on returns per pulse, footprint and leaf-off timing than on headline density.

### Gaps
- No vendor-published density calculator or table was found for ROCK, the L2 or the L3. DJI Pilot 2's in-app estimate was not verified.
- The R3 Pro's usable aerial FOV (the angular crop ROCK Desktop applies) is unknown. The R3 Pro rows above rest on an assumed value.
- LAZ bytes/point for these specific datasets was not measured.

## 3. Processing path, software dependencies and costs

### Takeaway
Both ecosystems lock raw-to-point-cloud conversion (trajectory plus boresight) into vendor software. ROCK Desktop is included for year 1 and then needs a paid annual renewal with no published price. One user reports about $2,000. DJI Terra lists L2 point cloud processing as free. Paid Standard tiers are $1,850/yr or $5,280 permanent per a reseller blog citing the DJI Store. LAS output is unrestricted on both platforms once processed. Third-party raw processing of DJI data exists (LP360 DJI add-on, cloud-based) but is not free or open source. No open-source raw .LDR path was found.

### Cited Findings

ROCK
- The R3 Pro package includes ROCK Cloud Business Pro (priority processing, 1 TB storage), a ROCK Desktop Pro license (aerial, mobile and SLAM processing) and Pro Calibration (boresight with certification). Price is by quote. Source: [ROCK R3 Pro page](https://www.rockrobotic.com/r3-pro)
- The datasheet lists "Rock Desktop (offline processing)" and Rock Cloud, Rock Photogrammetry, Rock SLAM and Rock Inspect. Source: [ROCK datasheet](https://learn.rockrobotic.com/r3-pro-v2-specifications-download-datasheet)
- ROCK's own article (1 Apr 2026, vendor marketing): Desktop is "included with hardware purchase (1-year license)". After year 1, "continued access to raw data processing requires an annual renewal", priced by contacting ROCK. Cloud is optional and pay-for-use. ROCK states it "imposes no restrictions on what you do with your data" and that standard LAS can go to CloudCompare, LP360, LiDAR360, Global Mapper or ArcGIS Pro. Trajectory export is not mentioned. The same article claims DJI Terra costs of $440/yr maintenance, $999+/yr subscription and $2,040/yr with Modify. These conflict with the reseller figures below and are a competitor's claims. Source: [ROCK software costs article](https://www.rockrobotic.com/articles/rock-software-costs-truth-vs-dji-2026/)
- Anecdotal (ROCK community forum): a user quoted a Desktop renewal of about $2,000 and said sales pricing was hard to obtain. ROCK SLAM was reported at $5,000. Source: [ROCK forum: Licence Renewal](https://community.rockrobotic.com/t/licence-renewal/1869)
- Anecdotal (ROCK forum, undated): Cloud processing moved to a token-based structure. Users reported quotes more than doubling and opaque Business and Enterprise plan pricing. One user said a "Business Plus" promise of 5 projects/month up to 20 acres was not honoured. Source: [ROCK forum: credit structure change](https://community.rockrobotic.com/t/rock-cloud-processing-credit-structure-change/1653)

DJI
- DJI Terra FAQ (primary, applies to Terra 5.0.0): for the L2, "Point cloud processing is a free feature", including accuracy optimisation. For the L1, basic point cloud output is free but advanced functions need Standard or higher. The L3 is not addressed in the FAQ. LiDAR outputs are .pnts, .las, .s3mb, .ply, .pcd. Trajectory files ("route document") are output as .out and .txt (SBET and SMRMSG formats). After binding, paid features work offline. Calibration flights can use RTK or PPK. Source: [DJI Terra FAQ](https://enterprise.dji.com/dji-terra/faq)
- The L3 spec page names DJI Terra as the processor and lists PNTS, LAS, LAZ, PLY, PCD and S3MB point cloud exports. Source: [DJI L3 specs](https://enterprise.dji.com/zenmuse-l3/specs)
- Reseller blog (DroneBundle, 7 Oct 2026, citing the DJI Store): Terra 5.x tiers are Agriculture $300/yr, Standard $1,850/yr or $5,280 permanent (LiDAR, photogrammetry, cluster), Flagship $3,700/yr or $10,580 permanent. Per this blog, LiDAR point cloud reconstruction became free in V5.1 with L3 support. The free trial is 1 month, with a 500-photo cap and an 8 GB LiDAR import cap. Online licences stop after 3 days offline, and offline licences are dealer-only. LAZ export was added in V5.0. Source: [DroneBundle DJI Terra](https://dronebundle.com/blog/dji-terra)
- European reseller prices: Standard EUR 1,860/yr or EUR 5,280 permanent online (EUR 5,760 offline). Source: [Drone Parts Center](https://drone-parts-center.com/en/dji-terra-licenses/)
- Third party: LP360 (v2024.2+) can process raw L1/L2 data via a "DJI L1/L2 Cloud batch processing" add-on with a separate cloud licence, including PPK with base observation files. It is paid and cloud-based. LP360 also documents a hybrid path in which Terra does the trajectory and initial point cloud. Source: [LP360 DJI tab](https://support.lp360.com/hc/en-us/articles/41424651491603-DJI-Tab); [LP360 Terra pre-processing](https://support.lp360.com/hc/en-us/articles/41534267481747-DJI-Terra-Pre-processing-for-the-L1-L2-Processed-workflow)
- Anecdotal (Trimble forum): .LDR/.LDRT raw files cannot be opened outside DJI software. Source: [Trimble community](https://community.trimble.com/discussion/import-dji-l2-processing)

### Inferences
- For a small operator, the realistic L2/L3 path is DJI Terra (free LiDAR point cloud tier, per the DJI FAQ and the reseller), then LAS/LAZ, then open-source tools (PDAL, lidR, CloudCompare). The free status for the L3 comes from a reseller, not DJI. Confirm it in the current Terra release notes before relying on it.
- On the R3 Pro, the borrowed unit's ROCK Desktop licence status is the critical dependency. If the owner's year-1 licence has lapsed, raw data cannot be converted without a renewal (about $2,000 anecdotal) or paid Cloud tokens. Confirm the licence and seat transfer with the owner before flying.
- Both vendors' processing involves sending raw data to a cloud (ROCK Cloud, LP360 cloud) in some paths. Under the CLAUDE.md boundaries, cloud processing of client or hunting-land data needs Beau's approval. Offline Desktop/Terra is the default-compliant path.
- Open-source raw LiDAR georeferencing (trajectory plus boresight from raw packets) for these sensors was not found. Building one is out of scope and would duplicate vendor calibration.

### Gaps
- No primary-source price was found for ROCK Desktop renewal or ROCK Cloud tokens.
- Whether ROCK Desktop exports the trajectory (SBET or similar) and raw XT32 pcap was not confirmed.
- DJI's own price page was not fetched. Prices come from resellers and the competitor article, and they conflict.
- Free L3 processing in Terra V5.1 is from a reseller blog only.
- Whether Terra's free tier accepts a base station RINEX for PPK without a paid licence was not confirmed.

## 4. Forestry performance: canopy penetration, ground returns and known issues

### Takeaway
Evidence is thin and mostly vendor-produced. The L2 captures ground under canopy in some tests but tents or leaves gaps under dense cover. The 1535 nm, 16-return L3 performed better in one reseller's conifer test. No deciduous or leaf-on versus leaf-off quantitative study for these exact sensors was found. A peer-reviewed comparison of low-cost Livox, Hesai Pandar40, Velodyne and RIEGL scanners shows broadleaf tree heights are much less accurate than conifer ones.

### Cited Findings
- ACRS 2025 (Korean conference paper, M350 + L1/L2, forest slopes, 50-150 m): the L2 gave higher density and better Z precision than the L1 and captured ground in dense forest. Multi-return data "may not always provide reliable point cloud data" depending on conditions (secondary summary). Source: [ACRS 2025 abstract](https://acrs-aars.org/proceeding/ACRS2025/fp_abs-152.pdf)
- Interpine (2026, reseller): the L2 penetrated moderate to dense canopy poorly, with intermittent ground gaps and DTM "tenting". The L3 gave reliable ground detection and more branch detail. Results are qualitative, with no accuracy statistics. Noise, strip alignment and intensity were not covered. Source: [Interpine](https://interpine.nz/analysis-of-lidar-dji-l2-vs-l3-lidar-sensors/)
- Heliguy/Forestry England (Kielder, vendor blog): the L2 collected ground points under canopy for DEMs, with no quantitative results. Source: [Heliguy vegetation penetration](https://www.heliguy.com/blogs/posts/vegetation-penetration-and-dem-dji-zenmuse-l2/)
- Sphere Drones (reseller): the L2 had more uniform density below canopy than the L1. Source: [Sphere Drones](https://spheredrones.com.au/resources/blog/dji-zenmuse-l2-l1-comparison-workflow-data)
- Peer-reviewed (Remote Sensing 2021, 13(1):77; summary via search, full text returned 403): Livox MID40 against RIEGL miniVUX-1, Hesai Pandar40 and Velodyne Puck LITE, across 52 flights at varying heights and speeds over conifer and broadleaf sites (500-3,000 trees/ha). Tree height R2 = 0.96 and RMSE 0.59 m at the conifer site, against R2 = 0.70 and RMSE 1.63 m at the broadleaf site. Source: [MDPI Remote Sensing 13(1):77](https://www.mdpi.com/2072-4292/13/1/77) (abstract content via [Crossref listing](https://api.crossref.org/works/10.3390%2FRS13010077))
- Scientific Reports 2024 open dataset (UAV LiDAR + RGB, dense mixed forest, 3,600 field trees): broadleaf crowns often lack a single height maximum, so local-maxima tree detection is largely ineffective. Source: [PMC11415378](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC11415378/)

### Inferences
- For Wisconsin deciduous or mixed stands, expect structural and segmentation performance on broadleaves to be the weak link regardless of sensor. Leaf-off flights should improve ground and stem capture, but they remove leaf-on canopy cues for species. Two-season acquisition may be needed. This is a hypothesis, not a finding.
- With 2 returns, a 905 nm laser and 120 m range, the R3 Pro is likely the weakest of the three for ground under dense leaf-on canopy. That is an inference from specs, not a field result.

### Gaps
- No quantitative deciduous or leaf-on versus leaf-off ground-return study was found for the XT32, L2 or L3.
- No primary or peer-reviewed source on strip misalignment, noise (for example Livox edge noise or XT32 near-range noise) or intensity calibration for these sensors was found within the search budget. All three sensors provide uncalibrated or relative intensity per the absence of radiometric specs. That needs verification.

## 5. Recommended flight parameters and ortho-to-LiDAR registration

### Takeaway
No vendor forestry flight guide was found for the R3 Pro, L2 or L3. DJI's published benchmarks use 120-150 m and 15 m/s for mapping, which is too high and fast for crown-level forestry. Reported forestry trials use 100-120 m, 8 m/s and 50% overlap. Crown relief displacement in standard orthomosaics is a known registration problem. True orthophotos built on a LiDAR DSM are the standard remedy.

### Cited Findings
- DJI L2 benchmark: 150 m, 15 m/s, 20% side overlap, repetitive, RTK FIX, IMU calibration enabled, terrain follow off. The press release cites a typical altitude of 120-150 m, and its versions conflict. Source: [DJI L2 FAQ](https://enterprise.dji.com/zenmuse-l2/faq); [sUAS News press release](https://www.suasnews.com/2023/10/new-dji-zenmuse-l2-increases-precision-efficiency-and-reliability-of-3d-data-acquisition/)
- DJI L3: PRR choice is tied to AGL (1000 kHz below 100 m, 350 kHz below 300 m). Source: [DJI L3 specs](https://enterprise.dji.com/zenmuse-l3/specs)
- Trials: Heliguy L2 at 100 m, 8 m/s, 50% overlap, 5 returns. Interpine at 120 m, 50% overlap, terrain following on. Source: [Heliguy](https://us.heliguy.com/blogs/posts/dji-zenmuse-l2-vs-dji-zenmuse-l1-datasets-and-accuracy); [Interpine](https://interpine.nz/analysis-of-lidar-dji-l2-vs-l3-lidar-sensors/)
- ROCK markets R3 Pro detail "captured at 60m AGL". Source: [ROCK R3 Pro page](https://www.rockrobotic.com/r3-pro)
- The R3 Pro is a PPK system (GNSS "PPK operating mode", PPK attitude specs). The DJI L-series uses RTK and can add base RINEX/RTCM for PPK. DJI calibration flights accept RTK or PPK. Source: [ROCK datasheet](https://learn.rockrobotic.com/r3-pro-v2-specifications-download-datasheet); [DJI Terra FAQ](https://enterprise.dji.com/dji-terra/faq)
- Relief displacement pushes tree crowns radially outward in imagery. Standard orthos rectified on a terrain model misplace elevated objects. True orthophotos rectified on a LiDAR surface model place crowns upright. Source: [ITC orthophoto lecture](https://ftp.itc.nl/pub/maathuis/EENSAT_batch1_Course_Materials/EENSAT_week2/photogrammetry/EENSAT_orthophoto_lecture.pdf); [Boise State true ortho via LiDAR DSM](https://experts.boisestate.edu/en/publications/true-orthophoto-creation-through-fusion-of-lidar-derived-digital-/)
- St-Onge (SilviLaser 2008) built a 10 cm true orthomosaic matched to a LiDAR DSM, measured and corrected the residual image-to-LiDAR misregistration, and filled DSM cavities first. Accurate correspondence is a premise for combined structure and species analysis. Source: [St-Onge SilviLaser 2008](https://geography.swan.ac.uk/silvilaser/papers/poster_papers/St-Onge.pdf)
- The Moni1000 UAV-LiDAR dataset (Japan) has a 5 cm DTM, CHM, 2.2-2.6 cm orthos and crown polygons for 151 species and 4,328 trees. It is a possible registration and species benchmark, with the licence to be checked. Source: [DIAS Moni1000 release](https://diasjp.net/en/?p=7972)

### Inferences (proposed starting parameters, to validate; not vendor recommendations)
- 60-80 m AGL with terrain follow. 5-8 m/s. 50-70% side overlap. Consider crosshatch (perpendicular second grid) for crown-side and stem coverage. RTK or PPK with a logged base station. A handful of surveyed GCP/checkpoints in openings for vertical accuracy checks.
- R3 Pro: stay near or below about 60-80 m because of the 120 m range at 60% reflectivity. L3: below 100 m, the 1000 kHz/4-8 return mode is available. Compare it with 350 kHz/16 returns for penetration.
- For RGB-LiDAR fusion, use the LiDAR-colourised point cloud or a true ortho built on the LiDAR DSM. Avoid a photogrammetric ortho rectified on its own DSM or a DTM. Higher image overlap (for example 80/70) and nadir-heavy coverage reduce crown lean. Measure the residual crown offset on known isolated trees as an explicit QA metric.

### Gaps
- No peer-reviewed or vendor forestry flight guideline specific to these three sensors was found.
- No study was found that quantifies how ortho-LiDAR misregistration degrades species classification accuracy.
- The ROCK flight-planning guidance (recommended AGL, speed, overlap, IMU warm-up and figure-8 manoeuvres) was not retrieved within the search budget.
