# Handoff: Forest LiDAR + Ortho Species Estimation Pilot

From: Claude (cloud chat) to Claude Code (on Beau's main rig, same host as Codex)
Date: 2026-10-13
Owner: Beau Koltz (Ledge Top Technologies LLC / Ledge Top Drone Services)

## Goal

Build a self-hosted pipeline that takes drone LiDAR plus orthophoto data over Wisconsin forest land and produces per-tree estimates (height, crown shape, species with calibrated confidence). The end product is a data layer for Thomas Mlsna (Untamed Ambition), whose land management clients want all their land data in one place for hunting and habitat decisions.

Beau may be the only one in the region who can offer this. The aim is to be best in class for data collection and accuracy, not just to ship something. It may turn out not to work or not to be useful, and that is an acceptable outcome of the pilot as long as we learn why.

## Context

- Beau flies borrowed gear for now: DJI M350 with a Rock Robotics R3 Pro. He may later buy an M400 and DJI L3. Do not assume a purchase.
- Money is tight. Prefer free and open source tools and self-hosting on Beau's own servers.
- Hunting land data is sensitive. Client data must not go to public archives or third-party services without Beau's explicit approval.
- Beau has access to Jev for decision support, so treat it as available for judgment calls (see Beau for details).
- 3Dtrees.earth (https://3dtrees.earth/) is the inspiration. It is a research platform for forest LiDAR run by European institutions, with processing on Galaxy Europe. Its storage region and the full platform license were NOT confirmed. Verify before relying on anything.

## Working hypothesis (to test, not assume)

1. Structure and terrain from LiDAR (bare earth, canopy height, density, openings, individual tree heights) are reliable and can be sold now.
2. Species from LiDAR alone is weak. Fusing it with orthophoto color and texture should beat either alone.
3. Crown shape features (height to width ratio, vertical foliage distribution, gap fraction, return intensity, first to later return ratio) add signal, mainly for open-grown trees and edges, less in closed canopy.
4. Two seasons help: leaf-off LiDAR for branching and stem structure, and fall color imagery (ideally 2 to 3 dates, since maples color before oaks) for species separation.
5. Higher point density comes from slower speed, lower altitude, tighter line spacing, more side overlap, and crosshatch passes, not from more returns per pulse. Diminishing returns apply, so test one density first.
6. Pretrained models from European forests may not transfer to Wisconsin hardwoods. Local labeled data is the bottleneck.
7. A random forest or gradient-boosted model on per-tree features, with calibrated probabilities, is the right starting point. Deep models come later if labels allow.

Where hypotheses are unverified, say so in your findings. Beau explicitly wants disagreement, so push back if the evidence says otherwise.

## Tasks

### 1. Survey open source tools (research, no client data)
Find, read, and record license, activity, and fit for:
- Tree segmentation from point clouds (candidates to check, names and licenses unverified: SegmentAnyTree, ForAINet, lidR in R, PDAL, CloudCompare, Forest Structural Complexity Tool, TreeLearn, 3DForest)
- Species classification from LiDAR and imagery (look for models, training code, and what regions and species they were trained on)
- The public 3Dtrees-earth GitHub organization (tool-interface and anything else public): what is open, under what license, and what depends on Galaxy, Supabase, or S3
- Point cloud tiling, LOD, and viewing (Potree, COPC, EPT) as they relate to Beau's existing custom 3D Viewer

Deliver a table: tool, purpose, license, last activity, training region and species, runs self-hosted (yes/no), recommendation.

### 2. Reverse engineer for understanding, within license
- Read public repos, papers, and documentation to document how 3Dtrees.earth structures its pipeline: ingestion, standardization, tiling, instance segmentation, species prediction, trait extraction, visualization.
- Use only public code, public docs, and published papers. Do not bypass authentication, scrape private data, or violate terms of service.
- Produce an architecture diagram and a list of which stages Beau should reuse, replace, or build.

### 3. Sensor and data feasibility
- Pull real published specs for the Rock Robotics R3 Pro and DJI L3 (return count, ranging accuracy, point density versus altitude and speed, beam divergence). Mark anything not from a primary source as unverified.
- Estimate point density at candidate flight settings and storage per acre.
- Recommend a pilot flight plan: altitude, speed, line spacing, overlap, crosshatch, terrain following, RTK or GCP use. Registration between ortho and LiDAR must be tight enough to match crowns.

### 4. Feature and model plan
- Define the per-tree feature list (structure, intensity, return ratios, crown metrics, color and texture from ortho, multi-date color change).
- Propose a baseline classifier (random forest or gradient boosting), probability calibration method, and how to report confidence honestly.
- Define the validation design: field plots, number of trees needed per species, train and test split by location to avoid leakage, and metrics (per-class precision and recall, calibration curve).

### 5. Prototype on public data first
- Find open LiDAR with forest labels (for example NEON and USGS 3DEP are possible sources, verify availability and licensing) and build a minimal end-to-end run: segment trees, extract features, train a baseline, report accuracy.
- Keep everything reproducible with scripts and fixtures. Full end-to-end tests are required, not just syntax checks.

## Deliverables

1. `findings.md` with the tool survey table, architecture notes, sensor feasibility, and recommendations.
2. A proposed pilot plan: one property, LiDAR plus fall RGB, roughly 20 to 30 field-verified plots, success criteria, and a go or no-go rule.
3. A prototype repo or folder with working scripts and tests on public data.
4. A short list of open questions for Beau.

## Ground rules

- Read-only on LedgeTop-Ops and Project Alpha. Codex owns the Operations backend and is actively testing PA staging. Do not touch those repos. Work in a new separate repo or folder.
- Keep Project Alpha generic since it is open source. Nothing from this project that is specific to Beau's businesses goes into PA.
- Minimal Docker. Prefer real testing over mocks. Protect data and use canaries before anything risky.
- Do not upload any client or hunting land data to external services or public archives.
- Do not message clients. Do not send anything outbound unless Beau asks.
- Stop and ask Beau before anything that touches production, client data, or spends money.
- Check licenses before copying or depending on any code. Record each one.
- Shared notes and architecture decisions go in the Agent Message Bus (the MCP). Repo-specific notes stay in the repo. If the bus is unreachable, keep notes in the repo and flag it.
- Writing style for anything Beau reads: no em dashes mid-sentence, give exactly what was asked, no extra padding. Use .md files, not .txt.

## Known unknowns

- Storage region and full license of the 3Dtrees.earth platform
- Whether the pretrained species models cover or transfer to Wisconsin hardwoods
- Real specs of the R3 Pro and L3 for this use
- Property sizes and whether Thomas has a forester or field crew for plot identification
- Whether the final product is something clients will pay for

## First step

Start with Task 1 and Task 3 in parallel (survey and sensor specs), report back with a short summary, then confirm direction with Beau before building the prototype.