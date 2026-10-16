"""Self-contained HTML page for the owner to label crowns by clicking.

Crown chips are embedded as small JPEGs so the page works offline from a
local file. Choices are kept in the browser's localStorage and exported as a
labels CSV (tree_id,label,group) that `treedar classify` reads directly.
The page contains imagery of the property: keep it local.
"""

from __future__ import annotations

import base64
import csv
import html
import io
import json
import random
from pathlib import Path

from PIL import Image

CHOICES = [("conifer", "Conifer (pine, spruce...)", "1"),
           ("broadleaf", "Broadleaf (maple, oak...)", "2"),
           ("", "Not sure / skip", "3")]


def sample_trees(tree_area_rows, per_area=15, seed=0, fraction=0.12):
    """From every named area, max(per_area, fraction of its trees) trees, at random."""
    by_area: dict[str, list[str]] = {}
    for r in tree_area_rows:
        if r["area"]:
            by_area.setdefault(r["area"], []).append(r["tree_id"])
    rng = random.Random(seed)
    picked = []
    for area in sorted(by_area):
        ids = by_area[area][:]
        rng.shuffle(ids)
        k = max(per_area, int(round(fraction * len(ids))))
        picked += [(tid, area) for tid in ids[:k]]
    return picked


def _thumb(path, size=240):
    im = Image.open(path).convert("RGB")
    im.thumbnail((size, size))
    buf = io.BytesIO()
    im.save(buf, "JPEG", quality=80)
    return base64.b64encode(buf.getvalue()).decode()


def build(run_dir, chips_dir, out_html, per_area=15, seed=0) -> int:
    run_dir = Path(run_dir)
    with open(run_dir / "tree_areas.csv", newline="") as fh:
        rows = list(csv.DictReader(fh))
    trees = {r["tree_id"]: r for r in csv.DictReader(open(run_dir / "trees.csv", newline=""))}
    items = []
    for tid, area in sample_trees(rows, per_area, seed):
        chip = Path(chips_dir) / f"tree_{tid}.png"
        if chip.exists():
            items.append({"id": tid, "area": area, "h": trees[tid]["height_m"],
                          "d": trees[tid]["crown_diameter_m"], "img": _thumb(chip)})
    page = TEMPLATE.replace("__ITEMS__", json.dumps(items)).replace(
        "__CHOICES__", json.dumps(CHOICES)).replace("__N__", str(len(items))).replace(
        "__KEY__", html.escape(f"treedar-labels-{run_dir.name}"))
    Path(out_html).write_text(page, encoding="utf-8")
    return len(items)


TEMPLATE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Label Trees</title>
<style>
:root { --bg:#f6f7f5; --fg:#1d2a1f; --card:#fff; --line:#d7ddd5; --pick:#1b6e3a; --pick2:#7cb342; --muted:#6b756c; }
@media (prefers-color-scheme: dark) { :root { --bg:#141814; --fg:#e8eee7; --card:#1e241e; --line:#334033; --muted:#9aa59a; } }
body { margin:0; background:var(--bg); color:var(--fg); font:15px/1.4 system-ui, sans-serif; }
header { position:sticky; top:0; background:var(--bg); padding:12px 16px; border-bottom:1px solid var(--line); z-index:2; }
h1 { font-size:18px; margin:0 0 4px; }
.bar { display:flex; gap:12px; align-items:center; flex-wrap:wrap; }
.grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(240px,1fr)); gap:12px; padding:16px; }
.card { background:var(--card); border:2px solid var(--line); border-radius:10px; overflow:hidden; }
.card.active { outline:3px solid #f0b400; }
.card img { width:100%; display:block; aspect-ratio:1; object-fit:contain; background:#000; }
.meta { padding:6px 8px; color:var(--muted); font-size:13px; }
.btns { display:flex; gap:4px; padding:0 8px 8px; }
.btns button { flex:1; padding:7px 2px; border:1px solid var(--line); border-radius:6px; background:transparent; color:var(--fg); cursor:pointer; font-size:12px; }
.btns button.on { color:#fff; border-color:transparent; }
.btns button.on[data-v=conifer] { background:var(--pick); }
.btns button.on[data-v=broadleaf] { background:var(--pick2); }
.btns button.on[data-v=""] { background:#888; }
.primary { padding:8px 14px; border-radius:8px; border:0; background:var(--pick); color:#fff; cursor:pointer; font-size:14px; }
small { color:var(--muted); }
</style></head><body>
<header>
  <h1>Label trees: conifer or broadleaf?</h1>
  <div class="bar">
    <span id="count"></span>
    <button class="primary" id="export">Save labels file</button>
    <small>Keys: 1 conifer, 2 broadleaf, 3 not sure, arrows move. Yellow outline marks the crown. Your choices are kept in this browser.</small>
  </div>
</header>
<div class="grid" id="grid"></div>
<script>
const ITEMS = __ITEMS__, CHOICES = __CHOICES__, KEY = "__KEY__";
let picks = {};
try { picks = JSON.parse(localStorage.getItem(KEY) || "{}"); } catch (e) { picks = {}; }
let active = 0;
const grid = document.getElementById("grid");
function save() { try { localStorage.setItem(KEY, JSON.stringify(picks)); } catch (e) {} }
function render() {
  grid.innerHTML = "";
  ITEMS.forEach((it, i) => {
    const c = document.createElement("div");
    c.className = "card" + (i === active ? " active" : "");
    c.innerHTML = `<img alt="crown ${it.id}" src="data:image/jpeg;base64,${it.img}">
      <div class="meta">Area ${it.area} &middot; ${(+it.h).toFixed(1)} m tall &middot; ${(+it.d).toFixed(1)} m wide</div>
      <div class="btns">${CHOICES.map(([v, t]) => `<button data-v="${v}" class="${it.id in picks && picks[it.id] === v ? "on" : ""}">${t}</button>`).join("")}</div>`;
    c.querySelectorAll("button").forEach(b => b.onclick = () => { pick(i, b.dataset.v); });
    c.onclick = (e) => { if (e.target.tagName !== "BUTTON") { active = i; render(); } };
    grid.appendChild(c);
  });
  const n = Object.values(picks).filter(v => v).length;
  const tally = {}; Object.values(picks).forEach(v => { if (v) tally[v] = (tally[v] || 0) + 1; });
  document.getElementById("count").textContent = `${Object.keys(picks).length} of ${ITEMS.length} reviewed, ${n} labelled (` +
    Object.entries(tally).map(([k, v]) => `${k} ${v}`).join(", ") + ")";
}
function pick(i, v) { if (!ITEMS.length || i < 0 || i >= ITEMS.length) return; picks[ITEMS[i].id] = v; save(); active = Math.min(i + 1, ITEMS.length - 1); render();
  grid.children[active]?.scrollIntoView({block: "nearest"}); }
document.addEventListener("keydown", e => {
  if (!ITEMS.length) return;
  if (["1","2","3"].includes(e.key)) pick(active, CHOICES[+e.key - 1][0]);
  else if (e.key === "ArrowRight" || e.key === "ArrowDown") { active = Math.min(active + 1, ITEMS.length - 1); render(); }
  else if (e.key === "ArrowLeft" || e.key === "ArrowUp") { active = Math.max(active - 1, 0); render(); }
});
document.getElementById("export").onclick = () => {
  const rows = ["tree_id,label,group"];
  ITEMS.forEach(it => { const v = picks[it.id]; if (v) rows.push(`${it.id},${v},${it.area}`); });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(new Blob([rows.join("\\n") + "\\n"], {type: "text/csv"}));
  a.download = "labels.csv"; a.click();
};
render();
</script></body></html>
"""
