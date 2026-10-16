// Run the generated page's actual script with a minimal offline DOM adapter.
// This checks event/export logic; it is not a browser layout engine.
const fs = require('node:fs');
const vm = require('node:vm');
const config = JSON.parse(fs.readFileSync(0, 'utf8'));
const html = fs.readFileSync(config.html, 'utf8');
const scripts = [...html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi)];
if (scripts.length !== 1) throw new Error('Expected one local page script');

class Element {
  constructor(tagName) { this.tagName = tagName.toUpperCase(); this.children = []; }
  set innerHTML(value) { this.markup = value; this.children = []; }
  get innerHTML() { return this.markup || ''; }
  appendChild(child) { this.children.push(child); }
  querySelectorAll() { return []; }
  scrollIntoView() {}
  click() {}
}
const elements = Object.fromEntries(['grid', 'count', 'export'].map(id => [id, new Element('div')]));
const events = {};
const anchors = [];
let stored = JSON.stringify(config.picks || {});
let exported = null;
const context = {
  document: {
    getElementById: id => elements[id],
    createElement: tag => { const node = new Element(tag); if (tag === 'a') anchors.push(node); return node; },
    addEventListener: (event, handler) => { events[event] = handler; },
  },
  localStorage: { getItem: () => stored, setItem: (_key, value) => { stored = value; } },
  Blob: class { constructor(parts, options) { this.parts = parts; this.type = options.type; } },
  URL: { createObjectURL: blob => { exported = {text: blob.parts.join(''), type: blob.type}; return 'blob:offline'; } },
};
vm.createContext(context);
vm.runInContext(scripts[0][1], context, {timeout: 1000});
for (const key of config.keys || []) events.keydown({key});
elements.export.onclick();
process.stdout.write(JSON.stringify({
  exported, download: anchors.at(-1)?.download,
  cards: elements.grid.children.map(node => node.innerHTML),
  count: elements.count.textContent, stored: JSON.parse(stored),
}));
