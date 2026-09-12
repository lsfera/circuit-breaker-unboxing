/**
 * Parses every mermaid diagram in the documentation.
 *
 * These diagrams render in the reader's browser, so a diagram mermaid cannot
 * parse does not fail a build — it draws a grey box reading "Syntax error in
 * text" where the explanation should be, and every other page keeps working.
 * Three of them were doing exactly that: journey.md uses the edge-id syntax
 * (`Producer p@-->|Publish| RabbitMQ`), which arrived in mermaid 11.6, against
 * a vendored 11.4.1 that had never heard of it.
 *
 * So the version the site ships is pinned in one place — docs/Dockerfile's
 * MERMAID_VERSION — and this reads that pin rather than taking its own, because
 * a checker that tests a different version from the one being served is not
 * checking anything.
 *
 *   pnpm run check:diagrams
 *
 * Opt-in locally for the same reason as the Docker-backed suites: it needs
 * dependencies most work in this repo does not. CI runs it on every change to
 * docs/.
 */
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { JSDOM } from "jsdom";

const ROOT = new URL("..", import.meta.url).pathname;

/** The version the site actually serves, read from the pin rather than guessed. */
const pinnedVersion = () => {
  const dockerfile = readFileSync(join(ROOT, "docs/Dockerfile"), "utf8");
  const match = dockerfile.match(/^ARG MERMAID_VERSION=(\S+)/m);
  if (!match) throw new Error("no MERMAID_VERSION pin in docs/Dockerfile");
  return match[1];
};

/** Every ```mermaid block in the documentation, with where it came from. */
const diagrams = function* (dir) {
  for (const entry of readdirSync(dir)) {
    const path = join(dir, entry);
    if (statSync(path).isDirectory()) {
      if (entry.startsWith("_") || entry === "assets" || entry === "vendor") continue;
      yield* diagrams(path);
      continue;
    }
    if (!entry.endsWith(".md") && !entry.endsWith(".html")) continue;
    yield* diagramsIn(path);
  }
};

/**
 * Both ways a diagram is written here: a fenced block in a document, and a
 * `<pre class="mermaid">` in a hand-written page. The second is how the
 * overview's architecture diagram is written, and leaving it unscanned would
 * have put the one diagram everybody sees first outside the check.
 */
const PATTERNS = [
  /```mermaid\n([\s\S]*?)\n```/g,
  /<pre class="mermaid">\n([\s\S]*?)\n<\/pre>/g,
];

/** Every diagram in one file, whichever way it is written. */
const diagramsIn = function* (path) {
  const source = readFileSync(path, "utf8");
  const inHtml = path.endsWith(".html");
  let index = 0;
  for (const pattern of PATTERNS) {
    for (const block of source.matchAll(pattern)) {
      // The line the block opens on, so a failure names somewhere to look.
      const line = source.slice(0, block.index).split("\n").length;
      // A block inside a hand-written page is parsed as HTML before mermaid
      // sees it: entities are decoded, and anything that looks like a tag
      // becomes an element that `textContent` drops. Both are mimicked here,
      // so what is parsed is what mermaid will actually receive.
      const raw = inHtml ? [...block[1].matchAll(/<[a-zA-Z/!][^>]*>/g)].map((m) => m[0]) : [];
      const text = inHtml ? decode(block[1]) : block[1];
      yield { file: relative(ROOT, path), line, index: index++, text, raw };
    }
  }
};

const decode = (s) =>
  s.replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&quot;/g, '"')
   .replace(/&#39;/g, "'").replace(/&amp;/g, "&");

const dom = new JSDOM("<!doctype html><body></body>", { pretendToBeVisual: true });
global.window = dom.window;
global.document = dom.window.document;
Object.defineProperty(global, "navigator", { value: dom.window.navigator, configurable: true });
for (const name of [
  "Element", "HTMLElement", "SVGElement", "Node", "DocumentFragment",
  "NodeFilter", "XMLSerializer", "DOMParser", "getComputedStyle",
  "requestAnimationFrame", "cancelAnimationFrame",
]) {
  if (dom.window[name] !== undefined && global[name] === undefined) global[name] = dom.window[name];
}

const { default: mermaid } = await import("mermaid");

const want = pinnedVersion();
const have = JSON.parse(
  readFileSync(join(ROOT, "node_modules/mermaid/package.json"), "utf8"),
).version;
if (have !== want) {
  console.error(
    `mermaid ${have} is installed but docs/Dockerfile serves ${want}.\n` +
      `Checking a different version from the one readers get proves nothing — ` +
      `align package.json with the pin, or the pin with package.json.`,
  );
  process.exit(1);
}

// The README is not part of the site, but its diagrams are documentation and
// GitHub renders them with a mermaid of its own; a version skew shows up here
// first.
// `history/` is not published, but its diagrams are still diagrams — a record
// nobody can render is not much of a record.
const all = [
  ...diagrams(join(ROOT, "docs")),
  ...diagrams(join(ROOT, "history")),
  ...diagramsIn(join(ROOT, "README.md")),
];
const failures = [];

for (const diagram of all) {
  // Raw markup never reaches mermaid — the DOM has already removed it, joining
  // whatever was either side. `<br/>` written literally in a hand-written page
  // silently becomes no line break at all, and the diagram still parses.
  if (diagram.raw.length > 0) {
    failures.push({
      ...diagram,
      why: [
        `raw HTML in a mermaid block: ${[...new Set(diagram.raw)].join(" ")}`,
        "the DOM removes it before mermaid sees it — write it escaped (&lt;br/&gt;)",
      ],
    });
    continue;
  }
  try {
    await mermaid.parse(diagram.text);
  } catch (error) {
    failures.push({ ...diagram, why: String(error?.message ?? error).split("\n").slice(0, 3) });
  }
}

console.log(`mermaid ${have}: ${all.length - failures.length}/${all.length} diagrams parse`);

for (const failure of failures) {
  console.error(`\n${failure.file}:${failure.line} — diagram ${failure.index + 1}`);
  for (const line of failure.why) console.error(`  ${line}`);
}

if (failures.length > 0) process.exit(1);
