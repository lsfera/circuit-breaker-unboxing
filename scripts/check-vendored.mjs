/**
 * The vendored Effect source is the version this repository runs, or it is
 * misleading.
 *
 *   node scripts/check-vendored.mjs
 *
 * `repos/effect` exists so that an agent — or a person — reading how an Effect
 * API behaves reads the implementation and tests of the version actually
 * installed. The failure it invites is quiet: someone bumps `effect` in the
 * catalog, the subtree stays where it was, and from then on the reference
 * material describes a library the code no longer compiles against. Worse than
 * no reference at all, because it is trusted. The day this was added, Effect's
 * `main` was already two release candidates ahead of the lockfile.
 *
 * So every `effect` and `@effect/*` dependency in packages/*, with `catalog:`
 * resolved through pnpm-workspace.yaml, must equal the version of the package
 * of the same name under repos/effect.
 */
import { readFileSync, readdirSync, existsSync } from "node:fs";
import { join } from "node:path";

const VENDORED = "repos/effect/packages";

// The catalog is a flat `name: version` block; nothing else in the file is read.
const catalogBlock = /^catalog:\n((?:[ \t]+.+\n?)*)/m.exec(readFileSync("pnpm-workspace.yaml", "utf8"))?.[1] ?? "";
const catalog = new Map(
  [...catalogBlock.matchAll(/^[ \t]+["']?([^"':\s]+)["']?:\s*["']?([^"'\s#]+)/gm)].map((m) => [m[1], m[2]]),
);

/** Every package.json under repos/effect/packages, one or two levels down —
 *  `effect` is packages/effect, `@effect/platform-node` is packages/platform/node. */
const vendored = new Map();
for (const top of readdirSync(VENDORED)) {
  for (const dir of [join(VENDORED, top), ...readdirSync(join(VENDORED, top), { withFileTypes: true }).filter((d) => d.isDirectory()).map((d) => join(VENDORED, top, d.name))]) {
    const manifest = join(dir, "package.json");
    if (!existsSync(manifest)) continue;
    const { name, version } = JSON.parse(readFileSync(manifest, "utf8"));
    if (name) vendored.set(name, { version, dir });
  }
}

const problems = [];
const checked = new Map();
for (const pkg of readdirSync("packages")) {
  const manifest = join("packages", pkg, "package.json");
  if (!existsSync(manifest)) continue;
  const { dependencies = {}, devDependencies = {} } = JSON.parse(readFileSync(manifest, "utf8"));
  for (const [name, spec] of Object.entries({ ...dependencies, ...devDependencies })) {
    if (name !== "effect" && !name.startsWith("@effect/")) continue;
    const wanted = spec === "catalog:" ? catalog.get(name) : spec;
    const have = vendored.get(name);
    if (!wanted) problems.push(`${pkg}: ${name} is "catalog:" but the catalog has no entry for it`);
    else if (!have) problems.push(`${pkg}: ${name}@${wanted} is not in ${VENDORED}`);
    else if (have.version !== wanted) problems.push(`${pkg}: ${name}@${wanted} declared, ${have.version} vendored at ${have.dir}`);
    else checked.set(name, `${wanted} → ${have.dir}`);
  }
}

for (const [name, where] of checked) console.log(`  ${name.padEnd(24)} ${where}`);
if (problems.length > 0) {
  console.error(`\n${problems.join("\n")}\n`);
  console.error(
    "Move the subtree to the tag for the installed version:\n" +
      "  git subtree pull --prefix=repos/effect https://github.com/Effect-TS/effect.git effect@<version> --squash",
  );
  process.exit(1);
}
console.log("vendored Effect matches every installed Effect package");
