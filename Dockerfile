# syntax=docker/dockerfile:1

# One image for every process this repo runs (the producer and the consumer fleet); they differ only by argv,
# set by `command:` in docker-compose.yml.
#
# Pinned by digest, not by tag: a moving image is one nobody can reproduce. This digest is node v26.8.2.
# To refresh: docker pull node:26-alpine && docker inspect --format '{{index .RepoDigests 0}}' node:26-alpine
ARG NODE_IMAGE=node:26-alpine@sha256:ef24c5053d50fdc3e4e56eb4e7ddb7861874ab0fdc797046ba897581deb8e868

# ---------------------------------------------------------------------------
# deps — the workspace's production dependencies, and nothing else. Only the manifests are copied in, so this
# layer is rebuilt when a dependency changes and reused when source changes.
# ---------------------------------------------------------------------------
FROM ${NODE_IMAGE} AS deps
WORKDIR /app
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
# Same pnpm the workspace pins in package.json#packageManager, read from it
# rather than named twice. npm, not corepack: Node 25 stopped shipping corepack.
RUN npm install --global "$(node -p 'require("./package.json").packageManager')" >/dev/null
# One line per workspace, kept in step by hand: a package missing here installs none of its dependencies, and
# the failure surfaces as ERR_MODULE_NOT_FOUND at runtime, not as a build error.
COPY packages/consumer/package.json packages/consumer/
COPY packages/config/package.json packages/config/
COPY packages/rmq/package.json packages/rmq/
COPY packages/rmq-producer/package.json packages/rmq-producer/
COPY packages/tracing/package.json packages/tracing/
# --prod drops typescript and testcontainers, which exist for `pnpm run check` and the opt-in integration suites.
RUN pnpm install --frozen-lockfile --prod

# ---------------------------------------------------------------------------
# runtime — dependencies, then source. No build step, deliberately: every process runs TypeScript directly
# through node's type stripping, so the artifact and the thing developers run are the same files. The runtime
# must therefore be a version whose stripping behaviour has actually been run, hence the digest above.
# ---------------------------------------------------------------------------
FROM ${NODE_IMAGE} AS runtime
ENV NODE_ENV=production
WORKDIR /app

# The pnpm store first, then the per-package symlink farms that point into it. --chown on every copy: COPY
# preserves the host's file mode and this workspace has files at 0600, so a root-owned copy builds clean and then
# dies on its first import with EACCES on its own source.
COPY --from=deps --chown=node:node /app/node_modules ./node_modules
COPY --from=deps --chown=node:node /app/packages ./packages
# Then the source, merged on top: .dockerignore keeps the host's node_modules
# out, so this adds files and never replaces a symlink.
COPY --chown=node:node package.json pnpm-workspace.yaml ./
COPY --chown=node:node packages ./packages

# Nothing here writes to disk, so it has no reason to be root.
USER node

# Overridden per service in docker-compose.yml. The default is the consumer, so a container started with no
# arguments does the main job.
CMD ["node", "packages/consumer/src/main.ts"]
