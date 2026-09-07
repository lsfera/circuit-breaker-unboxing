# syntax=docker/dockerfile:1

# One image for every process this repo runs: the aggregator pair, the RabbitMQ
# producer and daemon fleet, the subscriber, and the demo driver. They differ
# only by argv, which is what `command:` in docker-compose.yml is for — an
# image per package would be five images that install the same workspace.
#
# Pinned by digest, not by tag. `node:22-alpine` moves, and an image that moves
# is an image nobody can reproduce: the whole point of building an artifact is
# that the thing you tested is the thing you ship. This digest is node v22.23.2.
# To refresh: docker pull node:22-alpine && docker inspect --format \
#   '{{index .RepoDigests 0}}' node:22-alpine
ARG NODE_IMAGE=node:22-alpine@sha256:c610fcdfb1d5b4740dd70c284ed3cb16bb857e0f7166196e36a5501df7a3aa32

# ---------------------------------------------------------------------------
# deps — the workspace's production dependencies, and nothing else.
#
# Only the manifests are copied in, so this layer is rebuilt when a dependency
# changes and reused when source changes. That ordering is the entire reason
# for a multi-stage build here.
# ---------------------------------------------------------------------------
FROM ${NODE_IMAGE} AS deps
WORKDIR /app
# Same pnpm the workspace pins in package.json#packageManager, resolved by
# corepack rather than named twice.
RUN corepack enable
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
# One line per workspace, and it has to be kept in step by hand: a package
# missing here installs none of its dependencies, and the failure surfaces as
# ERR_MODULE_NOT_FOUND at runtime rather than as a build error. Adding
# @egress/tracing is exactly how that was learned.
COPY packages/aggregator/package.json packages/aggregator/
COPY packages/demo/package.json packages/demo/
COPY packages/domain/package.json packages/domain/
COPY packages/rmq/package.json packages/rmq/
COPY packages/rmq-consumer/package.json packages/rmq-consumer/
COPY packages/rmq-producer/package.json packages/rmq-producer/
COPY packages/subscriber/package.json packages/subscriber/
COPY packages/tracing/package.json packages/tracing/
# --prod drops typescript and testcontainers, which exist for `pnpm run check`
# and the opt-in integration suites and have no business in a runtime image.
RUN pnpm install --frozen-lockfile --prod

# ---------------------------------------------------------------------------
# runtime — dependencies, then source.
#
# No build step, deliberately. This repo has never had one: every process runs
# TypeScript directly through node's type stripping, which is why `tsc
# --noEmit` is load-bearing in CI rather than cosmetic. Adding a compile here
# would mean the artifact and the thing developers run are no longer the same
# files. The cost is that the runtime must be a version whose stripping
# behaviour we have actually run — hence the digest above rather than a tag.
# ---------------------------------------------------------------------------
FROM ${NODE_IMAGE} AS runtime
ENV NODE_ENV=production
WORKDIR /app

# The pnpm store first, then the per-package symlink farms that point into it.
#
# --chown on every copy, because COPY preserves the *host's* file mode and this
# workspace has files at 0600. Copied as root that is a container which builds
# clean and then dies on its first import with EACCES on its own source — a
# failure that reads like a missing file rather than a permission bit.
COPY --from=deps --chown=node:node /app/node_modules ./node_modules
COPY --from=deps --chown=node:node /app/packages ./packages
# Then the source, merged on top: .dockerignore keeps the host's node_modules
# out, so this adds files and never replaces a symlink.
COPY --chown=node:node package.json pnpm-workspace.yaml ./
COPY --chown=node:node packages ./packages

# Nothing here writes to disk, so it has no reason to be root.
USER node

# Overridden per service in docker-compose.yml. The default is the aggregator
# because a container started with no arguments should do the thing this repo
# is named after.
CMD ["node", "--experimental-strip-types", "packages/aggregator/src/main.ts"]
