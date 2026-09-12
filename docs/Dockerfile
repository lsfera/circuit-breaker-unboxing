# The documentation site, as an image you can read on a train.
#
#   docker build -f docs/Dockerfile -t egress-site .
#   docker run --rm -p 4000:80 egress-site     # then http://localhost:4000
#
# The build context is the repository root, not docs/, because README.md is
# part of the site: it is the repository's own front page and four documents
# link into its sections.
#
# 4000 because it is Jekyll's own default and nothing in docker-compose.yml
# wants it. 8080 is not free here: flaky-upstream binds 8080-8085 and
# 8090-8096, so the obvious choice collides with the stack these documents are
# about. `SITE_PORT=… pnpm run site` moves it.
#
# Two stages: Jekyll builds, nginx serves. CI builds this same image and
# publishes what comes out of it, so the site on GitHub Pages and the site in
# this image are the same bytes rather than two builds that agree today.
#
# `BASEURL` is the one thing that differs between them. A project site lives at
# <owner>.github.io/<repo>/ and every asset needs that prefix; a container on
# localhost is served from the root and must not have one, which is why the
# default here is empty.

# ---------------------------------------------------------------------------
FROM ruby:3.3-slim AS build

ARG BASEURL=""

# `jekyll build` shells out to nothing, but nokogiri and ffi build natively.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential zlib1g-dev libxml2-dev libxslt1-dev curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /site

# Gems first: the documents change constantly, the dependencies almost never,
# and this keeps a rebuild to the seconds Jekyll itself takes.
COPY docs/Gemfile docs/Gemfile.lock ./
RUN bundle config set --local without "" \
 && bundle install --jobs 4 --retry 3

# Mermaid is vendored rather than loaded from a CDN. Diagrams are content here —
# four of these documents explain themselves with one — and an image whose whole
# point is reading offline cannot fetch a renderer at page load. The layout
# falls back to the CDN if this file is absent, so a bare `jekyll serve` with no
# vendor step still draws them while online.
# 12.0.0. The bundle still ends with
# `globalThis["mermaid"] = ...`, so a plain <script src> defines the global the
# layout looks for, and `startOnLoad`, `securityLevel` and the `dark`/`neutral`
# themes all survive the major. It is 5.6MB against 11.x's 3.6MB, which is the
# price of this image being readable without a network.
ARG MERMAID_VERSION=12.0.0
RUN mkdir -p assets/vendor \
 && curl -fsSL --retry 3 \
      "https://cdn.jsdelivr.net/npm/mermaid@${MERMAID_VERSION}/dist/mermaid.min.js" \
      -o assets/vendor/mermaid.min.js \
 && test -s assets/vendor/mermaid.min.js

COPY docs/ .

# Front matter prepended here rather than committed into README.md, because a
# `---` block at the top of that file is the first thing GitHub would render.
# `repo_root` tells the layout that this page's links are written relative to
# the repository, not to docs/.
COPY README.md /tmp/README.md
# No `title:` here on purpose. `jekyll-titles-from-headings` takes the title
# from the page's own first heading and, with `strip_title`, removes it from the
# body — but only when it derived the title itself. Setting one explicitly left
# the README's `# Per-API egress circuit breaker events` in place under the
# layout's rendering of the same string, so the page opened with its name twice.
RUN { \
      printf -- '---\nrepo_root: true\n---\n\n'; \
      cat /tmp/README.md; \
    } > readme.md

RUN bundle exec jekyll build --destination /out --baseurl "${BASEURL}" \
 # A Jekyll build exits 0 having rendered nothing at all, so the things whose
 # absence would ship silently are asserted here rather than discovered later.
 && pages="$(find /out -name '*.html' | wc -l)" \
 && echo "built ${pages} pages" \
 && test "${pages}" -ge 30 \
 && test -f /out/index.html \
 && test -f /out/assets/site.css \
 && test -f /out/assets/vendor/mermaid.min.js

# ---------------------------------------------------------------------------
FROM nginx:1.27-alpine AS serve

LABEL org.opencontainers.image.title="Egress circuit breaker — documentation"
LABEL org.opencontainers.image.description="The docs/ tree, built and served offline."

# Emptied first: nginx ships its own index.html and 50x.html here, and CI
# deploys whatever this directory holds — so anything left behind is published
# as part of the site.
RUN rm -rf /usr/share/nginx/html/*
COPY --from=build /out /usr/share/nginx/html
COPY docs/nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80
HEALTHCHECK --interval=30s --timeout=3s --start-period=3s \
  CMD wget -qO- http://127.0.0.1/index.html >/dev/null || exit 1
