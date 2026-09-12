# The documentation site, as an image you can read on a train.
#
#   docker build -t egress-site docs
#   docker run --rm -p 4000:80 egress-site     # then http://localhost:4000
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
COPY Gemfile Gemfile.lock ./
RUN bundle config set --local without "" \
 && bundle install --jobs 4 --retry 3

# Mermaid is vendored rather than loaded from a CDN. Diagrams are content here —
# four of these documents explain themselves with one — and an image whose whole
# point is reading offline cannot fetch a renderer at page load. The layout
# falls back to the CDN if this file is absent, so a bare `jekyll serve` with no
# vendor step still draws them while online.
ARG MERMAID_VERSION=11.4.1
RUN mkdir -p assets/vendor \
 && curl -fsSL --retry 3 \
      "https://cdn.jsdelivr.net/npm/mermaid@${MERMAID_VERSION}/dist/mermaid.min.js" \
      -o assets/vendor/mermaid.min.js \
 && test -s assets/vendor/mermaid.min.js

COPY . .

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
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80
HEALTHCHECK --interval=30s --timeout=3s --start-period=3s \
  CMD wget -qO- http://127.0.0.1/index.html >/dev/null || exit 1
