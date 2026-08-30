# Copyright (c) 2026 Crestway AI LLC. All rights reserved.
# Licensed under the Elastic License 2.0 — see LICENSE. NOT open source:
# publishing this image does not change what the licence permits, and the
# LICENSE file ships inside it so nobody has to go looking.
#
# WHAT THIS IMAGE IS FOR, and what it is not.
#
# It is a DISTRIBUTION artifact: a self-hoster who wants to try Flint, and a
# developer who wants a durable node on their laptop, should not have to build
# Rust and RocksDB or launch an AMI first. That friction is real and this
# removes it.
#
# It is NOT how the managed fleet runs, and should not become that. flintctl
# spawns seats and tracks them by pidfile and port, reaching other hosts via
# `host-spawn` over ssh — one implementation of each invariant, two
# transports. Containerising the data plane means rewriting that surface, and
# every drill and gate hangs off it. The AMI also solves the part that
# actually matters for performance: flint-nvme.service finds the instance
# store by device MODEL (enumeration order swaps between boots) and mounts
# /var/lib/flint on it. A container inherits whatever the host gives it.
#
# BUILD STAGE. Pinned to the toolchain in rust-toolchain.toml on purpose: an
# unpinned compiler reds this repo on Rust's release schedule rather than ours
# (docs/bugs/0039), and that reasoning does not stop at the image boundary.
FROM rust:1.98.0-bookworm AS build

# RocksDB is built from source by the `rocksdb` crate: it needs a C++
# toolchain and libclang for bindgen. None of this survives into the runtime
# stage — the whole point of splitting them.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      clang libclang-dev cmake make g++ \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

# STAMP THE BUILD. .dockerignore excludes .git (it is the largest thing in the
# repo and the build cannot read it anyway), so flint-build falls back to the
# crate version and reports `unstamped`. That word is load-bearing in the
# field — it is how an operator tells a released binary from someone's local
# build — so pass the real version when publishing:
#     docker build --build-arg FLINT_BUILD_VERSION=v0.1.0-rc.65 ...
# and leave it alone for a dev build, which SHOULD say unstamped.
ARG FLINT_BUILD_VERSION=""
ENV FLINT_BUILD_VERSION=${FLINT_BUILD_VERSION}

# --locked: build the dependency graph the repo committed, not whatever
# resolves today. An image that silently picks up a different crate set than
# the release bundle is a different product wearing the same version.
#
# rocks features are NOT optional here. Without them flint-server has no
# durable engine, and a "durable cache" image that quietly runs in memory is
# the worst artifact we could ship.
RUN cargo build --release --locked \
      -p flint-server -p flint-proxy -p flint-controlplane \
      -p flint-controller -p flint-ctl -p flint-backup -p flint-exporter \
      --features flint-server/rocks,flint-backup/rocks

# RUNTIME STAGE. rustls+ring means no OpenSSL at runtime; RocksDB bundles its
# compression libraries statically. That leaves libstdc++ and CA certificates.
FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates libstdc++6 \
 && rm -rf /var/lib/apt/lists/*

COPY --from=build \
     /src/target/release/flint-server \
     /src/target/release/flint-proxy \
     /src/target/release/flint-controlplane \
     /src/target/release/flint-controller \
     /src/target/release/flintctl \
     /src/target/release/flint-backup \
     /src/target/release/flint-exporter \
     /usr/local/bin/
COPY --from=build /src/LICENSE /LICENSE

# Non-root, and the data directory is owned by it. A durable store whose files
# are root-owned inside the container becomes an unreadable volume the moment
# anyone runs the same image with a different user.
# `useradd --system` with an explicit uid of 10001 prints a warning that the
# uid exceeds SYS_UID_MAX. It is cosmetic and this line is the one that was
# BUILT AND TESTED, so it stays: 10001 is worth keeping (it stays clear of
# host uids a bind-mounted volume might collide with), and swapping flags to
# silence a warning would ship a variant nobody has run.
RUN useradd --system --uid 10001 --home-dir /var/lib/flint --shell /usr/sbin/nologin flint \
 && mkdir -p /var/lib/flint \
 && chown flint:flint /var/lib/flint
USER flint

# DECLARED so `docker run` without -v still keeps data across a restart of the
# same container, and so `docker volume` sees it. It does NOT make an
# anonymous volume durable across `docker rm` — mount a named volume or a host
# path for anything you care about; the compose file does.
VOLUME ["/var/lib/flint"]

# 7001 seat, 7379 proxy edge, 7500 control plane, 9464 metrics.
EXPOSE 7001 7379 7500 9464

# rocks, not the in-memory engine: see the build stage. --bind 0.0.0.0 because
# a container that listens only on loopback is reachable from nothing.
ENTRYPOINT ["flint-server"]
CMD ["--port", "7001", "--bind", "0.0.0.0", "--engine", "rocks", "--data-dir", "/var/lib/flint"]
