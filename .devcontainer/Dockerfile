# SPDX-License-Identifier: Elastic-2.0
# A container that can build Flint AND run its whole gate — not just cargo.
FROM mcr.microsoft.com/devcontainers/rust:1-bookworm

# RocksDB is compiled from source by the `rocksdb` crate, so a C++ toolchain
# and libclang are hard requirements, not conveniences. python3 is used by
# several drills; jq by the release tooling.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential clang libclang-dev pkg-config libssl-dev \
        python3 jq lsof \
    && rm -rf /var/lib/apt/lists/*

# The conformance oracle, built the same way .github/workflows/gate.yml builds
# it — same pinned version, same BUILD_TLS. Three things are load-bearing:
#
#   * NOT apt. Valkey forked from Redis in March 2024, after the Debian
#     bookworm freeze, so there is no valkey-server to install.
#   * NOT redis-tools with a symlink. redis-cli under another name would make
#     the compatibility oracle Redis while the corpus claims Valkey — a
#     substituted reference is worse than a missing one.
#   * BUILD_TLS=yes is REQUIRED. Valkey's default make produces a valkey-cli
#     with no --tls flag, and cert_reload_fleet_drill.sh dials a client-tls
#     fleet with it. Without TLS that drill reports a cert-hot-reload product
#     bug that does not exist.
#
# Pinned, not "latest": a floating oracle turns a Valkey release into a
# mysterious Flint conformance failure.
ARG VALKEY_VERSION=9.1.0
RUN git clone --depth 1 --branch ${VALKEY_VERSION} \
        https://github.com/valkey-io/valkey.git /tmp/valkey \
    && make -C /tmp/valkey BUILD_TLS=yes -j"$(nproc)" \
    && make -C /tmp/valkey install \
    && rm -rf /tmp/valkey \
    && valkey-server --version && valkey-cli --version
