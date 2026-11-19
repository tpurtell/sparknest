# sparknestd in a container: an off-grid host (ADR-047) on any Docker host,
# a NAS above all. See packaging/docker/README.md.
#
# Built from a published release tarball, for amd64 and arm64:
#   docker buildx build --platform linux/amd64,linux/arm64 \
#     --build-arg VERSION=0.9.0 -t sparknest:0.9.0 packaging/docker
# Or from a local tarball (scripts/release.sh output), for one platform:
#   docker build --build-arg VERSION=0.9.0 \
#     --build-context dist=dist/release/v0.9.0 -t sparknest:0.9.0 packaging/docker
#
# SPDX-License-Identifier: MIT OR Apache-2.0

ARG UBUNTU=24.04

# Local release tarballs, if `--build-context dist=DIR` names a directory of
# them; empty otherwise.
FROM scratch AS dist

# The release tarball for this platform: from `dist`, else downloaded from
# the GitHub release and checked against its SHA256SUMS.
FROM ubuntu:${UBUNTU} AS fetch
ARG VERSION
ARG TARGETARCH
ARG REPO=tpurtell/sparknest
RUN test -n "$VERSION" || { echo "build with --build-arg VERSION=x.y.z" >&2; exit 1; }
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /fetch
COPY --from=dist . /fetch/
RUN set -eu; \
    case "$TARGETARCH" in \
      amd64) arch=x86_64 ;; arm64) arch=aarch64 ;; \
      *) echo "unsupported architecture $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    tgz="sparknest-${VERSION}-linux-${arch}.tar.gz"; \
    if [ ! -f "$tgz" ]; then \
      curl -fsSL -o "$tgz" "https://github.com/${REPO}/releases/download/v${VERSION}/${tgz}"; \
      curl -fsSL -o SHA256SUMS "https://github.com/${REPO}/releases/download/v${VERSION}/SHA256SUMS"; \
    fi; \
    if [ -f SHA256SUMS ]; then grep " ${tgz}\$" SHA256SUMS | sha256sum -c -; fi; \
    mkdir /opt/sparknest; \
    tar xzf "$tgz" -C /opt/sparknest --strip-components=1

FROM ubuntu:${UBUNTU}
ARG VERSION
LABEL org.opencontainers.image.title="sparknest" \
      org.opencontainers.image.description="sparknest node daemon (sparknestd) as an off-grid host" \
      org.opencontainers.image.source="https://github.com/tpurtell/sparknest" \
      org.opencontainers.image.licenses="MIT OR Apache-2.0" \
      org.opencontainers.image.version="${VERSION}"
# libibverbs: sparknestd links it (RDMA), though an off-grid host never uses
# it. fuse3: fusermount3, only for a container that mounts the namespace.
RUN apt-get update \
 && apt-get install -y --no-install-recommends libibverbs1 fuse3 ca-certificates tini \
 && rm -rf /var/lib/apt/lists/* \
 && echo user_allow_other >> /etc/fuse.conf
COPY --from=fetch /opt/sparknest /opt/sparknest
COPY entrypoint.sh /usr/local/bin/sparknest-entrypoint
RUN chmod 0755 /usr/local/bin/sparknest-entrypoint \
 && ln -s /opt/sparknest/bin/sparknestd /opt/sparknest/bin/nest /usr/local/bin/
ENV SPARKNEST_CONFIG=/srv/sparknest/node.toml
# The state dir: node.toml, cluster.secret, metadata and this host's objects.
VOLUME ["/srv/sparknest"]
# Control (Raft, data service) and the management API / web UI.
EXPOSE 7400 7401
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/sparknest-entrypoint"]
