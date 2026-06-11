#!/bin/sh
# Content-addressed tag for commit-invariant docker images published to ghcr
# (golang, ubuntu 18.04/22.04, base), see dockertests.yml & load_docker_common.
# Covers every build input: Dockerfiles, COPY'd config, submodule gitlinks
# (golang image compiles brotli from submodules/)
set -eu
cd "$(dirname "$0")"
{
    cat docker/golang/Dockerfile \
        docker/ubuntu/Dockerfile \
        docker/ubuntu/Dockerfile_22_04 \
        docker/base/Dockerfile
    find docker/ubuntu/config -type f | sort | xargs -r sha256sum
    git ls-tree HEAD submodules
} | sha256sum | cut -c1-16
