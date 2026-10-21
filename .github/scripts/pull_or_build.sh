#!/bin/sh
# Pull a prebuilt image and tag it locally; build it with docker compose if the
# pull fails or if the Dockerfile is newer than the pulled image.
# usage: pull_or_build.sh <remote_image> <local_tag> <dockerfile> <compose_service>
set -eu

remote=$1
local_tag=$2
dockerfile=$3
service=$4

if docker pull "$remote"; then
    docker tag "$remote" "$local_tag"
    image_ts=$(date -d "$(docker inspect -f '{{.Created}}' "$remote")" +%s)
    if ! git diff --quiet HEAD -- "$dockerfile"; then
        dockerfile_ts=$(date +%s)
    else
        dockerfile_ts=$(git log -1 --format=%ct -- "$dockerfile")
    fi
    if [ -n "$dockerfile_ts" ] && [ "$dockerfile_ts" -gt "$image_ts" ]; then
        echo "$dockerfile is newer than the downloaded image, rebuilding"
        docker compose build "$service"
    fi
else
    docker compose build "$service"
fi
