#!/bin/sh
# Third-party Docker Hub images started by CI compose services
# (graphite & fdb excluded: no CI job starts those services).
# Test jobs load these from a tar passed via actions/cache, refreshed by
# buildimages from ghcr mirror (mirror-dockerhub.yml) with Docker Hub fallback;
# anonymous Hub pulls from shared GHA runner IPs hit per-IP rate limits
set -eu
IMAGES='openstackswift/saio:py3'

case "${1-}" in
list)
	echo "$IMAGES";;
pull)
	for image in $IMAGES; do
		mirror="ghcr.io/wal-g/dockerhub/$image"
		if docker pull "$mirror"; then
			docker tag "$mirror" "$image"
		else
			docker pull "$image"
		fi
	done;;
save)
	"$0" pull
	docker save -o "$2" $IMAGES;;
load)
	# cache entry may be evicted between buildimages & test job, repull
	if [ -f "$2" ]; then
		docker load -i "$2"
	else
		"$0" pull
	fi;;
*)
	echo "usage: $0 list|pull|save <tar>|load <tar>" >&2
	exit 1;;
esac
