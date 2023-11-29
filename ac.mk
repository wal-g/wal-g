# Copyright AppsCode Inc. and Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

SHELL=/bin/bash -o pipefail

BIN      := wal-g

# Where to push the docker image.
REGISTRY ?= ghcr.io/kubedb
SRC_REG  ?=

# This version-strategy uses git tags to set the version string
git_branch       := $(shell git rev-parse --abbrev-ref HEAD)
git_tag          := $(shell git describe --exact-match --abbrev=0 2>/dev/null || echo "")
commit_hash      := $(shell git rev-parse --verify HEAD)
commit_timestamp := $(shell date --date="@$$(git show -s --format=%ct)" --utc +%FT%T)

VERSION          := $(shell git describe --tags --always --dirty)
version_strategy := commit_hash
ifdef git_tag
	VERSION := $(git_tag)
	version_strategy := tag
else
	ifeq (,$(findstring $(git_branch),master HEAD))
		ifneq (,$(patsubst release-%,,$(git_branch)))
			VERSION := $(git_branch)
			version_strategy := branch
		endif
	endif
endif

DATABASES ?= fdb gp mongo mysql pg redis sqlserver
DB        ?= pg

###
### These variables should not need tweaking.
###

DOCKER_PLATFORMS := linux/amd64 linux/arm64
BIN_PLATFORMS    := $(DOCKER_PLATFORMS)

# Used internally.  Users should pass GOOS and/or GOARCH.
OS   := $(if $(GOOS),$(GOOS),$(shell go env GOOS))
ARCH := $(if $(GOARCH),$(GOARCH),$(shell go env GOARCH))

# bash required
BASEIMAGE        ?= debian:bookworm

IMAGE            := $(REGISTRY)/$(BIN)
VERSION          := $(VERSION)_$(DB)
TAG              := $(VERSION)_$(OS)_$(ARCH)

GO_VERSION       ?= 1.21

# Directories that we need created to build/test.
BUILD_DIRS  := bin/$(OS)_$(ARCH)

DOCKERFILE  = Dockerfile

# If you want to build all binaries, see the 'all-build' rule.
# If you want to build all containers, see the 'all-container' rule.
# If you want to build AND push all containers, see the 'all-push' rule.
all: fmt build

# For the following OS/ARCH expansions, we transform OS/ARCH into OS_ARCH
# because make pattern rules don't match with embedded '/' characters.

build-%:
	@$(MAKE) build                        \
		-f ac.mk                          \
		--no-print-directory              \
		GOOS=$(firstword $(subst _, ,$*)) \
		GOARCH=$(lastword $(subst _, ,$*))

container-%:
	@$(MAKE) container                    \
		-f ac.mk                          \
		--no-print-directory              \
		DB=$(firstword $(subst _, ,$*))   \
		GOOS=$(word 2, $(subst _, ,$*))   \
		GOARCH=$(lastword $(subst _, ,$*))

push-%:
	@$(MAKE) push                         \
		-f ac.mk                          \
		--no-print-directory              \
		DB=$(firstword $(subst _, ,$*))   \
		GOOS=$(word 2, $(subst _, ,$*))   \
		GOARCH=$(lastword $(subst _, ,$*))

docker-manifest-%:
	@$(MAKE) docker-manifest              \
		-f ac.mk                          \
		--no-print-directory              \
		DB=$*

all-container: $(addprefix container-, $(subst /,_, $(foreach X,$(DATABASES),$(foreach Y,$(DOCKER_PLATFORMS),$X/$Y))))

all-push: $(addprefix push-, $(subst /,_, $(foreach X,$(DATABASES),$(foreach Y,$(DOCKER_PLATFORMS),$X/$Y))))

all-docker-manifest: $(addprefix docker-manifest-, $(subst /,_, $(DATABASES)))

version:
	@echo IMAGE=$(IMAGE)
	@echo TAG=$(TAG)
	@echo BIN=$(BIN)
	@echo version=$(VERSION)
	@echo version_strategy=$(version_strategy)
	@echo git_tag=$(git_tag)
	@echo git_branch=$(git_branch)
	@echo commit_hash=$(commit_hash)
	@echo commit_timestamp=$(commit_timestamp)

# Used to track state in hidden files.
DOTFILE_IMAGE    = $(subst /,_,$(IMAGE))-$(TAG)

container: bin/.container-$(DOTFILE_IMAGE)
ifeq (,$(SRC_REG))
bin/.container-$(DOTFILE_IMAGE): $(BUILD_DIRS) $(DOCKERFILE)
	@echo "container: $(IMAGE):$(TAG)"
	@sed \
		-e 's|{ARG_FROM}|$(BASEIMAGE)|g'    \
		-e 's|{GO_VERSION}|$(GO_VERSION)|g' \
		$(DOCKERFILE) > bin/.dockerfile-$(DB)-$(OS)_$(ARCH)
	@docker build --platform $(OS)/$(ARCH) --build-arg="DB=$(DB)" --load --pull -t $(IMAGE):$(TAG) -f bin/.dockerfile-$(DB)-$(OS)_$(ARCH) .
	@docker images -q $(IMAGE):$(TAG) > $@
	@echo
else
bin/.container-$(DOTFILE_IMAGE):
	@echo "container: $(IMAGE):$(TAG)"
	@docker tag $(SRC_REG)/$(BIN):$(TAG) $(IMAGE):$(TAG)
	@echo
endif

push: bin/.push-$(DOTFILE_IMAGE)
bin/.push-$(DOTFILE_IMAGE): bin/.container-$(DOTFILE_IMAGE)
	@docker push $(IMAGE):$(TAG)
	@echo "pushed: $(IMAGE):$(TAG)"
	@echo

.PHONY: docker-manifest
docker-manifest:
	docker manifest create -a $(IMAGE):$(VERSION) $(foreach PLATFORM,$(DOCKER_PLATFORMS),$(IMAGE):$(VERSION)_$(subst /,_,$(PLATFORM)))
	docker manifest push $(IMAGE):$(VERSION)

$(BUILD_DIRS):
	@mkdir -p $@

.PHONY: dev
dev: gen fmt push

.PHONY: verify
verify: verify-modules

.PHONY: verify-modules
verify-modules:
	go mod tidy
	go mod vendor
	@if !(git diff --exit-code HEAD); then \
		echo "go module files are out of date"; exit 1; \
	fi

.PHONY: release
release:
	@$(MAKE) all-push all-docker-manifest -f ac.mk --no-print-directory

.PHONY: clean
clean:
	rm -rf .go bin
