#
# Version 0.2.0b - 12/22/2021
# Source: https://github.com/Nextdoor/ecr-subsys
#
ROOT_DIR      := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

# The AWS_BIN variable can be overridden by the calling Makefile if they have
# their own location for it. As long as the binary exists, we're good.
AWS_BIN       ?= $(shell which aws || echo aws)

# The repository name should be easy enough to get - We append the .git so that
# we are matching exactly how it looks in Github.
_REPO_NAME         := $(shell basename `git config --get remote.origin.url` 2>/dev/null || pwd)
REPO_NAME          := $(shell basename -s .git $(_REPO_NAME))

# Repo information that informs our tags
SHA1               := $(shell git rev-parse --short HEAD)
BRANCH             := $(shell basename $(shell git symbolic-ref HEAD))

# see go/ecr
ECR_REGION         := us-west-2
ECR_ACCOUNT_ID     := 364942603424
ECR_REGISTRY       := $(ECR_ACCOUNT_ID).dkr.ecr.$(ECR_REGION).amazonaws.com
ECR_NAMESPACE      ?= nextdoor

# When doing Kubernetes development, we use "kind" to run clusters locally and
# in our CI systems. The standard cluster-name is "default". We have a few
# hooks in this Docker build process to help you side-load your locally built
# images into the Kubernetes cluster.
KIND               ?= $(shell which kind || echo kind)
KIND_CLUSTER_NAME  ?= default

# This setting is used by the docker_buildx target which builds a multiarch
# image. The difference with this build command is that it builds _and pushes_
# the image all in one step because it must create a "manifest" along with the
# two images and push them all together.
#
# This is considered experimental, but is intended to support us building
# multi-arch images that run natively on our M1 Laptops, or even ARM64-based
# EC2 Instances.
PLATFORMS          ?= linux/amd64

# Dynamically discover the docker-buildx plugin based on the few locations
# it should be. If it is not in our path, this will be evaluated to "docker-buildx"
# and this is used as our build-target to auto-install the docker buildx plugin
# if it isn't there.
DOCKER_BUILDX := $(shell PATH=/Applications/Docker.app/Contents/Resources/cli-plugins:~/.docker/cli-plugins:$$PATH which docker-buildx || echo docker-buildx)
DOCKER_BUILDX_VER := v0.7.1
DOCKER_BUILDX_URL := https://github.com/docker/buildx/releases/download/$(DOCKER_BUILDX_VER)/buildx-$(DOCKER_BUILDX_VER).$(shell uname -s | tr '[:upper:]' '[:lower:]')-$(shell arch | sed 's/x86_64/amd64/')

# This setup builds consistent DOCKER_TAG logic that we simplifies our build
# processes and reduces the amount of clutter in our .circle/config.yaml
# files.
#
# On master/main branch builds, we always use the "main-$SHA1" tag. Period.
# On production branch builds, we use "release-$SHA1".
# On any other branch build, we use "test-$SHA1"
# On any build where $CIRCLE_TAG is set, we use "release-$CIRCLE_TAG".
#
# At any point, if DOCKER_TAG was set outside of this Makefile, then go ahead
# and use that value.
ifeq ($(CIRCLE_TAG),)
  ifeq ($(BRANCH),main)
    DOCKER_TAG ?= main-$(SHA1)
  endif
  ifeq ($(BRANCH),master)
    DOCKER_TAG ?= main-$(SHA1)
  endif
  ifeq ($(BRANCH),production)
    DOCKER_TAG ?= release-$(SHA1)
  else
    DOCKER_TAG ?= test-$(SHA1)
  endif
else
  DOCKER_TAG ?= release-$(CIRCLE_TAG)
endif

# If this is a Linux host, we need to pre-install some packages via apt to make
# multi-arch building work completely properly. So if the Uname matches Linux,
# then we run the "docker_buildx_apt_prep" target before we run the
# "docker_buildx_apt_prep" target.
UNAME := $(shell uname)
ifeq ($(UNAME),Linux)
	BUILDX_CREATE_PREP_STEP := docker_buildx_apt_prep
endif

# Docker Build Flags
DOCKER             ?= $(shell which docker)
DOCKERFILE         ?= .
DOCKER_IMAGE       ?= $(REPO_NAME)
DOCKER_NAME        ?= $(ECR_NAMESPACE)/$(DOCKER_IMAGE):$(DOCKER_TAG)
DOCKER_FQDN        ?= $(ECR_REGISTRY)/$(DOCKER_NAME)
DOCKER_BUILDX_CACHE_REF := $(ECR_REGISTRY)/$(ECR_NAMESPACE)/$(DOCKER_IMAGE):buildcache
CONTAINER_NAME     := $(REPO_NAME)

# This tag is used for most Kubernetes local development - "sideloading" docker
# images into the Kubernetes environment after the Docker build command, we use
# this tag as the target tag.
LOCAL_IMAGE_TAG    ?= local

# This is a simple attempt to install the missing awscli command if it cannot
# be found. Its better for the virtual environment to already be loaded up (or
# the awscli to be installed via Apt) ahead of this..
aws_bin: $(AWS_BIN)
$(AWS_BIN):
	pip install awscli

# This target is primarily used by CircleCI - local developers should already
# have the ecr-login helper in place.
.PHONY: ecr_login
ecr_login: $(AWS_BIN)
	@echo "Getting Amazon ECR Credentials..."
	$(AWS_BIN) --region $(ECR_REGION) ecr get-login-password | \
		docker login --username AWS --password-stdin $(ECR_ACCOUNT_ID).dkr.ecr.$(ECR_REGION).amazonaws.com

.PHONY: docker_pull
docker_pull:
	$(DOCKER) pull $(DOCKER_FQDN)
	$(DOCKER) tag $(DOCKER_FQDN) $(DOCKER_IMAGE)

.PHONY: docker_build
docker_build:
	@echo "Building pg-bifrost docker image"
	@$(DOCKER) build  $(DOCKERFILE) \
		--build-arg is_ci="${CI}" \
		-t $(DOCKER_IMAGE) \
		-t $(DOCKER_IMAGE):$(DOCKER_TAG) \
		-t $(DOCKER_NAME) \
		-t $(DOCKER_FQDN)



docker_buildx_install: $(DOCKER_BUILDX)
$(DOCKER_BUILDX):
	@echo "Docker-buildx plugin not found. Installing it now..."
	mkdir -vp ~/.docker/cli-plugins/ && \
	curl --silent -L "$(DOCKER_BUILDX_URL)" -o ~/.docker/cli-plugins/docker-buildx && \
	chmod a+x ~/.docker/cli-plugins/docker-buildx && \
	$(DOCKER) buildx version && \
	$(DOCKER) buildx install

# If we are on Linux, install the qemu tools..
# https://medium.com/@artur.klauser/building-multi-architecture-docker-images-with-buildx-27d80f7e2408
.PHONY: docker_buildx_apt_prep
docker_buildx_apt_prep:
	sudo apt-get update && sudo apt-get install -y binfmt-support qemu-user-static
	$(DOCKER) run --rm --privileged multiarch/qemu-user-static --reset -p yes

# This make target is only intended for running in CircleCI/Github Actions. Use
# this to prepare the CI environment to run a Multi-Arch build.
.PHONY: docker_buildx_create
docker_buildx_create: $(BUILDX_CREATE_PREP_STEP) $(DOCKER_BUILDX)
	$(DOCKER) run --rm --privileged tonistiigi/binfmt:latest --install "$(PLATFORMS)" ;\
	$(DOCKER) context create buildcontext
	$(DOCKER) buildx create buildcontext --use

.PHONY: docker_buildx
docker_buildx: docker_buildx_install
	DOCKER_BUILDKIT=1 $(DOCKER) buildx build \
		$(EXTRA_ARGS) \
		--cache-from type=local,src=.buildx_cache \
		--cache-to type=local,dest=.buildx_cache \
		--platform $(PLATFORMS) \
		--build-arg BUILD_SHA1=$(SHA1) \
		-t $(DOCKER_FQDN) \
		$(DOCKERFILE) \

# This build target ensures that the docker push happens from within the buildx container, which is necessary jk
# NOTE: The --cache-to feature does not work on ECR. See https://github.com/aws/containers-roadmap/issues/876.
.PHONY: docker_buildx_and_push
docker_buildx_and_push: EXTRA_ARGS=--push
docker_buildx_and_push: docker_buildx

.PHONY: docker_buildx_and_load
docker_buildx_and_load: EXTRA_ARGS=--load
docker_buildx_and_load: docker_buildx

.PHONY: docker_tag
docker_tag:
	$(DOCKER) tag $(DOCKER_IMAGE) $(DOCKER_FQDN)

.PHONY: docker_push
docker_push: docker_tag
	$(DOCKER) push $(DOCKER_FQDN)

.PHONY: docker_sideload
docker_sideload:
	@if [ ! -x $(KIND) ]; then echo 'Missing "kind" binary. Try "make tools"?'; exit 1; fi
	$(DOCKER) tag "$(DOCKER_FQDN)" "$(DOCKER_IMAGE):${LOCAL_IMAGE_TAG}" && \
		$(KIND) load docker-image --name "$(KIND_CLUSTER_NAME)" "$(DOCKER_IMAGE):${LOCAL_IMAGE_TAG}"
