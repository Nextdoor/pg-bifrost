# Makefile for building and testing

GO_LDFLAGS ?= -w -extldflags "-static"

ifeq ($(CI),true)
	GO_TEST_EXTRAS ?= "-coverprofile=c.out"
	GIT_REVISION := $(shell git rev-parse --short HEAD)
	VERSION := $(shell git tag -l --points-at HEAD)
	GO_LDFLAGS += -X main.GitRevision=$(GIT_REVISION) -X main.Version=$(VERSION)
endif

SHA1 := $(shell git rev-parse --short HEAD)
BRANCH := $(shell basename $(shell git symbolic-ref HEAD))

vendor:
	dep ensure

vet:
	@echo "Running go vet ..."
	go list ./...  | xargs go vet

generate:
	@if [ -z "$(CI)" ]; then \
		echo "Running go generate ..." ;\
		go generate ./... ;\
	fi

test: vendor generate vet
	go clean -testcache || true
	@echo "Executing tests ..."
	go test -race -v ${GO_TEST_EXTRAS} ./...

itests-circleci:
	@echo "Running integration tests"
	cd ./itests && ./circleci_split_itests.sh

itests:
	@echo "Running integration tests"
	cd ./itests && ./integration_tests.bats -r tests

docker:
	@echo "Building docker integration test images"
	TEST_NAME=test_basic docker-compose -f itests/docker-compose.yml build

clean:
	@echo "Removing vendor deps"
	rm -rf vendor
	@echo "Cleaning build cache"
	go clean -cache
	@echo "Cleaning test cache"
	go clean -testcache
	@echo "Cleaning binary"
	rm -rf target || true

build: vendor generate
	@echo "Creating GO binary"
	mkdir -p target
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "$(GO_LDFLAGS)" -o target/pg-bifrost github.com/Nextdoor/pg-bifrost.git/main


# Standard settings that will be used later
DOCKER := $(shell which docker)

# DOCKER_TAG is only used in the `docker_tag` target below. Its used when you
# take your built image, and you want to tag it prior to uploading it to a
# target repository.
ifeq ($(BRANCH),master)
    DOCKER_TAG ?= latest
else
    DOCKER_TAG ?= $(VERSION)
endif

DOCKER_IMAGE ?= pg-bifrost
DOCKER_REGISTRY ?= hub.corp.nextdoor.com
DOCKER_NAMESPACE ?= nextdoor

# Create two "fully qualified" image names. One is our "local build" name --
# its used when we create, run, and stop the image locally. The second is our
# "target build" name, which is used as a final destination when uploading the
# image to a repository.
LOCAL_DOCKER_NAME ?= "${DOCKER_IMAGE}:${SHA1}"
TARGET_DOCKER_NAME := "${DOCKER_REGISTRY}/${DOCKER_NAMESPACE}/${DOCKER_IMAGE}:${DOCKER_TAG}"

docker_login:
	@echo "Logging into ${DOCKER_REGISTRY}"
	@$(DOCKER) login \
		-u "${DOCKER_USER}" \
		-p "$(value DOCKER_PASS)" "${DOCKER_REGISTRY}"

docker_populate_cache:
	@echo "Attempting to download ${DOCKER_IMAGE}"
	@$(DOCKER) pull "${DOCKER_REGISTRY}/${DOCKER_NAMESPACE}/${DOCKER_IMAGE}" && \
		$(DOCKER) images -a || exit 0

docker_build:
	@echo "Building ${LOCAL_DOCKER_NAME}"
	    @$(DOCKER) build -t "${LOCAL_DOCKER_NAME}" .

docker_stop:
	@echo "Stopping ${LOCAL_DOCKER_NAME}"
	@$(DOCKER) stop "${DOCKER_IMAGE}" && $(DOCKER) rm "${DOCKER_IMAGE}" \
		|| echo "No existing container running."

docker_tag:
	@echo "Tagging ${LOCAL_DOCKER_NAME} as ${TARGET_DOCKER_NAME}"
	@$(DOCKER) tag "${LOCAL_DOCKER_NAME}" "${TARGET_DOCKER_NAME}"

docker_push: docker_tag
	@echo "Pushing ${LOCAL_DOCKER_NAME} to ${TARGET_DOCKER_NAME}"
	@$(DOCKER) push "${TARGET_DOCKER_NAME}"

.PHONY: clean test itests docker_tag docker_push docker_build docker_populate_cache docker_login
