# Makefile for building and testing

GO_LDFLAGS ?= -w -extldflags "-static"

GIT_REVISION := $(shell git rev-parse --short HEAD)
GIT_TAG_VERSION := $(shell git tag -l --points-at HEAD | grep -v latest)

ifeq ($(CI),true)
	GO_TEST_EXTRAS ?= "-coverprofile=c.out"
	GO_LDFLAGS += -X main.GitRevision=$(GIT_REVISION) -X main.Version=$(GIT_TAG_VERSION)
endif

vendor:
	dep ensure --vendor-only

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

itests-functional:
	@echo "Running functional integration tests"
	cd ./itests && ./itests_runner_functional.sh

itests-postgres:
	@echo "Running postgres integration tests"
	cd ./itests && ./itests_runner_postgres.sh

itests: itests-functional itests-postgres

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

build_mac: vendor generate
	@echo "Creating GO binary"
	mkdir -p target
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -o target/pg-bifrost github.com/Nextdoor/pg-bifrost.git/main

# Standard settings that will be used later
DOCKER := $(shell which docker)

docker_build:
	@echo "Building pg-bifrost docker image"
	@$(DOCKER) build -t "pg-bifrost:latest" --build-arg is_ci="${CI}" .

docker_get_binary:
	@echo "Copying binary from docker image"
	mkdir -p target
	@$(DOCKER) rm "pg-bifrost-build" || true
	@$(DOCKER) create --name "pg-bifrost-build" "pg-bifrost:latest" /pg-bifrost
	@$(DOCKER) cp "pg-bifrost-build":/pg-bifrost target/
	@$(DOCKER) rm "pg-bifrost-build"

.PHONY: clean test itests-functional itests-postgres itests docker_build docker_get_binary
