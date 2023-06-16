# Makefile for building and testing

GO_LDFLAGS ?= -w -extldflags "-static"

GIT_REVISION := $(shell git rev-parse --short HEAD)
GIT_TAG_VERSION := $(shell git tag -l --points-at HEAD | grep -v latest)

ifeq ($(CI),true)
	GO_TEST_EXTRAS ?= "-coverprofile=c.out"
	GO_LDFLAGS += -X main.GitRevision=$(GIT_REVISION) -X main.Version=$(GIT_TAG_VERSION)
endif

vendor: go.sum go.mod
	go mod vendor -v

check_imports:
	@echo "Checking goimports for imports and formatting..."
	goimports -l -d .
	@goimports -l -d . | xargs echo | xargs test -z 2> /dev/null

lint: vet check_imports
	@echo "Running golangci-lint"
	golangci-lint run

vet:
	@echo "Running go vet ..."
	go list ./...  | xargs go vet

generate:
	@if [ -z "$(CI)" ]; then \
		echo "Running go generate ..." ;\
		go generate ./... ;\
	fi

test: generate check_imports
	go clean -testcache || true
	@echo "Executing tests ..."
	go test -race -v ${GO_TEST_EXTRAS} ./...

itests:
	@echo "Running integration tests"
	cd ./itests && ./itests_runner.sh

clean:
	@echo "Cleaning build cache"
	go clean -cache
	@echo "Cleaning test cache"
	go clean -testcache
	@echo "Cleaning binary"
	rm -rf target || true

build:
	@echo "Creating GO binary"
	mkdir -p target
	CGO_ENABLED=0 GOOS=linux go build -ldflags "$(GO_LDFLAGS)" -o target/pg-bifrost github.com/Nextdoor/pg-bifrost.git/main

build_mac: generate
	@echo "Creating GO binary"
	mkdir -p target
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -o target/pg-bifrost github.com/Nextdoor/pg-bifrost.git/main

# Standard settings that will be used later
DOCKER := $(shell which docker)

docker_build: generate
	@echo "Building pg-bifrost docker image"
	@$(DOCKER) build -t "pg-bifrost:latest" --build-arg is_ci="${CI}" .

docker_get_binary:
	@echo "Copying binary from docker image"
	mkdir -p target
	@$(DOCKER) rm "pg-bifrost-build" || true
	@$(DOCKER) create --name "pg-bifrost-build" "pg-bifrost:latest" /pg-bifrost
	@$(DOCKER) cp "pg-bifrost-build":/pg-bifrost target/
	@$(DOCKER) rm "pg-bifrost-build"

.PHONY: clean test itests docker_build docker_get_binary vendor lint check_imports
