# Makefile for building and testing

GO_LDFLAGS ?= -w -extldflags "-static"

ifeq ($(CI),true)
	GO_TEST_EXTRAS ?= "-coverprofile=c.out"
	GIT_REVISION := $(shell git rev-parse --short HEAD)
	VERSION := $(shell git tag -l --points-at HEAD)
	GO_LDFLAGS += -X main.GitRevision=$(GIT_REVISION) -X main.Version=$(VERSION)
endif

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
	ln -f target/pg-bifrost itests/containers/pg-bifrost/app/pg-bifrost

.PHONY: clean test itests
