# Version 0.1.7

###############################################################################
# Helpers for installing tooling..
###############################################################################
# Quick OS detection so we can pull down the right binaries
OSNAME         ?= $(shell uname -s | tr A-Z a-z)
ROOT_DIR       := $(shell git rev-parse --show-toplevel)
BRANCH_NAME    := $(shell git rev-parse --abbrev-ref HEAD | sed 's/[_-]//g')
BIN_DIR        ?= $(ROOT_DIR)/bin
VENV_CMD       := python3 -m venv
VENV_DIR       := $(ROOT_DIR)/.venv
VENV_BIN       := $(VENV_DIR)/bin

# This is the standare kind cluster-name that we use for local development.
KIND_CLUSTER_NAME := default

export PATH    := $(VENV_BIN):$(BIN_DIR):$(PATH)

ifeq ($(OSNAME),linux)
	GET_HELM_DOCS_URL ?= https://github.com/norwoodj/helm-docs/releases/download/v1.5.0/helm-docs_1.5.0_Linux_x86_64.tar.gz
else
	GET_HELM_DOCS_URL ?= https://github.com/norwoodj/helm-docs/releases/download/v1.5.0/helm-docs_1.5.0_Darwin_x86_64.tar.gz
endif

ifeq ($(HELM_TEST_ALL_CHARTS),true)
	ALL_FLAG := --all
endif

HELM_VERSION   ?= v3.12.0
GET_HELM_URL   ?= https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
GET_CT_URL     ?= https://github.com/helm/chart-testing/releases/download/v3.4.0/chart-testing_3.4.0_$(OSNAME)_amd64.tar.gz
GET_KIND_URL   ?= https://kind.sigs.k8s.io/dl/v0.11.1/kind-$(OSNAME)-amd64

# First, try to detect helm/ct in the users local env.. if thats there, we'll
# use it, otherwise fall back to our BIN_DIR which will trigger the Makefile to
# install the tools.
HELM           := $(shell which helm || echo $(BIN_DIR)/helm)
CT             := $(shell which ct || echo $(BIN_DIR)/ct)
KIND           := $(shell which kind || echo $(BIN_DIR)/kind)
HELM_DOCS      := $(shell which helm-docs || echo $(BIN_DIR)/helm-docs)
YAMLLINT       := $(shell which yamllint || echo $(VENV_BIN)/yamllint)
YAMALE         := $(shell which yamale || echo $(VENV_BIN)/yamale)

###############################################################################
# Python-based Pre-reqs
#
# These are support tools needed by the helm chart-testing tool
###############################################################################
.PHONY: venv
venv: $(VENV_BIN)/activate
pytools: $(YAMALE) $(YAMLLINT)

$(VENV_BIN)/activate:
	$(VENV_CMD) $(VENV_DIR)

$(YAMALE): $(VENV_BIN)/activate
	$(VENV_BIN)/pip3 install yamale==3.0.4

$(YAMLLINT): $(VENV_BIN)/activate
	$(VENV_BIN)/pip3 install yamllint==1.25.0

###############################################################################
# Tooling Installation
#
# Most or all of these tools should be installed by dotfiles.git.. but we have
# them here in this Makefile helper just in case.
###############################################################################
.PHONY: helm ct tools kind
helm: $(HELM)
ct: $(CT) pytools
kind: $(KIND)
helm-docs: $(HELM_DOCS)
tools: $(HELM) $(CT) $(KIND) $(HELM_DOCS) pytools

$(HELM):
	@echo "Installing $(HELM) from $(GET_HELM_URL)..."
	@mkdir -p $(BIN_DIR)
	@curl -fsSL $(GET_HELM_URL) | USE_SUDO=false HELM_INSTALL_DIR=$(BIN_DIR) bash -s -- --version $(HELM_VERSION)

$(CT):
	@echo "Installing $(CT) from $(GET_CT_URL)..."
	@mkdir -p $(BIN_DIR)
	@cd $(BIN_DIR) && curl -fsSL $(GET_CT_URL) | tar -zxv ct && chmod +x ct

$(KIND):
	@echo "Downloading $(KIND_URL) to $(KIND)..."
	@mkdir -p $(BIN_DIR)
	@curl -fsSL -o $(KIND) $(GET_KIND_URL) && chmod +x $(KIND)

$(HELM_DOCS):
	@echo "Downloading $(GET_HELM_DOCS_URL) to $(HELM_DOCS)..."
	@mkdir -p $(BIN_DIR)
	@cd $(BIN_DIR) && curl -fsSL $(GET_HELM_DOCS_URL) | tar -zxv helm-docs && chmod +x helm-docs

###############################################################################
# Common Chart Testing Targets
###############################################################################
.PHONY: _helm_clean
_helm_clean:
	@/bin/rm -f charts/*/Chart.lock charts/*/requirements.lock

.PHONY: helm_list_changed
helm_list_changed:
	@cd $(ROOT_DIR) && $(CT) list-changed --config $(ROOT_DIR)/ct.yaml \
			--excluded-charts="$(EXCLUDED_CHARTS_FROM_TESTS)"

.PHONY: helm_lint
helm_lint: _helm_clean
	@cd $(ROOT_DIR) && $(CT) lint --config $(ROOT_DIR)/ct.yaml

.PHONY: helm_docs
helm_docs: $(HELM_DOCS)
	$(HELM_DOCS)
	git diff --exit-code

.PHONY: helm_cluster
helm_cluster: $(KIND)
	$(KIND) create cluster \
		--name "$(KIND_CLUSTER_NAME)" \
		--config "$(ROOT_DIR)/contrib/kind-config.yaml" \
		--image "kindest/node:v1.21.2" \
		--wait 60s

.PHONY: helm_test
helm_test: $(TEST_PREREQS)
	docker run --rm --network host --name ct \
		--volume "$(ROOT_DIR)/ct.yaml:/etc/ct/ct.yaml" \
		--volume "$(ROOT_DIR):/workdir" \
		--volume "$(HOME)/.kube/config:/root/.kube/config" \
		--workdir /workdir \
		quay.io/helmpack/chart-testing:v3.4.0 \
		ct install \
			--excluded-charts="$(EXCLUDED_CHARTS_FROM_TESTS)" \
			$(ALL_FLAG)
