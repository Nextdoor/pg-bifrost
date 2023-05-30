# Version 0.1.2

# Pick the chart-name based the name of the directory we are in. This can be
# overriden - but it picks the "short name" of the directory.
CHART_NAME ?= $(notdir $(CURDIR))

# By default we look for a `values.local.yaml` file in the directory for the
# chart when it comes to doing local testing.
VALUES ?= values.local.yaml

# Target Namespace - this setting is _usually_ overriden.. but for testing of
# simple charts, the default NS should be fine on a local test cluster.
NAMESPACE ?= default

# If the NAMESPACE parameter is set to an empty string, then we do not set that
# flag OR add the --create-namespace flag. This ensures that a chart can be
# tested against whatever namespace a user is currently logged into. This is
# valuable for certain kinds of "testing" or "benchmarking" charts where you
# might want to selectively just run the chart against an existing application.
ifeq ($(NAMESPACE),default)
else
	export HELM_NAMESPACE=$(NAMESPACE)
	CREATE_NS := --create-namespace
endif

###############################################################################
# Construct the final VALUE_ARGS setting that is used to pass into Helm.
###############################################################################
VALUE_ARGS := --values values.yaml --values $(VALUES)

###############################################################################
# The default make target is a non-destructive call
###############################################################################
.DEFAULT_GOAL := template

###############################################################################
# Cleans up the current workspace
#
# If you set the optional CLEAN_DEPS_TARGET variable, that target will be
# called first during the cleanup process.
###############################################################################
.PHONY: clean
clean: $(CLEAN_DEPS_TARGET)
	rm -rf charts

###############################################################################
# Optional local dependencies that can be installed.
#
# Example - if you need to set up a dependency before your helm chart is run
# that is specific to a local development experience, you can do it like this
###############################################################################
#
# LOCAL_DEPS_TARGET := install_my_thing
#
# install_my_thing:
# 	kubectl apply -f ....
#
# ...
#
# include ../Common.mk


###############################################################################
# If the $VALUES file does not exist, then we create it as an empty file. The
# developer can choose to fill it in if they wish - or not. Helm though requires
# that all files passed into the `--values` flag must exist.
###############################################################################
$(VALUES):
	touch $(VALUES)

###############################################################################
# Sets up the dependencies for any subcharts in the charts/* directory
###############################################################################
.PHONY: deps
deps:
	@echo Pulling dependencies in...
	helm dependency update .

.PHONY: local_postgres
local_postgres:
	@echo createing bitnami postgres release
	helm install my-postgresql bitnami/postgresql -f psql.yaml

###############################################################################
# Shortcut for generating a printed-out template of all the resources that
# will be created for this target. This is the most useful script for testing
# helm chart changes and getting the differences from one build to the next.
###############################################################################
.PHONY: template
template: $(VALUES) deps
	helm template --debug $(VALUE_ARGS) $(CHART_NAME) .

###############################################################################
# If the LOCAL_DEPS_TARGET variable is set, then before we do the install we
# will call that target to do any dependency setup. Most of the time this
# shouldn't be necessary, but in some cases it is.
#
# Please document WHY you are setting this if you do.
###############################################################################
.PHONY: install
install: $(LOCAL_DEPS_TARGET) $(VALUES) local_postgres deps
	helm install $(VALUE_ARGS) $(CREATE_NS) $(CHART_NAME) .

###############################################################################
# Once an initial helm chart has been installed, the user should call the
# upgrade command going forward.
###############################################################################
.PHONY: upgrade
upgrade: $(VALUES)
	helm upgrade --install $(VALUE_ARGS) $(CHART_NAME) .

###############################################################################
# See the status of the current Helm deployment for an pg-bifrost
###############################################################################
.PHONY: status
status:
	helm status $(CHART_NAME)

###############################################################################
# List the helm charts launched in our namespace
###############################################################################
.PHONY: list
list:
	helm list

###############################################################################
# Deleting the charts should fully clean up your environment
# Delete any installed hooks (e.g. localstack hooks)
###############################################################################
.PHONY: uninstall
uninstall: uninstall_local_postgres
	helm delete $(CHART_NAME)
	kubectl -n $(NAMESPACE) delete svc,cm,sa,pod,deployment,ingress,job -l 'app.kubernetes.io/managed-by=Helm,app.kubernetes.io/instance=$(CHART_NAME)'

.PHONY: uninstall_local_postgres
uninstall_local_postgres:
	helm delete my-postgresql


