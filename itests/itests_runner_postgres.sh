#!/bin/bash

# Copyright 2019 Nextdoor.com, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

# List of postgres versions to test
POSTGRES_VERSIONS_LIST=${ITEST_POSTGRES_VERSIONS_LIST:-9.6 10.7 11.2}

run_itests() {
    # Source the kinesis transport sink's context
    set -a ; . contexts/kinesis.env ; set +a

    # Select the single test
    TEST_NAME=base/test_basic

    # Start itests
    echo '' ; echo '################################'
    echo "Starting postgres versions itests:"
    echo '################################' ; echo ''

    echo "POSTGRES_VERSIONS_LIST:"
    echo $POSTGRES_VERSIONS_LIST | tr ' ' '\n' ; echo ''

    for POSTGRES_VERSION in $POSTGRES_VERSIONS_LIST
    do
       # Set postgres version for the docker-compose file
       export POSTGRES_VERSION

       TEST_NAME=$TEST_NAME docker-compose -f docker-compose.yml build

       echo "running test postgres:$POSTGRES_VERSION"
       ./integration_tests.bats -r tests -f $TEST_NAME

       echo "Cleaning up containers and volumes..."
       TEST_NAME=$TEST_NAME docker-compose -f docker-compose.yml rm -f -s -v
    done

    echo '' ; echo '################################'
    echo "postgres versions itests successful!"
    echo '################################' ; echo ''
}

# Start the itests
run_itests
