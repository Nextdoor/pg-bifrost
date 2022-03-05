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

# Used to manually run itests against a single transport sink
ITESTS_TRANSPORT_SINK=${ITESTS_TRANSPORT_SINK:-}

# Used to manually run a specific test
ITESTS_TEST=${ITESTS_TEST:-}

get_testfiles() {
    _transport_sink=$1

    if [ "$CI" == "true" ]; then
        _base_testfiles=$(cd tests/base && circleci tests glob "*" | circleci tests split --split-by=timings | sed -e 's/^/base\//')
        if [ -d "tests/${_transport_sink}" ]; then
            _specific_testfiles=$(cd tests/${_transport_sink} && circleci tests glob "*" | circleci tests split --split-by=timings | sed -e "s/^/${_transport_sink}\//")
        fi
    else
        _base_testfiles=$(cd tests/base && ls -d */ | sed 's#/##' | sed -e 's/^/base\//')
        if [ -d "tests/${_transport_sink}" ]; then
            _specific_testfiles=$(cd tests/${_transport_sink} && ls -d */ | sed 's#/##' | sed -e "s/^/${_transport_sink}\//")
        fi
    fi

    echo "${_base_testfiles} ${_specific_testfiles}"
}

run_itests_on_transport_sink() {
    _file=$1

    # Source the transport sink's context
    set -a ; . $_file ; set +a

    # Setup docker images & get test cases
    TEST_NAME=base/test_basic docker-compose -f docker-compose.yml build
    TESTFILES=$(get_testfiles $TRANSPORT_SINK)

    # Start itests
    echo '' ; echo '################################'
    echo "Starting ${TRANSPORT_SINK} itests:"
    echo '################################' ; echo ''

    echo "TESTFILES:"
    echo $TESTFILES | tr ' ' '\n' ; echo ''

    for TEST in $TESTFILES
    do
       if [[ "$ITESTS_TEST" != "" ]] && [[ "$ITESTS_TEST" != "$TEST" ]]; then continue; fi
       echo "running test $TEST"
       ./integration_tests.bats -r tests -f "$TEST"
    done

    echo '' ; echo '################################'
    echo "${TRANSPORT_SINK} itests successful in this slice!"
    echo '################################' ; echo ''

    echo "Cleaning up containers, volumes and environment variables..."
    TEST_NAME=base/test_basic docker-compose -f docker-compose.yml rm -f -s -v
    unset $(cat $_file | awk -F= '{print $1}' | xargs)
}

# Start the itests
if [ ! -z "${ITESTS_TRANSPORT_SINK}" ] ; then
    run_itests_on_transport_sink "contexts/${ITESTS_TRANSPORT_SINK}.env"
else
    # Iterate through our transport sink contexts (e.g., kinesis, s3, etc.)
    for file in ./contexts/*; do
        run_itests_on_transport_sink $file
    done
fi
