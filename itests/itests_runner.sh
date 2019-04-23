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

# Iterate through our transport sink contexts (e.g., kinesis, s3, etc.)
for file in ./contexts/*; do
    # Source the transport sink's context
    set -a ; . $file ; set +a

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
       echo "running test $TEST"
       ./integration_tests.bats -r tests -f "$TEST"
    done

    unset $(cat $file | awk -F= '{print $1}' | xargs)

    echo '' ; echo '################################'
    echo "${TRANSPORT_SINK} itests successful in this slice!"
    echo '################################' ; echo ''
done
