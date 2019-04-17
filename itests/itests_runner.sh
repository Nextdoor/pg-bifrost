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
        _testfiles=$(cd tests && circleci tests glob "*" | circleci tests split --split-by=timings)
    else
        _testfiles=$(cd tests && ls -d */ | sed 's#/##')
    fi

    echo "$_testfiles"
}

# Kinesis
set -a ; . contexts/kinesis.env ; set +a

TEST_NAME=test_basic docker-compose -f docker-compose.yml build
TESTFILES=$(get_testfiles $TRANSPORT_SINK)

echo "TESTFILES=$TESTFILES"
for TEST in $TESTFILES
do
   echo "running test $TEST"
   ./integration_tests.bats -r tests -f "$TEST"
done
