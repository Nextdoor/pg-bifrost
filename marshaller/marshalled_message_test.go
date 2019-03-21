/*
  Copyright 2019 Nextdoor.com, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */

package marshaller

import (
	"reflect"
	"testing"
)

func TestString(t *testing.T) {
	m := MarshalledMessage{"INSERT", "test_table", []byte("json"), "key", 1, "0", "0"}
	expected := "Operation: INSERT Table: test_table Json: json TimeBasedKey: key CommitWalStart: 1 Transaction: 0"

	eq := reflect.DeepEqual(expected, m.String())
	if !eq {
		t.Error(
			"For\n", "INSERT", "json", "key", 1,
			"expected\n", expected,
			"got\n", m.String())
	}
}
