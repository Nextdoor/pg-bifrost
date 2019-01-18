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

package progress

import (
	"github.com/cevaris/ordered_map"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIsEqual(t *testing.T) {

	omapA := ordered_map.NewOrderedMap()
	omapA.Set("1-1", &Written{"1", "1-1", 1})

	omapB := ordered_map.NewOrderedMap()
	omapB.Set("1-1", &Written{"1", "1-1", 1})

	ret := CompareBatchTransactions(omapA, omapB)
	assert.Equal(t, true, ret)
}

func TestIsNotEqualLength(t *testing.T) {
	omapA := ordered_map.NewOrderedMap()
	omapA.Set("1-1", Written{"1", "1-1", 1})

	omapB := ordered_map.NewOrderedMap()
	omapA.Set("1-1", &Written{"1", "1-1", 1})
	omapA.Set("2-1", &Written{"2", "2-1", 1})

	ret := CompareBatchTransactions(omapA, omapB)
	assert.Equal(t, false, ret)
}

func TestIsNotEqualCount(t *testing.T) {
	omapA := ordered_map.NewOrderedMap()
	omapA.Set("1-1", &Written{"1", "1-1", 1})

	omapB := ordered_map.NewOrderedMap()
	omapA.Set("1-1", &Written{"1", "1-1", 5})

	ret := CompareBatchTransactions(omapA, omapB)
	assert.Equal(t, false, ret)
}

func TestIsNotEqualTransaction(t *testing.T) {
	omapA := ordered_map.NewOrderedMap()
	omapA.Set("1-1", &Written{"1", "1-1", 1})

	omapB := ordered_map.NewOrderedMap()
	omapA.Set("2-1", &Written{"2", "2-1", 1})

	ret := CompareBatchTransactions(omapA, omapB)
	assert.Equal(t, false, ret)
}

func TestIsNotEqualTimeBasedKey(t *testing.T) {
	omapA := ordered_map.NewOrderedMap()
	omapA.Set("1-1", &Written{"1", "1-1", 1})

	omapB := ordered_map.NewOrderedMap()
	omapA.Set("1-2", &Written{"1", "1-2", 1})

	ret := CompareBatchTransactions(omapA, omapB)
	assert.Equal(t, false, ret)
}

func TestIsNotEqualMultiple(t *testing.T) {
	omapA := ordered_map.NewOrderedMap()
	omapA.Set("1-1", &Written{"1", "1-1", 1})
	omapA.Set("2-1", &Written{"2", "2-1", 1})


	omapB := ordered_map.NewOrderedMap()
	omapB.Set("1-1", &Written{"1", "1-1", 1})
	omapB.Set("2-1", &Written{"2", "2-1", 4})

	ret := CompareBatchTransactions(omapA, omapB)
	assert.Equal(t, false, ret)
}
