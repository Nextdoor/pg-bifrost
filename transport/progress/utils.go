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
	"github.com/Nextdoor/pg-bifrost.git/marshaller"
	"github.com/cevaris/ordered_map"
)

// CompareBatchTransactions compares two BatchTransactions for equality of their fields.
func CompareBatchTransactions(a *ordered_map.OrderedMap, b *ordered_map.OrderedMap) bool {
	if a.Len() != b.Len() {
		return false
	}

	iterA := a.IterFunc()

	for kv, ok := iterA(); ok; kv, ok = iterA() {
		key := kv.Key.(string)
		value := kv.Value.(*Written)

		if packedVal, ok := b.Get(key); ok == true {
			val := packedVal.(*Written)

			if val.Transaction != value.Transaction {
				return false
			}

			if val.Count != value.Count {
				return false
			}

			if val.TimeBasedKey != value.TimeBasedKey {
				return false
			}
		} else {
			// false if key doesn't exist in other omap
			return false
		}
	}

	return true
}

func UpdateTransactions(msg *marshaller.MarshalledMessage, transactions *ordered_map.OrderedMap) {
	var transaction *Written
	txn, ok := transactions.Get(msg.TimeBasedKey)

	// If does not exist already then create new entry
	if !ok {
		transaction = &Written{msg.Transaction, msg.TimeBasedKey, 1}
		transactions.Set(msg.TimeBasedKey, transaction)
		return
	}

	// Update existing count
	transaction = txn.(*Written)
	transaction.Count += 1
}
