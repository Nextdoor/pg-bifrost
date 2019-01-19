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

import "fmt"

type MarshalledMessage struct {
	Operation    string // SQL operation
	Json         []byte // Serialized JSON blob
	TimeBasedKey string // Used in BatchTransactions to seperate temporally different but same messages
	WalStart     uint64 // id or offset of the wal message
	Transaction  string // id of the transaction
	PartitionKey string // partitioning key used by the batcher to group messages into a batch and potentially by transporters to write entire batch into a location
}

func (m MarshalledMessage) String() string {
	return fmt.Sprintf("Operation: %v Json: %s TimeBasedKey: %s CommitWalStart: %d Transaction: %s", m.Operation, m.Json, m.TimeBasedKey, m.WalStart, m.Transaction)
}
