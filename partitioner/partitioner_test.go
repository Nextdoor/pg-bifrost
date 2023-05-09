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

package partitioner

import (
	"testing"

	"github.com/Nextdoor/parselogical"
	"github.com/Nextdoor/pg-bifrost.git/replication"
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/stretchr/testify/assert"
)

func _setupPartitioner(partMethod PartitionMethod, buckets int) (Partitioner, chan *replication.WalMessage, chan stats.Stat) {
	sh := shutdown.NewShutdownHandler()
	in := make(chan *replication.WalMessage)
	statsChan := make(chan stats.Stat, 1000)

	p := New(sh, in, statsChan, partMethod, buckets)

	return p, in, statsChan
}

func _testMessage(relation string, transaction string) *replication.WalMessage {
	pr := parselogical.ParseResult{
		State:       parselogical.ParseState{},
		Transaction: transaction,
		Relation:    relation,
		Operation:   "INSERT",
		NoTupleData: false,
		Columns:     nil,
		OldColumns:  nil,
	}

	walMessage := replication.WalMessage{
		WalStart:     1,
		ServerWalEnd: 0,
		ServerTime:   0,
		TimeBasedKey: "0-0",
		Pr:           &pr,
	}

	return &walMessage
}

func TestNone(t *testing.T) {
	msg := _testMessage("users", "1")

	p, in, _ := _setupPartitioner(PART_METHOD_NONE, 1)

	go p.Start()

	in <- msg
	outMsg := <-p.OutputChan

	assert.Equal(t, "", outMsg.PartitionKey)
}

func TestTableName(t *testing.T) {
	msg := _testMessage("users", "1")

	p, in, _ := _setupPartitioner(PART_METHOD_TABLENAME, 1)

	go p.Start()

	in <- msg
	outMsg := <-p.OutputChan

	assert.Equal(t, "users", outMsg.PartitionKey)
}

func TestTransaction(t *testing.T) {
	msg := _testMessage("users", "19")

	p, in, _ := _setupPartitioner(PART_METHOD_TXN, 1)

	go p.Start()

	in <- msg
	outMsg := <-p.OutputChan

	assert.Equal(t, "19", outMsg.PartitionKey)
}

func TestTransactionBuckets(t *testing.T) {
	p, in, _ := _setupPartitioner(PART_METHOD_TXN_BUCKET, 5)

	go p.Start()

	in <- _testMessage("users", "111111")
	outMsgA := <-p.OutputChan
	assert.Equal(t, "4", outMsgA.PartitionKey)

	in <- _testMessage("users", "333333")
	outMsgB := <-p.OutputChan
	assert.Equal(t, "1", outMsgB.PartitionKey)
}
