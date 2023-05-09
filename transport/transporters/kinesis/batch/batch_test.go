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

package batch

import (
	"testing"

	"github.com/Nextdoor/pg-bifrost.git/marshaller"
	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/Nextdoor/pg-bifrost.git/transport/progress"
	"github.com/Nextdoor/pg-bifrost.git/transport/transporters/kinesis/utils"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/cevaris/ordered_map"
	"github.com/stretchr/testify/assert"
)

func TestAddTransaction(t *testing.T) {
	batch := NewKinesisBatch("", utils.KINESIS_PART_WALSTART)

	begin := &marshaller.MarshalledMessage{
		Operation:    "BEGIN",
		Json:         []byte("{}"),
		TimeBasedKey: "1-1",
		WalStart:     900,
		Transaction:  "1",
	}

	insert := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{INSERT}"),
		TimeBasedKey: "1-1",
		WalStart:     901,
		Transaction:  "1",
	}

	commit := &marshaller.MarshalledMessage{
		Operation:    "COMMIT",
		Json:         []byte("{}"),
		TimeBasedKey: "1-1",
		WalStart:     902,
		Transaction:  "1",
	}

	_, _ = batch.Add(begin)
	_, _ = batch.Add(insert)
	_, _ = batch.Add(commit)

	payload := batch.GetPayload()
	prre, ok := payload.([]*kinesis.PutRecordsRequestEntry)

	if !ok {
		assert.Fail(t, "GetPayload does not return []*marshaller.MarshalledMessage")
		return
	}

	assert.Equal(t, 1, len(prre), "Batch should contain 1 record")
	assert.Equal(t, insert.Json, prre[0].Data, "Batch should only contain INSERT")

	omap := ordered_map.NewOrderedMap()
	omap.Set("1-1", &progress.Written{Transaction: "1", TimeBasedKey: "1-1", Count: 1})

	assert.Equal(t, true, progress.CompareBatchTransactions(omap, batch.GetTransactions()), "Batch should have expected transactions")
}

func TestFullBatchCount(t *testing.T) {
	batch := NewKinesisBatch("", utils.KINESIS_PART_WALSTART)

	// Add MAX_RECORDS to batch
	for i := 0; i < MAX_RECORDS; i++ {
		walStart := uint64(100) + uint64(i)
		msg := &marshaller.MarshalledMessage{
			Operation:    "INSERT",
			Json:         []byte("{}"),
			TimeBasedKey: "1",
			WalStart:     walStart,
		}
		ok, _ := batch.Add(msg)

		assert.Equal(t, true, ok, "Adding to batch should have succeeded %d/%d", i, MAX_RECORDS)

		fullExpected := i+1 >= MAX_RECORDS
		assert.Equal(t, fullExpected, batch.IsFull(), "Batch should not be full %d/%d", i, MAX_RECORDS)
	}

	// Batch should now be full
	assert.Equal(t, true, batch.IsFull(), "Batch should be full")

	// Add 1 more record (batch should be full)
	insert2 := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{}"),
		TimeBasedKey: "1-1",
		WalStart:     900,
		Transaction:  "1",
	}

	ok2, _ := batch.Add(insert2)
	assert.Equal(t, false, ok2, "Should not be able to add to full batch")
}

func TestFullBatchCommit(t *testing.T) {
	batch := NewKinesisBatch("", utils.KINESIS_PART_WALSTART)

	// Add MAX_RECORDS to batch
	for i := 0; i < MAX_RECORDS; i++ {
		walStart := uint64(100) + uint64(i)
		msg := &marshaller.MarshalledMessage{
			Operation:    "INSERT",
			Json:         []byte("{}"),
			TimeBasedKey: "0-0",
			WalStart:     walStart,
			Transaction:  "0",
		}
		ok, _ := batch.Add(msg)

		assert.Equal(t, true, ok, "Adding to batch should have succeeded %d/%d", i, MAX_RECORDS)

		fullExpected := i+1 >= MAX_RECORDS
		assert.Equal(t, fullExpected, batch.IsFull(), "Batch should not be full %d/%d", i, MAX_RECORDS)
	}

	// Add COMMIT (batch should be full)
	assert.Equal(t, true, batch.IsFull(), "Batch should be full")

	commit := &marshaller.MarshalledMessage{
		Operation:    "COMMIT",
		Json:         []byte("{}"),
		TimeBasedKey: "1-1",
		WalStart:     900,
		Transaction:  "1",
	}

	ok2, _ := batch.Add(commit)
	assert.Equal(t, true, ok2, "Should be able to add COMMIT to a full batch")

	// Verify COMMIT recorded in transactions
	omap := ordered_map.NewOrderedMap()
	omap.Set("0-0", &progress.Written{Transaction: "0", TimeBasedKey: "0-0", Count: MAX_RECORDS})

	assert.Equal(t, true, progress.CompareBatchTransactions(omap, batch.GetTransactions()), "Batch should have expected transactions")
}

func TestCantFit(t *testing.T) {
	batch := NewKinesisBatch("", utils.KINESIS_PART_WALSTART)

	data := make([]byte, MAX_RECORD_SIZE_BYTES)
	recordSize := len(data) + 3 // size of data plus str(CommitWalStart)

	expectedMaxRecordCount := (MAX_BATCH_SIZE_BYTES / recordSize) + 1

	// Add expectedMaxRecordCount-1 records (this should succeed)
	for i := 0; i < expectedMaxRecordCount-1; i++ {
		walStart := uint64(100) + uint64(i)
		msg := &marshaller.MarshalledMessage{
			Operation:    "INSERT",
			Json:         data,
			TimeBasedKey: "1-1",
			WalStart:     walStart,
			Transaction:  "1",
		}
		ok, _ := batch.Add(msg)

		assert.Equal(t, true, ok, "Adding to batch should have succeeded at %d/%d records", i, expectedMaxRecordCount)
		assert.Equal(t, false, batch.IsFull(), "Batch should not be full at %d/%d records", i, expectedMaxRecordCount)
	}

	// Batch should still have room
	assert.Equal(t, false, batch.IsFull(), "Batch should not be full")

	// Add 1 more large message (it won't fit)
	insert := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         data,
		TimeBasedKey: "1-1",
		WalStart:     901,
		Transaction:  "1",
	}

	ok2, err := batch.Add(insert)
	assert.Equal(t, false, ok2, "Should not be able to fit into batch")
	assert.Equal(t, transport.ERR_CANT_FIT, err.Error(), "Error should be ERR_CANT_FIT")
}

func TestTooBig(t *testing.T) {
	batch := NewKinesisBatch("", utils.KINESIS_PART_WALSTART)

	data := make([]byte, MAX_RECORD_SIZE_BYTES+1)

	// Add 1 more large message (it won't fit)
	insert := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         data,
		TimeBasedKey: "1-1",
		WalStart:     901,
		Transaction:  "1",
	}

	ok, err := batch.Add(insert)

	assert.Equal(t, false, ok, "Should not have been added to batch")
	assert.Equal(t, transport.ERR_MSG_TOOBIG, err.Error(), "Error should be ERR_MSG_TOOBIG")

	txn, ok := batch.GetTransactions().Get("1-1")
	assert.Equal(t, true, ok, "Should have gotten a progress for message even though it was too big to fit.")
	assert.Equal(t, "1-1", txn.(*progress.Written).TimeBasedKey)
}

func TestInvalid(t *testing.T) {
	batch := NewKinesisBatch("", utils.KINESIS_PART_WALSTART)

	// Add 1 more large message (it won't fit)
	insert := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         nil,
		TimeBasedKey: "1-1",
		WalStart:     901,
		Transaction:  "1",
	}

	ok, err := batch.Add(insert)

	assert.Equal(t, false, ok, "Should not have been added to batch")
	assert.Equal(t, transport.ERR_MSG_INVALID, err.Error(), "Error should be ERR_MSG_INVALID")
}

func TestClose(t *testing.T) {
	batch := NewKinesisBatch("", utils.KINESIS_PART_WALSTART)

	ok, err := batch.Close()
	assert.Equal(t, true, ok)
	assert.Equal(t, nil, err)
}

func TestIsEmptyTrue(t *testing.T) {
	batch := NewKinesisBatch("", utils.KINESIS_PART_WALSTART)

	assert.Equal(t, true, batch.IsEmpty())
	assert.Equal(t, 0, batch.NumMessages())
}

func TestIsEmptyFalse(t *testing.T) {
	batch := NewKinesisBatch("", utils.KINESIS_PART_WALSTART)

	insert := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{}"),
		TimeBasedKey: "1-1",
		WalStart:     901,
		Transaction:  "1",
	}

	ok, err := batch.Add(insert)
	assert.Equal(t, true, ok)
	assert.Equal(t, nil, err)

	assert.Equal(t, false, batch.IsEmpty())
	assert.Equal(t, 1, batch.NumMessages())
}
