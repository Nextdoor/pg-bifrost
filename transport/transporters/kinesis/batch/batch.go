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
	"fmt"
	"time"

	"github.com/Nextdoor/pg-bifrost.git/marshaller"
	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/Nextdoor/pg-bifrost.git/transport/progress"
	"github.com/Nextdoor/pg-bifrost.git/transport/transporters/kinesis/utils"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/cevaris/ordered_map"
	"github.com/pkg/errors"
)

const (
	// Limits as documented by https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html
	MAX_RECORDS           = 500             // max number of messages in a batch
	MAX_BATCH_SIZE_BYTES  = 1024 * 1024 * 5 // max byte size a batch can be
	MAX_RECORD_SIZE_BYTES = 1024 * 1024     // max size of an individual message
)

type KinesisBatch struct {
	records         []*kinesis.PutRecordsRequestEntry
	transactions    *ordered_map.OrderedMap
	batchSizeBytes  int64
	mtime           int64
	ctime           int64
	partitionMethod utils.KinesisPartitionMethod
	partitionKey    string
}

func NewKinesisBatch(partitionKey string, partitionMethod utils.KinesisPartitionMethod) transport.Batch {
	records := []*kinesis.PutRecordsRequestEntry{}
	transactions := ordered_map.NewOrderedMap()

	return &KinesisBatch{records, transactions, 0, time.Now().UnixNano(), time.Now().UnixNano(), partitionMethod, partitionKey}
}

func (b *KinesisBatch) Add(msg *marshaller.MarshalledMessage) (bool, error) {
	// Don't add BEGIN and COMMITS to batch
	if msg.Operation == "BEGIN" || msg.Operation == "COMMIT" {
		return true, nil
	}

	// Verify max message size
	if len(msg.Json) > MAX_RECORD_SIZE_BYTES {
		// Still update transactions even though message is being dropped. If we do not
		// then the progress ledger will block waiting for this message.
		progress.UpdateTransactions(msg, b.transactions)
		return false, errors.New(transport.ERR_MSG_TOOBIG)
	}

	// Verify batch is not already full (count and batch wise)
	if b.IsFull() {
		return false, errors.New(transport.ERR_FULL)
	}

	// Pick the kinesis partition key
	var partitionKey string

	switch b.partitionMethod {
	case utils.KINESIS_PART_WALSTART:
		partitionKey = fmt.Sprintf("%v", msg.WalStart)
		break
	case utils.KINESIS_PART_BATCH:
		partitionKey = msg.PartitionKey
	}

	// Verify batch has room (byte wise) to add the message
	recordSizeBytes := int64(len(msg.Json) + len(partitionKey))
	if recordSizeBytes+b.batchSizeBytes > MAX_BATCH_SIZE_BYTES {
		return false, errors.New(transport.ERR_CANT_FIT)
	}

	// Construct Record from MarshalledMessage
	record := kinesis.PutRecordsRequestEntry{Data: msg.Json, PartitionKey: &partitionKey}

	// Validate
	err := record.Validate()
	if err != nil {
		return false, errors.New(transport.ERR_MSG_INVALID)
	}

	// Add it to the batch
	b.records = append(b.records, &record)
	b.batchSizeBytes = b.batchSizeBytes + recordSizeBytes

	// Record transaction
	progress.UpdateTransactions(msg, b.transactions)

	// Update the modify time of the batch
	b.mtime = time.Now().UnixNano()

	return true, nil
}

func (b *KinesisBatch) GetTransactions() *ordered_map.OrderedMap {
	return b.transactions
}

func (b *KinesisBatch) Close() (bool, error) {
	// Nothing special required
	return true, nil
}

func (b *KinesisBatch) GetPayload() interface{} {
	return b.records
}

func (b *KinesisBatch) GetPayloadByteSize() int64 {
	return b.batchSizeBytes
}

func (b *KinesisBatch) GetPartitionKey() string {
	return b.partitionKey
}

func (b *KinesisBatch) ModifyTime() int64 {
	return b.mtime
}

func (b *KinesisBatch) CreateTime() int64 {
	return b.ctime
}

func (b *KinesisBatch) IsFull() bool {
	if len(b.records) >= MAX_RECORDS {
		return true
	}

	return false
}

func (b *KinesisBatch) IsEmpty() bool {
	return len(b.records) == 0
}

func (b *KinesisBatch) NumMessages() int {
	return len(b.records)
}
