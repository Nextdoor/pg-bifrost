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

package transport

import (
	"github.com/Nextdoor/pg-bifrost.git/marshaller"
	"github.com/cevaris/ordered_map"
)

type TransportType string

const (
	STDOUT   TransportType = "stdout"
	KINESIS  TransportType = "kinesis"
	S3       TransportType = "s3"
	RABBITMQ TransportType = "rabbitmq"
)

type Transporter interface {
	StartTransporting()
}

const (
	// ERR_MSG_TOOBIG indicates a message is larger than the individual
	// max message size as defined by the Batch. Handling of this is controlled
	// by the Batcher and may be non-fatal.
	ERR_MSG_TOOBIG = "message too big to fit"

	// ERR_FULL indicates a message could not be added because the batch is full.
	ERR_FULL = "batch is full"

	// ERR_CANT_FIT indicates a message could not be added due to size. However,
	// the batch is not yet full. Make a new batch and add the message to that
	// one.
	ERR_CANT_FIT = "message can't fit into batch but batch is not full"

	// ERR_MSG_INVALID indicates that a message was not properly formatted and
	// failed validation in the Batch
	ERR_MSG_INVALID = "message invalid"
)

//go:generate mockgen -destination mocks/mock_batch.go -package=mocks github.com/Nextdoor/pg-bifrost.git/transport Batch,BatchFactory
type Batch interface {
	Add(msg *marshaller.MarshalledMessage) (bool, error)
	GetPayload() interface{}
	GetPayloadByteSize() int64
	GetTransactions() *ordered_map.OrderedMap // This is <transaction:progress.Written>
	GetPartitionKey() string
	Close() (bool, error)
	IsFull() bool
	IsEmpty() bool
	NumMessages() int
	ModifyTime() int64
	CreateTime() int64
}

type BatchFactory interface {
	NewBatch(partitionKey string) Batch
}
