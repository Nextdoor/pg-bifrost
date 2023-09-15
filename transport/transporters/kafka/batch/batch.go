/*
  Copyright 2023 Nextdoor.com, Inc.

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
	"time"

	"github.com/google/uuid"

	"github.com/Nextdoor/pg-bifrost.git/marshaller"
	"github.com/Nextdoor/pg-bifrost.git/transport/progress"
	"github.com/Nextdoor/pg-bifrost.git/transport/transporters/kafka/utils"
	"github.com/Shopify/sarama"

	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/cevaris/ordered_map"
	"github.com/pkg/errors"
)

type KafkaBatch struct {
	maxBatchSize      int
	maxMessageBytes   int
	kafkaMessages     []*sarama.ProducerMessage
	transactions      *ordered_map.OrderedMap
	byteSize          int64
	mtime             int64
	ctime             int64
	partitionKey      string // this is the partition key for bifrost not kafka
	topic             string
	kafkaPartMethod   utils.KafkaPartitionMethod
	kafkaPartitionKey string
}

func NewKafkaBatch(topic string, partitionKey string, maxBathSize, maxMessageBytes int, kafkaPartMethod utils.KafkaPartitionMethod) transport.Batch {
	messages := []*sarama.ProducerMessage{}
	transactions := ordered_map.NewOrderedMap()

	// random kafka partition key when using batch partitioning
	kafkaPartitionKey := ""
	if kafkaPartMethod == utils.KAFKA_PART_BATCH {
		kafkaPartitionKey = uuid.NewString()
	}

	return &KafkaBatch{
		maxBathSize,
		maxMessageBytes,
		messages,
		transactions,
		0,
		time.Now().UnixNano(),
		time.Now().UnixNano(),
		partitionKey,
		topic,
		kafkaPartMethod,
		kafkaPartitionKey,
	}
}

func (b *KafkaBatch) Add(msg *marshaller.MarshalledMessage) (bool, error) {
	// Don't add BEGIN and COMMITS to batch
	if msg.Operation == "BEGIN" || msg.Operation == "COMMIT" {
		return true, nil
	}

	if len(b.kafkaMessages) == b.maxBatchSize {
		return false, errors.New("batch is full")
	}

	kafkaMsg := &sarama.ProducerMessage{
		Topic: b.topic,
		Value: sarama.ByteEncoder(msg.Json),
	}

	// Set partition key
	switch b.kafkaPartMethod {
	case utils.KAFKA_PART_TXN:
		// Use the time based key which is a composite of the txn + time because it adds more entropy
		// when the sarama hasher picks a partition based on hashing this value.
		kafkaMsg.Key = sarama.StringEncoder(msg.TimeBasedKey)
	case utils.KAFKA_PART_TXN_CONST:
		// Similar to above but low entropy so that it's consistent for testing
		kafkaMsg.Key = sarama.StringEncoder(msg.Transaction)
	case utils.KAFKA_PART_BATCH:
		kafkaMsg.Key = sarama.StringEncoder(b.kafkaPartitionKey)
	case utils.KAFKA_PART_TABLE_NAME:
		kafkaMsg.Key = sarama.StringEncoder(msg.Table)
	case utils.KAFKA_PART_RANDOM:
		// causes Sarama to use random partitioning
		kafkaMsg.Key = nil
	}

	// Sarama client only permits messages up to size `MaxMessageBytes`
	if kafkaMsg.ByteSize(2) > b.maxMessageBytes {
		// Still update transactions even though message is being dropped. If we do not
		// then the progress ledger will block waiting for this message.
		progress.UpdateTransactions(msg, b.transactions)
		return false, errors.New(transport.ERR_MSG_TOOBIG)
	}

	// Update counts
	b.kafkaMessages = append(b.kafkaMessages, kafkaMsg)
	b.byteSize += int64(len(msg.Json))

	// Add msg to list of transactions
	progress.UpdateTransactions(msg, b.transactions)

	// Update the modify time of the batch
	b.mtime = time.Now().UnixNano()

	return true, nil
}

func (b *KafkaBatch) GetTransactions() *ordered_map.OrderedMap {
	return b.transactions
}

func (b *KafkaBatch) Close() (bool, error) {
	// Nothing special required
	return true, nil
}

func (b *KafkaBatch) GetPayload() interface{} {
	return b.kafkaMessages
}

func (b *KafkaBatch) GetPayloadByteSize() int64 {
	return b.byteSize
}

func (b *KafkaBatch) GetPartitionKey() string {
	return b.partitionKey
}

func (b *KafkaBatch) IsFull() bool {
	return len(b.kafkaMessages) >= b.maxBatchSize
}

func (b *KafkaBatch) IsEmpty() bool {
	return len(b.kafkaMessages) == 0
}

func (b *KafkaBatch) NumMessages() int {
	return len(b.kafkaMessages)
}

func (b *KafkaBatch) ModifyTime() int64 {
	return b.mtime
}

func (b *KafkaBatch) CreateTime() int64 {
	return b.ctime
}
