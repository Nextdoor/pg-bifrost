package batch

import (
	"time"

	"github.com/Nextdoor/pg-bifrost.git/marshaller"
	"github.com/Nextdoor/pg-bifrost.git/transport/progress"
	"github.com/Shopify/sarama"

	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/cevaris/ordered_map"
	"github.com/pkg/errors"
)

type KafkaBatch struct {
	maxBatchSize    int
	maxMessageBytes int
	kafkaMessages   []*sarama.ProducerMessage
	transactions    *ordered_map.OrderedMap
	byteSize        int64
	mtime           int64
	ctime           int64
	partitionKey    string
	topic           string
}

func NewKafkaBatch(topic string, partitionKey string, maxSize, maxMessageBytes int) transport.Batch {
	messages := []*sarama.ProducerMessage{}
	transactions := ordered_map.NewOrderedMap()

	return &KafkaBatch{maxSize, maxMessageBytes, messages, transactions, 0, time.Now().UnixNano(), time.Now().UnixNano(), partitionKey, topic}
}

func (b *KafkaBatch) Add(msg *marshaller.MarshalledMessage) (bool, error) {
	// Don't add BEGIN and COMMITS to batch
	if msg.Operation == "BEGIN" || msg.Operation == "COMMIT" {
		return true, nil
	}

	// Sarama client only permits messages up to size `MaxMessageBytes`
	if len(msg.Json) > b.maxMessageBytes {
		// Still update transactions even though message is being dropped. If we do not
		// then the progress ledger will block waiting for this message.
		progress.UpdateTransactions(msg, b.transactions)
		return false, errors.New(transport.ERR_MSG_TOOBIG)
	}

	if len(b.kafkaMessages) == b.maxBatchSize {
		return false, errors.New("batch is full")
	}

	kafkaMsg := &sarama.ProducerMessage{
		Topic: b.topic,
		Value: sarama.ByteEncoder(msg.Json),
		Key:   sarama.StringEncoder(msg.PartitionKey),
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
