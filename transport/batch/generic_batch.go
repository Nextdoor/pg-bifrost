package batch

import (
	"time"

	"github.com/Nextdoor/pg-bifrost.git/marshaller"
	"github.com/Nextdoor/pg-bifrost.git/transport/progress"

	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/cevaris/ordered_map"
	"github.com/pkg/errors"
)

type GenericBatch struct {
	maxSize      int
	messages     []*marshaller.MarshalledMessage
	transactions *ordered_map.OrderedMap
	byteSize     int64
	mtime        int64
	ctime        int64
	partitionKey string
}

func NewGenericBatch(partitionKey string, maxSize int) transport.Batch {
	messages := []*marshaller.MarshalledMessage{}
	transactions := ordered_map.NewOrderedMap()

	return &GenericBatch{maxSize, messages, transactions, 0, time.Now().UnixNano(), time.Now().UnixNano(), partitionKey}
}

func (b *GenericBatch) Add(msg *marshaller.MarshalledMessage) (bool, error) {
	// Don't add BEGIN and COMMITS to batch
	if msg.Operation == "BEGIN" || msg.Operation == "COMMIT" {
		return true, nil
	}

	if len(b.messages) == b.maxSize {
		return false, errors.New("batch is full")
	}

	// Update counts
	b.messages = append(b.messages, msg)

	if msg.Json != nil {
		b.byteSize += int64(len(msg.Json))
	}

	// Add msg to list of transactions
	progress.UpdateTransactions(msg, b.transactions)

	// Update the modify time of the batch
	b.mtime = time.Now().UnixNano()

	return true, nil
}

func (b *GenericBatch) GetTransactions() *ordered_map.OrderedMap {
	return b.transactions
}

func (b *GenericBatch) Close() (bool, error) {
	// Nothing special required
	return true, nil
}

func (b *GenericBatch) GetPayload() interface{} {
	return b.messages
}

func (b *GenericBatch) GetPayloadByteSize() int64 {
	return b.byteSize
}

func (b *GenericBatch) GetPartitionKey() string {
	return b.partitionKey
}

func (b *GenericBatch) IsFull() bool {
	return len(b.messages) >= b.maxSize
}

func (b *GenericBatch) IsEmpty() bool {
	return len(b.messages) == 0
}

func (b *GenericBatch) NumMessages() int {
	return len(b.messages)
}

func (b *GenericBatch) ModifyTime() int64 {
	return b.mtime
}

func (b *GenericBatch) CreateTime() int64 {
	return b.ctime
}

type GenericBatchFactory struct {
	size int
}

func NewGenericBatchFactory(size int) transport.BatchFactory {
	return GenericBatchFactory{size}
}

func (f GenericBatchFactory) NewBatch(partitionKey string) transport.Batch {
	return NewGenericBatch(partitionKey, f.size)
}
