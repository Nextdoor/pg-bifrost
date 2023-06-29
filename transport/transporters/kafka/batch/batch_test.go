package batch

import (
	"testing"

	"github.com/Nextdoor/pg-bifrost.git/marshaller"
	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/Nextdoor/pg-bifrost.git/transport/progress"
	"github.com/Shopify/sarama"
	"github.com/cevaris/ordered_map"
	"github.com/stretchr/testify/assert"
)

func TestAddTransaction(t *testing.T) {
	b := NewKafkaBatch("test-topic", "", 1, 1000000)

	begin := &marshaller.MarshalledMessage{
		Operation:    "BEGIN",
		Json:         []byte("{}"),
		TimeBasedKey: "1-1",
		WalStart:     900,
		Transaction:  "1",
	}

	insert := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{}"),
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

	_, _ = b.Add(begin)
	_, _ = b.Add(insert)
	_, _ = b.Add(commit)

	messages := b.GetPayload()
	messagesSlice, ok := messages.([]*sarama.ProducerMessage)

	if !ok {
		assert.Fail(t, "GetPayload does not return []*sarama.ProducerMessage")
		return
	}

	assert.Equal(t, 1, len(messagesSlice), "Batch should contain 1 message")
	assert.Equal(t, sarama.ByteEncoder(insert.Json), messagesSlice[0].Value, "Batch should only contain INSERT")

	omap := ordered_map.NewOrderedMap()
	omap.Set("1-1", &progress.Written{Transaction: "1", TimeBasedKey: "1-1", Count: 1})

	assert.Equal(t, true, progress.CompareBatchTransactions(omap, b.GetTransactions()), "Batch should have the expected transactions")
}

func TestFullBatch(t *testing.T) {
	batch := NewKafkaBatch("test-topic", "", 1, 1000000)

	insert1 := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{}"),
		TimeBasedKey: "1-1",
		WalStart:     900,
		Transaction:  "1",
	}

	insert2 := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{}"),
		TimeBasedKey: "1-1",
		WalStart:     901,
		Transaction:  "1",
	}

	ok1, _ := batch.Add(insert1)
	assert.Equal(t, true, ok1, "Should be able to add 1 message to empty batch")

	assert.Equal(t, true, batch.IsFull(), "Batch should be full")

	ok2, err := batch.Add(insert2)
	assert.Equal(t, false, ok2, "Should not be able to add to full batch")
	assert.Equal(t, "batch is full", err.Error(), "Should receive 'batch is full' error")
}

func TestMessageTooBig(t *testing.T) {
	batch := NewKafkaBatch("test-topic", "", 1, 0)

	data := make([]byte, 10)

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

func TestPartialTransaction(t *testing.T) {
	b := NewKafkaBatch("test-topic", "", 2, 1000000)

	begin1 := &marshaller.MarshalledMessage{
		Operation:    "BEGIN",
		Json:         []byte("{}"),
		TimeBasedKey: "1-1",
		WalStart:     800,
		Transaction:  "1",
	}

	insert1 := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{}"),
		TimeBasedKey: "1-1",
		WalStart:     801,
		Transaction:  "1",
	}

	commit1 := &marshaller.MarshalledMessage{
		Operation:    "COMMIT",
		Json:         []byte("{}"),
		TimeBasedKey: "1-1",
		WalStart:     802,
		Transaction:  "1",
	}

	begin2 := &marshaller.MarshalledMessage{
		Operation:    "BEGIN",
		Json:         []byte("{}"),
		TimeBasedKey: "2-1",
		WalStart:     900,
		Transaction:  "2",
	}

	insert2 := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{}"),
		TimeBasedKey: "2-1",
		WalStart:     901,
		Transaction:  "2",
	}

	_, _ = b.Add(begin1)
	_, _ = b.Add(insert1)
	_, _ = b.Add(commit1)
	_, _ = b.Add(begin2)
	_, _ = b.Add(insert2)
	assert.Equal(t, true, b.IsFull(), "Batch should be full")

	messages := b.GetPayload()
	messagesSlice, ok := messages.([]*sarama.ProducerMessage)

	if !ok {
		assert.Fail(t, "GetPayload does not return []*sarama.ProducerMessage")
		return
	}

	assert.Equal(t, 2, len(messagesSlice), "Batch should contain 1 message")
	assert.Equal(t, sarama.ByteEncoder(insert1.Json), messagesSlice[0].Value, "Batch should only contain INSERT 1")
	assert.Equal(t, sarama.ByteEncoder(insert2.Json), messagesSlice[1].Value, "Batch should only contain INSERT 2")

	omap := ordered_map.NewOrderedMap()
	omap.Set("1-1", &progress.Written{Transaction: "1", TimeBasedKey: "1-1", Count: 1})
	omap.Set("2-1", &progress.Written{Transaction: "2", TimeBasedKey: "2-1", Count: 1})

	assert.Equal(t, true, progress.CompareBatchTransactions(omap, b.GetTransactions()), "Batch should have expected transactions")
}

func TestClose(t *testing.T) {
	b := NewKafkaBatch("test-topic", "", 2, 1000000)
	success, err := b.Close()

	assert.Equal(t, true, success)
	assert.Equal(t, nil, err)
}

func TestIsFullFalse(t *testing.T) {
	b := NewKafkaBatch("test-topic", "", 2, 1000000)
	assert.Equal(t, false, b.IsFull())
}

func TestIsEmptyTrue(t *testing.T) {
	b := NewKafkaBatch("test-topic", "", 2, 1000000)
	assert.Equal(t, true, b.IsEmpty())
	assert.Equal(t, 0, b.NumMessages())
	assert.Equal(t, int64(0), b.GetPayloadByteSize())
}

func TestIsEmptyFalse(t *testing.T) {
	b := NewKafkaBatch("test-topic", "", 2, 1000000)

	insert := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{}"),
		TimeBasedKey: "2",
		WalStart:     901,
		Transaction:  "2",
	}

	_, _ = b.Add(insert)

	assert.Equal(t, false, b.IsEmpty())
	assert.Equal(t, 1, b.NumMessages())
	assert.Equal(t, int64(2), b.GetPayloadByteSize())

}
