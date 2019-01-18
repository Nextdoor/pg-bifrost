package batch

import (
	"github.com/Nextdoor/pg-bifrost.git/marshaller"
	"github.com/Nextdoor/pg-bifrost.git/transport/progress"
	"github.com/cevaris/ordered_map"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAddTransaction(t *testing.T) {
	b := NewGenericBatch("", 1)

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

	b.Add(begin)
	b.Add(insert)
	b.Add(commit)

	messages := b.GetPayload()
	messagesSlice, ok := messages.([]*marshaller.MarshalledMessage)

	if !ok {
		assert.Fail(t, "GetPayload does not return []*marshaller.MarshalledMessage")
		return
	}

	assert.Equal(t, 1, len(messagesSlice), "Batch should contain 1 message")
	assert.Equal(t, insert, messagesSlice[0], "Batch should only contain INSERT")

	omap := ordered_map.NewOrderedMap()
	omap.Set("1-1", &progress.Written{Transaction: "1", TimeBasedKey: "1-1", Count: 1})

	assert.Equal(t, true, progress.CompareBatchTransactions(omap, b.GetTransactions()), "Batch should have the expected transactions")
}

func TestFullBatch(t *testing.T) {
	batch := NewGenericBatch("", 1)

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

	ok2, _ := batch.Add(insert2)
	assert.Equal(t, false, ok2, "Should not be able to add to full batch")
}

func TestPartialTransaction(t *testing.T) {
	b := NewGenericBatch("", 2)

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

	b.Add(begin1)
	b.Add(insert1)
	b.Add(commit1)
	b.Add(begin2)
	b.Add(insert2)
	assert.Equal(t, true, b.IsFull(), "Batch should be full")

	messages := b.GetPayload()
	messagesSlice, ok := messages.([]*marshaller.MarshalledMessage)

	if !ok {
		assert.Fail(t, "GetPayload does not return []*marshaller.MarshalledMessage")
		return
	}

	assert.Equal(t, 2, len(messagesSlice), "Batch should contain 1 message")
	assert.Equal(t, insert1, messagesSlice[0], "Batch should only contain INSERT 1")
	assert.Equal(t, insert2, messagesSlice[1], "Batch should only contain INSERT 2")

	omap := ordered_map.NewOrderedMap()
	omap.Set("1-1", &progress.Written{Transaction: "1", TimeBasedKey: "1-1", Count: 1})
	omap.Set("2-1", &progress.Written{Transaction: "2", TimeBasedKey: "2-1", Count: 1})

	assert.Equal(t, true, progress.CompareBatchTransactions(omap, b.GetTransactions()), "Batch should have expected transactions")
}

func TestClose(t *testing.T) {
	b := NewGenericBatch("", 2)
	success, err := b.Close()

	assert.Equal(t, true, success)
	assert.Equal(t, nil, err)
}

func TestIsFullFalse(t *testing.T) {
	b := NewGenericBatch("", 2)
	assert.Equal(t, false, b.IsFull())
}

func TestIsEmptyTrue(t *testing.T) {
	b := NewGenericBatch("", 2)
	assert.Equal(t, true, b.IsEmpty())
	assert.Equal(t, 0, b.NumMessages())
	assert.Equal(t, int64(0), b.GetPayloadByteSize())
}

func TestIsEmptyFalse(t *testing.T) {
	b := NewGenericBatch("", 2)

	insert := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{}"),
		TimeBasedKey: "2",
		WalStart:     901,
		Transaction:  "2",
	}

	b.Add(insert)

	assert.Equal(t, false, b.IsEmpty())
	assert.Equal(t, 1, b.NumMessages())
	assert.Equal(t, int64(2), b.GetPayloadByteSize())

}

func TestNewBatchFromFactory(t *testing.T) {
	f := NewGenericBatchFactory(1)
	b := f.NewBatch("")

	assert.Equal(t, true, b.IsEmpty())
	assert.Equal(t, 0, b.NumMessages())

	insert := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{}"),
		TimeBasedKey: "2",
		WalStart:     901,
		Transaction:  "2",
	}

	b.Add(insert)

	b = f.NewBatch("")
	assert.Equal(t, true, b.IsEmpty())
	assert.Equal(t, 0, b.NumMessages())
}
