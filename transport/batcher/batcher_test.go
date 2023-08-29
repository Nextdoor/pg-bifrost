package batcher

import (
	"testing"

	"fmt"
	"time"

	"github.com/Nextdoor/pg-bifrost.git/marshaller"
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/Nextdoor/pg-bifrost.git/transport/batch"
	"github.com/Nextdoor/pg-bifrost.git/transport/mocks"
	"github.com/Nextdoor/pg-bifrost.git/transport/progress"
	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

var DEFAULT_TICK_RATE_DURATION = time.Duration(DEFAULT_TICK_RATE) * time.Millisecond

func TestBatchSizeOneOneTxnOneData(t *testing.T) {
	timeBasedKey := "1-1"
	transaction := "1"
	batchSize := 1

	in := make(chan *marshaller.MarshalledMessage, 1000)
	txnSeen := make(chan *progress.Seen, 1000)
	statsChan := make(chan stats.Stat, 1000)

	batchFactory := batch.NewGenericBatchFactory(batchSize)
	sh := shutdown.NewShutdownHandler()

	b := NewBatcher(sh, in, txnSeen, statsChan, DEFAULT_TICK_RATE, batchFactory, 1, 500, 1000, 1, DEFAULT_MAX_MEMORY_BYTES, BATCH_ROUTING_ROUND_ROBIN)
	outs := b.GetOutputChans()

	go b.StartBatching()

	begin := &marshaller.MarshalledMessage{
		Operation:    "BEGIN",
		Json:         []byte("{}"),
		TimeBasedKey: timeBasedKey,
		WalStart:     900,
		Transaction:  transaction,
	}

	message := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{}"),
		TimeBasedKey: timeBasedKey,
		WalStart:     901,
		Transaction:  transaction,
	}

	commit := &marshaller.MarshalledMessage{
		Operation:    "COMMIT",
		Json:         []byte("{}"),
		TimeBasedKey: timeBasedKey,
		WalStart:     902,
		Transaction:  transaction,
	}

	expectedSeen := &progress.Seen{
		Transaction:    transaction,
		TimeBasedKey:   timeBasedKey,
		CommitWalStart: commit.WalStart,
		TotalMsgs:      1,
	}

	in <- begin
	in <- message
	in <- commit

	result := <-outs[0]

	// Cast messages
	messages := result.GetPayload()
	messagesSlice, ok := messages.([]*marshaller.MarshalledMessage)

	if !ok {
		assert.Fail(t, "GetPayload does not return []*marshaller.MarshalledMessage")
		return
	}

	// Test output channel
	assert.Equal(t, batchSize, len(messagesSlice), "Batch receive should be size 1.")
	assert.Equal(t, message, messagesSlice[0], "That the message of the batch should be INSERT message.")
	if val, ok := result.GetTransactions().Get(timeBasedKey); ok {
		// Transaction set
		assert.Equal(t, begin.TimeBasedKey, val.(*progress.Written).TimeBasedKey)
		assert.Equal(t, begin.Transaction, val.(*progress.Written).Transaction)
		assert.Equal(t, 1, val.(*progress.Written).Count)
	} else {
		// Transaction not set
		assert.Fail(t, "Transaction of batch was not seen.")
	}

	// Test seen channel
	resultTxn := <-txnSeen
	assert.Equal(t, expectedSeen, resultTxn, "Transaction progress received should be correct.")

	// Verify stats
	expected := []stats.Stat{
		stats.NewStatHistogram("batcher", "batch_write_wait", 1, time.Now().UnixNano(), "ms"),
		stats.NewStatCount("batcher", "batches", 1, time.Now().UnixNano()),
		stats.NewStatHistogram("batcher", "batch_size", 1, time.Now().UnixNano(), "count"),
	}
	stats.VerifyStats(t, statsChan, expected)
}

func TestBatchSizeOneOneTxnTwoData(t *testing.T) {
	timeBasedKey := "1-1"
	batchSize := 1
	transaction := "1"

	in := make(chan *marshaller.MarshalledMessage, 1000)
	txnSeen := make(chan *progress.Seen, 1000)

	statsChan := make(chan stats.Stat, 1000)
	batchFactory := batch.NewGenericBatchFactory(batchSize)
	sh := shutdown.NewShutdownHandler()

	b := NewBatcher(sh, in, txnSeen, statsChan, DEFAULT_TICK_RATE, batchFactory, 1, 500, 1000, 1, DEFAULT_MAX_MEMORY_BYTES, BATCH_ROUTING_ROUND_ROBIN)
	outs := b.GetOutputChans()

	go b.StartBatching()

	begin := &marshaller.MarshalledMessage{
		Operation:    "BEGIN",
		Json:         []byte("{}"),
		TimeBasedKey: timeBasedKey,
		WalStart:     900,
		Transaction:  transaction,
	}

	message1 := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{MSG1}"),
		TimeBasedKey: timeBasedKey,
		WalStart:     901,
		Transaction:  transaction,
	}

	message2 := &marshaller.MarshalledMessage{
		Operation:    "UPDATE",
		Json:         []byte("{MSG2}"),
		TimeBasedKey: timeBasedKey,
		WalStart:     901,
		Transaction:  transaction,
	}

	commit := &marshaller.MarshalledMessage{
		Operation:    "COMMIT",
		Json:         []byte("{}"),
		TimeBasedKey: timeBasedKey,
		WalStart:     902,
		Transaction:  transaction,
	}

	expectedSeen := &progress.Seen{
		Transaction:    transaction,
		TimeBasedKey:   timeBasedKey,
		CommitWalStart: commit.WalStart,
		TotalMsgs:      2,
	}

	in <- begin
	in <- message1
	in <- message2
	in <- commit

	// Read first batch
	result1 := <-outs[0]
	messages1 := result1.GetPayload()
	messagesSlice1, _ := messages1.([]*marshaller.MarshalledMessage)

	assert.Equal(t, batchSize, len(messagesSlice1), "Batch receive should be size 1.")
	assert.Equal(t, message1, messagesSlice1[0], "That the message of the batch should be the INSERT message.")
	if val, ok := result1.GetTransactions().Get(timeBasedKey); ok {
		// Transaction set
		assert.Equal(t, begin.TimeBasedKey, val.(*progress.Written).TimeBasedKey)
		assert.Equal(t, begin.Transaction, val.(*progress.Written).Transaction)
		assert.Equal(t, 1, val.(*progress.Written).Count)
	} else {
		// Transaction not set
		assert.Fail(t, "Transaction of batch was not seen")
	}

	// Read second batch
	result2 := <-outs[0]
	messages2 := result2.GetPayload()
	messagesSlice2, _ := messages2.([]*marshaller.MarshalledMessage)

	assert.Equal(t, batchSize, len(messagesSlice2), "Batch receive should be size 1.")
	assert.Equal(t, message2, messagesSlice2[0], "That the message of the batch should be the UPDATE message.")
	if val, ok := result2.GetTransactions().Get(timeBasedKey); ok {
		// Transaction set
		assert.Equal(t, begin.TimeBasedKey, val.(*progress.Written).TimeBasedKey)
		assert.Equal(t, begin.Transaction, val.(*progress.Written).Transaction)
		assert.Equal(t, 1, val.(*progress.Written).Count)
	} else {
		// Transaction not set
		assert.Fail(t, "Transaction of batch was not seen")
	}

	// Test Transaction channel
	resultTxn1 := <-txnSeen
	assert.Equal(t, expectedSeen, resultTxn1, "Transaction progress received should be correct.")
}

func TestBatchSizeThreeTwoTxnTwoData(t *testing.T) {
	timeBasedKeyA := "1-1"
	transactionA := "1"
	timeBasedKeyB := "2-1"
	transactionB := "2"
	batchSize := 3

	in := make(chan *marshaller.MarshalledMessage, 1000)
	txnSeen := make(chan *progress.Seen, 1000)
	statsChan := make(chan stats.Stat, 1000)
	batchFactory := batch.NewGenericBatchFactory(batchSize)
	sh := shutdown.NewShutdownHandler()

	b := NewBatcher(sh, in, txnSeen, statsChan, DEFAULT_TICK_RATE, batchFactory, 1, 500, 1000, 1, DEFAULT_MAX_MEMORY_BYTES, BATCH_ROUTING_ROUND_ROBIN)
	outs := b.GetOutputChans()

	go b.StartBatching()

	// Transaction 1
	beginA := &marshaller.MarshalledMessage{
		Operation:    "BEGIN",
		Json:         []byte("{}"),
		TimeBasedKey: timeBasedKeyA,
		WalStart:     800,
		Transaction:  transactionA,
	}

	messageA1 := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{MSG1}"),
		TimeBasedKey: timeBasedKeyA,
		WalStart:     801,
		Transaction:  transactionA,
	}

	messageA2 := &marshaller.MarshalledMessage{
		Operation:    "UPDATE",
		Json:         []byte("{MSG2}"),
		TimeBasedKey: timeBasedKeyA,
		WalStart:     801,
		Transaction:  transactionA,
	}

	commitA := &marshaller.MarshalledMessage{
		Operation:    "COMMIT",
		Json:         []byte("{}"),
		TimeBasedKey: timeBasedKeyA,
		WalStart:     802,
		Transaction:  transactionA,
	}

	expectedSeenA := &progress.Seen{
		Transaction:    transactionA,
		TimeBasedKey:   timeBasedKeyA,
		CommitWalStart: commitA.WalStart,
		TotalMsgs:      2,
	}

	// Transaction 2
	beginB := &marshaller.MarshalledMessage{
		Operation:    "BEGIN",
		Json:         []byte("{}"),
		TimeBasedKey: timeBasedKeyB,
		WalStart:     900,
		Transaction:  transactionB,
	}

	messageB1 := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{MSG1}"),
		TimeBasedKey: timeBasedKeyB,
		WalStart:     901,
		Transaction:  transactionB,
	}

	messageB2 := &marshaller.MarshalledMessage{
		Operation:    "UPDATE",
		Json:         []byte("{MSG2}"),
		TimeBasedKey: timeBasedKeyB,
		WalStart:     901,
		Transaction:  transactionB,
	}

	commitB := &marshaller.MarshalledMessage{
		Operation:    "COMMIT",
		Json:         []byte("{}"),
		TimeBasedKey: timeBasedKeyB,
		WalStart:     902,
		Transaction:  transactionB,
	}

	expectedSeenB := &progress.Seen{
		Transaction:    transactionB,
		TimeBasedKey:   timeBasedKeyB,
		CommitWalStart: commitB.WalStart,
		TotalMsgs:      2,
	}

	in <- beginA
	in <- messageA1
	in <- messageA2
	in <- commitA

	in <- beginB
	in <- messageB1
	in <- messageB2
	in <- commitB

	// Read batch 0
	result1 := <-outs[0]
	messages1 := result1.GetPayload()
	messagesSlice1, _ := messages1.([]*marshaller.MarshalledMessage)

	assert.Equal(t, batchSize, len(messagesSlice1), "Batch receive should be size 3.")
	assert.Equal(t, messageA1, messagesSlice1[0], "The batch should contain the appropriate message 0.")
	assert.Equal(t, messageA2, messagesSlice1[1], "The batch should contain the appropriate message 1.")
	assert.Equal(t, messageB1, messagesSlice1[2], "The batch should contain the appropriate message 2.")
	if val, ok := result1.GetTransactions().Get(timeBasedKeyA); ok {
		// Transaction set
		assert.Equal(t, beginA.TimeBasedKey, val.(*progress.Written).TimeBasedKey)
		assert.Equal(t, beginA.Transaction, val.(*progress.Written).Transaction)
		assert.Equal(t, 2, val.(*progress.Written).Count)
	} else {
		// Transaction not set
		assert.Fail(t, "A transaction of batch was not seen")
	}

	if val, ok := result1.GetTransactions().Get(timeBasedKeyB); ok {
		// Transaction set
		assert.Equal(t, beginB.TimeBasedKey, val.(*progress.Written).TimeBasedKey)
		assert.Equal(t, beginB.Transaction, val.(*progress.Written).Transaction)
		assert.Equal(t, 1, val.(*progress.Written).Count)
	} else {
		// Transaction not set
		assert.Fail(t, "A transaction of batch was not seen")
	}

	// Read batch 1
	result2 := <-outs[0]
	messages2 := result2.GetPayload()
	messagesSlice2, _ := messages2.([]*marshaller.MarshalledMessage)

	assert.Equal(t, 1, len(messagesSlice2), "Batch receive should be size 1.")
	assert.Equal(t, messageB2, messagesSlice2[0], "The batch should contain the appropriate message 0.")
	if val, ok := result2.GetTransactions().Get(timeBasedKeyB); ok {
		assert.Equal(t, beginB.TimeBasedKey, val.(*progress.Written).TimeBasedKey)
		assert.Equal(t, beginB.Transaction, val.(*progress.Written).Transaction)
		assert.Equal(t, 1, val.(*progress.Written).Count)
	} else {
		// Transaction not set
		assert.Fail(t, "A transaction of batch was not seen")
	}

	// Test Transaction channel
	resultTxn1 := <-txnSeen
	resultTxn2 := <-txnSeen
	assert.Equal(t, expectedSeenA, resultTxn1, "Transaction progress received should be correct.")
	assert.Equal(t, expectedSeenB, resultTxn2, "Transaction progress received should be correct.")

	// Verify stats
	expectedStats := []stats.Stat{
		stats.NewStatHistogram("batcher", "batch_write_wait", 1, time.Now().UnixNano(), "ms"),
		stats.NewStatCount("batcher", "batches", 1, time.Now().UnixNano()),
		stats.NewStatHistogram("batcher", "batch_size", 3, time.Now().UnixNano(), "count"),
		stats.NewStatHistogram("batcher", "batch_write_wait", 1, time.Now().UnixNano(), "ms"),
		stats.NewStatCount("batcher", "batches", 1, time.Now().UnixNano()),
		stats.NewStatHistogram("batcher", "batch_size", 1, time.Now().UnixNano(), "count"),
		stats.NewStatCount("batcher", "batch_closed_early", 1, time.Now().UnixNano()),
	}

	stats.VerifyStats(t, statsChan, expectedStats)
}

func TestBatchSizeOneTwoTxnTwoWorkers(t *testing.T) {
	timeBasedKeyA := "1-1"
	transactionA := "1"
	timeBasedKeyB := "2-1"
	transactionB := "2"
	batchSize := 1

	in := make(chan *marshaller.MarshalledMessage, 1000)
	txnSeen := make(chan *progress.Seen, 1000)
	statsChan := make(chan stats.Stat, 1000)
	batchFactory := batch.NewGenericBatchFactory(1)
	sh := shutdown.NewShutdownHandler()

	b := NewBatcher(sh, in, txnSeen, statsChan, DEFAULT_TICK_RATE, batchFactory, 2, 500, 1000, 1, DEFAULT_MAX_MEMORY_BYTES, BATCH_ROUTING_ROUND_ROBIN)
	outs := b.GetOutputChans()

	go b.StartBatching()

	// Transaction 1
	beginA := &marshaller.MarshalledMessage{
		Operation:    "BEGIN",
		Json:         []byte("{}"),
		TimeBasedKey: timeBasedKeyA,
		WalStart:     800,
		Transaction:  transactionA,
	}
	messageA := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{MSG1}"),
		TimeBasedKey: timeBasedKeyA,
		WalStart:     801,
		Transaction:  transactionA,
	}

	commitA := &marshaller.MarshalledMessage{
		Operation:    "COMMIT",
		Json:         []byte("{}"),
		TimeBasedKey: timeBasedKeyA,
		WalStart:     802,
		Transaction:  transactionA,
	}

	expectedSeenA := &progress.Seen{
		Transaction:    transactionA,
		TimeBasedKey:   timeBasedKeyA,
		CommitWalStart: commitA.WalStart,
		TotalMsgs:      1,
	}

	// Transaction 2
	beginB := &marshaller.MarshalledMessage{
		Operation:    "BEGIN",
		Json:         []byte("{}"),
		TimeBasedKey: timeBasedKeyB,
		WalStart:     900,
		Transaction:  transactionB,
	}
	messageB := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{MSG1}"),
		TimeBasedKey: timeBasedKeyB,
		WalStart:     901,
		Transaction:  transactionB,
	}
	commitB := &marshaller.MarshalledMessage{
		Operation:    "COMMIT",
		Json:         []byte("{}"),
		TimeBasedKey: timeBasedKeyB,
		WalStart:     902,
		Transaction:  transactionB,
	}

	expectedSeenB := &progress.Seen{
		Transaction:    transactionB,
		TimeBasedKey:   timeBasedKeyB,
		CommitWalStart: commitB.WalStart,
		TotalMsgs:      1,
	}

	in <- beginA
	in <- messageA
	in <- commitA

	in <- beginB
	in <- messageB
	in <- commitB

	// Read worker 0
	result1 := <-outs[0]
	messages1 := result1.GetPayload()
	messagesSlice1, _ := messages1.([]*marshaller.MarshalledMessage)

	assert.Equal(t, batchSize, len(messagesSlice1), "Batch receive should be size 1.")
	assert.Equal(t, messageA, messagesSlice1[0], "The batch should contain the appropriate message 0.")
	if val, ok := result1.GetTransactions().Get(timeBasedKeyA); ok {
		// Transaction set
		assert.Equal(t, beginA.TimeBasedKey, val.(*progress.Written).TimeBasedKey)
		assert.Equal(t, beginA.Transaction, val.(*progress.Written).Transaction)
		assert.Equal(t, 1, val.(*progress.Written).Count)
	} else {
		// Transaction not set
		assert.Fail(t, "A transaction of batch was not seen")
	}

	// Read worker 1
	result2 := <-outs[1]
	messages2 := result2.GetPayload()
	messagesSlice2, _ := messages2.([]*marshaller.MarshalledMessage)

	assert.Equal(t, batchSize, len(messagesSlice2), "Batch receive should be size 1.")
	assert.Equal(t, messageB, messagesSlice2[0], "The batch should contain the appropriate message 0.")
	if val, ok := result2.GetTransactions().Get(timeBasedKeyB); ok {
		// Transaction set
		assert.Equal(t, beginB.TimeBasedKey, val.(*progress.Written).TimeBasedKey)
		assert.Equal(t, beginB.Transaction, val.(*progress.Written).Transaction)
		assert.Equal(t, 1, val.(*progress.Written).Count)
	} else {
		// Transaction not set
		assert.Fail(t, "A transaction of batch was not seen")
	}

	// Test Transaction channel
	resultTxn1 := <-txnSeen
	resultTxn2 := <-txnSeen
	assert.Equal(t, expectedSeenA, resultTxn1, "Transaction progress received should be correct.")
	assert.Equal(t, expectedSeenB, resultTxn2, "Transaction progress received should be correct.")
}

func TestInputChannelClose(t *testing.T) {
	// Setup
	in := make(chan *marshaller.MarshalledMessage, 1000)
	txnSeen := make(chan *progress.Seen, 1000)
	statsChan := make(chan stats.Stat, 1000)
	batchFactory := batch.NewGenericBatchFactory(1)
	sh := shutdown.NewShutdownHandler()

	b := NewBatcher(sh, in, txnSeen, statsChan, DEFAULT_TICK_RATE, batchFactory, 1, 500, 1000, 1, DEFAULT_MAX_MEMORY_BYTES, BATCH_ROUTING_ROUND_ROBIN)
	outs := b.GetOutputChans()

	go b.StartBatching()

	// Close input channel
	close(in)

	// Verify output channels get closed
	_, ok1 := <-txnSeen
	fmt.Println("checking if transactions channel is closed")
	if ok1 {
		assert.Fail(t, "transactions channel not properly closed")
	}

	// Verify output channels get closed
	_, ok2 := <-outs[0]
	fmt.Println("checking if output channel is closed")
	if ok2 {
		assert.Fail(t, "output channel not properly closed")
	}

	_, ok3 := <-b.shutdownHandler.TerminateCtx.Done()
	if ok3 {
		assert.Fail(t, "context not cancelled")
	}
}

func TestErrMsgTooBig(t *testing.T) {
	// Setup mock
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockBatch := mocks.NewMockBatch(mockCtrl)
	mockBatchFactory := mocks.NewMockBatchFactory(mockCtrl)

	// Setup IO
	in := make(chan *marshaller.MarshalledMessage, 1000)
	txnSeen := make(chan *progress.Seen, 1000)
	statsChan := make(chan stats.Stat, 1000)
	sh := shutdown.NewShutdownHandler()

	b := NewBatcher(sh, in, txnSeen, statsChan, DEFAULT_TICK_RATE, mockBatchFactory, 1, 500, 1000, 1, DEFAULT_MAX_MEMORY_BYTES, BATCH_ROUTING_ROUND_ROBIN)

	message := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{MSG1}"),
		TimeBasedKey: "1",
		WalStart:     901,
		PartitionKey: "foo",
	}

	// Expects
	mockBatchFactory.EXPECT().NewBatch("foo").Return(mockBatch)

	// Loop iteration 1 - add a message that is too big
	in <- message
	mockBatch.EXPECT().IsFull().Return(false)
	mockBatch.EXPECT().Add(message).Return(false, errors.New(transport.ERR_MSG_TOOBIG))

	// Loop iteration 2 - add a normal message
	in <- message
	mockBatch.EXPECT().IsFull().Return(false)
	mockBatch.EXPECT().Add(message).Return(true, nil)

	go b.StartBatching()

	// Let test run
	time.Sleep(time.Millisecond * 5)

	// Shutdown test
	close(in)
	time.Sleep(time.Millisecond * 5)

	// Verify stats
	expected := []stats.Stat{stats.NewStatCount("batcher", "dropped_too_big", 1, time.Now().UnixNano())}
	stats.VerifyStats(t, statsChan, expected)
}

func TestErrCantFit(t *testing.T) {
	// Setup mock
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockBatch := mocks.NewMockBatch(mockCtrl)
	mockBatchFactory := mocks.NewMockBatchFactory(mockCtrl)

	// Setup IO
	in := make(chan *marshaller.MarshalledMessage, 1000)
	txnSeen := make(chan *progress.Seen, 1000)
	statsChan := make(chan stats.Stat, 1000)
	sh := shutdown.NewShutdownHandler()

	b := NewBatcher(sh, in, txnSeen, statsChan, DEFAULT_TICK_RATE, mockBatchFactory, 1, 500, 1000, 1, DEFAULT_MAX_MEMORY_BYTES, BATCH_ROUTING_ROUND_ROBIN)

	message := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{MSG1}"),
		TimeBasedKey: "1",
		WalStart:     901,
		PartitionKey: "foo",
	}

	// Expects
	mockBatchFactory.EXPECT().NewBatch("foo").Return(mockBatch)

	// Loop iteration 1 - add a message that can't fit into existing batch
	in <- message
	mockBatch.EXPECT().IsFull().Return(false)
	mockBatch.EXPECT().Add(message).Return(false, errors.New(transport.ERR_CANT_FIT))

	// Exiting batch should be closed
	mockBatch.EXPECT().Close().Return(true, nil)
	// Existing batch transactions should be sent
	mockBatch.EXPECT().NumMessages().Return(1)

	// New batch should be created
	mockBatchFactory.EXPECT().NewBatch("foo").Return(mockBatch)

	// Add message to batch
	mockBatch.EXPECT().Add(message).Return(true, nil)

	// Loop iteration 2 - add another message to new batch
	in <- message
	mockBatch.EXPECT().IsFull().Return(false)
	mockBatch.EXPECT().Add(message).Return(true, nil)

	go b.StartBatching()

	// Let test run
	time.Sleep(time.Millisecond * 5)

	// Shutdown test
	close(in)
	time.Sleep(time.Millisecond * 5)
}

func TestErrMsgInvalid(t *testing.T) {
	// Setup mock
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockBatch := mocks.NewMockBatch(mockCtrl)
	mockBatchFactory := mocks.NewMockBatchFactory(mockCtrl)

	// Setup IO
	in := make(chan *marshaller.MarshalledMessage, 1000)
	txnSeen := make(chan *progress.Seen, 1000)
	statsChan := make(chan stats.Stat, 1000)
	sh := shutdown.NewShutdownHandler()

	b := NewBatcher(sh, in, txnSeen, statsChan, DEFAULT_TICK_RATE, mockBatchFactory, 1, 500, 1000, 1, DEFAULT_MAX_MEMORY_BYTES, BATCH_ROUTING_ROUND_ROBIN)

	message := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{MSG1}"),
		TimeBasedKey: "1",
		WalStart:     901,
		PartitionKey: "foo",
	}

	// Expects
	mockBatchFactory.EXPECT().NewBatch("foo").Return(mockBatch)

	// Loop iteration 1 - add a message that can't fit into existing batch
	in <- message
	mockBatch.EXPECT().IsFull().Return(false)
	mockBatch.EXPECT().Add(message).Return(false, errors.New(transport.ERR_MSG_INVALID))

	// Loop iteration 2 - add another message to new batch
	in <- message
	mockBatch.EXPECT().IsFull().Return(false)
	mockBatch.EXPECT().Add(message).Return(true, nil)

	go b.StartBatching()

	// Let test run
	time.Sleep(time.Millisecond * 5)

	// Shutdown test
	close(in)
	time.Sleep(time.Millisecond * 5)

	// Verify stats
	expected := []stats.Stat{stats.NewStatCount("batcher", "dropped_msg_invalid", 1, time.Now().UnixNano())}
	stats.VerifyStats(t, statsChan, expected)
}

func TestErrUnknown(t *testing.T) {
	// Setup mock
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockBatch := mocks.NewMockBatch(mockCtrl)
	mockBatchFactory := mocks.NewMockBatchFactory(mockCtrl)

	// Setup IO
	in := make(chan *marshaller.MarshalledMessage, 1000)
	txnSeen := make(chan *progress.Seen, 1000)
	statsChan := make(chan stats.Stat, 1000)
	sh := shutdown.NewShutdownHandler()

	b := NewBatcher(sh, in, txnSeen, statsChan, DEFAULT_TICK_RATE, mockBatchFactory, 1, 500, 1000, 1, DEFAULT_MAX_MEMORY_BYTES, BATCH_ROUTING_ROUND_ROBIN)

	outs := b.GetOutputChans()

	message := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{MSG1}"),
		TimeBasedKey: "1",
		WalStart:     901,
		PartitionKey: "foo",
	}

	// Expects
	mockBatchFactory.EXPECT().NewBatch("foo").Return(mockBatch)

	// Loop iteration 1 - add a message that can't fit into existing batch
	in <- message
	mockBatch.EXPECT().IsFull().Return(false)
	mockBatch.EXPECT().Add(message).Return(false, errors.New("new error"))

	go b.StartBatching()

	// Let test run
	time.Sleep(time.Millisecond * 5)

	// Verify shutdown is called and channels are closed
	// Verify output channels get closed
	_, ok1 := <-txnSeen
	fmt.Println("checking if transactions channel is closed")
	if ok1 {
		assert.Fail(t, "transactions channel not properly closed")
	}

	// Verify output channels get closed
	_, ok2 := <-outs[0]
	fmt.Println("checking if output channel is closed")
	if ok2 {
		assert.Fail(t, "output channel not properly closed")
	}
}

func TestFlushBatchTimeoutUpdate(t *testing.T) {
	// Setup mock
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockBatch := mocks.NewMockBatch(mockCtrl)
	mockBatchFactory := mocks.NewMockBatchFactory(mockCtrl)

	// Setup IO
	in := make(chan *marshaller.MarshalledMessage, 1000)
	txnSeen := make(chan *progress.Seen, 1000)
	statsChan := make(chan stats.Stat, 1000)
	sh := shutdown.NewShutdownHandler()

	flushBatchUpdateAge := 500
	flushBatchMaxAge := 1000000

	b := NewBatcher(sh, in, txnSeen, statsChan, DEFAULT_TICK_RATE, mockBatchFactory, 1, flushBatchUpdateAge, flushBatchMaxAge, 1, DEFAULT_MAX_MEMORY_BYTES, BATCH_ROUTING_ROUND_ROBIN)

	insert := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{MSG1}"),
		TimeBasedKey: "1",
		WalStart:     901,
		PartitionKey: "foo",
	}

	// Expects
	mockBatchFactory.EXPECT().NewBatch("foo").Return(mockBatch)

	// Loop iteration 1 - add commit to full batch
	in <- insert
	mockBatch.EXPECT().Add(insert).Return(true, nil)
	mockBatch.EXPECT().IsFull().Return(false).Times(2)

	// Loop iteration 2 - timeout
	mockBatch.EXPECT().Close().Return(true, nil)
	mockBatch.EXPECT().IsEmpty().Return(false).Times(2)
	mockBatch.EXPECT().NumMessages().Return(1)
	mockBatch.EXPECT().ModifyTime().Return(time.Now().UnixNano())
	mockBatch.EXPECT().CreateTime().Return(time.Now().UnixNano())

	go b.StartBatching()

	// Sleep enough to trigger ticker timeout
	time.Sleep(DEFAULT_TICK_RATE_DURATION + time.Millisecond*100)

	// Shutdown test
	close(in)
	time.Sleep(time.Millisecond * 5)
}

func TestFlushBatchTimeoutMaxAge(t *testing.T) {
	// Setup mock
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockBatch := mocks.NewMockBatch(mockCtrl)
	mockBatchFactory := mocks.NewMockBatchFactory(mockCtrl)

	// Setup IO
	in := make(chan *marshaller.MarshalledMessage, 1000)
	txnSeen := make(chan *progress.Seen, 1000)
	statsChan := make(chan stats.Stat, 1000)
	sh := shutdown.NewShutdownHandler()

	flushBatchUpdateAge := 9000
	flushBatchMaxAge := 1000

	b := NewBatcher(sh, in, txnSeen, statsChan, DEFAULT_TICK_RATE, mockBatchFactory, 1, flushBatchUpdateAge, flushBatchMaxAge, 1, DEFAULT_MAX_MEMORY_BYTES, BATCH_ROUTING_ROUND_ROBIN)

	insert := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{MSG1}"),
		TimeBasedKey: "1",
		WalStart:     901,
		PartitionKey: "foo",
	}

	// Expects
	mockBatchFactory.EXPECT().NewBatch("foo").Return(mockBatch)

	// Loop iteration 1 - add commit to full batch
	in <- insert
	mockBatch.EXPECT().Add(insert).Return(true, nil)
	mockBatch.EXPECT().IsFull().Return(false).Times(2)

	// Loop iteration 2 - timeout
	mockBatch.EXPECT().Close().Return(true, nil)
	mockBatch.EXPECT().IsEmpty().Return(false).Times(2)
	mockBatch.EXPECT().NumMessages().Return(1)
	mockBatch.EXPECT().ModifyTime().Return(time.Now().UnixNano())
	mockBatch.EXPECT().CreateTime().Return(time.Now().UnixNano())

	go b.StartBatching()

	// Sleep enough to trigger ticker timeout
	time.Sleep(DEFAULT_TICK_RATE_DURATION + time.Millisecond*100)

	// Shutdown test
	close(in)
	time.Sleep(time.Millisecond * 5)
}

func TestFlushFullBatch(t *testing.T) {
	// Setup mock
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockBatch := mocks.NewMockBatch(mockCtrl)
	mockBatchFactory := mocks.NewMockBatchFactory(mockCtrl)

	// Setup IO
	in := make(chan *marshaller.MarshalledMessage, 1000)
	txnSeen := make(chan *progress.Seen, 1000)
	statsChan := make(chan stats.Stat, 1000)
	sh := shutdown.NewShutdownHandler()

	flushBatchUpdateAge := 500
	flushBatchMaxAge := 1000

	b := NewBatcher(sh, in, txnSeen, statsChan, DEFAULT_TICK_RATE, mockBatchFactory, 1, flushBatchUpdateAge, flushBatchMaxAge, 1, DEFAULT_MAX_MEMORY_BYTES, BATCH_ROUTING_ROUND_ROBIN)

	commit := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{MSG1}"),
		TimeBasedKey: "1",
		WalStart:     901,
		PartitionKey: "foo",
	}

	// Expects
	mockBatchFactory.EXPECT().NewBatch("foo").Return(mockBatch)

	// Loop iteration 1 - add commit to full batch
	in <- commit
	mockBatch.EXPECT().Add(commit).Return(true, nil)
	mockBatch.EXPECT().IsFull().Return(false).Times(1)

	// Loop iteration 2 - timeout
	mockBatch.EXPECT().IsEmpty().Return(false).Times(2)
	mockBatch.EXPECT().IsFull().Return(true).Times(1)
	mockBatch.EXPECT().Close().Return(true, nil)
	mockBatch.EXPECT().NumMessages().Return(1)

	mockBatch.EXPECT().ModifyTime().Return(time.Now().UnixNano())
	mockBatch.EXPECT().CreateTime().Return(time.Now().UnixNano())

	go b.StartBatching()

	// Sleep enough to trigger ticker timeout
	time.Sleep(DEFAULT_TICK_RATE_DURATION + time.Millisecond*100)

	// Shutdown test
	close(in)
	time.Sleep(time.Millisecond * 5)

	timeout := time.NewTimer(25 * time.Millisecond)
	select {
	case c, ok := <-b.outputChans[0]:
		if !ok {
			assert.Fail(t, "Batch should have been sent.")
		}

		assert.Equal(t, mockBatch, c)
	case <-timeout.C:
		assert.Fail(t, "Batch should have been sent.")
	}
}

func TestFlushEmptyBatchTimeout(t *testing.T) {
	// Setup mock
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockBatchFactory := mocks.NewMockBatchFactory(mockCtrl)

	// Setup IO
	in := make(chan *marshaller.MarshalledMessage, 1000)
	txnSeen := make(chan *progress.Seen, 1000)
	statsChan := make(chan stats.Stat, 1000)
	sh := shutdown.NewShutdownHandler()

	flushBatchUpdateAge := 500
	flushBatchMaxAge := 1000

	b := NewBatcher(sh, in, txnSeen, statsChan, DEFAULT_TICK_RATE, mockBatchFactory, 1, flushBatchUpdateAge, flushBatchMaxAge, 1, DEFAULT_MAX_MEMORY_BYTES, BATCH_ROUTING_ROUND_ROBIN)

	go b.StartBatching()

	// Sleep enough to trigger ticker timeout
	time.Sleep(time.Duration(DEFAULT_TICK_RATE) + time.Millisecond*100)

	// Shutdown test
	close(in)
	time.Sleep(time.Millisecond * 5)

	// Verify empty batch was not sent
	timeout := time.NewTimer(25 * time.Millisecond)

	select {
	case _, ok := <-b.outputChans[0]:
		if ok {
			assert.Fail(t, "No batch should have been sent.")
		}
	case <-timeout.C:
		// Pass
	}
}

func TestTxnsSeenTimeout(t *testing.T) {
	// Setup mock
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockBatch := mocks.NewMockBatch(mockCtrl)
	mockBatchFactory := mocks.NewMockBatchFactory(mockCtrl)

	// Setup IO
	in := make(chan *marshaller.MarshalledMessage, 1000)
	txnSeen := make(chan *progress.Seen) // unbuffered channel which will block (this is the test)
	statsChan := make(chan stats.Stat, 1000)
	sh := shutdown.NewShutdownHandler()

	flushBatchUpdateAge := 500
	flushBatchMaxAge := 1000
	b := NewBatcher(sh, in, txnSeen, statsChan, DEFAULT_TICK_RATE, mockBatchFactory, 1, flushBatchUpdateAge, flushBatchMaxAge, 1, DEFAULT_MAX_MEMORY_BYTES, BATCH_ROUTING_ROUND_ROBIN)
	b.txnsSeenTimeout = time.Millisecond * 50

	commit := &marshaller.MarshalledMessage{
		Operation:    "COMMIT",
		Json:         []byte("{MSG1}"),
		TimeBasedKey: "1-1",
		WalStart:     901,
		PartitionKey: "foo",
		Transaction:  "1",
	}

	// Expects
	mockBatchFactory.EXPECT().NewBatch("foo").Return(mockBatch).Times(1)

	// Loop iteration 1 - add commit
	in <- commit

	go b.StartBatching()

	// Sleep enough to trigger timeout
	time.Sleep(b.txnsSeenTimeout + time.Millisecond*50)
	close(in)
	time.Sleep(time.Millisecond * 20)

	// Verify nothing came out
	var timeout = time.NewTimer(25 * time.Millisecond)

	select {
	case _, ok := <-txnSeen:
		if ok {
			assert.Fail(t, "No transactions should have been seen.")
		}
	case <-timeout.C:
		// Pass
	}
}

func getBasicSetup(t *testing.T) (*gomock.Controller, chan *marshaller.MarshalledMessage, chan *progress.Seen, *Batcher, *mocks.MockBatchFactory, *mocks.MockBatch) {
	// Setup mock
	mockCtrl := gomock.NewController(t)

	mockBatchFactory := mocks.NewMockBatchFactory(mockCtrl)
	mockBatch := mocks.NewMockBatch(mockCtrl)

	// Setup IO
	in := make(chan *marshaller.MarshalledMessage, 1000)
	txnSeen := make(chan *progress.Seen, 1000)
	statsChan := make(chan stats.Stat, 1000)
	sh := shutdown.NewShutdownHandler()

	flushBatchUpdateAge := 500
	flushBatchMaxAge := 1000
	b := NewBatcher(sh, in, txnSeen, statsChan, DEFAULT_TICK_RATE, mockBatchFactory, 1, flushBatchUpdateAge, flushBatchMaxAge, 1, DEFAULT_MAX_MEMORY_BYTES, BATCH_ROUTING_ROUND_ROBIN)

	return mockCtrl, in, txnSeen, b, mockBatchFactory, mockBatch
}

func TestTerminationContext(t *testing.T) {
	// Setup mock
	mockCtrl, _, txnSeen, b, _, _ := getBasicSetup(t)
	defer mockCtrl.Finish()

	go b.StartBatching()

	time.Sleep(time.Millisecond * 5)
	b.shutdownHandler.CancelFunc()
	time.Sleep(time.Millisecond * 5)

	_, ok1 := <-txnSeen
	fmt.Println("checking if transactions channel is closed")
	if ok1 {
		assert.Fail(t, "transactions channel not properly closed")
	}

	// Verify output channels get closed
	_, ok2 := <-b.outputChans[0]
	fmt.Println("checking if output channel is closed")
	if ok2 {
		assert.Fail(t, "output channel not properly closed")
	}
}

func TestAddToBatchSendFatal(t *testing.T) {
	// Setup mock
	mockCtrl, in, _, b, mockBatchFactory, mockBatch := getBasicSetup(t)
	defer mockCtrl.Finish()
	b.txnsSeenTimeout = time.Millisecond * 1

	mockBatchFactory.EXPECT().NewBatch("foo").Return(mockBatch)

	// Loop iteration 1 - add commit to full batch
	commit := &marshaller.MarshalledMessage{
		Operation:    "UPDATE",
		Json:         []byte("{MSG1}"),
		TimeBasedKey: "1-1",
		WalStart:     901,
		PartitionKey: "foo",
		Transaction:  "1",
	}

	in <- commit

	txnSeen := make(chan *progress.Seen)
	b.txnsSeenChan = txnSeen

	mockBatch.EXPECT().IsFull().Return(false)
	mockBatch.EXPECT().Add(commit).Return(false, errors.New(transport.ERR_CANT_FIT))
	mockBatch.EXPECT().Close().Return(false, errors.New("expected error"))

	go b.StartBatching()

	time.Sleep(time.Millisecond * 5)
	b.shutdownHandler.CancelFunc()
	time.Sleep(time.Millisecond * 10)

	// Verify output channels get closed
	_, ok2 := <-b.outputChans[0]
	fmt.Println("checking if output channel is closed")
	if ok2 {
		assert.Fail(t, "output channel not properly closed")
	}
}

func TestPanicRecovery(t *testing.T) {
	// Setup mock
	mockCtrl, in, _, b, mockBatchFactory, _ := getBasicSetup(t)
	defer mockCtrl.Finish()
	b.txnsSeenTimeout = time.Millisecond * 1

	mockBatchFactory.EXPECT().NewBatch("foo").Do(func(_ interface{}) { panic("expected") })

	commit := &marshaller.MarshalledMessage{
		Operation:    "UPDATE",
		Json:         []byte("{MSG1}"),
		TimeBasedKey: "1-1",
		WalStart:     901,
		PartitionKey: "foo",
		Transaction:  "1",
	}

	in <- commit

	go b.StartBatching()

	time.Sleep(time.Millisecond * 5)

	// Verify output channels get closed
	_, ok2 := <-b.outputChans[0]
	fmt.Println("checking if output channel is closed")
	if ok2 {
		assert.Fail(t, "output channel not properly closed")
	}
}

func TestBatchPicking(t *testing.T) {
	// Setup mock
	mockCtrl, in, _, b, mockBatchFactory, _ := getBasicSetup(t)
	defer mockCtrl.Finish()

	data1 := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{MSG1}"),
		TimeBasedKey: "1-1",
		WalStart:     901,
		PartitionKey: "foo",
		Transaction:  "1",
	}

	data2 := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{MSG2}"),
		TimeBasedKey: "2-1",
		WalStart:     902,
		PartitionKey: "bar",
		Transaction:  "2",
	}

	data3 := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{MSG3}"),
		TimeBasedKey: "3-1",
		WalStart:     903,
		PartitionKey: "foo",
		Transaction:  "3",
	}

	mockBatch1 := mocks.NewMockBatch(mockCtrl)
	mockBatch2 := mocks.NewMockBatch(mockCtrl)

	mockBatchFactory.EXPECT().NewBatch("foo").Return(mockBatch1)
	mockBatch1.EXPECT().IsFull().Return(false)
	mockBatch1.EXPECT().Add(data1).Return(true, nil)

	mockBatchFactory.EXPECT().NewBatch("bar").Return(mockBatch2)
	mockBatch2.EXPECT().IsFull().Return(false)
	mockBatch2.EXPECT().Add(data2).Return(true, nil)

	mockBatch1.EXPECT().IsFull().Return(false)
	mockBatch1.EXPECT().Add(data3).Return(true, nil)

	in <- data1
	in <- data2
	in <- data3
	close(in)

	go b.StartBatching()

	time.Sleep(time.Millisecond * 10)
}

func TestFlushMemoryPressure(t *testing.T) {
	// Setup mock
	mockCtrl, in, _, b, mockBatchFactory, _ := getBasicSetup(t)
	defer mockCtrl.Finish()

	data1 := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{MSG1}"),
		TimeBasedKey: "1-1",
		WalStart:     901,
		PartitionKey: "foo",
		Transaction:  "1",
	}

	// Add data
	mockBatch := mocks.NewMockBatch(mockCtrl)
	mockBatchFactory.EXPECT().NewBatch("foo").Return(mockBatch)
	mockBatch.EXPECT().IsFull().Return(false)
	mockBatch.EXPECT().Add(data1).Return(true, nil)

	// Ticker ticked checks
	mockBatch.EXPECT().IsEmpty().Return(false).Times(2)
	mockBatch.EXPECT().ModifyTime().Return(time.Now().UnixNano() + int64(2*DEFAULT_TICK_RATE_DURATION))
	mockBatch.EXPECT().CreateTime().Return(time.Now().UnixNano() + int64(2*DEFAULT_TICK_RATE_DURATION))
	mockBatch.EXPECT().IsFull().Return(false)
	mockBatch.EXPECT().GetPayloadByteSize().Return(b.batcherMemorySoftLimit + 1)
	mockBatch.EXPECT().GetPayloadByteSize().Return(b.batcherMemorySoftLimit + 1)
	mockBatch.EXPECT().Close().Return(true, nil)
	mockBatch.EXPECT().NumMessages().Return(1)

	in <- data1

	go b.StartBatching()

	// Block waiting for output
	select {
	case <-b.outputChans[0]:
		close(in)
		time.Sleep(time.Millisecond * 10)
	case <-time.After(DEFAULT_TICK_RATE_DURATION + time.Millisecond*200):
		assert.Fail(t, "did not get output in time")
	}
}

func TestFlushMemoryPressureTwoBatches(t *testing.T) {
	// Setup mock
	mockCtrl, in, _, b, mockBatchFactory, _ := getBasicSetup(t)
	defer mockCtrl.Finish()

	data1 := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{MSG1}"),
		TimeBasedKey: "1-1",
		WalStart:     901,
		PartitionKey: "foo",
		Transaction:  "1",
	}

	data2 := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{MSG1}"),
		TimeBasedKey: "1-1",
		WalStart:     902,
		PartitionKey: "bar",
		Transaction:  "1",
	}

	// Add data1 to mockBatch1
	mockBatch1 := mocks.NewMockBatch(mockCtrl)
	mockBatchFactory.EXPECT().NewBatch("foo").Return(mockBatch1)
	mockBatch1.EXPECT().IsFull().Return(false)
	mockBatch1.EXPECT().Add(data1).Return(true, nil)

	// Add data2 to mockBatch2
	mockBatch2 := mocks.NewMockBatch(mockCtrl)
	mockBatchFactory.EXPECT().NewBatch("bar").Return(mockBatch2)
	mockBatch2.EXPECT().IsFull().Return(false)
	mockBatch2.EXPECT().Add(data2).Return(true, nil)

	// Ticker ticked
	mockBatch1.EXPECT().IsEmpty().Return(false)
	mockBatch1.EXPECT().ModifyTime().Return(time.Now().UnixNano() + int64(2*DEFAULT_TICK_RATE_DURATION))
	mockBatch1.EXPECT().CreateTime().Return(time.Now().UnixNano() + int64(2*DEFAULT_TICK_RATE_DURATION))
	mockBatch1.EXPECT().IsFull().Return(false)
	mockBatch1.EXPECT().GetPayloadByteSize().Return(b.batcherMemorySoftLimit + 1)

	mockBatch2.EXPECT().IsEmpty().Return(false)
	mockBatch2.EXPECT().ModifyTime().Return(time.Now().UnixNano() + int64(2*DEFAULT_TICK_RATE_DURATION))
	mockBatch2.EXPECT().CreateTime().Return(time.Now().UnixNano() + int64(2*DEFAULT_TICK_RATE_DURATION))
	mockBatch2.EXPECT().IsFull().Return(false)
	mockBatch2.EXPECT().GetPayloadByteSize().Return(b.batcherMemorySoftLimit - 1)

	// Flush batch
	mockBatch1.EXPECT().IsEmpty().Return(false)
	mockBatch1.EXPECT().GetPayloadByteSize().Return(b.batcherMemorySoftLimit + 1)
	mockBatch1.EXPECT().Close().Return(true, nil)
	mockBatch1.EXPECT().NumMessages().Return(1)

	in <- data1
	in <- data2

	go b.StartBatching()

	// Block waiting for output
	select {
	case <-b.outputChans[0]:
		close(in)
	case <-time.After(DEFAULT_TICK_RATE_DURATION + time.Millisecond*100):
		assert.Fail(t, "did not get output in time")
	}

	time.Sleep(time.Millisecond * 50)
}

func TestCloseBatchError(t *testing.T) {
	// Setup mock
	mockCtrl, in, _, b, mockBatchFactory, _ := getBasicSetup(t)
	defer mockCtrl.Finish()

	data1 := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{MSG1}"),
		TimeBasedKey: "1-1",
		WalStart:     901,
		PartitionKey: "foo",
		Transaction:  "1",
	}

	// Add data
	mockBatch := mocks.NewMockBatch(mockCtrl)
	mockBatchFactory.EXPECT().NewBatch("foo").Return(mockBatch)
	mockBatch.EXPECT().IsFull().Return(false)
	mockBatch.EXPECT().Add(data1).Return(true, nil)

	// Ticker ticked checks
	mockBatch.EXPECT().ModifyTime().Return(time.Now().UnixNano() - 10000000)
	mockBatch.EXPECT().CreateTime().Return(time.Now().UnixNano() - 10000000)
	mockBatch.EXPECT().IsEmpty().Return(false).Times(2)
	mockBatch.EXPECT().IsFull().Return(false)
	mockBatch.EXPECT().Close().Return(false, errors.New("expected"))

	in <- data1

	go b.StartBatching()

	// Sleep enough to trigger ticker timeout
	time.Sleep(DEFAULT_TICK_RATE_DURATION + time.Millisecond*100)

	// Verify output channels get closed
	_, ok2 := <-b.outputChans[0]
	fmt.Println("checking if output channel is closed")
	if ok2 {
		assert.Fail(t, "output channel not properly closed")
	}
}

func TestPartitionRouting(t *testing.T) {
	// Setup mock
	mockCtrl := gomock.NewController(t)

	mockBatchFactory := mocks.NewMockBatchFactory(mockCtrl)

	// Setup IO
	in := make(chan *marshaller.MarshalledMessage, 1000)
	txnSeen := make(chan *progress.Seen, 1000)
	statsChan := make(chan stats.Stat, 1000)
	sh := shutdown.NewShutdownHandler()

	flushBatchUpdateAge := 500
	flushBatchMaxAge := 1000
	b := NewBatcher(sh, in, txnSeen, statsChan, DEFAULT_TICK_RATE, mockBatchFactory, 2, flushBatchUpdateAge, flushBatchMaxAge, 1, DEFAULT_MAX_MEMORY_BYTES, BATCH_ROUTING_PARTITION)

	defer mockCtrl.Finish()

	data1 := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{MSG1}"),
		TimeBasedKey: "1-1",
		WalStart:     901,
		PartitionKey: "foo",
		Transaction:  "1",
	}

	data2 := &marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{MSG1}"),
		TimeBasedKey: "1-1",
		WalStart:     902,
		PartitionKey: "bar",
		Transaction:  "1",
	}

	// Add data1 to mockBatch1
	mockBatch1 := mocks.NewMockBatch(mockCtrl)
	mockBatchFactory.EXPECT().NewBatch("foo").Return(mockBatch1)
	mockBatch1.EXPECT().IsFull().Return(false)
	mockBatch1.EXPECT().Add(data1).Return(true, nil)

	// Add data2 to mockBatch2
	mockBatch2 := mocks.NewMockBatch(mockCtrl)
	mockBatchFactory.EXPECT().NewBatch("bar").Return(mockBatch2)
	mockBatch2.EXPECT().IsFull().Return(false)
	mockBatch2.EXPECT().Add(data2).Return(true, nil)

	// Ticker ticked
	mockBatch1.EXPECT().ModifyTime().Return(time.Now().UnixNano())
	mockBatch1.EXPECT().CreateTime().Return(time.Now().UnixNano())
	mockBatch1.EXPECT().IsFull().Return(true)
	mockBatch1.EXPECT().IsEmpty().Return(false).Times(2)

	mockBatch2.EXPECT().ModifyTime().Return(time.Now().UnixNano())
	mockBatch2.EXPECT().CreateTime().Return(time.Now().UnixNano())
	mockBatch2.EXPECT().IsFull().Return(true)
	mockBatch2.EXPECT().IsEmpty().Return(false).Times(2)

	// Flush batch
	mockBatch1.EXPECT().Close().Return(true, nil)
	mockBatch1.EXPECT().NumMessages().Return(1)
	mockBatch1.EXPECT().GetPartitionKey().Return("foo")

	mockBatch2.EXPECT().Close().Return(true, nil)
	mockBatch2.EXPECT().NumMessages().Return(1)
	mockBatch2.EXPECT().GetPartitionKey().Return("bar")

	// Called by this test
	mockBatch1.EXPECT().GetPartitionKey().Return("foo")
	mockBatch2.EXPECT().GetPartitionKey().Return("bar")

	in <- data1
	in <- data2

	go b.StartBatching()

	// Sleep enough to trigger ticker timeout
	time.Sleep(DEFAULT_TICK_RATE_DURATION + time.Millisecond*100)
	close(in)
	time.Sleep(time.Millisecond * 100)

	// Verify each batch went to the correct channel
	a1, ok := <-b.outputChans[0]
	if !ok {
		assert.Fail(t, "output channel was closed")
	}
	assert.Equal(t, "bar", a1.GetPartitionKey())

	a2, ok := <-b.outputChans[1]
	if !ok {
		assert.Fail(t, "output channel was closed")
	}
	assert.Equal(t, "foo", a2.GetPartitionKey())
}
