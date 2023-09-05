package batcher

import (
	"container/heap"
	"os"
	"time"

	"github.com/Nextdoor/pg-bifrost.git/marshaller"
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/Nextdoor/pg-bifrost.git/transport/batcher/queue"
	"github.com/Nextdoor/pg-bifrost.git/transport/progress"
	"github.com/Nextdoor/pg-bifrost.git/utils"
	"github.com/sirupsen/logrus"
)

var (
	logger = logrus.New()
	log    = logger.WithField("package", "batcher")
)

type BatchAddError int

// Batcher's responses to issues adding to a batch.
const (
	BATCH_ADD_OK    BatchAddError = iota // Succeeded adding message to batch.
	BATCH_ADD_FAIL                       // A non-fatal issue with adding to a batch. Could be message can't fit.
	BATCH_ADD_FATAL                      // Batcher is unable to proceed. This may occur if batch is unable to be sent to transport.
)

type BatchRouting int

const (
	BATCH_ROUTING_ROUND_ROBIN BatchRouting = iota // Round-robin based routing
	BATCH_ROUTING_PARTITION                       // Partition based routing
)

var (
	nameToRoutingMethod = map[string]BatchRouting{
		"partition":   BATCH_ROUTING_PARTITION,
		"round-robin": BATCH_ROUTING_ROUND_ROBIN,
	}
)

func GetRoutingMethod(name string) BatchRouting {
	return nameToRoutingMethod[name]
}

const (
	DEFAULT_MAX_MEMORY_BYTES = int64(1024 * 1024 * 100)
	DEFAULT_TICK_RATE        = 1000
)

func init() {
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.InfoLevel)
}

type Batcher struct {
	shutdownHandler shutdown.ShutdownHandler

	inputChan    <-chan *marshaller.MarshalledMessage // receive single MarshalledMessages
	outputChans  []chan transport.Batch               // one output channel per Transporter worker
	txnsSeenChan chan<- []*progress.Seen              // channel to report transactions seen to ProgressTracker
	statsChan    chan stats.Stat

	tickRate               time.Duration              // controls frequency that batcher looks for input. This should be non-zero to avoid CPU spin.
	batchFactory           transport.BatchFactory     // factory to use to create new empty batches
	workers                int                        // number of downstream transport workers. A batcher output channel is created for each worker.
	batches                map[string]transport.Batch // keeps track of incomplete (not full) batches that have not yet been output
	flushBatchUpdateAge    time.Duration              // force batches to be flushed if no new messages are added after this time period
	flushBatchMaxAge       time.Duration              // force batches to be flushed after this time period
	batcherMemorySoftLimit int64                      // total maximum amount of memory to use when storing incomplete (not full) batches
	routingMethod          BatchRouting               // method to use when routing a batch to an output channel/worker
	roundRobinPosition     int                        // in Round-robin routing mode this keeps track of the position
	txnsSeenTimeout        time.Duration              // timeout to wait on writing seen transactions. If this limit is reached then batcher errors out.
	seenList               []*progress.Seen
}

// NewBatcher returns a new Batcher with output channels to use as inputs for Transporters. Note the caller must
// provide a BatchFactory which is compatible with the downstream Transporter. The Batcher does not check
// check compatibility.
func NewBatcher(shutdownHandler shutdown.ShutdownHandler,
	inputChan <-chan *marshaller.MarshalledMessage,
	txnsSeenChan chan<- []*progress.Seen,
	statsChan chan stats.Stat,

	tickRate int, // number of milliseconds that batcher will wait to check for input.
	batchFactory transport.BatchFactory, // factory to use to create new empty batches
	workers int, // number of downstream transport workers. A batcher output channel is created for each worker.
	flushBatchUpdateAge int, // number of milliseconds to wait for new messages before flushing a batch
	flushBatchMaxAge int, // number of milliseconds to wait before flushing incomplete (not full) batches
	batchQueueDepth int, // number of batches that can be queued per worker (channel size)
	maxMemoryBytes int64, // total maximum amount of memory to use when storing incomplete (not full) batches
	routingMethod BatchRouting, // method to use when routing a batch to an output channel/worker
) *Batcher {

	// Create an output channel for each Transporter worker
	var outputChans []chan transport.Batch
	for i := 0; i < workers; i++ {
		c := make(chan transport.Batch, batchQueueDepth)
		outputChans = append(outputChans, c)
	}

	// Keep track of partitionKey:batch
	var batches = make(map[string]transport.Batch)

	return &Batcher{shutdownHandler,
		inputChan,
		outputChans,
		txnsSeenChan,
		statsChan,
		time.Duration(tickRate) * time.Millisecond,
		batchFactory,
		workers,
		batches,
		time.Duration(flushBatchUpdateAge) * time.Millisecond,
		time.Duration(flushBatchMaxAge) * time.Millisecond,
		maxMemoryBytes,
		routingMethod,
		0,
		time.Second * 12, // max time to wait for sending transactions to ProgressTracker via txnsSeenChan
		[]*progress.Seen{},
	}
}

// safeCloseChan is a shutdown helper which recovers closing on a closed channel panic
func safeCloseChan(c chan transport.Batch) {
	defer func() {
		// recover if channel is already closed
		_ = recover()
	}()

	close(c)
}

// shutdown idempotently closes the output channel
func (b *Batcher) shutdown() {
	log.Info("shutting down batcher")
	b.shutdownHandler.CancelFunc() // initiate shutdown on other modules as well

	if r := recover(); r != nil {
		log.Warn("Recovering ", r)
	}

	//Safely close all the output channels
	for i, r := range b.outputChans {
		log.Debugf("closing output channel %d", i)
		safeCloseChan(r)
	}

	// Also close the transaction progress channel
	defer func() {
		// recover if channel is already closed
		_ = recover()
	}()

	close(b.txnsSeenChan)
}

// StartBatching kicks off the batcher which passes filled Batches to Transporters.
func (b *Batcher) StartBatching() {
	log.Info("starting batcher")
	defer b.shutdown()

	ticker := time.NewTicker(b.tickRate)
	defer ticker.Stop()

	var totalMsgsInTxn int // Counts messages in transaction. Does not include BEGIN and COMMITS
	var curTimeBasedKey string

	for {
		// Message channel vars
		var msg *marshaller.MarshalledMessage
		var ok bool
		var batchError BatchAddError
		var curBatch transport.Batch

		// Block waiting for a message or until a timeout is hit, then fall through.
		select {
		case <-b.shutdownHandler.TerminateCtx.Done():
			log.Debug("received terminateCtx cancellation")
			return
		case msg, ok = <-b.inputChan:
			if !ok {
				log.Warn("input channel is closed")
				return
			}
		case <-ticker.C:
			// If ticker ticked then prune/send all batches that need to be sent.
			ok := b.handleTicker()

			if !ok {
				return
			}
			continue
		}

		// Get batch based on partition key
		curBatch, ok = b.batches[msg.PartitionKey]

		// if batch doesn't exist then make a new one
		if !ok {
			curBatch = b.batchFactory.NewBatch(msg.PartitionKey)
			b.batches[msg.PartitionKey] = curBatch
		}

		// Update ledger when a COMMIT is seen
		if msg.Operation == "COMMIT" {
			b.seenList = append(b.seenList, &progress.Seen{
				Transaction:    msg.Transaction,
				TimeBasedKey:   msg.TimeBasedKey,
				TotalMsgs:      totalMsgsInTxn,
				CommitWalStart: msg.WalStart})
		}

		// Reset counter for number of messages in transaction when
		// the time based key changes
		if curTimeBasedKey != msg.TimeBasedKey {
			// Reset counter
			curTimeBasedKey = msg.TimeBasedKey
			//curTransaction = msg.Transaction
			totalMsgsInTxn = 0
		}

		// Before adding a message to a batch check if the batch is full and should be sent
		if curBatch.IsFull() {
			ok := b.sendBatch(curBatch)

			if !ok {
				// Fatal error sending batch
				return
			}

			// Create new batch
			curBatch = b.batchFactory.NewBatch(msg.PartitionKey)
			b.batches[msg.PartitionKey] = curBatch
		}

		// Batch is not full. Add message to batch.
		// Don't put BEGIN/COMMIT into batch
		if msg.Operation == "BEGIN" || msg.Operation == "COMMIT" {
			continue
		}

		// Add to batch, note addToBatch may send the batch and return a new batch if
		// for example the message could not fit into the batch.
		curBatch, batchError = b.addToBatch(curBatch, msg)

		// Could have returned a new batch here so we need to update ref
		b.batches[msg.PartitionKey] = curBatch

		if batchError == BATCH_ADD_FATAL {
			log.Error("fatal error while adding to batch")
			return
		}

		// Increment counter
		totalMsgsInTxn += 1
	}
}

func (b *Batcher) handleTicker() bool {
	toFlush := make([]string, 0)
	totalMemory := int64(0)

	bq := make(queue.BatchQueue, 0)

	// Loop through and mark batches to be flushed
	//  - empty batches with transactions
	//  - batches that haven't been modified recently (since the ticker last ticked)
	//  - batches that are full
	//  - if memory pressure is too large then large batches are also flushed
	queueIndex := 0
	for batchKey, curBatch := range b.batches {
		log.Debugf("checking batch with key %s", batchKey)
		flush := false

		// If batch is empty then discard it. No need to keep it around.
		if curBatch.IsEmpty() {
			log.Debugf("adding %s to be flushed due to emptiness", batchKey)
			flush = true
		}

		// Was the batch last modified (added to) before the last ticker? If no then it needs to be flushed.
		if curBatch.ModifyTime() < time.Now().UnixNano()-b.flushBatchUpdateAge.Nanoseconds() {
			flush = true
			log.Debugf("adding %s to be flushed due to last update age timeout", batchKey)
		}

		// Is the batch older than the max age? If yes then flush
		if curBatch.CreateTime() < time.Now().UnixNano()-b.flushBatchMaxAge.Nanoseconds() {
			flush = true
			log.Debugf("adding %s to be flushed due to max age timeout", batchKey)
		}

		// Full? Needs to be flushed
		if curBatch.IsFull() {
			flush = true
			log.Debugf("adding %s to be flushed due to fill", batchKey)
		}

		// Either add the batch to be flushed OR keep track of the batch
		// in a priority queue. If we find that the total memory is too high
		// then we will flush batches in order of largest byte size.
		if flush {
			toFlush = append(toFlush, batchKey)
		} else {
			batchMemorySize := curBatch.GetPayloadByteSize()
			totalMemory += batchMemorySize

			a := &queue.BatchQueueItem{
				Batch:    curBatch,
				Index:    queueIndex,
				Priority: batchMemorySize,
				Key:      batchKey,
			}

			bq.Push(a)
			queueIndex += 1
		}
	}

	// If total memory of remaining batches is too high then flush largest
	// batches until below limit.
	if totalMemory >= b.batcherMemorySoftLimit {
		// Sorts the batches based on byte size
		heap.Init(&bq)

		// Keep adding batches to be flushed until under memory limit
		for {
			if totalMemory < b.batcherMemorySoftLimit {
				break
			}
			item := heap.Pop(&bq).(*queue.BatchQueueItem)
			toFlush = append(toFlush, item.Key)
			totalMemory -= item.Batch.GetPayloadByteSize()
			log.Debugf("adding %s to be flushed due to memory pressure", item.Key)
		}
	}

	// Flush batches
	for _, key := range toFlush {
		log.Debugf("flushing %s", key)
		curBatch := b.batches[key]

		// If the batch is empty then don't send it. This could be the case
		// when a new batch was created but nothing added to it.
		if !curBatch.IsEmpty() {
			ok := b.sendBatch(curBatch)

			if !ok {
				return false
			}
		}

		b.statsChan <- stats.NewStatCount("batcher", "batch_closed_early", 1, time.Now().UnixNano())

		delete(b.batches, key)
	}

	return true
}

func (b *Batcher) sendBatch(batch transport.Batch) bool {

	// First flush out seen list
	if len(b.seenList) > 0 {
		select {
		case b.txnsSeenChan <- b.seenList:
			b.seenList = []*progress.Seen{}
			log.Debug("seen a BatchTransaction")
		case <-time.After(b.txnsSeenTimeout):
			log.Panic("fatal time out sending a BatchTransaction to the ProgressTracker")
		}
	}

	ok, err := batch.Close()
	if !ok {
		log.Error(err)
		return false
	}

	channelIndex := 0
	switch b.routingMethod {
	case BATCH_ROUTING_ROUND_ROBIN:
		channelIndex = b.roundRobinPosition

		// Then increment round robin position for next batch
		if b.roundRobinPosition == b.workers-1 {
			b.roundRobinPosition = 0
		} else {
			b.roundRobinPosition++
		}
	case BATCH_ROUTING_PARTITION:
		channelIndex = utils.QuickHash(batch.GetPartitionKey(), b.workers)
	}

	log.Debugf("sending batch to worker %d", channelIndex)
	start := time.Now()
	b.outputChans[channelIndex] <- batch
	now := time.Now()

	b.statsChan <- stats.NewStatHistogram("batcher", "batch_write_wait", now.Sub(start).Milliseconds(), now.UnixNano(), "ms")
	b.statsChan <- stats.NewStatCount("batcher", "batches", 1, now.UnixNano())
	b.statsChan <- stats.NewStatHistogram("batcher", "batch_size", int64(batch.NumMessages()), now.UnixNano(), "count")

	return true
}

func (b *Batcher) addToBatch(batch transport.Batch, msg *marshaller.MarshalledMessage) (transport.Batch, BatchAddError) {
	ok, err := batch.Add(msg)

	var status BatchAddError
	if !ok {
		switch err.Error() {

		// Message can't fit. Make a new batch.
		case transport.ERR_CANT_FIT:

			// Send exiting batch
			ok := b.sendBatch(batch)

			if !ok {
				// Fatal error sending batch
				return batch, BATCH_ADD_FATAL
			}

			// Create new batch
			batch = b.batchFactory.NewBatch(msg.PartitionKey)

			// Add message to batch (call this function again)
			batch, status = b.addToBatch(batch, msg)
			return batch, status

		// See if message was bigger than restriction on size of single messages.
		// Treat this as a non-fatal error and log that a message could not be
		// put in a batch.
		case transport.ERR_MSG_TOOBIG:
			log.Warn(err)
			b.statsChan <- stats.NewStatCount("batcher", "dropped_too_big", 1, time.Now().UnixNano())
			return batch, BATCH_ADD_FAIL

		// Message failed internal validation by the Batch. Non-fatal.
		case transport.ERR_MSG_INVALID:
			log.Warn("message was invalid and was not added to batch")
			b.statsChan <- stats.NewStatCount("batcher", "dropped_msg_invalid", 1, time.Now().UnixNano())
			return batch, BATCH_ADD_FAIL

		// Handle all other errors (unknown ones) as fatal.
		default:
			log.Error("fatal error while adding to batch", err)
			return batch, BATCH_ADD_FATAL

		}
	}

	return batch, BATCH_ADD_OK
}

// GetOutputChan returns the outputChans
func (b *Batcher) GetOutputChans() []chan transport.Batch {
	return b.outputChans
}
