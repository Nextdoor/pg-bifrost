// Transport TransportManager creates a multiplexer and N number of transport workers to send messages out.
//
// Example Usage:
//
//	import "github.com/Nextdoor/pg-bifrost.git/transport/manager"
//
//	// Setup
//	m := manager.New(in, errorChan, statsChan, transportType, transportConfig, workerNum, batchFlushTimeout, batchQueueDepth, maxMemoryBytes, routingMethod)
//	go m.StartBatcher()
//	go m.StartTransporterGroup()
//	go m.Start()
//
package manager

import (
	"github.com/Nextdoor/pg-bifrost.git/app/config"
	"github.com/Nextdoor/pg-bifrost.git/marshaller"
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/Nextdoor/pg-bifrost.git/transport/batcher"
	"github.com/Nextdoor/pg-bifrost.git/transport/factory"
	"github.com/Nextdoor/pg-bifrost.git/transport/progress"
	"github.com/cevaris/ordered_map"
	"log"
)

type TransportManager struct {
	inputChan   <-chan *marshaller.MarshalledMessage
	txnsSeen    chan *progress.Seen
	txnsWritten chan *ordered_map.OrderedMap
	statsChan   chan stats.Stat

	transportType   transport.TransportType
	transportConfig map[string]interface{}
	workerNum       int

	batcher          *batcher.Batcher
	transporterGroup []*transport.Transporter
}

// New creates a Batcher and group of Transporters based on the number of workers and
// returns a TransportManager which can start the Batcher and Transporters as go routines.
func New(shutdownHandler shutdown.ShutdownHandler,
	inputChan <-chan *marshaller.MarshalledMessage,
	txnsSeen chan *progress.Seen,
	txnsWritten chan *ordered_map.OrderedMap, // Map of <transaction:progress.Written>
	statsChan chan stats.Stat,
	transportType transport.TransportType,
	transportConfig map[string]interface{},
	flushBatchUpdateAge int,
	flushBatchMaxAge int,
	batchQueueDepth int,
	batcherMemorySoftLimit int64,
	routingMethod batcher.BatchRouting) TransportManager {

	workerNum, ok := transportConfig[config.VAR_NAME_WORKERS].(int)
	if !ok {
		log.Panic("Wrong type assertion")
	}

	b, t := factory.NewTransport(
		shutdownHandler,
		transportType,
		transportConfig,
		inputChan,
		txnsSeen,
		txnsWritten,
		statsChan,
		workerNum,
		flushBatchUpdateAge,
		flushBatchMaxAge,
		batchQueueDepth,
		batcherMemorySoftLimit,
		routingMethod)

	m := TransportManager{
		inputChan,
		txnsSeen,
		txnsWritten,
		statsChan,
		transportType,
		transportConfig,
		workerNum,
		b,
		t,
	}

	return m
}

// GetBatcher is a getter for batcher
func (m TransportManager) GetBatcher() *batcher.Batcher {
	return m.batcher
}

// StartBatcher starts the batcher. This is a helper for testing.
func (m TransportManager) StartBatcher() {
	b := *m.batcher
	go b.StartBatching()
}

// StartTransporterGroup starts the transporters. This is a helper for testing.
func (m TransportManager) StartTransporterGroup() {
	for i := 0; i < m.workerNum; i++ {
		t := *m.transporterGroup[i]
		go t.StartTransporting()
	}
}

// Start starts the set of Transports and Batcher, in that order.
func (m TransportManager) Start() {
	m.StartTransporterGroup()
	m.StartBatcher()
}
