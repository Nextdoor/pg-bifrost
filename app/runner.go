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

package app

import (
	"time"

	"github.com/Nextdoor/pg-bifrost.git/app/config"
	"github.com/Nextdoor/pg-bifrost.git/partitioner"
	"github.com/Nextdoor/pg-bifrost.git/transport/batcher"
	"github.com/jackc/pgx/v5/pgconn"

	"os"

	"github.com/Nextdoor/pg-bifrost.git/filter"
	"github.com/Nextdoor/pg-bifrost.git/marshaller"
	"github.com/Nextdoor/pg-bifrost.git/replication/client"
	"github.com/Nextdoor/pg-bifrost.git/replication/client/conn"
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/Nextdoor/pg-bifrost.git/stats/aggregator"
	"github.com/Nextdoor/pg-bifrost.git/stats/reporters"
	"github.com/Nextdoor/pg-bifrost.git/stats/reporters/factory"
	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/Nextdoor/pg-bifrost.git/transport/manager"
	"github.com/Nextdoor/pg-bifrost.git/transport/progress"
	"github.com/cevaris/ordered_map"
	"github.com/sirupsen/logrus"
)

var (
	logger = logrus.New()
	log    = logger.WithField("package", "app")
)

func init() {
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.InfoLevel)
}

type Runner struct {
	shutdownHandler shutdown.ShutdownHandler

	statsChan chan stats.Stat

	replicationClient   *client.Replicator
	filterInstance      *filter.Filter
	partitionerInstance *partitioner.Partitioner
	marshallerInstance  *marshaller.Marshaller
	transportManager    *manager.TransportManager
	statsAggregator     *aggregator.Aggregator
	progressTracker     *progress.ProgressTracker
	statsReporter       reporters.Reporter
}

// New initializes replication connection with Postgres and sets up internal pipeline.

func New(shutdownHandler shutdown.ShutdownHandler,
	sourceConfig *pgconn.Config,
	replicationSlot string,
	clientConfig map[string]interface{},
	filterConfig map[string]interface{},
	marshallerConfig map[string]interface{},
	partitionConfig map[string]interface{},
	batcherConfig map[string]interface{},
	transportType transport.TransportType,
	transportConfig map[string]interface{},
	reporterConfig map[string]interface{}) (*Runner, error) {

	log.Info("replicationSlot=", replicationSlot)

	clientBufferSize, ok := clientConfig[config.VAR_NAME_CLIENT_BUFFER_SIZE].(int)
	if !ok {
		log.Panic("Wrong type assertion")
	}
	log.Info("clientBufferSize=", clientBufferSize)

	// Get filter configurations
	whitelist, ok := filterConfig["whitelist"].(bool)
	if !ok {
		log.Panic("Wrong type assertion")
	}
	log.Info("whitelist=", whitelist)

	tablelist, ok := filterConfig["tablelist"].([]string)
	if !ok {
		log.Panic("Wrong type assertion")
	}
	log.Info("tablelist=", tablelist)

	regex, ok := filterConfig["regex"].(bool)
	if !ok {
		log.Panic("Wrong type assertion")
	}

	// Get marshaller configurations
	noMarshalOldValue, ok := marshallerConfig[config.VAR_NAME_NO_MARSHAL_OLD_VALUE].(bool)
	if !ok {
		log.Panic("Wrong type assertion")
	}
	log.Info("noMarshalOldValue=", noMarshalOldValue)

	// Get partitioner configurations
	partMethod, ok := partitionConfig[config.VAR_NAME_PARTITION_METHOD].(partitioner.PartitionMethod)
	if !ok {
		log.Panic("Wrong type assertion")
	}
	log.Info("partMethod=", partMethod)

	partPartitions, ok := partitionConfig[config.VAR_NAME_PARTITION_COUNT].(int)
	if !ok {
		log.Panic("Wrong type assertion")
	}
	log.Info("partPartitions=", partPartitions)

	// Get batcher configurations
	batchFlushUpdateAge, ok := batcherConfig[config.VAR_NAME_BATCH_FLUSH_UPDATE_AGE].(int)
	if !ok {
		log.Panic("Wrong type assertion")
	}
	log.Info("batchFlushUpdateAge=", batchFlushUpdateAge)

	batchFlushMaxAge, ok := batcherConfig[config.VAR_NAME_BATCH_FLUSH_MAX_AGE].(int)
	if !ok {
		log.Panic("Wrong type assertion")
	}
	log.Info("batchFlushMaxAge=", batchFlushMaxAge)

	batchQueueDepth, ok := batcherConfig[config.VAR_NAME_BATCH_QUEUE_DEPTH].(int)
	if !ok {
		log.Panic("Wrong type assertion")
	}
	log.Info("batchQueueDepth=", batchQueueDepth)

	batcherMemorySoftLimit, ok := batcherConfig[config.VAR_NAME_BATCHER_MEMORY_SOFT_LIMIT].(int64)
	if !ok {
		log.Panic("Wrong type assertion")
	}
	log.Info("batcherMemorySoftLimit=", batcherMemorySoftLimit)

	batcherRoutingMethod, ok := batcherConfig[config.VAR_NAME_BATCHER_ROUTING_METHOD].(batcher.BatchRouting)
	if !ok {
		log.Panic("Wrong type assertion")
	}
	log.Info("batcherRoutingMethod=", batcherRoutingMethod)

	// Make replication connection
	connManager := conn.NewManager(sourceConfig, replicationSlot)

	// "Global" Channels
	statsChan := make(chan stats.Stat, clientBufferSize*5)

	// Transactions channels for progress reporting to the progress tracker
	txnsSeen := make(chan *progress.Seen) // Must be unbuffered to maintain seen -> written ordering
	txnsWritten := make(chan *ordered_map.OrderedMap, clientBufferSize*5)

	// Initialize in reverse order
	replicationClient := client.New(shutdownHandler, statsChan, connManager, clientBufferSize)

	filterInstance := filter.New(
		shutdownHandler,
		replicationClient.GetOutputChan(),
		statsChan,
		whitelist,
		regex,
		tablelist)

	partitionerInstance := partitioner.New(
		shutdownHandler,
		filterInstance.OutputChan,
		statsChan,
		partMethod,
		partPartitions,
	)

	marshallerInstance := marshaller.New(
		shutdownHandler,
		partitionerInstance.OutputChan,
		statsChan,
		noMarshalOldValue)

	transportManager := manager.New(
		shutdownHandler,
		marshallerInstance.OutputChan,
		txnsSeen,
		txnsWritten,
		statsChan,
		transportType,
		transportConfig,
		batchFlushUpdateAge,
		batchFlushMaxAge,
		batchQueueDepth,
		batcherMemorySoftLimit,
		batcherRoutingMethod)

	progressTracker := progress.New(shutdownHandler, txnsSeen, txnsWritten, statsChan)
	statsAggregator := aggregator.New(shutdownHandler, statsChan)

	statsReporter, err := factory.New(shutdownHandler, statsAggregator.GetOutputChan(), reporters.DATADOG, reporterConfig)
	return &Runner{
		shutdownHandler,
		statsChan,
		&replicationClient,
		&filterInstance,
		&partitionerInstance,
		&marshallerInstance,
		&transportManager,
		&statsAggregator,
		&progressTracker,
		statsReporter,
	}, err
}

// shutdown idempotently closes the output channels and cancels the termination context
func (m Runner) shutdown() {
	log.Info("shutting down")
	m.shutdownHandler.CancelFunc() // initiate shutdown on other modules as well

	if r := recover(); r != nil {
		log.Error("recovering from panic ", r)
	}

	defer func() {
		// recover if channel is already closed
		_ = recover()
	}()

	log.Debug("closing global channels")
	close(m.statsChan)
}

// Start begins all module routines in reverse order and blocks on cancellation
// to clean up any "global" channels on shutdown
func (r Runner) Start() {
	defer r.shutdown()

	// Start in reverse order so downstream is ready when replication starts
	go r.progressTracker.Start(time.Millisecond * 5000)
	go r.statsAggregator.Start()
	go r.statsReporter.Start()
	go r.transportManager.Start()
	go r.marshallerInstance.Start()
	go r.partitionerInstance.Start()
	go r.filterInstance.Start()
	// Note that there is a dependency cycle here between the replication client and the
	// progress tracker. The replication client must be started last with the progress
	// channel.
	// +--------------------+
	// | Replication Client <------+
	// +---------+----------+      |
	//           |                 |
	//           |                 |
	// +---------v----------+      |
	// |       Filter       |      |
	// +---------+----------+      |
	//           |                 |
	//           |                 |
	// +---------v----------+      |
	// |    Partitioner     |      |
	// +---------+----------+      |
	//           |                 |
	//           |                 |
	// +---------v----------+      |
	// |     Marshaller     |      |
	// +---------+----------+      |
	//           |                 |
	//           |                 |
	// +---------v----------+      |
	// |      Batcher       |      |
	// +---------+----------+      |
	//           |                 |
	//           |                 |
	// +---------v----------+      |
	// |     Transport      |      |
	// +---------+----------+      |
	//           |                 |
	//           |                 |
	// +---------v----------+      |
	// |  Progress Tracker  +------+
	// +--------------------+
	//
	go r.replicationClient.Start(r.progressTracker.OutputChan)

	<-r.shutdownHandler.TerminateCtx.Done()
}
