package factory

import (
	"os"

	"github.com/Nextdoor/pg-bifrost.git/transport/progress"
	"github.com/Nextdoor/pg-bifrost.git/transport/transporters/kafka"
	"github.com/Nextdoor/pg-bifrost.git/transport/transporters/s3"

	"github.com/Nextdoor/pg-bifrost.git/marshaller"
	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/Nextdoor/pg-bifrost.git/transport/batcher"
	"github.com/Nextdoor/pg-bifrost.git/transport/transporters/kinesis"
	"github.com/Nextdoor/pg-bifrost.git/transport/transporters/rabbitmq"
	"github.com/Nextdoor/pg-bifrost.git/transport/transporters/stdout"

	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/Nextdoor/pg-bifrost.git/transport/batch"
	"github.com/cevaris/ordered_map"
	"github.com/sirupsen/logrus"
)

var (
	log = logrus.New()
)

func init() {
	log.SetOutput(os.Stdout)
	log.SetLevel(logrus.WarnLevel)
}

// NewTransport returns out a transport (batcher, transporters) based on the type
func NewTransport(shutdownHandler shutdown.ShutdownHandler,
	transportType transport.TransportType,
	transportConfig map[string]interface{},
	inputChan <-chan *marshaller.MarshalledMessage,
	txnsSeen chan<- *progress.Seen,
	txnsWritten chan<- *ordered_map.OrderedMap, // Map of <transaction:progress.Written>
	statsChan chan stats.Stat,
	workers int,
	flushBatchUpdateAge int,
	flushBatchMaxAge int,
	batchQueueDepth int,
	batcherTickRate int,
	batcherMemorySoftLimit int64,
	routingMethod batcher.BatchRouting) (*batcher.Batcher, []*transport.Transporter) {

	var t []*transport.Transporter
	var b *batcher.Batcher

	// Create an appropriate batch factory for the type of transport
	var batchFactory transport.BatchFactory

	switch transportType {
	case transport.STDOUT:
		batchFactory = batch.NewGenericBatchFactory(1)
	case transport.KINESIS:
		batchFactory = kinesis.NewBatchFactory(transportConfig)
	case transport.S3:
		batchFactory = s3.NewBatchFactory(transportConfig)
	case transport.RABBITMQ:
		batchFactory = rabbitmq.NewBatchFactory(transportConfig)
	case transport.KAFKA:
		batchFactory = kafka.NewBatchFactory(transportConfig)
	default:
		panic("unrecognized TransportType")
	}

	b = batcher.NewBatcher(
		shutdownHandler,
		inputChan,
		txnsSeen,
		statsChan,
		batcherTickRate,
		batchFactory,
		workers,
		flushBatchUpdateAge,
		flushBatchMaxAge,
		batchQueueDepth,
		batcherMemorySoftLimit,
		routingMethod)

	// For safety convert R/W chan to be a Read only.
	transportInputChans := make([]<-chan transport.Batch, len(b.GetOutputChans()))
	for i, ch := range b.GetOutputChans() {
		transportInputChans[i] = ch
	}

	//  Create an appropriate transport from type of transport
	switch transportType {
	case transport.STDOUT:
		t = stdout.New(shutdownHandler, txnsWritten, statsChan, workers, transportInputChans)
	case transport.KINESIS:
		t = kinesis.New(shutdownHandler, txnsWritten, statsChan, workers, transportInputChans, transportConfig)
	case transport.S3:
		t = s3.New(shutdownHandler, txnsWritten, statsChan, workers, transportInputChans, transportConfig)
	case transport.RABBITMQ:
		t = rabbitmq.New(shutdownHandler, txnsWritten, statsChan, workers, transportInputChans, transportConfig)
	case transport.KAFKA:
		t = kafka.New(shutdownHandler, txnsWritten, statsChan, workers, transportInputChans, transportConfig)
	default:
		panic("unrecognized TransportType")
	}

	return b, t
}
