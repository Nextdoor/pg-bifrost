package transporter

import (
	"context"
	"sync"
	"time"

	"github.com/Nextdoor/pg-bifrost.git/marshaller"
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/Nextdoor/pg-bifrost.git/transport/batch"
	"github.com/Nextdoor/pg-bifrost.git/utils"
	"github.com/Shopify/sarama"
	"github.com/cenkalti/backoff/v4"
	"github.com/cevaris/ordered_map"
	"github.com/sirupsen/logrus"
)

var (
	TimeSource utils.TimeSource = &utils.RealTime{}
)

type Client struct {
	producer      sarama.AsyncProducer
	listening     *sync.WaitGroup
	pendingMsgs   uint64
	trackInterval time.Duration
	stopTracking  chan bool
	//statsClient        stats.NDStatsdClient //TODO: use correct stats client.
	log                logrus.Entry
	bufferClosed       chan bool
	bufferCloseTimeout chan bool
}

type KafkaTransporter struct {
	shutdownHandler shutdown.ShutdownHandler
	inputChan       <-chan transport.Batch         // receive a batch (slice) of MarshalledMessages
	txnsWritten     chan<- *ordered_map.OrderedMap // Map of <transaction:progress.Written>
	statsChan       chan stats.Stat
	log             logrus.Entry
	retryPolicy     backoff.BackOff
	client          *Client
}

func NewTransporter(
	shutdownHandler shutdown.ShutdownHandler,
	inputChan <-chan transport.Batch,
	statsChan chan stats.Stat,
	txnsWritten chan<- *ordered_map.OrderedMap,
	log logrus.Entry,
	retryPolicy backoff.BackOff,
	producer sarama.AsyncProducer) transport.Transporter {

	return &KafkaTransporter{
		shutdownHandler,
		inputChan,
		txnsWritten,
		statsChan,
		log,
		retryPolicy,
		&Client{
			producer:    producer,
			pendingMsgs: 0,
			listening:   &sync.WaitGroup{},
			//statsClient:        statsClient,
		},
	}

}

// shutdown idempotently closes the output channel
func (t *KafkaTransporter) shutdown() {
	t.log.Info("shutting down transporter")
	t.log.Info("Closing kafka client")
	t.client.producer.Close()
	t.client.listening.Wait()
	t.log.Info("Kafka client closed")
	t.shutdownHandler.CancelFunc() // initiate shutdown on other modules as well

	if r := recover(); r != nil {
		t.log.Warnf("Recovered in KafkaTransporter %s", r)
	}

	defer func() {
		// recover if channel is already closed
		_ = recover()
	}()

	t.log.Debug("closing progress channel")
	close(t.txnsWritten)
}

func (t *KafkaTransporter) trackSuccess() {
	t.log.Info("Starting success tracker")
	for {
		select {
		case _, open := <-t.client.producer.Successes():
			if !open {
				t.client.listening.Done()
				return
			} else {
				t.log.Info("successfully sent message")
			}
		}
	}
}

func (t *KafkaTransporter) trackError() {
	t.log.Info("Starting error tracker")
	for {
		select {
		case _, open := <-t.client.producer.Errors():
			if !open {
				t.client.listening.Done()
				return
			} else {
				t.log.Info("Error sending message")
			}
		}
	}
}

func (t *KafkaTransporter) transportWithRetry(ctx context.Context, produceMessages []*sarama.ProducerMessage) (error, bool) {
	for _, msg := range produceMessages {
		t.client.producer.Input() <- msg
	}
	t.log.Info()
	return nil, false
}

func (t *KafkaTransporter) StartTransporting() {
	t.log.Info("starting transporter")

	defer t.shutdown()
	t.log.Info("adding to wait group")
	t.client.listening.Add(2)

	go t.trackError()
	go t.trackSuccess()

	var b interface{}
	var ok bool
	var ts = TimeSource

	for {
		select {
		case <-t.shutdownHandler.TerminateCtx.Done():
			t.log.Info("received terminateCtx cancellation")
			return

		case b, ok = <-t.inputChan:
			// pass
		}

		select {
		case <-t.shutdownHandler.TerminateCtx.Done():
			t.log.Info("received terminateCtx cancellation")
			return
		default:
			// pass
		}

		if !ok {
			t.log.Info("input channel is closed")
			return
		}

		genericBatch, ok := b.(*batch.GenericBatch)
		messages := genericBatch.GetPayload()
		messagesSlice, ok := messages.([]*marshaller.MarshalledMessage)
		var producerMessageSlice []*sarama.ProducerMessage
		for _, message := range messagesSlice {
			msg := &sarama.ProducerMessage{
				Topic: "topic",
				Value: sarama.ByteEncoder(message.Json),
			}
			producerMessageSlice = append(producerMessageSlice, msg)
		}

		if !ok {
			panic("Batch is not a GenericBatch")
		}

		// Begin timer
		start := ts.UnixNano()

		err, cancelled := t.transportWithRetry(t.shutdownHandler.TerminateCtx, producerMessageSlice)

		// End timer and send stat

		total := (ts.UnixNano() - start) / int64(time.Millisecond)
		t.statsChan <- stats.NewStatHistogram("kinesis_transport", "duration", total, ts.UnixNano(), "ms")

		if err != nil {
			t.log.Error("max retries exceeded")
			return
		}

		// if transportWithRetry was cancelled, then loop back around to process another potential batch or shutdown
		if cancelled {
			continue
		}

		t.log.Debug("successfully wrote batch")

		// report transactions written in this batch
		t.txnsWritten <- genericBatch.GetTransactions()
	}
}
