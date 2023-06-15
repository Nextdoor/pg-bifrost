package transporter

import (
	"context"
	"fmt"
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
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	TimeSource utils.TimeSource = &utils.RealTime{}
)

type Client struct {
	producer           sarama.SyncProducer
	listening          *sync.WaitGroup
	pendingMsgs        uint64
	trackInterval      time.Duration
	stopTracking       chan bool
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
	topic           string
	client          *Client
}

func NewTransporter(
	shutdownHandler shutdown.ShutdownHandler,
	inputChan <-chan transport.Batch,
	statsChan chan stats.Stat,
	txnsWritten chan<- *ordered_map.OrderedMap,
	log logrus.Entry,
	retryPolicy backoff.BackOff,
	producer sarama.SyncProducer,
	topic string) transport.Transporter {

	return &KafkaTransporter{
		shutdownHandler,
		inputChan,
		txnsWritten,
		statsChan,
		log,
		retryPolicy,
		topic,
		&Client{
			producer:    producer,
			pendingMsgs: 0,
			listening:   &sync.WaitGroup{},g
		},
	}

}

// shutdown idempotently closes the output channel
func (t *KafkaTransporter) shutdown() {
	t.log.Info("shutting down transporter")
	t.client.producer.Close()
	t.client.listening.Wait()
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

func (t *KafkaTransporter) transportWithRetry(ctx context.Context, produceMessages []*sarama.ProducerMessage) (error, bool) {
	var cancelled bool
	var ts = TimeSource

	operation := func() error {
		select {
		case <-ctx.Done():
			t.log.Debug("received terminateCtx cancellation")
			cancelled = true
			return nil
		default:
		}

		err := t.client.producer.SendMessages(produceMessages)
		produceErrors, ok := err.(sarama.ProducerErrors)
		if !ok {
			panic("produceErrors is not type sarama.ProducerErrors")
		}

		if len(produceErrors) == 0 {
			t.statsChan <- stats.NewStatCount("kafka_transport", "success", 1, ts.UnixNano())
			return nil
		}

		produceMessages = produceMessages[:0]
		errorMessages := map[string]int{} // Keep track of error messages

		for i := 0; i < len(produceErrors); i++ {
			e := produceErrors[i].Err.Error()
			errorMessages[e] += 1
			r := produceErrors[i].Msg
			produceMessages = append(produceMessages, r)
		}

		// TODO: log errors and stats
		err = errors.New(fmt.Sprintf("%d messages failed to be written to kafka: %v", len(errorMessages), errorMessages))
		t.log.Warnf("err %s", err)
		t.statsChan <- stats.NewStatCount("kafka_transport", "failure", 1, ts.UnixNano())

		// return err to signify a retry is needed
		return err

	}

	defer func() {
		t.retryPolicy.Reset()
	}()

	err := backoff.Retry(operation, t.retryPolicy)

	if err != nil {
		return err, cancelled
	}

	return nil, cancelled
}

func (t *KafkaTransporter) StartTransporting() {
	t.log.Info("starting transporter")

	defer t.shutdown()
	t.client.listening.Add(2)

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

		if !ok {
			panic("Batch is not a GenericBatch")
		}

		messages := genericBatch.GetPayload()
		messagesSlice, ok := messages.([]*marshaller.MarshalledMessage)

		if !ok {
			panic("Batch payload is not a []*marshaller.MarshalledMessage")
		}

		var producerMessageSlice []*sarama.ProducerMessage

		for _, message := range messagesSlice {
			msg := &sarama.ProducerMessage{
				Topic: t.topic,
				Value: sarama.ByteEncoder(message.Json),
				Key:   sarama.StringEncoder(message.PartitionKey),
			}
			t.log.Info("key: %v", msg.Key)
			producerMessageSlice = append(producerMessageSlice, msg)
		}

		// Begin timer
		start := ts.UnixNano()

		err, cancelled := t.transportWithRetry(t.shutdownHandler.TerminateCtx, producerMessageSlice)

		// End timer and send stat

		total := (ts.UnixNano() - start) / int64(time.Millisecond)
		t.statsChan <- stats.NewStatHistogram("kafka_transport", "duration", total, ts.UnixNano(), "ms")

		if err != nil {
			t.log.Error("max retries exceeded")
			return
		}

		// if transportWithRetry was cancelled, then loop back around to process another potential batch or shutdown
		if cancelled {
			continue
		}

		t.log.Debug("successfully wrote batch to kafka")
		t.statsChan <- stats.NewStatCount("kafka_transport", "written", int64(len(producerMessageSlice)), ts.UnixNano())

		// report transactions written in this batch
		t.txnsWritten <- genericBatch.GetTransactions()
	}
}
