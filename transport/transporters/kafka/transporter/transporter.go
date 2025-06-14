/*
  Copyright 2023 Nextdoor.com, Inc.

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

package transporter

import (
	"context"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/Nextdoor/pg-bifrost.git/transport/transporters/kafka/batch"
	"github.com/Nextdoor/pg-bifrost.git/utils"
	"github.com/cevaris/ordered_map"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	TimeSource utils.TimeSource = &utils.RealTime{}
	// shutdownDelay needs to be longer than config.Producer.Flush.Frequency in order to ensure all messages are
	// flushed during shutdown
	shutdownDelay = 3 * time.Second
)

type KafkaTransporter struct {
	shutdownHandler shutdown.ShutdownHandler
	inputChan       <-chan transport.Batch         // receive a batch (slice) of MarshalledMessages
	txnsWritten     chan<- *ordered_map.OrderedMap // Map of <transaction:progress.Written>
	statsChan       chan stats.Stat
	log             logrus.Entry
	kafkaProducer   sarama.SyncProducer
	topic           string
}

func NewTransporter(
	shutdownHandler shutdown.ShutdownHandler,
	inputChan <-chan transport.Batch,
	statsChan chan stats.Stat,
	txnsWritten chan<- *ordered_map.OrderedMap,
	log logrus.Entry,
	producer sarama.SyncProducer,
	topic string) transport.Transporter {

	return &KafkaTransporter{
		shutdownHandler,
		inputChan,
		txnsWritten,
		statsChan,
		log,
		producer,
		topic,
	}

}

// shutdown idempotently closes the output channel
func (t *KafkaTransporter) shutdown() {
	t.log.Info("shutting down transporter")

	t.log.Info("Waiting for internal Kafka buffers to be flushed")
	time.Sleep(shutdownDelay)

	_ = t.kafkaProducer.Close()
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

func (t *KafkaTransporter) sendBatchToKafka(ctx context.Context, produceMessages []*sarama.ProducerMessage) (error, bool) {
	var cancelled bool
	var ts = TimeSource

	select {
	case <-ctx.Done():
		t.log.Debug("received terminateCtx cancellation")
		cancelled = true
		return nil, cancelled
	default:
	}

	// Let the underlying Sarama client handle the individual retries of messages.
	err := t.kafkaProducer.SendMessages(produceMessages)
	if err == nil {
		t.statsChan <- stats.NewStatCount("kafka_transport", "success", 1, ts.UnixNano())
		return nil, cancelled
	}

	produceErrors := err.(sarama.ProducerErrors)
	errorMessages := map[string]int{} // Keep track of error messages

	for i := 0; i < len(produceErrors); i++ {
		e := produceErrors[i].Err.Error()
		errorMessages[e] += 1
	}

	err = errors.New(fmt.Sprintf("%d messages failed to be written to kafka: %v", len(produceErrors), errorMessages))
	t.log.Warnf("err %s", err)
	t.statsChan <- stats.NewStatCount("kafka_transport", "failure", int64(len(produceErrors)), ts.UnixNano())

	return err, cancelled
}

func (t *KafkaTransporter) StartTransporting() {
	t.log.Info("starting transporter")

	defer t.shutdown()

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

		kafkaBatch, ok := b.(*batch.KafkaBatch)

		if !ok {
			panic("Batch is not a KafkaBatch")
		}

		messages := kafkaBatch.GetPayload()
		producerMessageSlice, ok := messages.([]*sarama.ProducerMessage)

		if !ok {
			panic("Batch payload is not a []*sarama.ProducerMessage")
		}

		// Begin timer
		start := ts.UnixNano()

		err, cancelled := t.sendBatchToKafka(t.shutdownHandler.TerminateCtx, producerMessageSlice)

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
		t.txnsWritten <- kafkaBatch.GetTransactions()
	}
}
