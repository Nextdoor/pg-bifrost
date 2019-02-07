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

package transporter

import (
	"errors"
	"strings"
	"time"

	"github.com/NeowayLabs/wabbit"
	"github.com/Nextdoor/pg-bifrost.git/marshaller"
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/Nextdoor/pg-bifrost.git/transport/batch"
	"github.com/Nextdoor/pg-bifrost.git/utils"
	"github.com/cevaris/ordered_map"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

var (
	TimeSource utils.TimeSource = utils.RealTime{}
)

type RabbitMQTransporter struct {
	shutdownHandler shutdown.ShutdownHandler
	inputChan       <-chan transport.Batch         // receive a batch (slice) of MarshalledMessages
	txnsWritten     chan<- *ordered_map.OrderedMap // Map of <transaction:progress.Written>

	statsChan chan stats.Stat

	log           logrus.Entry
	id            int
	exchangeName  string
	batchSize     int
	connMan       ConnectionGetter
	channel       wabbit.Channel
	publishNotify chan wabbit.Confirmation
}

// NewTransporter returns a rabbitmq transporter
func NewTransporter(shutdownHandler shutdown.ShutdownHandler,
	inputChan <-chan transport.Batch,
	txnsWritten chan<- *ordered_map.OrderedMap, // Map of <transaction:progress.Written>
	statsChan chan stats.Stat,
	log logrus.Entry,
	id int,
	exchangeName string,
	connMan ConnectionGetter,
	batchSize int) transport.Transporter {

	log = *log.WithField("routine", "transporter").WithField("id", id)

	return RabbitMQTransporter{
		shutdownHandler: shutdownHandler,
		inputChan:       inputChan,
		txnsWritten:     txnsWritten,
		statsChan:       statsChan,
		log:             log,
		id:              id,
		exchangeName:    exchangeName,
		connMan:         connMan,
		batchSize:       batchSize,
	}
}

// shutdown idempotently closes the output channel
func (t RabbitMQTransporter) shutdown() {
	t.log.Info("shutting down transporter")
	t.shutdownHandler.CancelFunc() // initiate shutdown on other modules as well

	if r := recover(); r != nil {
		t.log.Warnf("Recovered in RabbitMQTransporter %s", r)
	}

	defer func() {
		// recover if channel is already closed
		recover()
	}()

	t.log.Debug("closing progress channel")
	close(t.txnsWritten)
}

// StartTransporting gets a RabbitMQ connection from the ConnectionGetter and
// passes it to transport()
func (t RabbitMQTransporter) StartTransporting() {
	t.log.Info("starting transporter")
	defer t.shutdown()

	for {
		select {
		case <-t.shutdownHandler.TerminateCtx.Done():
			t.log.Debug("received terminateCtx cancellation")
			return
		default:
		}

		conn, err := t.connMan.GetConnection(t.shutdownHandler.TerminateCtx)
		if err != nil {
			return
		}
		t.transport(conn)
	}
}

// transport reads in message batches, outputs its String to RabbitMQ and then sends a progress report on the batch
func (t RabbitMQTransporter) transport(conn wabbit.Conn) {
	var b interface{}
	var ok bool
	var err error

	t.channel, err = conn.Channel()
	if err != nil {
		t.log.WithError(err).Fatal("Could not open channel to RabbitMQ")
	}
	defer func() {
		err = t.channel.Close()
		if err != nil {
			t.log.WithError(err).Error("Error while closing channel")
		}
	}()
	t.log.Info("Created new channel")
	closeNotify := t.channel.NotifyClose(make(chan wabbit.Error))
	t.publishNotify = t.channel.NotifyPublish(make(chan wabbit.Confirmation, t.batchSize))
	err = t.channel.Confirm(false)
	if err != nil {
		t.log.WithError(err).Fatal("Could not turn on confirmations for channel")
	}
	confirms := uint64(0)

	for {
		select {
		case <-t.shutdownHandler.TerminateCtx.Done():
			t.log.Debug("received terminateCtx cancellation")
			return
		case <-closeNotify:
			t.log.Debug("channel closed")
			return

		case b, ok = <-t.inputChan:
			// pass
		}

		select {
		case <-t.shutdownHandler.TerminateCtx.Done():
			t.log.Debug("received terminateCtx cancellation")
			return
		case <-closeNotify:
			return
		default:
			// pass
		}

		if !ok {
			t.log.Warn("input channel is closed")
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

		// Begin timer
		start := TimeSource.UnixNano()

		err = t.sendMessages(closeNotify, messagesSlice)
		if err != nil {
			t.log.WithError(err).Error("Could not transport messages")
			t.statsChan <- stats.NewStatCount("rabbitmq_transport", "failure", 1, TimeSource.UnixNano())
			return
		}

		confirms, err = t.waitForConfirmations(closeNotify, uint64(len(messagesSlice)), confirms)
		if err != nil {
			t.log.WithError(err).Error("Could not confirm message publication")
			return
		}

		// End timer and send stat
		total := (TimeSource.UnixNano() - start) / int64(time.Millisecond)
		t.statsChan <- stats.NewStatHistogram("rabbitmq_transport", "duration", total, TimeSource.UnixNano(), "ms")

		t.log.Debug("Messages delivered")
		t.statsChan <- stats.NewStatCount("rabbitmq_transport", "written", int64(len(messagesSlice)), TimeSource.UnixNano())

		// report transactions written in this batch
		t.txnsWritten <- genericBatch.GetTransactions()
	}
}

func (t RabbitMQTransporter) sendMessages(closeNotify chan wabbit.Error, messagesSlice []*marshaller.MarshalledMessage) error {
	var err error
	for _, msg := range messagesSlice {
		select {
		case <-t.shutdownHandler.TerminateCtx.Done():
			return errors.New("Shutting down.  Did not publish all messages")
		case <-closeNotify:
			return errors.New("Channel closed.  Could not publish all messages")
		default:
			// pass
		}
		key := strings.Join([]string{msg.Table, msg.Operation}, ".")
		options := wabbit.Option{
			"deliveryMode": amqp.Persistent,
			"contentType":  "application/json",
		}

		err = t.channel.Publish(t.exchangeName, key, msg.Json, options)
		if err != nil {
			t.log.WithError(err).Error("Could not publish to RabbitMQ")
			return err
		}
	}
	return nil
}

func (t RabbitMQTransporter) waitForConfirmations(closeNotify chan wabbit.Error, messageCount, confirms uint64) (uint64, error) {
	desiredCount := messageCount + confirms
	t.log.WithField("desiredCount", desiredCount).Debug("Waiting for desired confirms count")
	for confirms < desiredCount {
		select {
		case confirm := <-t.publishNotify:
			if !confirm.Ack() {
				t.log.Error("Message was not delivered to RabbitMQ")
				return confirms, errors.New("Message was not acknowledged")
			}
			confirms = confirm.DeliveryTag()
		case <-t.shutdownHandler.TerminateCtx.Done():
			return confirms, errors.New("Shutting down.  Could not confirm all messages were published")
		case <-closeNotify:
			return confirms, errors.New("Channel closed.  Could not confirm all messages were published")
		}
	}

	return confirms, nil
}
