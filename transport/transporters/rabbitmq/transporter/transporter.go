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
	"encoding/json"
	"strings"
	"time"

	"github.com/Nextdoor/pg-bifrost.git/marshaller"
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/Nextdoor/pg-bifrost.git/transport/batch"
	"github.com/cevaris/ordered_map"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type operation struct {
	Operation string `json:"operation"`
	Table     string `json:"table"`
}

type RabbitMQTransporter struct {
	shutdownHandler shutdown.ShutdownHandler
	inputChan       <-chan transport.Batch         // receive a batch (slice) of MarshalledMessages
	txnsWritten     chan<- *ordered_map.OrderedMap // Map of <transaction:progress.Written>

	statsChan chan stats.Stat

	log          logrus.Entry
	id           int
	exchangeName string
	batchSize    int
	conn         *amqp.Connection
}

// NewTransporter returns a rabbitmq transporter
func NewTransporter(shutdownHandler shutdown.ShutdownHandler,
	inputChan <-chan transport.Batch,
	txnsWritten chan<- *ordered_map.OrderedMap, // Map of <transaction:progress.Written>
	statsChan chan stats.Stat,
	log logrus.Entry,
	id int,
	exchangeName string,
	conn *amqp.Connection,
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
		conn:            conn,
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

// StartTransporting reads in message batches, outputs its String to RabbitMQ and then sends a progress report on the batch
func (t RabbitMQTransporter) StartTransporting() {
	t.log.Info("starting transporter")
	defer t.shutdown()

	var b interface{}
	var ok bool

	channel, err := t.conn.Channel()
	if err != nil {
		t.log.WithError(err).Fatal("Could not open channel to RabbitMQ")
	}
	defer func() {
		err = channel.Close()
		if err != nil {
			t.log.WithError(err).Error("Error while closing channel")
		}
	}()
	closeNotify := channel.NotifyClose(make(chan *amqp.Error))
	publishNotify := channel.NotifyPublish(make(chan amqp.Confirmation, t.batchSize))
	err = channel.Confirm(false)
	if err != nil {
		t.log.WithError(err).Fatal("Could not turn on confirmations for channel")
	}
	confirms := uint64(0)

	for {
		select {
		case <-t.shutdownHandler.TerminateCtx.Done():
			t.log.Debug("received terminateCtx cancellation")
			return

		case b, ok = <-t.inputChan:
			// pass
		}

		select {
		case <-t.shutdownHandler.TerminateCtx.Done():
			t.log.Debug("received terminateCtx cancellation")
			return
		case err := <-closeNotify:
			t.log.Error(err.Error())
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

		op := operation{}
		for _, msg := range messagesSlice {
			err = json.Unmarshal(msg.Json, &op)
			if err != nil {
				t.log.WithError(err).Error("Could not unmarshal WAL json")
			}
			key := strings.Join([]string{op.Table, op.Operation}, ".")
			amqpMsg := amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				Timestamp:    time.Now(),
				ContentType:  "application/json",
				Body:         msg.Json,
			}

			err = channel.Publish(t.exchangeName, key, false, false, amqpMsg)
			if err != nil {
				t.log.WithError(err).Error("Could not publish to RabbitMQ")
			}
		}

		desiredCount := uint64(len(messagesSlice)) + confirms
		t.log.WithField("desiredCount", desiredCount).Debug("Waiting for desired confirms count")
		for confirms < desiredCount {
			confirm := <-publishNotify
			if !confirm.Ack {
				t.log.Error("Message was not delievered to RabbitMQ")
				return
			}
			confirms = confirm.DeliveryTag
		}
		t.log.Info("Messages delivered")

		// report transactions written in this batch
		t.txnsWritten <- genericBatch.GetTransactions()
	}
}
