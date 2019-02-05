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
	"net/url"
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
	operation string `json:"operation"`
	table     string `json:"table"`
}

type RabbitMQTransporter struct {
	shutdownHandler shutdown.ShutdownHandler
	inputChan       <-chan transport.Batch         // receive a batch (slice) of MarshalledMessages
	txnsWritten     chan<- *ordered_map.OrderedMap // Map of <transaction:progress.Written>

	statsChan chan stats.Stat

	log          logrus.Entry
	id           int
	amqpURL      *url.URL
	exchangeName string
	conn         *amqp.Connection
	channel      *amqp.Channel
}

// NewTransporter returns a rabbitmq transporter
func NewTransporter(shutdownHandler shutdown.ShutdownHandler,
	inputChan <-chan transport.Batch,
	txnsWritten chan<- *ordered_map.OrderedMap, // Map of <transaction:progress.Written>
	statsChan chan stats.Stat,
	log logrus.Entry,
	id int,
	amqpURL *url.URL,
	exchangeName string) transport.Transporter {

	log = *log.WithField("routine", "transporter").WithField("id", id)

	return RabbitMQTransporter{
		shutdownHandler: shutdownHandler,
		inputChan:       inputChan,
		txnsWritten:     txnsWritten,
		statsChan:       statsChan,
		log:             log,
		id:              id,
		amqpURL:         amqpURL,
		exchangeName:    exchangeName,
		conn:            nil,
		channel:         nil,
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

	err := t.channel.Close()
	if err != nil {
		t.log.WithError(err).Error("Error while closing channel")
	}
	err = t.conn.Close()
	if err != nil {
		t.log.WithError(err).Error("Error while closing connection")
	}
}

// StartTransporting reads in message batches, outputs its String to RabbitMQ and then sends a progress report on the batch
func (t RabbitMQTransporter) StartTransporting() {
	t.log.Info("starting transporter")
	defer t.shutdown()

	var b interface{}
	var ok bool
	var err error

	t.conn, err = amqp.Dial(t.amqpURL.String())
	if err != nil {
		t.log.WithError(err).Fatal("Could not connect to RabbitMQ")
	}
	t.channel, err = t.conn.Channel()
	if err != nil {
		t.log.WithError(err).Fatal("Could not open channel to RabbitMQ")
	}

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
			key := strings.Join([]string{op.table, op.operation}, ".")
			msg := amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				Timestamp:    time.Now(),
				ContentType:  "application/json",
				Body:         msg.Json,
			}

			err = t.channel.Publish(t.exchangeName, key, false, false, msg)
			if err != nil {
				t.log.WithError(err).Error("Could not publish to RabbitMQ")
			}
		}

		// report transactions written in this batch
		t.txnsWritten <- genericBatch.GetTransactions()
	}
}
