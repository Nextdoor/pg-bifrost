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
	"fmt"
	"github.com/Nextdoor/pg-bifrost.git/marshaller"
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/Nextdoor/pg-bifrost.git/transport/batch"
	"github.com/cevaris/ordered_map"
	"github.com/sirupsen/logrus"
)

type StdoutTransporter struct {
	shutdownHandler shutdown.ShutdownHandler
	inputChan       <-chan transport.Batch         // receive a batch (slice) of MarshalledMessages
	txnsWritten     chan<- *ordered_map.OrderedMap // Map of <transaction:progress.Written>

	statsChan chan stats.Stat

	log logrus.Entry
	id  int
}

// NewTransporter returns a stdout transporter
func NewTransporter(shutdownHandler shutdown.ShutdownHandler,
	inputChan <-chan transport.Batch,
	txnsWritten chan<- *ordered_map.OrderedMap, // Map of <transaction:progress.Written>
	statsChan chan stats.Stat,
	log logrus.Entry,
	id int) transport.Transporter {

	log = *log.WithField("routine", "transporter").WithField("id", id)

	return StdoutTransporter{shutdownHandler,
		inputChan,
		txnsWritten,
		statsChan,
		log,
		id,
	}
}

// shutdown idempotently closes the output channel
func (t StdoutTransporter) shutdown() {
	t.log.Info("shutting down transporter")
	t.shutdownHandler.CancelFunc() // initiate shutdown on other modules as well

	if r := recover(); r != nil {
		t.log.Warnf("Recovered in StdoutTransporter %s", r)
	}

	defer func() {
		// recover if channel is already closed
		recover()
	}()

	t.log.Debug("closing progress channel")
	close(t.txnsWritten)
}

// StartTransporting reads in message batches, outputs its String to STDOUT and then sends a progress report on the batch
func (t StdoutTransporter) StartTransporting() {
	t.log.Info("starting transporter")
	defer t.shutdown()

	var b interface{}
	var ok bool

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

		for _, msg := range messagesSlice {
			fmt.Println(fmt.Sprintf("%d: %s", t.id, string(msg.Json)))
		}

		// report transactions written in this batch
		t.txnsWritten <- genericBatch.GetTransactions()
	}
}
