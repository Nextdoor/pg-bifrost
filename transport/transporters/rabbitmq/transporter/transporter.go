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
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/NeowayLabs/wabbit"
	"github.com/Nextdoor/pg-bifrost.git/marshaller"
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/Nextdoor/pg-bifrost.git/transport/batch"
	"github.com/Nextdoor/pg-bifrost.git/utils"
	"github.com/cenkalti/backoff"
	"github.com/cevaris/ordered_map"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

var (
	TimeSource utils.TimeSource = utils.RealTime{}
)

// RabbitMQTransporter handles sending messages to RabbitMQ, retrying if there is
// is a failure.
type RabbitMQTransporter struct {
	shutdownHandler shutdown.ShutdownHandler
	inputChan       <-chan transport.Batch         // receive a batch (slice) of MarshalledMessages
	txnsWritten     chan<- *ordered_map.OrderedMap // Map of <transaction:progress.Written>
	statsChan       chan stats.Stat

	log             logrus.Entry
	id              int
	exchangeName    string
	batchSize       int
	retryPolicy     backoff.BackOff
	connMan         ConnectionGetter
	channelLock     sync.Mutex
	channel         wabbit.Channel
	channelConfirms uint64
	publishNotify   chan wabbit.Confirmation
	closeNotify     chan wabbit.Error
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
	batchSize int,
	retryPolicy backoff.BackOff) transport.Transporter {

	log = *log.WithField("routine", "transporter").WithField("id", id)

	return &RabbitMQTransporter{
		shutdownHandler: shutdownHandler,
		inputChan:       inputChan,
		txnsWritten:     txnsWritten,
		statsChan:       statsChan,
		log:             log,
		id:              id,
		exchangeName:    exchangeName,
		connMan:         connMan,
		batchSize:       batchSize,
		retryPolicy:     retryPolicy,
	}
}

// shutdown idempotently closes the output channel
func (t *RabbitMQTransporter) shutdown() {
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

// StartTransporting reads in message batches, publishes its messages to RabbitMQ
// and then sends a progress report on the batch
func (t *RabbitMQTransporter) StartTransporting() {
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

		// Begin timer
		start := TimeSource.UnixNano()

		// send to RabbitMQ with some retry logic
		cancelled, err := t.transportWithRetry(t.shutdownHandler.TerminateCtx, messagesSlice)

		// End timer and send stat

		total := (TimeSource.UnixNano() - start) / int64(time.Millisecond)
		t.statsChan <- stats.NewStatHistogram("rabbitmq_transport", "duration", total, TimeSource.UnixNano(), "ms")

		if err != nil {
			t.log.Error("max retries exceeded")
			return
		}

		// if transportWithRetry was cancelled, then loop back around to process another potential batch or shutdown
		if cancelled {
			continue
		}

		t.log.Debug("successfully wrote batch")
		t.statsChan <- stats.NewStatCount("rabbitmq_transport", "written", int64(genericBatch.NumMessages()), TimeSource.UnixNano())

		// report transactions written in this batch
		t.txnsWritten <- genericBatch.GetTransactions()
	}
}

func (t *RabbitMQTransporter) transportWithRetry(ctx context.Context, messagesSlice []*marshaller.MarshalledMessage) (bool, error) {
	var cancelled bool

	// An operation that may fail.
	operation := func() error {
		select {
		case <-ctx.Done():
			t.log.Debug("received terminateCtx cancellation")
			cancelled = true
			return nil
		default:
		}

		err := t.setupChannel(ctx)
		if err != nil {
			t.log.WithError(err).Error("Could not open channel to RabbitMQ")
			return err
		}

		err = t.sendMessages(ctx, messagesSlice)

		// If any errors occurred during sending then entire batch will be retried
		if err != nil {
			t.log.WithError(err).Error("Could not transport messages")
			t.statsChan <- stats.NewStatCount("rabbitmq_transport", "failure", 1, TimeSource.UnixNano())
			return err
		}

		remaining, err := t.waitForConfirmations(ctx, uint64(len(messagesSlice)))
		// If there are no failures then all messages were sent and received
		if err == nil {
			t.statsChan <- stats.NewStatCount("rabbitmq_transport", "success", 1, TimeSource.UnixNano())
			return nil
		}

		messagesSlice = messagesSlice[uint64(len(messagesSlice))-remaining:]

		// record the error
		err = fmt.Errorf("%d records failed to be acknowledged by RabbitMQ: %v", remaining, err)
		t.log.Warnf("err %s", err)
		t.statsChan <- stats.NewStatCount("rabbitmq_transport", "failure", 1, TimeSource.UnixNano())

		// return err to signify a retry is needed
		return err
	}

	// Reset retrier
	defer func() {
		t.retryPolicy.Reset()
	}()

	err := backoff.Retry(operation, t.retryPolicy)
	return cancelled, err
}

// setupChannel checks if there is a channel, if not it sets one up along
// with a closeHandler and reset the confirms count.
func (t *RabbitMQTransporter) setupChannel(ctx context.Context) error {
	t.channelLock.Lock()
	defer t.channelLock.Unlock()

	if t.channel == nil {
		conn, err := t.connMan.GetConnection(ctx)
		if err != nil {
			t.log.WithError(err).Error("Could not open connection to RabbitMQ")
			return err
		}

		t.channel, err = conn.Channel()
		if err != nil {
			t.log.WithError(err).Error("Could not open channel to RabbitMQ")
			return err
		}

		t.channelConfirms = 0
		t.closeNotify = t.channel.NotifyClose(make(chan wabbit.Error))
		t.publishNotify = t.channel.NotifyPublish(make(chan wabbit.Confirmation))
		err = t.channel.Confirm(false)
		if err != nil {
			t.log.WithError(err).Error("Could not turn on confirmations for channel")
			return err
		}

		go t.closeHandler(ctx, t.closeNotify)

		t.log.Info("Created new channel")
	}

	return nil
}

// closeHandler listens for channel close signals and sets channel
// to nil so that it can be recreated
func (t *RabbitMQTransporter) closeHandler(ctx context.Context, closeNotify chan wabbit.Error) {
	t.log.Info("Listening for channel close")
	select {
	case <-ctx.Done():
		t.log.Debug("Connection Manager received context cancellation")
	case <-closeNotify:
		t.channelLock.Lock()
		defer t.channelLock.Unlock()
		t.channel = nil
		t.closeNotify = nil
		t.publishNotify = nil
	}
	t.log.Info("Existing transport closeHandler")
}

// sendMessages publishes messages to RabbitMQ
func (t *RabbitMQTransporter) sendMessages(ctx context.Context, messagesSlice []*marshaller.MarshalledMessage) error {
	var err error
	for _, msg := range messagesSlice {
		select {
		case <-ctx.Done():
			return errors.New("Shutting down.  Did not publish all messages")
		case <-t.closeNotify:
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

// waitForConfirmations loops until the channel's the expected number of
// confirmations have returned.
func (t *RabbitMQTransporter) waitForConfirmations(ctx context.Context, messageCount uint64) (uint64, error) {
	desiredCount := messageCount + t.channelConfirms
	var remaining uint64
	t.log.WithField("desiredCount", desiredCount).Info("Waiting for desired confirms count")
	for t.channelConfirms < desiredCount {
		remaining = desiredCount - t.channelConfirms
		select {
		case confirm := <-t.publishNotify:
			t.log.Info("received confirmation")
			if !confirm.Ack() {
				t.log.Error("Message was not delivered to RabbitMQ")
				return remaining, errors.New("Message was not acknowledged")
			}
			t.channelConfirms = confirm.DeliveryTag()
		case <-ctx.Done():
			return remaining, errors.New("Shutting down.  Could not confirm all messages were published")
		case <-t.closeNotify:
			return remaining, errors.New("Channel closed.  Could not confirm all messages were published")
		}
	}

	return remaining, nil
}
