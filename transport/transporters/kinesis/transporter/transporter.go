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
	"time"

	"fmt"
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/Nextdoor/pg-bifrost.git/transport/transporters/kinesis/batch"
	"github.com/Nextdoor/pg-bifrost.git/utils"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/cenkalti/backoff"
	"github.com/cevaris/ordered_map"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Generate mock for kinesis client
//go:generate mockgen -destination=mocks/mock_kinesis.go -package=mocks github.com/aws/aws-sdk-go/service/kinesis/kinesisiface KinesisAPI

var (
	TimeSource utils.TimeSource = utils.RealTime{}
)

type KinesisTransporter struct {
	shutdownHandler shutdown.ShutdownHandler

	inputChan   <-chan transport.Batch         // receive a batch (slice) of MarshalledMessages
	txnsWritten chan<- *ordered_map.OrderedMap // Map of <transaction:progress.Written>

	statsChan chan stats.Stat

	log         logrus.Entry
	client      kinesisiface.KinesisAPI
	streamName  string
	retryPolicy backoff.BackOff
}

func NewTransporterWithInterface(shutdownHandler shutdown.ShutdownHandler,
	inputChan <-chan transport.Batch,
	txnsWritten chan<- *ordered_map.OrderedMap,
	statsChan chan stats.Stat,
	log logrus.Entry,
	id int,
	streamName string,
	client kinesisiface.KinesisAPI,
	retryPolicy backoff.BackOff) transport.Transporter {

	log = *log.WithField("routine", "transporter").WithField("id", id)

	return &KinesisTransporter{
		shutdownHandler,
		inputChan,
		txnsWritten,
		statsChan,
		log,
		client,
		streamName,
		retryPolicy,
	}
}

// NewTransporter returns a kinesis transporter
func NewTransporter(shutdownHandler shutdown.ShutdownHandler,
	inputChan <-chan transport.Batch,
	txnsWritten chan<- *ordered_map.OrderedMap,
	statsChan chan stats.Stat,
	log logrus.Entry,
	id int,
	streamName string,
	retryPolicy backoff.BackOff,
	awsRegion *string,
	awsAccessKeyId *string,
	awsSecretAccessKey *string,
	endpoint *string) transport.Transporter {

	var sess *session.Session

	creds := credentials.NewStaticCredentials(*awsAccessKeyId, *awsSecretAccessKey, "")
	sess = session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(*awsRegion),
		Credentials: creds,
		Endpoint:    endpoint,
	}))

	client := kinesis.New(sess)

	return NewTransporterWithInterface(
		shutdownHandler,
		inputChan,
		txnsWritten,
		statsChan,
		log,
		id,
		streamName,
		client,
		retryPolicy)
}

// shutdown idempotently closes the output channel
func (t *KinesisTransporter) shutdown() {
	t.log.Info("shutting down transporter")
	t.shutdownHandler.CancelFunc() // initiate shutdown on other modules as well

	if r := recover(); r != nil {
		t.log.Warnf("Recovered in KinesisTransporter %s", r)
	}

	defer func() {
		// recover if channel is already closed
		recover()
	}()

	t.log.Debug("closing progress channel")
	close(t.txnsWritten)
}

func (t *KinesisTransporter) transportWithRetry(ctx context.Context, pri *kinesis.PutRecordsInput) (error, bool) {
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

		pro, err := t.client.PutRecords(pri)

		// If entire put had an error then entire batch needs retrying
		if err != nil {
			t.log.Warnf("err %s", err)
			t.statsChan <- stats.NewStatCount("kinesis_transport", "failure", 1, TimeSource.UnixNano())
			return err
		}

		// If there are no failures then put was successful
		if *pro.FailedRecordCount == 0 {
			t.statsChan <- stats.NewStatCount("kinesis_transport", "success", 1, TimeSource.UnixNano())
			return nil
		}

		// Verify output size is same as input size. It will cause the below
		// loop to have unexpected results.
		if len(pri.Records) != len(pro.Records) {
			panic("Put record input size does not match put record output size")
		}

		// Find successful records and remove them from the PutRecordsInput.
		// Then return an error to signify a retry is required
		errorMessages := map[string]int{} // Keep track of error messages
		toRetry := pri.Records[:0]        // Trick to re-use existing slice

		for i, element := range pro.Records {
			if element.ErrorCode != nil {
				errorMessages[*element.ErrorCode] += 1
				r := pri.Records[i]
				toRetry = append(toRetry, r)
			}
		}

		pri.SetRecords(toRetry)

		// record the error
		err = errors.New(fmt.Sprintf("%d records failed to be put to Kinesis: %v", *pro.FailedRecordCount, errorMessages))
		t.log.Warnf("err %s", err)
		t.statsChan <- stats.NewStatCount("kinesis_transport", "failure", 1, TimeSource.UnixNano())

		// return err to signify a retry is needed
		return err
	}

	// Reset retrier
	defer func() {
		t.retryPolicy.Reset()
	}()

	err := backoff.Retry(operation, t.retryPolicy)
	if err != nil {
		// Handle error.
		return err, cancelled
	}

	return nil, cancelled
}

// StartTransporting reads in message batches, outputs its data to Kinesis and then sends a progress report on the batch
func (t *KinesisTransporter) StartTransporting() {
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

		kinesisBatch, ok := b.(*batch.KinesisBatch)

		if !ok {
			panic("Batch is not a KinesisBatch")
		}

		payload := kinesisBatch.GetPayload()
		prre, ok := payload.([]*kinesis.PutRecordsRequestEntry)

		if !ok {
			panic("Batch payload is not a []*kinesis.PutRecordsRequestEntry")
		}

		// construct request
		pri := kinesis.PutRecordsInput{Records: prre, StreamName: &t.streamName}

		// Begin timer
		start := TimeSource.UnixNano()

		// send to Kinesis with some retry logic
		err, cancelled := t.transportWithRetry(t.shutdownHandler.TerminateCtx, &pri)

		// End timer and send stat

		total := (TimeSource.UnixNano() - start) / int64(time.Millisecond)
		t.statsChan <- stats.NewStatHistogram("kinesis_transport", "duration", total, TimeSource.UnixNano(), "ms")

		if err != nil {
			t.log.Error("max retries exceeded")
			return
		}

		// if transportWithRetry was cancelled, then loop back around to process another potential batch or shutdown
		if cancelled {
			continue
		}

		t.log.Debug("successfully wrote batch")
		t.statsChan <- stats.NewStatCount("kinesis_transport", "written", int64(kinesisBatch.NumMessages()), TimeSource.UnixNano())

		// report transactions written in this batch
		t.txnsWritten <- kinesisBatch.GetTransactions()
	}
}
