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
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/Nextdoor/pg-bifrost.git/marshaller"
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/Nextdoor/pg-bifrost.git/transport/batch"
	"github.com/Nextdoor/pg-bifrost.git/utils"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/cenkalti/backoff"
	"github.com/cevaris/ordered_map"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Generate mock for s3 client
//go:generate mockgen -destination=mocks/mock_s3.go -package=mocks github.com/aws/aws-sdk-go/service/s3/s3iface S3API

var (
	TimeSource utils.TimeSource = utils.RealTime{}
)

// key_join is a helper to concatenate strings to form an S3 key
func key_join(strs ...string, ) string {
	var sb strings.Builder
	for i, str := range strs {
		if str == "" || str == "/" {
			continue
		}

		// clean any leading and trailing slashes
		str = strings.TrimRight(str, "/")
		str = strings.TrimLeft(str, "/")

		// concatenate string and add trailing slash
		sb.WriteString(str)

		if i != len(strs) - 1 {
			sb.WriteString("/")
		}
	}

	sb.WriteString(".gz")

	return sb.String()
}

type S3Transporter struct {
	shutdownHandler shutdown.ShutdownHandler

	inputChan   <-chan transport.Batch         // receive a batch (slice) of MarshalledMessages
	txnsWritten chan<- *ordered_map.OrderedMap // Map of <transaction:progress.Written>

	statsChan chan stats.Stat

	log         logrus.Entry
	client      s3iface.S3API
	bucketName  string
	keySpace    string
	retryPolicy backoff.BackOff
}

func NewTransporterWithInterface(shutdownHandler shutdown.ShutdownHandler,
	inputChan <-chan transport.Batch,
	txnsWritten chan<- *ordered_map.OrderedMap,
	statsChan chan stats.Stat,
	log logrus.Entry,
	id int,
	bucketName string,
	keySpace string,
	client s3iface.S3API,
	retryPolicy backoff.BackOff) transport.Transporter {

	log = *log.WithField("routine", "transporter").WithField("id", id)

	return &S3Transporter{
		shutdownHandler,
		inputChan,
		txnsWritten,
		statsChan,
		log,
		client,
		bucketName,
		keySpace,
		retryPolicy,
	}
}

// NewTransporter returns a s3 transporter
func NewTransporter(shutdownHandler shutdown.ShutdownHandler,
	inputChan <-chan transport.Batch,
	txnsWritten chan<- *ordered_map.OrderedMap,
	statsChan chan stats.Stat,
	log logrus.Entry,
	id int,
	bucketName string,
	keySpace string,
	retryPolicy backoff.BackOff,
	awsRegion *string,
	awsAccessKeyId *string,
	awsSecretAccessKey *string,
	endpointPtr *string,) transport.Transporter {

	awsConfig := &aws.Config{
		Region:      		aws.String(*awsRegion),
		MaxRetries:		aws.Int(0), // We disable the client retry policy because of our own retry logic
	}

	if *awsAccessKeyId != "" || *awsSecretAccessKey != "" {
		// Force static credentials from pg-bifrost configuration.
		// Note: if we expect to fail and not infer credentials if only one of the ID or Key was specified
		awsConfig.Credentials = credentials.NewStaticCredentials(*awsAccessKeyId, *awsSecretAccessKey, "")
	}

	if endpointPtr != nil {
		// If specifying a custom endpoint (such as for localstack) then configure that and use Path Style
		awsConfig.Endpoint = aws.String(*endpointPtr)
		awsConfig.S3ForcePathStyle = aws.Bool(true)
	}

	sess := session.Must(session.NewSession(awsConfig))
	client := s3.New(sess)

	return NewTransporterWithInterface(
		shutdownHandler,
		inputChan,
		txnsWritten,
		statsChan,
		log,
		id,
		bucketName,
		keySpace,
		client,
		retryPolicy)
}

// shutdown idempotently closes the output channel
func (t *S3Transporter) shutdown() {
	t.log.Info("shutting down transporter")
	t.shutdownHandler.CancelFunc() // initiate shutdown on other modules as well

	if r := recover(); r != nil {
		t.log.Warnf("Recovered in S3Transporter %s", r)
	}

	defer func() {
		// recover if channel is already closed
		recover()
	}()

	t.log.Debug("closing progress channel")
	close(t.txnsWritten)
}


// transportWithRetry does a PUT on a full batch as a single key/file to S3
func (t *S3Transporter) transportWithRetry(ctx context.Context, messagesSlice []*marshaller.MarshalledMessage) (error, bool) {
	var cancelled bool
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)

	select {
	case <-ctx.Done():
		t.log.Debug("received terminateCtx cancellation")
		cancelled = true
	default:
	}

	// add all messages into a gzipped buffer
	for _, msg := range messagesSlice {
		if _, err := gz.Write(msg.Json); err != nil {
			return err, cancelled
		}
		gz.Write([]byte("\n"))
	}

	if err := gz.Flush(); err != nil {
		return err, cancelled
	}
	if err := gz.Close(); err != nil {
		return err, cancelled
	}

	gz.Reset(ioutil.Discard)

	firstWalStart := messagesSlice[0].WalStart

	// Free up messagesSlice now that we're done with it
	messagesSlice = nil

	// Clear out every allocation in the conversion
	byteArray := buf.Bytes()
	buf.Reset()

	byteReader := bytes.NewReader(byteArray)
	byteArray = nil

	// Partition the S3 keys into days
	year, month, day, hour, full := TimeSource.DateString()
	baseFilename := fmt.Sprintf("%s_%d", full, firstWalStart)

	fullKey := key_join(t.keySpace, year, month, day, hour, baseFilename)

	// An operation that may fail.
	operation := func() error {
		// Do the upload and let S3 handle retries
		_, err := t.client.PutObjectWithContext(ctx, &s3.PutObjectInput{
			Bucket:          aws.String(t.bucketName),
			Key:             aws.String(fullKey),
			Body:            byteReader,
			ContentEncoding: aws.String("gzip"),
		})

		// If any errors occurred during sending the entire batch
		if err != nil {
			t.log.WithError(err).Errorf("%s failed to be uploaded to S3", fullKey)
			t.statsChan <- stats.NewStatCount("s3_transport", "failure", 1, TimeSource.UnixNano())

			// Rewind the reader for retries
			_, seekErr := byteReader.Seek(0, 0)
			if seekErr != nil {
				return errors.New(
					fmt.Sprintf("Seek error on io.Reader rewind when preparing for retry: %s", seekErr.Error()))
			}

			return err
		}

		// If there are no failures then all messages were sent
		t.log.Infof("successful PUT: %s/%s", t.bucketName, fullKey)
		t.statsChan <- stats.NewStatCount("s3_transport", "success", 1, TimeSource.UnixNano())
		return nil
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


// StartTransporting reads in message batches, outputs its data to S3 and then sends a progress report on the batch
func (t *S3Transporter) StartTransporting() {
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

		// send to S3
		err, cancelled := t.transportWithRetry(t.shutdownHandler.TerminateCtx, messagesSlice)

		// End timer and send stat
		total := (TimeSource.UnixNano() - start) / int64(time.Millisecond)
		t.statsChan <- stats.NewStatHistogram("s3_transport", "duration", total, TimeSource.UnixNano(), "ms")

		if err != nil {
			t.log.Error("max retries exceeded")
			return
		}

		// if transportWithRetry was cancelled, then loop back around to shutdown
		if cancelled {
			continue
		}

		t.log.Debug("successfully wrote batch")
		t.statsChan <- stats.NewStatCount("s3_transport", "written", int64(genericBatch.NumMessages()), TimeSource.UnixNano())

		// report transactions written in this batch
		t.txnsWritten <- genericBatch.GetTransactions()
	}
}
