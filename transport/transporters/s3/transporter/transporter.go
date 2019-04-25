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
	"github.com/cevaris/ordered_map"
	"github.com/sirupsen/logrus"
)

// Generate mock for s3 client
//go:generate mockgen -destination=mocks/mock_s3.go -package=mocks github.com/aws/aws-sdk-go/service/s3/s3iface S3API

var (
	TimeSource utils.TimeSource = utils.RealTime{}
)

// key_join is a helper to concatenate strings to form an S3 key
func key_join(gzipped bool, strs ...string, ) string {
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

	if gzipped {
		sb.WriteString(".gz")
	}

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
}

func NewTransporterWithInterface(shutdownHandler shutdown.ShutdownHandler,
	inputChan <-chan transport.Batch,
	txnsWritten chan<- *ordered_map.OrderedMap,
	statsChan chan stats.Stat,
	log logrus.Entry,
	id int,
	bucketName string,
	keySpace string,
	client s3iface.S3API) transport.Transporter {

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
	awsRegion *string,
	awsAccessKeyId *string,
	awsSecretAccessKey *string,
	endpoint *string,) transport.Transporter {

	var sess *session.Session

	creds := credentials.NewStaticCredentials(*awsAccessKeyId, *awsSecretAccessKey, "")

	if *endpoint != "" {
		sess = session.Must(session.NewSession(&aws.Config{
			Region:      		aws.String(*awsRegion),
			Credentials: 		creds,
			S3ForcePathStyle: 	aws.Bool(true),
			Endpoint:    		aws.String(*endpoint),
		}))
	} else {
		sess = session.Must(session.NewSession(&aws.Config{
			Region:      aws.String(*awsRegion),
			Credentials: creds,
		}))
	}

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
		client)
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


// transport does a PUT on a full batch as a single key/file to S3
func (t *S3Transporter) transport(ctx context.Context, messagesSlice []*marshaller.MarshalledMessage) (error, bool) {
	var cancelled bool

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)

	select {
	case <-ctx.Done():
		t.log.Debug("received terminateCtx cancellation")
		return nil, true
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
	year, month, day, full := TimeSource.DateString()
	baseFilename := fmt.Sprintf("%s-%d", full, firstWalStart)

	fullKey := key_join(true, t.keySpace, year, month, day, baseFilename)

	// Do the upload and let S3 handle retries
	_, err := t.client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket:          aws.String(t.bucketName),
		Key:             aws.String(fullKey),
		Body:   		 byteReader,
		ContentEncoding: aws.String("gzip"),
	})

	// If there are no failures then all messages were sent
	if err == nil {
		t.log.Info(fmt.Sprintf("successful PUT: %s/%s", t.bucketName, fullKey))
		t.statsChan <- stats.NewStatCount("s3_transport", "success", 1, TimeSource.UnixNano())
		return nil, cancelled
	}

	// If any errors occurred during sending then entire batch
	t.log.WithError(err).Error(fmt.Sprintf("wal_start %d failed to be uploaded to S3 after client retries", firstWalStart))
	t.statsChan <- stats.NewStatCount("s3_transport", "failure", 1, TimeSource.UnixNano())

	return err, cancelled
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
		err, cancelled := t.transport(t.shutdownHandler.TerminateCtx, messagesSlice)

		// End timer and send stat
		total := (TimeSource.UnixNano() - start) / int64(time.Millisecond)
		t.statsChan <- stats.NewStatHistogram("s3_transport", "duration", total, TimeSource.UnixNano(), "ms")

		if err != nil {
			t.log.Error(err)
			return
		}

		// if transport was cancelled, then loop back around to shutdown
		if cancelled {
			continue
		}

		t.log.Debug("successfully wrote batch")
		t.statsChan <- stats.NewStatCount("s3_transport", "written", int64(genericBatch.NumMessages()), TimeSource.UnixNano())

		// report transactions written in this batch
		t.txnsWritten <- genericBatch.GetTransactions()
	}
}
