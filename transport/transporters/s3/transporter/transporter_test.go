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
	"encoding/base64"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/klauspost/pgzip"

	"github.com/Nextdoor/pg-bifrost.git/marshaller"
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/Nextdoor/pg-bifrost.git/transport/batch"
	"github.com/Nextdoor/pg-bifrost.git/transport/transporters/s3/transporter/mocks"
	"github.com/Nextdoor/pg-bifrost.git/utils"
	utils_mocks "github.com/Nextdoor/pg-bifrost.git/utils/mocks"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/cenkalti/backoff/v4"
	"github.com/cevaris/ordered_map"
	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

var (
	logger = logrus.New()
	log    = logger.WithField("package", "s3")
)

func resetTimeSource() {
	TimeSource = utils.RealTime{}
}

// streamSeekToBytes reads from a seeker once and return a string. Note that by nature, this will be a one time
// operations. Subsequent reads off of this io.ReadSeeker will not return any data.
func streamSeekToBytes(stream io.ReadSeeker) string {
	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(stream)
	return buf.String()
}

// gzipAsReadSeeker converts []*marshaller.MarshalledMessage to an io.ReadSeeker with newlines between each message
func gzipAsReadSeeker(messagesSlice []*marshaller.MarshalledMessage) io.ReadSeeker {
	var buf bytes.Buffer
	gz := pgzip.NewWriter(&buf)

	for _, msg := range messagesSlice {
		_, _ = gz.Write(msg.Json)
		_, _ = gz.Write([]byte("\n"))
	}

	_ = gz.Close()

	byteArray := buf.Bytes()
	return bytes.NewReader(byteArray)
}

// used to compare all fields of PutObjectInput while reading from the buffer for Body
type putObjectInputMatcher struct {
	Bucket          *string
	Key             *string
	BodyString      string
	ContentEncoding *string
}

// Matches uses putObjectInputMatcher to do a specific comparison
func (s *putObjectInputMatcher) Matches(x interface{}) bool {
	actual := x.(*s3.PutObjectInput)
	actualBodyString := streamSeekToBytes(actual.Body)

	if len(actualBodyString) < 1 || len(s.BodyString) < 1 {
		log.Error("TEST ERROR - expected or actual putObjectInputMatcher.Body are 0")
		return false
	}

	actualBase64 := base64.StdEncoding.EncodeToString([]byte(actualBodyString))
	expectedBase64 := base64.StdEncoding.EncodeToString([]byte(s.BodyString))
	fmt.Println("Base64 encoding of PutObjectInput.Body Matcher test: ")
	fmt.Printf("actual: %s\n", actualBase64)
	fmt.Printf("expected: %s\n", expectedBase64)

	return *actual.Bucket == *s.Bucket &&
		*actual.Key == *s.Key &&
		actualBodyString == s.BodyString &&
		*actual.ContentEncoding == *s.ContentEncoding
}

func (s *putObjectInputMatcher) String() string {
	return fmt.Sprintf("\nBucket: %s\nKey: %s\nBody: %s\nContentEncoding: %s\n",
		*s.Bucket, *s.Key, s.BodyString, *s.ContentEncoding)
}

// EqPutObjectInputWithBufferRead is a gomock.Matcher that we can pass to matcher as arguments
func EqPutObjectInputWithBufferRead(putObject *s3.PutObjectInput) gomock.Matcher {
	return &putObjectInputMatcher{
		putObject.Bucket,
		putObject.Key,
		streamSeekToBytes(putObject.Body),
		putObject.ContentEncoding,
	}
}

func TestSinglePutOk(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	defer resetTimeSource()

	in := make(chan transport.Batch, 1000)
	txns := make(chan *ordered_map.OrderedMap, 1000)
	statsChan := make(chan stats.Stat, 1000)

	bucketName := "test-bucket"
	keySpace := ""
	mockClient := mocks.NewMockS3API(mockCtrl)
	mockTime := utils_mocks.NewMockTimeSource(mockCtrl)
	TimeSource = mockTime
	sh := shutdown.NewShutdownHandler()

	batchSize := 1

	retryPolicy := backoff.WithMaxRetries(backoff.NewConstantBackOff(0), 5)
	transport := NewTransporterWithInterface(sh, in, txns, statsChan, *log, 0, bucketName, keySpace, mockClient, retryPolicy, 0)
	b := batch.NewGenericBatch("", batchSize)

	marshalledMessage := marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("data"),
		TimeBasedKey: "123",
		WalStart:     1234,
		Transaction:  "123",
	}
	_, _ = b.Add(&marshalledMessage)

	// Expects
	expectedInput := s3.PutObjectInput{
		Bucket:          aws.String(bucketName),
		Key:             aws.String("2000/01/02/03/456_1234.gz"),
		Body:            gzipAsReadSeeker(b.GetPayload().([]*marshaller.MarshalledMessage)),
		ContentEncoding: aws.String("gzip"),
	}

	mockClient.EXPECT().PutObjectWithContext(sh.TerminateCtx, EqPutObjectInputWithBufferRead(&expectedInput)).Return(nil, nil)

	mockTime.EXPECT().DateString().Return("2000", "01", "02", "03", "456")

	mockTime.EXPECT().UnixNano().Return(int64(0))
	mockTime.EXPECT().UnixNano().Return(int64(1000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(2000 * time.Millisecond))

	in <- b

	// Start test
	go transport.StartTransporting()

	// Wait for data to go through
	time.Sleep(time.Millisecond * 25)

	// Verify stats
	expected := []stats.Stat{
		stats.NewStatHistogram("s3_transport", "batch_waited", 0, int64(0*time.Millisecond), "ms"),
		stats.NewStatCount("s3_transport", "success", int64(1), int64(2000*time.Millisecond)),
		stats.NewStatHistogram("s3_transport", "duration", 2000, int64(3000*time.Millisecond), "ms"),
		stats.NewStatCount("s3_transport", "written", int64(1), int64(2000*time.Millisecond)),
	}
	stats.VerifyStats(t, statsChan, expected)
}

func TestSinglePutMultipleRecordsOk(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	defer resetTimeSource()

	in := make(chan transport.Batch, 1000)
	txns := make(chan *ordered_map.OrderedMap, 1000)
	statsChan := make(chan stats.Stat, 1000)

	bucketName := "test-bucket"
	keySpace := ""
	mockClient := mocks.NewMockS3API(mockCtrl)
	mockTime := utils_mocks.NewMockTimeSource(mockCtrl)
	TimeSource = mockTime
	sh := shutdown.NewShutdownHandler()

	batchSize := 2

	retryPolicy := backoff.WithMaxRetries(backoff.NewConstantBackOff(0), 5)
	transport := NewTransporterWithInterface(sh, in, txns, statsChan, *log, 0, bucketName, keySpace, mockClient, retryPolicy, 0)
	b := batch.NewGenericBatch("", batchSize)

	firstMarshalledMessage := marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("dataOne"),
		TimeBasedKey: "123",
		WalStart:     1234,
		Transaction:  "123",
	}
	_, _ = b.Add(&firstMarshalledMessage)

	secondMarshalledMessage := marshaller.MarshalledMessage{
		Operation:    "UPDATE",
		Json:         []byte("dataTwo"),
		TimeBasedKey: "124",
		WalStart:     1235,
		Transaction:  "123",
	}
	_, _ = b.Add(&secondMarshalledMessage)

	// Expects
	expectedInput := s3.PutObjectInput{
		Bucket:          aws.String(bucketName),
		Key:             aws.String("2000/01/02/03/456_1234.gz"),
		Body:            gzipAsReadSeeker(b.GetPayload().([]*marshaller.MarshalledMessage)),
		ContentEncoding: aws.String("gzip"),
	}

	mockClient.EXPECT().PutObjectWithContext(sh.TerminateCtx, EqPutObjectInputWithBufferRead(&expectedInput)).Return(nil, nil)

	mockTime.EXPECT().DateString().Return("2000", "01", "02", "03", "456")

	mockTime.EXPECT().UnixNano().Return(int64(0))
	mockTime.EXPECT().UnixNano().Return(int64(1000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(2000 * time.Millisecond))

	in <- b

	// Start test
	go transport.StartTransporting()

	// Wait for data to go through
	time.Sleep(time.Millisecond * 25)

	// Verify stats
	expected := []stats.Stat{
		stats.NewStatHistogram("s3_transport", "batch_waited", 0, 0, "ms"),
		stats.NewStatCount("s3_transport", "success", int64(1), int64(1000*time.Millisecond)),
		stats.NewStatHistogram("s3_transport", "duration", 2000, int64(2000*time.Millisecond), "ms"),
		stats.NewStatCount("s3_transport", "written", int64(2), int64(2000*time.Millisecond)),
	}
	stats.VerifyStats(t, statsChan, expected)
}

func TestSingleRecordSinglePutWithFailuresNoError(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	defer resetTimeSource()

	in := make(chan transport.Batch, 1000)
	txns := make(chan *ordered_map.OrderedMap, 1000)
	statsChan := make(chan stats.Stat, 1000)

	bucketName := "test-bucket"
	keySpace := ""
	mockClient := mocks.NewMockS3API(mockCtrl)
	mockTime := utils_mocks.NewMockTimeSource(mockCtrl)
	TimeSource = mockTime
	sh := shutdown.NewShutdownHandler()

	batchSize := 1

	retryPolicy := backoff.WithMaxRetries(backoff.NewConstantBackOff(0), 5)
	transport := NewTransporterWithInterface(sh, in, txns, statsChan, *log, 0, bucketName, keySpace, mockClient, retryPolicy, 0)
	b := batch.NewGenericBatch("", batchSize)

	marshalledMessage := marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("data"),
		TimeBasedKey: "123",
		WalStart:     1234,
		Transaction:  "123",
	}
	_, _ = b.Add(&marshalledMessage)

	// Expects
	expectedInputOne := s3.PutObjectInput{
		Bucket:          aws.String(bucketName),
		Key:             aws.String("2000/01/02/03/456_1234.gz"),
		Body:            gzipAsReadSeeker(b.GetPayload().([]*marshaller.MarshalledMessage)),
		ContentEncoding: aws.String("gzip"),
	}

	// We need two "identical" inputs because Body's io.Reader is mutable (reading again needs a Seek to rewind)
	expectedInputTwo := s3.PutObjectInput{
		Bucket:          aws.String(bucketName),
		Key:             aws.String("2000/01/02/03/456_1234.gz"),
		Body:            gzipAsReadSeeker(b.GetPayload().([]*marshaller.MarshalledMessage)),
		ContentEncoding: aws.String("gzip"),
	}

	mockClient.EXPECT().PutObjectWithContext(sh.TerminateCtx, EqPutObjectInputWithBufferRead(&expectedInputOne)).Return(nil, errors.New("some error"))
	mockClient.EXPECT().PutObjectWithContext(sh.TerminateCtx, EqPutObjectInputWithBufferRead(&expectedInputTwo)).Return(nil, nil)

	mockTime.EXPECT().DateString().Return("2000", "01", "02", "03", "456")

	mockTime.EXPECT().UnixNano().Return(int64(0))
	mockTime.EXPECT().UnixNano().Return(int64(1000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(2000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(3000 * time.Millisecond))

	in <- b

	// Start test
	go transport.StartTransporting()

	// Wait for data to go through
	time.Sleep(time.Millisecond * 25)

	// Verify stats
	expected := []stats.Stat{
		stats.NewStatHistogram("s3_transport", "batch_waited", 0, 0, "ms"),
		stats.NewStatCount("s3_transport", "failure", int64(1), int64(1000*time.Millisecond)),
		stats.NewStatCount("s3_transport", "success", int64(1), int64(2000*time.Millisecond)),
		stats.NewStatHistogram("s3_transport", "duration", 3000, int64(3000*time.Millisecond), "ms"),
		stats.NewStatCount("s3_transport", "written", int64(1), int64(3000*time.Millisecond)),
	}
	stats.VerifyStats(t, statsChan, expected)
}

func TestSingleRecordDoublePutRetriesExhaustedWithError(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	defer resetTimeSource()

	in := make(chan transport.Batch, 1000)
	txns := make(chan *ordered_map.OrderedMap, 1000)
	statsChan := make(chan stats.Stat, 1000)

	bucketName := "test-bucket"
	keySpace := ""
	mockClient := mocks.NewMockS3API(mockCtrl)
	mockTime := utils_mocks.NewMockTimeSource(mockCtrl)
	TimeSource = mockTime
	sh := shutdown.NewShutdownHandler()

	batchSize := 1

	retryPolicy := backoff.WithMaxRetries(backoff.NewConstantBackOff(0), 1)
	transport := NewTransporterWithInterface(sh, in, txns, statsChan, *log, 0, bucketName, keySpace, mockClient, retryPolicy, 0)
	b := batch.NewGenericBatch("", batchSize)

	marshalledMessage := marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("data"),
		TimeBasedKey: "123",
		WalStart:     1234,
		Transaction:  "123",
	}
	_, _ = b.Add(&marshalledMessage)

	// Expects
	expectedInputOne := s3.PutObjectInput{
		Bucket:          aws.String(bucketName),
		Key:             aws.String("2000/01/02/03/456_1234.gz"),
		Body:            gzipAsReadSeeker(b.GetPayload().([]*marshaller.MarshalledMessage)),
		ContentEncoding: aws.String("gzip"),
	}

	// We need two "identical" inputs because Body's io.Reader is mutable (reading needs a Seek back to start offset)
	expectedInputTwo := s3.PutObjectInput{
		Bucket:          aws.String(bucketName),
		Key:             aws.String("2000/01/02/03/456_1234.gz"),
		Body:            gzipAsReadSeeker(b.GetPayload().([]*marshaller.MarshalledMessage)),
		ContentEncoding: aws.String("gzip"),
	}

	mockClient.EXPECT().PutObjectWithContext(sh.TerminateCtx, EqPutObjectInputWithBufferRead(&expectedInputOne)).Return(nil, errors.New("some error"))
	mockClient.EXPECT().PutObjectWithContext(sh.TerminateCtx, EqPutObjectInputWithBufferRead(&expectedInputTwo)).Return(nil, errors.New("some error"))

	mockTime.EXPECT().DateString().Return("2000", "01", "02", "03", "456")

	mockTime.EXPECT().UnixNano().Return(int64(0))
	mockTime.EXPECT().UnixNano().Return(int64(1000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(2000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(2000 * time.Millisecond))
	in <- b

	// Start test
	go transport.StartTransporting()

	// Wait for data to go through
	time.Sleep(time.Millisecond * 25)

	// Verify stats
	expected := []stats.Stat{
		stats.NewStatHistogram("s3_transport", "batch_waited", 0, 0, "ms"),
		stats.NewStatCount("s3_transport", "failure", int64(1), int64(1000*time.Millisecond)),
		stats.NewStatCount("s3_transport", "failure", int64(1), int64(2000*time.Millisecond)),
		stats.NewStatHistogram("s3_transport", "duration", 2000, int64(2000*time.Millisecond), "ms"),
	}
	stats.VerifyStats(t, statsChan, expected)

	// Verify shutdown
	_, ok := <-sh.TerminateCtx.Done()
	if ok {
		assert.Fail(t, "context not cancelled")
	}
}
