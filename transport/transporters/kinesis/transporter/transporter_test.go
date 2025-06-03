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
	"github.com/Nextdoor/pg-bifrost.git/transport/transporters/kinesis/batch"
	"github.com/Nextdoor/pg-bifrost.git/transport/transporters/kinesis/transporter/mocks"
	kinesis_utils "github.com/Nextdoor/pg-bifrost.git/transport/transporters/kinesis/utils"
	"github.com/Nextdoor/pg-bifrost.git/utils"
	utils_mocks "github.com/Nextdoor/pg-bifrost.git/utils/mocks"

	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/cenkalti/backoff/v4"
	"github.com/cevaris/ordered_map"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

var (
	logger = logrus.New()
	log    = logger.WithField("package", "kinesis")
)

func resetTimeSource() {
	TimeSource = utils.RealTime{}
}

func TestPutOk(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	defer resetTimeSource()

	in := make(chan transport.Batch, 1000)
	txns := make(chan *ordered_map.OrderedMap, 1000)
	statsChan := make(chan stats.Stat, 1000)

	streamName := "stream"
	mockClient := mocks.NewMockKinesisAPI(mockCtrl)
	mockTime := utils_mocks.NewMockTimeSource(mockCtrl)
	TimeSource = mockTime
	sh := shutdown.NewShutdownHandler()

	transport := NewTransporterWithInterface(sh, in, txns, statsChan, *log, 1, streamName, mockClient, backoff.NewConstantBackOff(0))
	b := batch.NewKinesisBatch("", kinesis_utils.KINESIS_PART_WALSTART)

	marshalledMessage := marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("data"),
		TimeBasedKey: "123",
		WalStart:     1234,
		Transaction:  "123",
	}

	_, _ = b.Add(&marshalledMessage)

	// Expects
	pk := fmt.Sprintf("%v", marshalledMessage.WalStart)
	record := kinesis.PutRecordsRequestEntry{Data: marshalledMessage.Json, PartitionKey: &pk}
	records := []*kinesis.PutRecordsRequestEntry{&record}
	expectedInput := kinesis.PutRecordsInput{Records: records, StreamName: &streamName}

	i := int64(0)
	var count = &i
	output := kinesis.PutRecordsOutput{FailedRecordCount: count}

	mockClient.EXPECT().PutRecords(&expectedInput).Return(&output, nil)

	mockTime.EXPECT().UnixNano().Return(int64(0))
	mockTime.EXPECT().UnixNano().Return(int64(1000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(2000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(3000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(4000 * time.Millisecond))

	in <- b

	// Start test
	go transport.StartTransporting()

	// Wait for data to go through
	time.Sleep(time.Millisecond * 25)

	// Verify stats
	expected := []stats.Stat{
		stats.NewStatCount("kinesis_transport", "success", int64(1), int64(1000*time.Millisecond)),
		stats.NewStatHistogram("kinesis_transport", "duration", 2000, int64(3000*time.Millisecond), "ms"),
		stats.NewStatCount("kinesis_transport", "written", int64(1), int64(4000*time.Millisecond)),
	}
	stats.VerifyStats(t, statsChan, expected)
}

func TestPutRetryTimeout(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	defer resetTimeSource()

	in := make(chan transport.Batch, 1000)
	txns := make(chan *ordered_map.OrderedMap, 1000)
	statsChan := make(chan stats.Stat, 1000)

	streamName := "stream"
	mockClient := mocks.NewMockKinesisAPI(mockCtrl)

	sh := shutdown.NewShutdownHandler()

	retryPolicy := backoff.WithMaxRetries(backoff.NewConstantBackOff(0), 5)
	transport := NewTransporterWithInterface(
		sh,
		in,
		txns,
		statsChan,
		*log,
		1,
		streamName,
		mockClient,
		retryPolicy)

	b := batch.NewKinesisBatch("", kinesis_utils.KINESIS_PART_WALSTART)

	// Expects
	records := []*kinesis.PutRecordsRequestEntry{}
	expectedInput := kinesis.PutRecordsInput{Records: records, StreamName: &streamName}

	i := int64(0)
	var count = &i
	output := kinesis.PutRecordsOutput{FailedRecordCount: count}

	mockClient.EXPECT().PutRecords(&expectedInput).Return(&output, errors.New("Error")).MinTimes(2)

	// Start Test
	in <- b

	go transport.StartTransporting()

	// Sleep a little to ensure retries had time to run
	time.Sleep(time.Millisecond * 1)

	// Verify shutdown
	_, ok := <-txns
	fmt.Println("checking if channel is closed")
	if ok {
		assert.Fail(t, "output channel not properly closed")
	}
}

func TestPutRetryWithError(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	defer resetTimeSource()

	in := make(chan transport.Batch, 1000)
	txns := make(chan *ordered_map.OrderedMap, 1000)
	statsChan := make(chan stats.Stat, 1000)

	streamName := "stream"
	mockClient := mocks.NewMockKinesisAPI(mockCtrl)
	mockTime := utils_mocks.NewMockTimeSource(mockCtrl)
	TimeSource = mockTime

	sh := shutdown.NewShutdownHandler()

	transport := NewTransporterWithInterface(sh,
		in,
		txns,
		statsChan,
		*log,
		1,
		streamName,
		mockClient,
		backoff.NewConstantBackOff(0))
	b := batch.NewKinesisBatch("", kinesis_utils.KINESIS_PART_WALSTART)

	marshalledMessage := marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("data"),
		TimeBasedKey: "123",
		WalStart:     1234,
		Transaction:  "123",
	}

	_, _ = b.Add(&marshalledMessage)

	// Expects
	pk := fmt.Sprintf("%v", marshalledMessage.WalStart)
	record := kinesis.PutRecordsRequestEntry{Data: marshalledMessage.Json, PartitionKey: &pk}
	records := []*kinesis.PutRecordsRequestEntry{&record}
	expectedInput := kinesis.PutRecordsInput{Records: records, StreamName: &streamName}

	i := int64(0)
	var count = &i
	output := kinesis.PutRecordsOutput{FailedRecordCount: count}

	mockClient.EXPECT().PutRecords(&expectedInput).Return(&output, errors.New("Error"))
	mockClient.EXPECT().PutRecords(&expectedInput).Return(&output, errors.New("Error"))
	mockClient.EXPECT().PutRecords(&expectedInput).Return(&output, nil)

	mockTime.EXPECT().UnixNano().Return(int64(0))
	mockTime.EXPECT().UnixNano().Return(int64(1000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(2000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(3000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(4000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(5000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(6000 * time.Millisecond))

	// Start Test
	in <- b

	go transport.StartTransporting()

	// Sleep a little to ensure retries had time to run
	time.Sleep(time.Millisecond * 25)

	// Verify stats
	expected := []stats.Stat{
		stats.NewStatCount("kinesis_transport", "failure", int64(1), int64(1000000000)),
		stats.NewStatCount("kinesis_transport", "failure", int64(1), int64(2000000000)),
		stats.NewStatCount("kinesis_transport", "success", int64(1), int64(3000000000)),
		stats.NewStatHistogram("kinesis_transport", "duration", 4000, int64(5000000000), "ms"),
		stats.NewStatCount("kinesis_transport", "written", int64(1), int64(6000000000)),
	}
	stats.VerifyStats(t, statsChan, expected)
}

func TestPutWithFailuresNoError(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	defer resetTimeSource()

	in := make(chan transport.Batch, 1000)
	txns := make(chan *ordered_map.OrderedMap, 1000)
	statsChan := make(chan stats.Stat, 1000)

	streamName := "stream"
	mockClient := mocks.NewMockKinesisAPI(mockCtrl)
	mockTime := utils_mocks.NewMockTimeSource(mockCtrl)
	TimeSource = mockTime

	sh := shutdown.NewShutdownHandler()

	retryPolicy := backoff.WithMaxRetries(backoff.NewConstantBackOff(0), 5)
	transport := NewTransporterWithInterface(sh,
		in,
		txns,
		statsChan,
		*log,
		1,
		streamName,
		mockClient,
		retryPolicy)
	b := batch.NewKinesisBatch("", kinesis_utils.KINESIS_PART_WALSTART)

	marshalledMessage := marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("data"),
		TimeBasedKey: "123",
		WalStart:     1234,
		Transaction:  "123",
	}

	_, _ = b.Add(&marshalledMessage)

	// Expects
	pk := fmt.Sprintf("%v", marshalledMessage.WalStart)
	record := kinesis.PutRecordsRequestEntry{Data: marshalledMessage.Json, PartitionKey: &pk}
	records := []*kinesis.PutRecordsRequestEntry{&record}
	expectedInput := kinesis.PutRecordsInput{Records: records, StreamName: &streamName}

	i := int64(1)
	j := int64(0)
	var countFail = &i
	var countSuccess = &j

	errMsg := "ProvisionedThroughputExceededException"
	prres := []*kinesis.PutRecordsResultEntry{{ErrorCode: &errMsg}}

	outputFail := kinesis.PutRecordsOutput{FailedRecordCount: countFail, Records: prres}
	outputSuccess := kinesis.PutRecordsOutput{FailedRecordCount: countSuccess}

	mockClient.EXPECT().PutRecords(&expectedInput).Return(&outputFail, nil)
	mockClient.EXPECT().PutRecords(&expectedInput).Return(&outputFail, nil)
	mockClient.EXPECT().PutRecords(&expectedInput).Return(&outputSuccess, nil)

	mockTime.EXPECT().UnixNano().Return(int64(0))
	mockTime.EXPECT().UnixNano().Return(int64(1000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(2000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(3000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(4000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(5000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(6000 * time.Millisecond))

	// Start Test
	in <- b

	go transport.StartTransporting()

	// Sleep a little to ensure retries had time to run
	time.Sleep(time.Millisecond * 25)

	// Verify stats
	expected := []stats.Stat{
		stats.NewStatCount("kinesis_transport", "failure", int64(1), int64(1000000000)),
		stats.NewStatCount("kinesis_transport", "failure", int64(1), int64(2000000000)),
		stats.NewStatCount("kinesis_transport", "success", int64(1), int64(3000000000)),
		stats.NewStatHistogram("kinesis_transport", "duration", 4000, int64(5000000000), "ms"),
		stats.NewStatCount("kinesis_transport", "written", int64(1), int64(6000000000)),
	}
	stats.VerifyStats(t, statsChan, expected)
}

func TestRetryOnlyFailures(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	defer resetTimeSource()

	in := make(chan transport.Batch, 1000)
	txns := make(chan *ordered_map.OrderedMap, 1000)
	statsChan := make(chan stats.Stat, 1000)

	streamName := "stream"
	mockClient := mocks.NewMockKinesisAPI(mockCtrl)
	mockTime := utils_mocks.NewMockTimeSource(mockCtrl)
	TimeSource = mockTime

	sh := shutdown.NewShutdownHandler()

	retryPolicy := backoff.WithMaxRetries(backoff.NewConstantBackOff(0), 5)
	transport := NewTransporterWithInterface(sh,
		in,
		txns,
		statsChan,
		*log,
		1,
		streamName,
		mockClient,
		retryPolicy)
	b := batch.NewKinesisBatch("", kinesis_utils.KINESIS_PART_WALSTART)

	firstMessage := marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("foo"),
		TimeBasedKey: "123",
		WalStart:     1234,
		Transaction:  "123",
	}

	secondMessage := marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("bar"),
		TimeBasedKey: "124",
		WalStart:     4567,
		Transaction:  "124",
	}

	_, _ = b.Add(&firstMessage)
	_, _ = b.Add(&secondMessage)

	// Input
	firstPk := fmt.Sprintf("%v", firstMessage.WalStart)
	firstRecord := kinesis.PutRecordsRequestEntry{Data: firstMessage.Json, PartitionKey: &firstPk}

	secondPk := fmt.Sprintf("%v", secondMessage.WalStart)
	secondRecord := kinesis.PutRecordsRequestEntry{Data: secondMessage.Json, PartitionKey: &secondPk}

	i := int64(1)
	j := int64(0)
	var countFail = &i
	var countSuccess = &j

	fmt.Println("countFail: ", *countFail)

	// First put (firstRecord: success, secondRecord: failed)
	errMsg := "ProvisionedThroughputExceededException"
	firstRecords := []*kinesis.PutRecordsRequestEntry{&firstRecord, &secondRecord}
	firstInput := kinesis.PutRecordsInput{Records: firstRecords, StreamName: &streamName}
	firstOutput := kinesis.PutRecordsOutput{FailedRecordCount: countFail,
		Records: []*kinesis.PutRecordsResultEntry{{ErrorCode: nil}, {ErrorCode: &errMsg}}}
	mockClient.EXPECT().PutRecords(&firstInput).Return(&firstOutput, nil)

	// Second put (secondRecord: success)
	secondRecords := []*kinesis.PutRecordsRequestEntry{&secondRecord}
	secondOutput := kinesis.PutRecordsOutput{FailedRecordCount: countSuccess,
		Records: []*kinesis.PutRecordsResultEntry{{ErrorCode: nil}}}
	secondInput := kinesis.PutRecordsInput{Records: secondRecords, StreamName: &streamName}
	mockClient.EXPECT().PutRecords(&secondInput).Return(&secondOutput, nil)

	mockTime.EXPECT().UnixNano().Return(int64(0))
	mockTime.EXPECT().UnixNano().Return(int64(1000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(2000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(3000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(4000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(5000 * time.Millisecond))

	// Start Test
	in <- b

	go transport.StartTransporting()

	// Sleep a little to ensure retries had time to run
	time.Sleep(time.Millisecond * 205)

	// Verify stats
	expected := []stats.Stat{
		stats.NewStatCount("kinesis_transport", "failure", int64(1), int64(1000000000)),
		stats.NewStatCount("kinesis_transport", "success", int64(1), int64(2000000000)),
		stats.NewStatHistogram("kinesis_transport", "duration", 3000, int64(4000000000), "ms"),
		stats.NewStatCount("kinesis_transport", "written", int64(2), int64(5000000000)),
	}
	stats.VerifyStats(t, statsChan, expected)
}

func TestInputClose(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	defer resetTimeSource()

	in := make(chan transport.Batch, 1000)
	txns := make(chan *ordered_map.OrderedMap, 1000)
	statsChan := make(chan stats.Stat, 1000)

	streamName := "stream"
	mockClient := mocks.NewMockKinesisAPI(mockCtrl)

	sh := shutdown.NewShutdownHandler()

	retryPolicy := backoff.NewConstantBackOff(0)
	transport := NewTransporterWithInterface(sh,
		in,
		txns,
		statsChan,
		*log,
		1,
		streamName,
		mockClient,
		retryPolicy)

	go transport.StartTransporting()

	// Wait for transporter to start
	time.Sleep(time.Millisecond * 5)

	// Close input
	close(in)
	time.Sleep(time.Millisecond * 5)

	// Verify shutdown
	_, ok := <-txns
	fmt.Println("checking if channel is closed")
	if ok {
		assert.Fail(t, "output channel not properly closed")
	}

	_, ok = <-sh.TerminateCtx.Done()
	if ok {
		assert.Fail(t, "context not cancelled")
	}
}

func TestTerminationContext(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	defer resetTimeSource()

	in := make(chan transport.Batch, 1000)
	txns := make(chan *ordered_map.OrderedMap, 1000)
	statsChan := make(chan stats.Stat, 1000)

	streamName := "stream"
	mockClient := mocks.NewMockKinesisAPI(mockCtrl)

	sh := shutdown.NewShutdownHandler()

	retryPolicy := backoff.NewConstantBackOff(0)
	transport := NewTransporterWithInterface(sh,
		in,
		txns,
		statsChan,
		*log,
		1,
		streamName,
		mockClient,
		retryPolicy)

	go transport.StartTransporting()

	// Wait for transporter to start
	time.Sleep(time.Millisecond * 5)

	// Cancel
	sh.CancelFunc()
	time.Sleep(time.Millisecond * 5)

	// Verify shutdown
	_, ok := <-txns
	if ok {
		assert.Fail(t, "output channel not properly closed")
	}
}

func TestTerminationContextInRetry(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	defer resetTimeSource()

	in := make(chan transport.Batch, 1000)
	txns := make(chan *ordered_map.OrderedMap, 1000)
	statsChan := make(chan stats.Stat, 1000)

	streamName := "stream"
	mockClient := mocks.NewMockKinesisAPI(mockCtrl)
	mockTime := utils_mocks.NewMockTimeSource(mockCtrl)
	TimeSource = mockTime

	sh := shutdown.NewShutdownHandler()

	transport := NewTransporterWithInterface(sh,
		in,
		txns,
		statsChan,
		*log,
		1,
		streamName,
		mockClient,
		backoff.NewConstantBackOff(0))
	b := batch.NewKinesisBatch("", kinesis_utils.KINESIS_PART_WALSTART)

	marshalledMessage := marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("data"),
		TimeBasedKey: "123",
		WalStart:     1234,
		Transaction:  "123",
	}

	_, _ = b.Add(&marshalledMessage)

	// Expects
	pk := fmt.Sprintf("%v", marshalledMessage.WalStart)
	record := kinesis.PutRecordsRequestEntry{Data: marshalledMessage.Json, PartitionKey: &pk}
	records := []*kinesis.PutRecordsRequestEntry{&record}
	expectedInput := kinesis.PutRecordsInput{Records: records, StreamName: &streamName}

	i := int64(0)
	var count = &i
	output := kinesis.PutRecordsOutput{FailedRecordCount: count}

	mockClient.EXPECT().PutRecords(&expectedInput).Return(&output, errors.New("Error"))
	mockClient.EXPECT().PutRecords(&expectedInput).Do(func(a interface{}) {
		sh.CancelFunc()
	}).Return(&output, errors.New("Error"))

	mockTime.EXPECT().UnixNano().Return(int64(0))
	mockTime.EXPECT().UnixNano().Return(int64(1000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(2000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(3000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(4000 * time.Millisecond))

	// Start Test
	in <- b

	go transport.StartTransporting()

	// Sleep a little to ensure retries had time to run
	time.Sleep(time.Millisecond * 25)

	// Verify shutdown
	_, ok := <-sh.TerminateCtx.Done()
	if ok {
		assert.Fail(t, "context not cancelled")
	}
}

func TestPanicHandling(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	defer resetTimeSource()

	in := make(chan transport.Batch, 1000)
	txns := make(chan *ordered_map.OrderedMap, 1000)
	statsChan := make(chan stats.Stat, 1000)

	streamName := "stream"
	mockClient := mocks.NewMockKinesisAPI(mockCtrl)
	mockTime := utils_mocks.NewMockTimeSource(mockCtrl)
	TimeSource = mockTime

	sh := shutdown.NewShutdownHandler()

	transport := NewTransporterWithInterface(sh,
		in,
		txns,
		statsChan,
		*log,
		1,
		streamName,
		mockClient,
		backoff.NewConstantBackOff(0))
	b := batch.NewKinesisBatch("", kinesis_utils.KINESIS_PART_WALSTART)

	marshalledMessage := marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("data"),
		TimeBasedKey: "123",
		WalStart:     1234,
		Transaction:  "123",
	}

	_, _ = b.Add(&marshalledMessage)

	// Expects
	pk := fmt.Sprintf("%v", marshalledMessage.WalStart)
	record := kinesis.PutRecordsRequestEntry{Data: marshalledMessage.Json, PartitionKey: &pk}
	records := []*kinesis.PutRecordsRequestEntry{&record}
	expectedInput := kinesis.PutRecordsInput{Records: records, StreamName: &streamName}

	mockClient.EXPECT().PutRecords(&expectedInput).Do(func(a interface{}) {
		panic("expected")
	})

	mockTime.EXPECT().UnixNano().Return(int64(0))

	// Start Test
	in <- b

	go transport.StartTransporting()

	// Sleep a little to ensure retries had time to run
	time.Sleep(time.Millisecond * 25)

	// Verify shutdown
	_, ok := <-sh.TerminateCtx.Done()
	if ok {
		assert.Fail(t, "context not cancelled")
	}
}

func TestMissMatchReply(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	defer resetTimeSource()

	in := make(chan transport.Batch, 1000)
	txns := make(chan *ordered_map.OrderedMap, 1000)
	statsChan := make(chan stats.Stat, 1000)

	streamName := "stream"
	mockClient := mocks.NewMockKinesisAPI(mockCtrl)
	mockTime := utils_mocks.NewMockTimeSource(mockCtrl)
	TimeSource = mockTime

	sh := shutdown.NewShutdownHandler()

	transport := NewTransporterWithInterface(sh,
		in,
		txns,
		statsChan,
		*log,
		1,
		streamName,
		mockClient,
		backoff.NewConstantBackOff(0))
	b := batch.NewKinesisBatch("", kinesis_utils.KINESIS_PART_WALSTART)

	marshalledMessage := marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("data"),
		TimeBasedKey: "123",
		WalStart:     1234,
		Transaction:  "123",
	}

	_, _ = b.Add(&marshalledMessage)

	// Expects
	pk := fmt.Sprintf("%v", marshalledMessage.WalStart)
	record := kinesis.PutRecordsRequestEntry{Data: marshalledMessage.Json, PartitionKey: &pk}
	records := []*kinesis.PutRecordsRequestEntry{&record}
	expectedInput := kinesis.PutRecordsInput{Records: records, StreamName: &streamName}

	j := int64(1)
	var countSuccess = &j
	output := kinesis.PutRecordsOutput{FailedRecordCount: countSuccess,
		Records: []*kinesis.PutRecordsResultEntry{}}

	mockClient.EXPECT().PutRecords(&expectedInput).Return(&output, nil)
	mockTime.EXPECT().UnixNano().Return(int64(0))

	// Start Test
	in <- b

	go transport.StartTransporting()

	// Sleep a little to ensure retries had time to run
	time.Sleep(time.Millisecond * 25)

	// Verify shutdown
	_, ok := <-sh.TerminateCtx.Done()
	if ok {
		assert.Fail(t, "context not cancelled")
	}
}
