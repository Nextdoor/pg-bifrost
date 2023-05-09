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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/NeowayLabs/wabbit"
	w_amqp "github.com/NeowayLabs/wabbit/amqp"
	"github.com/NeowayLabs/wabbit/amqptest"
	"github.com/NeowayLabs/wabbit/amqptest/server"
	"github.com/Nextdoor/pg-bifrost.git/marshaller"
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/Nextdoor/pg-bifrost.git/transport/batch"
	"github.com/Nextdoor/pg-bifrost.git/transport/transporters/rabbitmq/transporter/mocks"
	"github.com/Nextdoor/pg-bifrost.git/utils"
	utils_mocks "github.com/Nextdoor/pg-bifrost.git/utils/mocks"
	"github.com/cenkalti/backoff/v4"
	"github.com/cevaris/ordered_map"
	"github.com/golang/mock/gomock"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

var (
	logger = logrus.New()
	log    = logger.WithField("package", "rabbitmq")
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

	connString := "amqp://anyhost:anyport/%2fanyVHost"
	fakeServer := server.NewServer(connString)
	err := fakeServer.Start()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		_ = fakeServer.Stop()
	}()

	connMan := NewConnectionManager(connString, log, makeDialer(connString))
	mockConn, err := connMan.GetConnection(context.Background())
	if err != nil {
		t.Error(err)
	}
	channel, err := mockConn.Channel()
	if err != nil {
		t.Error(err)
	}

	exchangeName := "exchange"
	err = channel.ExchangeDeclare(
		exchangeName, // name of the exchange
		"topic",      // type
		wabbit.Option{
			"durable":  true,
			"delete":   false,
			"internal": false,
			"noWait":   false,
		},
	)
	if err != nil {
		t.Error(err)
	}
	defer channel.Close()

	mockTime := utils_mocks.NewMockTimeSource(mockCtrl)
	TimeSource = mockTime
	sh := shutdown.NewShutdownHandler()
	defer sh.CancelFunc()

	transport := NewTransporter(
		sh,
		in,
		txns,
		statsChan,
		*log,
		1,
		exchangeName,
		connMan,
		1000, backoff.NewConstantBackOff(0),
	)
	b := batch.NewGenericBatch("", 1000)

	marshalledMessage := marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Table:        "test_table",
		Json:         []byte("data"),
		TimeBasedKey: "123",
		WalStart:     1234,
		Transaction:  "123",
	}

	_, _ = b.Add(&marshalledMessage)

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
		stats.NewStatCount("rabbitmq_transport", "success", int64(1), int64(1000*time.Millisecond)),
		stats.NewStatHistogram("rabbitmq_transport", "duration", 2000, int64(3000*time.Millisecond), "ms"),
		stats.NewStatCount("rabbitmq_transport", "written", int64(1), int64(4000*time.Millisecond)),
	}
	stats.VerifyStats(t, statsChan, expected)
}

func TestConnectionDies(t *testing.T) {
	var wg sync.WaitGroup

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	in := make(chan transport.Batch, 1000)
	txns := make(chan *ordered_map.OrderedMap, 1000)
	statsChan := make(chan stats.Stat, 1000)
	expectChan := make(chan interface{}, 10)

	connString := "amqp://anyhost:anyport/%2fanyVHost"
	fakeServer := server.NewServer(connString)
	err := fakeServer.Start()
	if err != nil {
		t.Error(err)
	}

	wg.Add(3)
	defer wg.Wait()

	connMan := mocks.NewMockConnectionGetter(mockCtrl)
	connMan.EXPECT().GetConnection(gomock.Any()).DoAndReturn(
		func(context.Context) (wabbit.Conn, error) {
			wg.Done()
			return amqptest.Dial(connString)
		},
	).MinTimes(3)
	mockConn, err := connMan.GetConnection(context.Background())
	if err != nil {
		t.Error(err)
	}
	channel, err := mockConn.Channel()
	if err != nil {
		t.Error(err)
	}

	exchangeName := "testexchange"
	err = channel.ExchangeDeclare(
		exchangeName, // name of the exchange
		"topic",      // type
		wabbit.Option{
			"durable":  true,
			"delete":   false,
			"internal": false,
			"noWait":   false,
		},
	)
	if err != nil {
		t.Error(err)
	}
	defer channel.Close()

	mockTime := utils_mocks.NewMockTimeSource(mockCtrl)
	TimeSource = mockTime
	sh := shutdown.NewShutdownHandler()
	defer sh.CancelFunc()

	transport := NewTransporter(
		sh,
		in,
		txns,
		statsChan,
		*log,
		1,
		"testexchange",
		connMan,
		1000,
		backoff.NewConstantBackOff(time.Millisecond*25),
	)

	// Start test
	go transport.StartTransporting()

	b := batch.NewGenericBatch("", 1000)

	marshalledMessage := marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Table:        "test_table",
		Json:         []byte("data"),
		TimeBasedKey: "123",
		WalStart:     1234,
		Transaction:  "123",
	}

	_, _ = b.Add(&marshalledMessage)

	mockTime.EXPECT().UnixNano().Return(int64(0))
	mockTime.EXPECT().UnixNano().Return(int64(1000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(2000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(3000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(4000 * time.Millisecond)).Do(
		func() {
			expectChan <- 1
		})

	_ = fakeServer.Stop()

	in <- b

	time.Sleep(time.Millisecond * 25)

	err = fakeServer.Start()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		_ = fakeServer.Stop()
	}()

	select {
	case <-time.After(400 * time.Millisecond):
		assert.Fail(t, "did not pass data through in time")
	case <-expectChan:
		// pass
	}
	// Verify stats
	expected := []stats.Stat{
		stats.NewStatCount("rabbitmq_transport", "success", int64(1), int64(1000*time.Millisecond)),
		stats.NewStatHistogram("rabbitmq_transport", "duration", 2000, int64(3000*time.Millisecond), "ms"),
		stats.NewStatCount("rabbitmq_transport", "written", int64(1), int64(4000*time.Millisecond)),
	}
	stats.VerifyStats(t, statsChan, expected)
}

func TestRetryAfterWaitForConnectionError(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	defer resetTimeSource()

	in := make(chan transport.Batch, 1000)
	txns := make(chan *ordered_map.OrderedMap, 1000)
	statsChan := make(chan stats.Stat, 1000)

	connString := "amqp://anyhost:anyport/%2fanyVHost"
	fakeServer := server.NewServer(connString)
	err := fakeServer.Start()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		_ = fakeServer.Stop()
	}()

	connMan := NewConnectionManager(connString, log, makeDialer(connString))
	mockConn, err := connMan.GetConnection(context.Background())
	if err != nil {
		t.Error(err)
	}
	channel, err := mockConn.Channel()
	if err != nil {
		t.Error(err)
	}

	exchangeName := "exchange"
	err = channel.ExchangeDeclare(
		exchangeName, // name of the exchange
		"topic",      // type
		wabbit.Option{
			"durable":  true,
			"delete":   false,
			"internal": false,
			"noWait":   false,
		},
	)
	if err != nil {
		t.Error(err)
	}
	defer channel.Close()

	mockTime := utils_mocks.NewMockTimeSource(mockCtrl)
	TimeSource = mockTime
	sh := shutdown.NewShutdownHandler()
	defer sh.CancelFunc()

	transport := &RabbitMQTransporter{
		shutdownHandler: sh,
		inputChan:       in,
		txnsWritten:     txns,
		statsChan:       statsChan,
		log:             *log,
		id:              1,
		exchangeName:    exchangeName,
		connMan:         connMan,
		batchSize:       1000,
		retryPolicy:     backoff.NewConstantBackOff(0),
	}
	b := batch.NewGenericBatch("", 1000)

	marshalledMessage := marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Table:        "test_table",
		Json:         []byte("data"),
		TimeBasedKey: "123",
		WalStart:     1234,
		Transaction:  "123",
	}

	_, _ = b.Add(&marshalledMessage)

	mockTime.EXPECT().UnixNano().Return(int64(0))
	mockTime.EXPECT().UnixNano().Return(int64(1000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(2000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(3000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(4000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(5000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(6000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(7000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(8000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(9000 * time.Millisecond))
	mockTime.EXPECT().UnixNano().Return(int64(10000 * time.Millisecond))

	in <- b

	// Start test
	go transport.StartTransporting()

	// Wait for data to go through
	time.Sleep(time.Millisecond * 25)

	// Verify stats
	expected := []stats.Stat{
		stats.NewStatCount("rabbitmq_transport", "success", int64(1), int64(1000*time.Millisecond)),
		stats.NewStatHistogram("rabbitmq_transport", "duration", 2000, int64(3000*time.Millisecond), "ms"),
		stats.NewStatCount("rabbitmq_transport", "written", int64(1), int64(4000*time.Millisecond)),
	}
	stats.VerifyStats(t, statsChan, expected)

	// Put fake Nack on publishNotify to trigger retry
	wabbitConf := w_amqp.Confirmation{
		Confirmation: amqp.Confirmation{
			DeliveryTag: 0,
			Ack:         false,
		},
	}
	transport.publishNotify <- wabbitConf
	in <- b

	// Wait for data to go through
	time.Sleep(time.Millisecond * 25)

	expected = []stats.Stat{
		stats.NewStatCount("rabbitmq_transport", "failure", 1, int64(6000*time.Millisecond)),
		stats.NewStatCount("rabbitmq_transport", "success", int64(1), int64(7000*time.Millisecond)),
		stats.NewStatHistogram("rabbitmq_transport", "duration", 3000, int64(9000*time.Millisecond), "ms"),
		stats.NewStatCount("rabbitmq_transport", "written", int64(1), int64(10000*time.Millisecond)),
	}
	stats.VerifyStats(t, statsChan, expected)
}
