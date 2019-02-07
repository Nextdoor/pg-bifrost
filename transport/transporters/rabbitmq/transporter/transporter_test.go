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
	"testing"
	"time"

	"github.com/NeowayLabs/wabbit"
	"github.com/NeowayLabs/wabbit/amqptest/server"
	"github.com/Nextdoor/pg-bifrost.git/marshaller"
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/Nextdoor/pg-bifrost.git/transport/batch"
	"github.com/Nextdoor/pg-bifrost.git/utils"
	utils_mocks "github.com/Nextdoor/pg-bifrost.git/utils/mocks"
	"github.com/cevaris/ordered_map"
	"github.com/golang/mock/gomock"
	"github.com/sirupsen/logrus"
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
	defer fakeServer.Stop()

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
	err = channel.Close()
	if err != nil {
		t.Error(err)
	}

	mockTime := utils_mocks.NewMockTimeSource(mockCtrl)
	TimeSource = mockTime
	sh := shutdown.NewShutdownHandler()

	transport := NewTransporter(sh, in, txns, statsChan, *log, 1, exchangeName, connMan, 1000)
	b := batch.NewGenericBatch("", 1000)

	marshalledMessage := marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Table:        "test_table",
		Json:         []byte("data"),
		TimeBasedKey: "123",
		WalStart:     1234,
		Transaction:  "123",
	}

	b.Add(&marshalledMessage)

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
		stats.NewStatHistogram("rabbitmq_transport", "duration", 1000, int64(2000*time.Millisecond), "ms"),
		stats.NewStatCount("rabbitmq_transport", "written", int64(1), int64(3000*time.Millisecond)),
	}
	stats.VerifyStats(t, statsChan, expected)
}
