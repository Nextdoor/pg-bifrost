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
	"testing"

	"time"

	"github.com/Nextdoor/pg-bifrost.git/marshaller"
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/Nextdoor/pg-bifrost.git/transport/batch"
	"github.com/Nextdoor/pg-bifrost.git/transport/progress"
	"github.com/cevaris/ordered_map"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

var (
	logger = logrus.New()
	log    = logger.WithField("package", "stdout")
)

func init() {
}

func TestBatchHasCommit(t *testing.T) {
	in := make(chan transport.Batch, 1000)
	txns := make(chan *ordered_map.OrderedMap, 1000)
	statsChan := make(chan stats.Stat, 1000)

	sh := shutdown.NewShutdownHandler()
	tp := NewTransporter(sh, in, txns, statsChan, *log, 123)

	go tp.StartTransporting()

	_msgPtr := marshaller.MarshalledMessage{
		Operation:    "SELECT",
		Json:         []byte("{}"),
		TimeBasedKey: "1-1",
		WalStart:     998,
		Transaction:  "1",
	}

	_commitPtr := marshaller.MarshalledMessage{
		Operation:    "COMMIT",
		Json:         []byte("{}"),
		TimeBasedKey: "1-1",
		WalStart:     999,
		Transaction:  "1",
	}

	b := batch.NewGenericBatch("", 1)
	_, _ = b.Add(&_msgPtr)
	_, _ = b.Add(&_commitPtr)

	in <- b
	result := <-txns

	omap := ordered_map.NewOrderedMap()
	omap.Set("1-1", &progress.Written{Transaction: "1", TimeBasedKey: "1-1", Count: 1})

	assert.Equal(t, omap, result, "A message sent to the transporter should have a matching progress result.")
}

func TestBatchHasNoCommit(t *testing.T) {
	in := make(chan transport.Batch, 1000)
	txns := make(chan *ordered_map.OrderedMap, 1000)
	statsChan := make(chan stats.Stat, 1000)
	sh := shutdown.NewShutdownHandler()

	tp := NewTransporter(sh, in, txns, statsChan, *log, 123)

	go tp.StartTransporting()

	_msgPtr := marshaller.MarshalledMessage{
		Operation:    "INSERT",
		Json:         []byte("{}"),
		TimeBasedKey: "1-1",
		WalStart:     999,
		Transaction:  "1",
	}

	b := batch.NewGenericBatch("", 1)
	_, _ = b.Add(&_msgPtr)

	in <- b
	result := <-txns

	omap := ordered_map.NewOrderedMap()
	omap.Set("1-1", &progress.Written{Transaction: "1", TimeBasedKey: "1-1", Count: 1})

	assert.Equal(t, omap, result, "A message sent to the transporter should have a matching progress result.")
}

func TestInputChannelClose(t *testing.T) {
	// Setup
	in := make(chan transport.Batch, 1000)
	txns := make(chan *ordered_map.OrderedMap, 1000)
	statsChan := make(chan stats.Stat, 1000)

	sh := shutdown.NewShutdownHandler()
	tp := NewTransporter(sh, in, txns, statsChan, *log, 123)

	go tp.StartTransporting()

	// Close input channel
	close(in)

	// Verify output channels get closed
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
	// Setup
	in := make(chan transport.Batch, 1000)
	txns := make(chan *ordered_map.OrderedMap, 1000)
	statsChan := make(chan stats.Stat, 1000)

	sh := shutdown.NewShutdownHandler()
	tp := NewTransporter(sh, in, txns, statsChan, *log, 123)

	go tp.StartTransporting()

	time.Sleep(5 * time.Millisecond)

	// Invoke shutdown
	sh.CancelFunc()

	time.Sleep(5 * time.Millisecond)

	// Verify output channels get closed
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
