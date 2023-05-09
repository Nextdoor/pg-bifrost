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

package partitioner

import (
	"os"
	"strconv"

	"github.com/Nextdoor/pg-bifrost.git/replication"
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/Nextdoor/pg-bifrost.git/utils"
	"github.com/sirupsen/logrus"
)

type PartitionMethod int

const (
	PART_METHOD_NONE       PartitionMethod = iota // No partitioning
	PART_METHOD_TABLENAME                         // Table name based partitioning
	PART_METHOD_TXN                               // Transaction based partitioning
	PART_METHOD_TXN_BUCKET                        // Transaction based partitioning with buckets
)

var (
	nameToPartitionMethod = map[string]PartitionMethod{
		"none":               PART_METHOD_NONE,
		"tablename":          PART_METHOD_TABLENAME,
		"transaction":        PART_METHOD_TXN,
		"transaction-bucket": PART_METHOD_TXN_BUCKET,
	}
)

func GetPartitionMethod(name string) PartitionMethod {
	return nameToPartitionMethod[name]
}

var (
	logger = logrus.New()
	log    = logger.WithField("package", "partitioner")
)

func init() {
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.InfoLevel)
}

type Partitioner struct {
	shutdownHandler shutdown.ShutdownHandler

	inputChan  <-chan *replication.WalMessage
	OutputChan chan *replication.WalMessage

	statsChan chan stats.Stat

	method  PartitionMethod
	buckets int
}

func New(shutdownHandler shutdown.ShutdownHandler,
	inputChan <-chan *replication.WalMessage,
	statsChan chan stats.Stat,
	partMethod PartitionMethod,
	buckets int, // Numbers of buckets to use for transaction-bucket method
) Partitioner {
	outputChan := make(chan *replication.WalMessage)

	return Partitioner{shutdownHandler,
		inputChan,
		outputChan,
		statsChan,
		partMethod,
		buckets,
	}
}

// shutdown idempotently closes the output channel and cancels the termination context
func (f *Partitioner) shutdown() {
	log.Info("shutting down")
	f.shutdownHandler.CancelFunc() // initiate shutdown on other modules as well

	if r := recover(); r != nil {
		log.Error("recovering from panic ", r)
	}

	defer func() {
		// recover if channel is already closed
		_ = recover()
	}()

	log.Debug("closing output channel")
	close(f.OutputChan)
}

func (f *Partitioner) Start() {
	log.Info("starting")

	defer f.shutdown()

	var msg *replication.WalMessage
	var ok bool

	for {
		select {
		case <-f.shutdownHandler.TerminateCtx.Done():
			log.Debug("received terminateCtx cancellation")
			return

		case msg, ok = <-f.inputChan:
			// pass
		}

		select {
		case <-f.shutdownHandler.TerminateCtx.Done():
			log.Debug("received terminateCtx cancellation")
			return
		default:
			// pass
		}

		if !ok {
			log.Debug("input channel is closed")
			return
		}

		if msg == nil {
			log.Error("message was nil")
		}

		var partitionKey string
		switch f.method {
		case PART_METHOD_NONE:
			partitionKey = ""
		case PART_METHOD_TABLENAME:
			partitionKey = msg.Pr.Relation
		case PART_METHOD_TXN:
			partitionKey = msg.Pr.Transaction
		case PART_METHOD_TXN_BUCKET:
			partitionKey = strconv.Itoa(utils.QuickHash(msg.Pr.Transaction, f.buckets))
		}

		msg.PartitionKey = partitionKey

		select {
		case f.OutputChan <- msg:
			// pass
		case <-f.shutdownHandler.TerminateCtx.Done():
			log.Debug("received terminateCtx cancellation")
			return
		}
	}
}
