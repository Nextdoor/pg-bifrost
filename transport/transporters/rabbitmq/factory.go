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

package rabbitmq

import (
	"os"

	"github.com/NeowayLabs/wabbit"
	"github.com/NeowayLabs/wabbit/amqp"
	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/Nextdoor/pg-bifrost.git/stats"
	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/Nextdoor/pg-bifrost.git/transport/batch"
	"github.com/Nextdoor/pg-bifrost.git/transport/transporters/rabbitmq/transporter"
	"github.com/cevaris/ordered_map"
	"github.com/sirupsen/logrus"
)

var (
	logger = logrus.New()
	log    = logger.WithField("package", "rabbitmq")
)

func init() {
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.InfoLevel)
}

func makeDialer(connStr string) DialerFn {
	return func() (wabbit.Conn, error) {
		return amqp.Dial(connStr)
	}
}

// New creates a rabbitmq transport which has rabbitmq batching and rabbitmq transporting, connected by channels
func New(shutdownHandler shutdown.ShutdownHandler,
	txnsWritten chan<- *ordered_map.OrderedMap, // Map of <transaction:progress.Written>
	statsChan chan stats.Stat,
	workers int,
	inputChans []<-chan transport.Batch,
	transportConfig map[string]interface{}) []*transport.Transporter {

	exchangeNameVar := transportConfig[ConfVarExchangeName]
	exchangeName, ok := exchangeNameVar.(string)

	if !ok {
		log.Fatalf("Expected type for %s is %s", ConfVarExchangeName, "string")
	}

	amqpURLVar := transportConfig[ConfVarURL]
	var amqpURL string
	if amqpURLVar != "" {
		amqpURL, ok = amqpURLVar.(string)

		if !ok {
			log.Fatalf("Expected type for %s is %s", ConfVarURL, "string")
		}
	}

	batchSizeVar := transportConfig[ConfVarWriteBatchSize]
	batchSize, ok := batchSizeVar.(int)

	if !ok {
		log.Fatalf("Expected type for %s is %s", ConfVarWriteBatchSize, "int")
	}

	connMan := transporter.NewConnectionManager(amqpURL, log, makeDialer(amqpURL))
	conn, err := connMan.GetConnection(shutdownHandler.TerminateCtx)
	if err != nil {
		log.WithError(err).Fatal("Shutting down")
	}

	channel, err := conn.Channel()
	if err != nil {
		log.WithError(err).Fatal("Could not open channel")
	}
	defer func() {
		err = channel.Close()
		if err != nil {
			log.WithError(err).Error("Error while closing channel")
		}
	}()

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
		log.WithField("exchangeName", exchangeName).WithError(err).Fatal("Could not declare exchange")
	}

	transports := make([]*transport.Transporter, workers)

	// Make and link transporters to the batcher's output channels
	for i := 0; i < workers; i++ {
		t := transporter.NewTransporter(shutdownHandler, inputChans[i], txnsWritten, statsChan, *log, i, exchangeName, connMan, batchSize)
		transports[i] = &t
	}

	return transports
}

// NewBatchFactory returns a GenericBatchFactory configured for RabbitMQ
func NewBatchFactory(transportConfig map[string]interface{}) transport.BatchFactory {
	batchSizeVar := transportConfig[ConfVarWriteBatchSize]
	batchSize, ok := batchSizeVar.(int)

	if !ok {
		log.Fatalf("Expected type for %s is %s", ConfVarWriteBatchSize, "int")
	}

	return batch.NewGenericBatchFactory(batchSize)
}
