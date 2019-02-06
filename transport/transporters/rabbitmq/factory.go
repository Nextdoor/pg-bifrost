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
	"net/url"
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

type config struct {
	host      string
	username  string
	password  string
	vhost     string
	exchange  string
	batchSize int
}

func init() {
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.InfoLevel)
}

// New creates a rabbitmq transport which has rabbitmq batching and rabbitmq transporting, connected by channels
func New(shutdownHandler shutdown.ShutdownHandler,
	txnsWritten chan<- *ordered_map.OrderedMap, // Map of <transaction:progress.Written>
	statsChan chan stats.Stat,
	workers int,
	inputChans []<-chan transport.Batch,
	transportConfig map[string]interface{}) []*transport.Transporter {

	conf := getConfig(transportConfig)

	conn := createConnection(conf.host, conf.username, conf.password, conf.vhost, log)
	channel, err := conn.Channel()
	if err != nil {
		log.WithError(err).Fatal("Could not open channel")
	}
	defer channel.Close()
	err = channel.ExchangeDeclare(
		conf.exchange, // name of the exchange
		"topic",       // type
		wabbit.Option{
			"durable":  true,
			"delete":   false,
			"internal": false,
			"noWait":   false,
		},
	)
	log.WithField("exchangeName", conf.exchange).WithError(err).Fatal("Could not declare exchange")

	transports := make([]*transport.Transporter, workers)

	// Make and link transporters to the batcher's output channels
	for i := 0; i < workers; i++ {
		t := transporter.NewTransporter(shutdownHandler, inputChans[i], txnsWritten, statsChan, *log, i, conf.exchange, conn, conf.batchSize)
		transports[i] = &t
	}

	go connectionCleanup(shutdownHandler, conn, log)

	return transports
}

func getConfig(transportConfig map[string]interface{}) *config {
	exchangeNameVar := transportConfig[ConfVarExchangeName]
	exchangeName, ok := exchangeNameVar.(string)

	if !ok {
		log.Fatalf("Expected type for %s is %s", ConfVarExchangeName, "string")
	}

	usernameVar := transportConfig[ConfVarUsername]
	var username string
	if usernameVar != "" {
		username, ok = usernameVar.(string)

		if !ok {
			log.Fatalf("Expected type for %s is %s", ConfVarUsername, "string")
		}
	}

	passwordVar := transportConfig[ConfVarPassword]
	var password string
	if passwordVar != "" {
		password, ok = passwordVar.(string)

		if !ok {
			log.Fatalf("Expected type for %s is %s", ConfVarPassword, "string")
		}
	}

	hostVar := transportConfig[ConfVarHost]
	var host string
	if hostVar != "" {
		host, ok = hostVar.(string)

		if !ok {
			log.Fatalf("Expected type for %s is %s", ConfVarHost, "string")
		}
	}

	virtualHostVar := transportConfig[ConfVarVirtualHost]
	var virtualHost string
	if virtualHostVar != "" {
		virtualHost, ok = virtualHostVar.(string)

		if !ok {
			log.Fatalf("Expected type for %s is %s", ConfVarVirtualHost, "string")
		}
	}

	batchSizeVar := transportConfig[ConfVarWriteBatchSize]
	batchSize, ok := batchSizeVar.(int)

	if !ok {
		log.Fatalf("Expected type for %s is %s", ConfVarWriteBatchSize, "int")
	}

	return &config{
		host:      host,
		username:  username,
		password:  password,
		vhost:     virtualHost,
		exchange:  exchangeName,
		batchSize: batchSize,
	}
}

// createConnection either returns a connection or logs a fatal message
func createConnection(host, user, pass, vhost string, log *logrus.Entry) wabbit.Conn {
	amqpURL := &url.URL{
		Scheme: "amqp",
		Host:   host,
		User:   url.UserPassword(user, pass),
		Path:   vhost,
	}
	conn, err := amqp.Dial(amqpURL.String())
	if err != nil {
		log.WithError(err).Fatal("Could not connect to RabbitMQ")
	}

	return conn
}

func connectionCleanup(shutdownHandler shutdown.ShutdownHandler, conn wabbit.Conn, log *logrus.Entry) {
	closeNotify := conn.NotifyClose(make(chan wabbit.Error))

	select {
	case <-shutdownHandler.TerminateCtx.Done():
		err := conn.Close()
		if err != nil {
			log.WithError(err).Error("Error while closing connection")
		}
	case amqpErr := <-closeNotify:
		log.Error(amqpErr.Error())
		shutdownHandler.CancelFunc()
	}
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
